/*
 * MIT License
 *
 * Copyright (c) 2024 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/hashicorp/memberlist"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/gokv/internal/errorschain"
	"github.com/tochemey/gokv/internal/http"
	"github.com/tochemey/gokv/internal/internalpb"
	"github.com/tochemey/gokv/internal/internalpb/internalpbconnect"
	"github.com/tochemey/gokv/internal/tcp"
)

// Node defines the cluster node
type Node struct {
	internalpbconnect.UnimplementedKVServiceHandler

	config *Config

	// state holds the node state
	// through memberlist this state will be eventually gossiped to the rest of the cluster
	state *State

	memberConfig *memberlist.Config
	memberlist   *memberlist.Memberlist

	// states whether the actor system has started or not
	started *atomic.Bool

	httpServer *nethttp.Server
	mu         *sync.Mutex

	clusterClient      *Client
	eventsChan         chan *Event
	stopEventsListener chan struct{}
	eventsLock         *sync.Mutex
}

// NewNode creates an instance of Node
func NewNode(config *Config) *Node {
	mconfig := memberlist.DefaultLANConfig()
	mconfig.BindAddr = config.host
	mconfig.BindPort = int(config.gossipPort)
	mconfig.AdvertisePort = mconfig.BindPort
	mconfig.LogOutput = newLogWriter(config.logger)
	mconfig.Name = net.JoinHostPort(mconfig.BindAddr, strconv.Itoa(mconfig.BindPort))

	meta := &internalpb.NodeMeta{
		Name:         config.name,
		Host:         config.host,
		Port:         config.port,
		GossipPort:   config.gossipPort,
		CreationTime: timestamppb.New(time.Now().UTC()),
	}
	state := newState(meta)
	mconfig.Delegate = state

	return &Node{
		mu:                 new(sync.Mutex),
		state:              state,
		memberConfig:       mconfig,
		started:            atomic.NewBool(false),
		eventsChan:         make(chan *Event, 1),
		stopEventsListener: make(chan struct{}, 1),
		eventsLock:         new(sync.Mutex),
		config:             config,
	}
}

// Start starts the cluster node
func (node *Node) Start(ctx context.Context) error {
	node.mu.Lock()
	if err := errorschain.
		New(errorschain.ReturnFirst()).
		AddError(node.config.Validate()).
		AddError(node.config.provider.Initialize()).
		AddError(node.config.provider.Register()).
		AddError(node.join()).
		AddError(node.serve(ctx)).
		Error(); err != nil {
		node.mu.Unlock()
		return err
	}

	// create enough buffer to house the cluster events
	// TODO: revisit this number
	eventsCh := make(chan memberlist.NodeEvent, 256)
	node.memberConfig.Events = &memberlist.ChannelEventDelegate{
		Ch: eventsCh,
	}
	node.clusterClient = newClient(node.config.host, int(node.config.port))
	node.started.Store(true)
	node.mu.Unlock()

	// start listening to events
	go node.eventsListener(eventsCh)

	return nil
}

// Stop stops gracefully the cluster node
func (node *Node) Stop(ctx context.Context) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	// no-op when the node has not started
	if !node.started.Load() {
		return nil
	}

	// no matter the outcome the node is officially off
	node.started.Store(false)
	ctx, cancel := context.WithTimeout(ctx, node.config.shutdownTimeout)
	defer cancel()

	// stop the events loop
	close(node.stopEventsListener)

	return errorschain.
		New(errorschain.ReturnFirst()).
		AddError(node.clusterClient.Close()).
		AddError(node.memberlist.Leave(node.config.shutdownTimeout)).
		AddError(node.config.provider.Close()).
		AddError(node.config.provider.Deregister()).
		AddError(node.memberlist.Shutdown()).
		AddError(node.httpServer.Shutdown(ctx)).
		Error()
}

// Put is used to distribute a key/value pair across a cluster of nodes
// nolint
func (node *Node) Put(ctx context.Context, request *connect.Request[internalpb.PutRequest]) (*connect.Response[internalpb.PutResponse], error) {
	node.mu.Lock()
	if !node.started.Load() {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNodeNotStarted)
	}

	req := request.Msg
	node.state.Put(req.GetKey(), req.GetValue())
	node.mu.Unlock()

	return connect.NewResponse(new(internalpb.PutResponse)), nil
}

// Get is used to retrieve a key/value pair in a cluster of nodes
// nolint
func (node *Node) Get(ctx context.Context, request *connect.Request[internalpb.GetRequest]) (*connect.Response[internalpb.GetResponse], error) {
	node.mu.Lock()
	if !node.started.Load() {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNodeNotStarted)
	}

	req := request.Msg
	kv := node.state.Get(req.GetKey())
	if kv == nil || proto.Equal(kv, new(internalpb.KV)) {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeNotFound, ErrKeyNotFound)
	}

	node.mu.Unlock()
	return connect.NewResponse(&internalpb.GetResponse{
		Kv: kv,
	}), nil
}

// Delete is used to remove a key/value pair from a cluster of nodes
// nolint
func (node *Node) Delete(ctx context.Context, request *connect.Request[internalpb.DeleteRequest]) (*connect.Response[internalpb.DeleteResponse], error) {
	node.mu.Lock()
	if !node.started.Load() {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNodeNotStarted)
	}

	req := request.Msg
	node.state.Delete(req.GetKey())
	node.mu.Unlock()

	return connect.NewResponse(new(internalpb.DeleteResponse)), nil
}

// Client returns the cluster Client
func (node *Node) Client() *Client {
	node.mu.Lock()
	client := node.clusterClient
	node.mu.Unlock()
	return client
}

// Events returns a channel where cluster events are published
func (node *Node) Events() <-chan *Event {
	node.eventsLock.Lock()
	ch := node.eventsChan
	node.eventsLock.Unlock()
	return ch
}

// serve start the underlying http server
func (node *Node) serve(ctx context.Context) error {
	// extract the actual TCP ip address
	host, port, err := tcp.GetHostPort(fmt.Sprintf("%s:%d", node.config.host, node.config.port))
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %w", err)
	}

	node.config.WithHost(host)
	node.config.WithPort(uint32(port))

	// hook the node as the KV service handler
	// TODO: add metric options to the handler
	pattern, handler := internalpbconnect.NewKVServiceHandler(node)

	mux := nethttp.NewServeMux()
	mux.Handle(pattern, handler)
	server := http.NewServer(ctx, node.config.host, int(node.config.port), mux)

	node.httpServer = server

	go func() {
		if err := node.httpServer.ListenAndServe(); err != nil {
			if !errors.Is(err, nethttp.ErrServerClosed) {
				// just panic
				node.config.logger.Panic(fmt.Errorf("failed to start service: %w", err))
			}
		}
	}()
	return nil
}

// join attempts to join an existing cluster if node peers is provided
func (node *Node) join() error {
	mlist, err := memberlist.Create(node.memberConfig)
	if err != nil {
		return err
	}

	// TODO: use a retry mechanism here
	peers, err := node.config.provider.DiscoverPeers()
	if err != nil {
		return err
	}

	// set the mlist
	node.memberlist = mlist
	if len(peers) > 0 {
		if _, err := node.memberlist.Join(peers); err != nil {
			return err
		}
	}
	return nil
}

// eventsListener listens to cluster events to handle them
func (node *Node) eventsListener(eventsCh chan memberlist.NodeEvent) {
	for {
		select {
		case event := <-eventsCh:
			// skip this node
			if event.Node == nil || event.Node.Name == node.config.name {
				continue
			}

			var eventType EventType
			switch event.Event {
			case memberlist.NodeJoin:
				eventType = NodeJoined
			case memberlist.NodeLeave:
				eventType = NodeLeft
			case memberlist.NodeUpdate:
				// TODO: maybe handle this
				continue
			}

			// parse the node meta information, log an eventual error during parsing and skip the event
			member, err := MemberFromMeta(event.Node.Meta)
			if err != nil {
				node.config.logger.Errorf("failed to marshal node meta from cluster event: %v", event)
				continue
			}

			// send the event to the event channels
			node.eventsLock.Lock()
			node.eventsChan <- &Event{
				Member: member,
				Time:   time.Now().UTC(),
				Type:   eventType,
			}
			node.eventsLock.Unlock()
		case <-node.stopEventsListener:
			// finish listening to cluster events
			close(node.eventsChan)
			return
		}
	}
}
