/*
 * MIT License
 *
 * Copyright (c) 2024 Arsene Tochemey Gandote
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

package gokv

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/flowchartsman/retry"
	"github.com/hashicorp/memberlist"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/gokv/internal/errorschain"
	"github.com/tochemey/gokv/internal/http"
	"github.com/tochemey/gokv/internal/internalpb"
	"github.com/tochemey/gokv/internal/internalpb/internalpbconnect"
	"github.com/tochemey/gokv/internal/lib"
	"github.com/tochemey/gokv/internal/tcp"
)

const (
	// NoExpiration is used to state there is no expiration
	NoExpiration time.Duration = -1
)

// Node defines the cluster node
type Node struct {
	internalpbconnect.UnimplementedKVServiceHandler

	config *Config

	// delegate holds the node delegate
	// through memberlist this delegate will be eventually gossiped to the rest of the cluster
	delegate *delegate

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

	discoveryAddress string
	cleaner          *cleaner
}

// newNode creates an instance of Node
func newNode(config *Config) (*Node, error) {
	mconfig := memberlist.DefaultLANConfig()
	mconfig.BindAddr = config.host
	mconfig.BindPort = int(config.discoveryPort)
	mconfig.AdvertisePort = mconfig.BindPort
	mconfig.LogOutput = newLogWriter(config.logger)
	mconfig.Name = net.JoinHostPort(mconfig.BindAddr, strconv.Itoa(mconfig.BindPort))
	mconfig.PushPullInterval = config.syncInterval

	// Enable gossip encryption if a key is defined.
	if len(config.secretKeys) != 0 {
		mconfig.Label = config.cookie
		mconfig.GossipVerifyIncoming = true
		mconfig.GossipVerifyOutgoing = true
		mconfig.Keyring = &memberlist.Keyring{}
		for i, key := range config.secretKeys {
			secret, err := base64.StdEncoding.DecodeString(strings.TrimSpace(key))
			if err != nil {
				return nil, fmt.Errorf("unable to base64 decode memberlist encryption key at index %d: %w", i, err)
			}

			if err = mconfig.Keyring.AddKey(secret); err != nil {
				return nil, fmt.Errorf("error adding memberlist encryption key at index %d: %w", i, err)
			}

			// set the first key as the default for encrypting outbound messages.
			if i == 0 {
				mconfig.SecretKey = secret
			}
		}
	}

	meta := &internalpb.NodeMeta{
		Name:          mconfig.Name,
		Host:          config.host,
		Port:          uint32(config.port),
		DiscoveryPort: uint32(config.discoveryPort),
		CreationTime:  timestamppb.New(time.Now().UTC()),
	}

	discoveryAddr := lib.HostPort(config.host, int(config.discoveryPort))
	delegate := newDelegate(discoveryAddr, meta)
	mconfig.Delegate = delegate

	node := &Node{
		mu:                 new(sync.Mutex),
		delegate:           delegate,
		memberConfig:       mconfig,
		started:            atomic.NewBool(false),
		eventsChan:         make(chan *Event, 1),
		stopEventsListener: make(chan struct{}, 1),
		eventsLock:         new(sync.Mutex),
		config:             config,
		discoveryAddress:   discoveryAddr,
	}

	if config.cleanerJobInterval > 0 {
		runCleaner(node, config.cleanerJobInterval)
		runtime.SetFinalizer(node, stopCleaner)
	}

	return node, nil
}

// Start starts the cluster node
func (node *Node) Start(ctx context.Context) error {
	node.mu.Lock()
	if err := errorschain.
		New(errorschain.ReturnFirst()).
		AddError(node.config.Validate()).
		AddError(node.config.provider.Initialize()).
		AddError(node.config.provider.Register()).
		AddError(node.join(ctx)).
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
	node.clusterClient = NewClient(node.config.host, int(node.config.port))
	node.started.Store(true)
	node.mu.Unlock()

	// start listening to events
	go node.eventsListener(eventsCh)

	node.config.logger.Infof("%s successfully started", node.discoveryAddress)
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

	if err := errorschain.
		New(errorschain.ReturnFirst()).
		AddError(node.clusterClient.Close()).
		AddError(node.memberlist.Leave(node.config.shutdownTimeout)).
		AddError(node.config.provider.Deregister()).
		AddError(node.config.provider.Close()).
		AddError(node.memberlist.Shutdown()).
		AddError(node.httpServer.Shutdown(ctx)).
		Error(); err != nil {
		node.config.logger.Error(fmt.Errorf("%s failed to stop: %w", node.discoveryAddress, err))
		return err
	}
	node.config.logger.Infof("%s successfully stopped", node.discoveryAddress)
	return nil
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
	node.delegate.Put(req.GetKey(), req.GetValue(), req.GetExpiry().AsDuration())
	node.mu.Unlock()

	return connect.NewResponse(new(internalpb.PutResponse)), nil
}

// Get is used to retrieve a key/value pair in a cluster of nodes
func (node *Node) Get(ctx context.Context, request *connect.Request[internalpb.GetRequest]) (*connect.Response[internalpb.GetResponse], error) {
	node.mu.Lock()
	if !node.started.Load() {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNodeNotStarted)
	}

	ctx, cancelFn := context.WithTimeout(ctx, node.config.readTimeout)
	defer cancelFn()

	req := request.Msg
	var (
		rerr  error
		entry *internalpb.Entry
	)

	retrier := retry.NewRetrier(2, node.config.readTimeout, node.config.syncInterval)
	if err := retrier.RunContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			entry, rerr = node.delegate.Get(req.GetKey())
			if rerr != nil {
				return rerr
			}
		}
		return nil
	}); err != nil {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	node.mu.Unlock()
	return connect.NewResponse(&internalpb.GetResponse{
		Entry: entry,
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
	node.delegate.Delete(req.GetKey())
	node.mu.Unlock()

	return connect.NewResponse(new(internalpb.DeleteResponse)), nil
}

// KeyExists is used to check the existence of a given key in the cluster
// nolint
func (node *Node) KeyExists(ctx context.Context, request *connect.Request[internalpb.KeyExistsRequest]) (*connect.Response[internalpb.KeyExistResponse], error) {
	node.mu.Lock()
	if !node.started.Load() {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNodeNotStarted)
	}

	req := request.Msg
	exists := node.delegate.Exists(req.GetKey())
	node.mu.Unlock()
	return connect.NewResponse(&internalpb.KeyExistResponse{Exists: exists}), nil
}

// List returns the list of all entries at a given point in time
// nolint
func (node *Node) List(ctx context.Context, request *connect.Request[internalpb.ListRequest]) (*connect.Response[internalpb.ListResponse], error) {
	node.mu.Lock()
	if !node.started.Load() {
		node.mu.Unlock()
		return nil, connect.NewError(connect.CodeFailedPrecondition, ErrNodeNotStarted)
	}

	entries := node.delegate.List()
	node.mu.Unlock()
	return connect.NewResponse(&internalpb.ListResponse{Entries: entries}), nil
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

// HostPort returns the node host:port address
func (node *Node) HostPort() string {
	node.mu.Lock()
	address := node.discoveryAddress
	node.mu.Unlock()
	return address
}

// Peers returns the list of peers
func (node *Node) Peers() ([]*Member, error) {
	node.mu.Lock()
	mnodes := node.memberlist.Members()
	node.mu.Unlock()
	members := make([]*Member, 0, len(mnodes))
	for _, mnode := range mnodes {
		member, err := memberFromMeta(mnode.Meta)
		if err != nil {
			return nil, err
		}
		if member != nil && member.DiscoveryAddress() != node.HostPort() {
			members = append(members, member)
		}
	}
	return members, nil
}

// serve start the underlying http server
func (node *Node) serve(ctx context.Context) error {
	// extract the actual TCP ip discoveryAddress
	hostPort := net.JoinHostPort(node.config.host, strconv.Itoa(int(node.config.port)))
	bindIP, err := tcp.GetBindIP(hostPort)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP discoveryAddress: %w", err)
	}

	node.config.WithHost(bindIP)
	node.config.WithPort(uint16(node.config.port))

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
func (node *Node) join(ctx context.Context) error {
	mlist, err := memberlist.Create(node.memberConfig)
	if err != nil {
		node.config.logger.Error(fmt.Errorf("failed to create memberlist: %w", err))
		return err
	}

	ctx2, cancel := context.WithTimeout(ctx, node.config.joinTimeout)
	var peers []string
	retrier := retry.NewRetrier(node.config.maxJoinAttempts, node.config.joinRetryInterval, node.config.joinRetryInterval)
	if err := retrier.RunContext(ctx2, func(ctx context.Context) error { // nolint
		peers, err = node.config.provider.DiscoverPeers()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		cancel()
		return err
	}

	cancel()

	// set the mlist
	node.memberlist = mlist
	if len(peers) > 0 {
		if _, err := node.memberlist.Join(peers); err != nil {
			node.config.logger.Error(fmt.Errorf("failed to join cluster: %w", err))
			return err
		}
		node.config.logger.Infof("%s successfully joined cluster: [%s]", node.discoveryAddress, strings.Join(peers, ","))
	}
	return nil
}

// eventsListener listens to cluster events to handle them
func (node *Node) eventsListener(eventsCh chan memberlist.NodeEvent) {
	for {
		select {
		case event := <-eventsCh:
			// skip this node
			if event.Node == nil {
				addr := net.JoinHostPort(event.Node.Addr.String(), strconv.Itoa(int(event.Node.Port)))
				if addr == node.HostPort() {
					continue
				}
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
			member, err := memberFromMeta(event.Node.Meta)
			if err != nil {
				node.config.logger.Errorf("failed to marshal node meta from cluster event: %v", err)
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
