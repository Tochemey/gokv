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
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
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

	"github.com/tochemey/gokv/internal/errorschain"
	"github.com/tochemey/gokv/internal/http"
	"github.com/tochemey/gokv/internal/internalpb"
	"github.com/tochemey/gokv/internal/internalpb/internalpbconnect"
	"github.com/tochemey/gokv/internal/tcp"
	"github.com/tochemey/gokv/log"
)

// Node defines the cluster node
type Node struct {
	internalpbconnect.UnimplementedKVServiceHandler

	//  addr:port of node in the cluster to join to; empty if it's the first node
	peers []string

	// host defines the host address
	host string
	// port defines the gRCP port for client connections
	port       int
	gossipPort int

	// state holds the node state
	// through memberlist this state will be eventually gossiped to the rest of the cluster
	state *State

	memberConfig *memberlist.Config
	memberlist   *memberlist.Memberlist

	// Specifies the actor system name
	name string
	// Specifies the logger to use
	logger log.Logger

	// states whether the actor system has started or not
	started         *atomic.Bool
	shutdownTimeout time.Duration

	httpServer *nethttp.Server
	mu         *sync.Mutex

	clusterClient *Client
}

// NewNode creates an instance of Node
func NewNode(name string,
	host string,
	port,
	gossipPort uint32,
	peers []string,
	logger log.Logger,
	shutdownTimeout time.Duration) *Node {
	// TODO: add more settings to memberlist config like cluster events listening and co
	config := memberlist.DefaultLANConfig()
	config.BindAddr = host
	config.BindPort = int(gossipPort)
	config.AdvertisePort = config.BindPort
	config.LogOutput = newLogWriter(logger)
	config.Name = net.JoinHostPort(config.BindAddr, strconv.Itoa(config.BindPort))

	md := make(map[string]string)
	md["port"] = strconv.Itoa(int(port))
	md["name"] = name
	meta := &internalpb.NodeMeta{
		Metadata: md,
	}
	state := newState(meta)
	config.Delegate = state

	return &Node{
		mu:              new(sync.Mutex),
		peers:           peers,
		host:            host,
		port:            int(port),
		gossipPort:      int(gossipPort),
		state:           state,
		memberConfig:    config,
		name:            name,
		logger:          logger,
		started:         atomic.NewBool(false),
		shutdownTimeout: shutdownTimeout,
	}
}

// Start starts the cluster node
func (node *Node) Start(ctx context.Context) error {
	node.mu.Lock()
	if err := errorschain.
		New(errorschain.ReturnFirst()).
		AddError(node.join()).
		AddError(node.serve(ctx)).
		Error(); err != nil {
		node.mu.Unlock()
		return err
	}

	node.clusterClient = newClient(node.host, node.port)
	node.started.Store(true)
	node.mu.Unlock()
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
	ctx, cancel := context.WithTimeout(ctx, node.shutdownTimeout)
	defer cancel()

	return errorschain.
		New(errorschain.ReturnFirst()).
		AddError(node.clusterClient.Close()).
		AddError(node.memberlist.Leave(node.shutdownTimeout)).
		AddError(node.memberlist.Shutdown()).
		AddError(node.httpServer.Shutdown(ctx)).
		Error()
}

// Put is used to distribute a key/value pair across a cluster of nodes
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

// serve start the underlying http server
func (node *Node) serve(ctx context.Context) error {
	// extract the actual TCP ip address
	host, port, err := tcp.GetHostPort(fmt.Sprintf("%s:%d", node.host, node.port))
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %w", err)
	}

	node.host = host
	node.port = port

	// hook the node as the KV service handler
	// TODO: add metric options to the handler
	pattern, handler := internalpbconnect.NewKVServiceHandler(node)

	mux := nethttp.NewServeMux()
	mux.Handle(pattern, handler)
	server := http.NewServer(ctx, node.host, node.port, mux)

	node.httpServer = server

	go func() {
		if err := node.httpServer.ListenAndServe(); err != nil {
			if !errors.Is(err, nethttp.ErrServerClosed) {
				// just panic
				node.logger.Panic(fmt.Errorf("failed to start service: %w", err))
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

	// set the mlist
	node.memberlist = mlist
	if len(node.peers) > 0 {
		if _, err := node.memberlist.Join(node.peers); err != nil {
			return err
		}
	}
	return nil
}
