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
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"

	"github.com/tochemey/gokv/discovery"
	"github.com/tochemey/gokv/discovery/nats"
	"github.com/tochemey/gokv/internal/lib"
	"github.com/tochemey/gokv/log"
)

func TestNodes(t *testing.T) {
	ctx := context.Background()
	// start the NATS server
	srv := startNatsServer(t)

	// create a cluster node1
	node1, sd1 := startNode(t, srv.Addr().String())
	require.NotNil(t, node1)

	// create a cluster node2
	node2, sd2 := startNode(t, srv.Addr().String())
	require.NotNil(t, node2)

	// create a cluster node2
	node3, sd3 := startNode(t, srv.Addr().String())
	require.NotNil(t, node3)

	// let us distribute a key in the cluster
	key := "some-key"
	value := []byte("some-value")
	err := node2.Client().Put(ctx, key, value, NoExpiration)
	require.NoError(t, err)

	// wait for the key to be distributed in the cluster
	lib.Pause(time.Second)

	// let us retrieve the key from the other nodes
	exists, err := node1.Client().Exists(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	actual, err := node1.Client().Get(ctx, key)
	require.NoError(t, err)
	require.NotEmpty(t, actual)
	require.Equal(t, value, actual)

	exists, err = node3.Client().Exists(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	actual, err = node3.Client().Get(ctx, key)
	require.NoError(t, err)
	require.NotEmpty(t, actual)
	require.Equal(t, value, actual)

	// let us remove the key
	require.NoError(t, node2.Client().Delete(ctx, key))

	// wait a bit for consistency
	lib.Pause(time.Second)

	exists, err = node3.Client().Exists(ctx, key)
	require.NoError(t, err)
	require.False(t, exists)

	lib.Pause(time.Second)

	t.Cleanup(func() {
		assert.NoError(t, node1.Stop(ctx))
		assert.NoError(t, node2.Stop(ctx))
		assert.NoError(t, node3.Stop(ctx))
		assert.NoError(t, sd1.Close())
		assert.NoError(t, sd2.Close())
		assert.NoError(t, sd3.Close())
		srv.Shutdown()
	})
}

func TestClusterEvents(t *testing.T) {
	ctx := context.Background()

	// start the NATS server
	srv := startNatsServer(t)

	// create a cluster node1
	node1, sd1 := startNode(t, srv.Addr().String())
	require.NotNil(t, node1)

	// create a cluster node2
	node2, sd2 := startNode(t, srv.Addr().String())
	require.NotNil(t, node2)

	// assert the node joined cluster event
	var events []*Event

	// define an events reader loop and read events for some time
L:
	for {
		select {
		case event, ok := <-node1.Events():
			if ok {
				events = append(events, event)
			}
		case <-time.After(time.Second):
			break L
		}
	}

	require.NotEmpty(t, events)
	require.Len(t, events, 1)
	event := events[0]
	require.NotNil(t, event)
	require.True(t, event.Type == NodeJoined)
	actualAddr := event.Member.DiscoveryAddress()
	require.Equal(t, node2.DiscoveryAddress(), actualAddr)
	peers, err := node1.Peers()
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, node2.DiscoveryAddress(), peers[0].DiscoveryAddress())

	// wait for some time
	lib.Pause(time.Second)

	// stop the second node
	require.NoError(t, node2.Stop(ctx))
	// wait for the event to propagate properly
	lib.Pause(time.Second)

	// reset the slice
	events = []*Event{}

	// define an events reader loop and read events for some time
L2:
	for {
		select {
		case event, ok := <-node1.Events():
			if ok {
				events = append(events, event)
			}
		case <-time.After(time.Second):
			break L2
		}
	}

	require.NotEmpty(t, events)
	require.Len(t, events, 1)
	event = events[0]
	require.NotNil(t, event)
	require.True(t, event.Type == NodeLeft)
	actualAddr = event.Member.DiscoveryAddress()
	require.Equal(t, node2.DiscoveryAddress(), actualAddr)

	t.Cleanup(func() {
		assert.NoError(t, node1.Stop(ctx))
		assert.NoError(t, sd1.Close())
		assert.NoError(t, sd2.Close())
		srv.Shutdown()
	})
}

func startNatsServer(t *testing.T) *natsserver.Server {
	t.Helper()
	serv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1",
		Port: -1,
	})

	require.NoError(t, err)

	ready := make(chan bool)
	go func() {
		ready <- true
		serv.Start()
	}()
	<-ready

	if !serv.ReadyForConnections(2 * time.Second) {
		t.Fatalf("nats-io server failed to start")
	}

	return serv
}

func startNode(t *testing.T, serverAddr string) (*Node, discovery.Provider) {
	ctx := context.TODO()
	logger := log.DiscardLogger

	// generate the ports for the single startNode
	nodePorts := dynaport.Get(2)
	gossipPort := nodePorts[0]
	clientPort := nodePorts[1]

	// create a Cluster startNode
	host := "127.0.0.1"
	// create the various config option
	natsSubject := "some-subject"
	// create the config
	config := nats.Config{
		Server:        fmt.Sprintf("nats://%s", serverAddr),
		Subject:       natsSubject,
		Host:          host,
		DiscoveryPort: uint16(gossipPort),
	}

	// create the instance of provider
	provider := nats.NewDiscovery(&config, nats.WithLogger(logger))

	node := NewNode(&Config{
		provider:          provider,
		port:              uint16(clientPort),
		discoveryPort:     uint16(gossipPort),
		shutdownTimeout:   time.Second,
		logger:            logger,
		host:              host,
		stateSyncInterval: 500 * time.Millisecond,
		joinRetryInterval: 500 * time.Millisecond,
		maxJoinAttempts:   5,
	})

	// start the node
	require.NoError(t, node.Start(ctx))
	lib.Pause(2 * time.Second)

	// return the cluster startNode
	return node, provider
}
