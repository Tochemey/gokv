/*
 * MIT License
 *
 * Copyright (c) 2024-2025 Arsene Tochemey Gandote
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

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/travisjeffery/go-dynaport"

	"github.com/tochemey/gokv"
	"github.com/tochemey/gokv/discovery/nats"
	"github.com/tochemey/gokv/internal/lib"
	"github.com/tochemey/gokv/log"
)

func main() {
	ctx := context.Background()
	logger := log.DefaultLogger

	// generate the ports for the single startNode
	nodePorts := dynaport.Get(2)
	discoveryPort := uint16(nodePorts[0])
	clientPort := uint16(nodePorts[1])
	host := "127.0.0.1"

	// start the nats server
	server, err := startNatsServer()
	if err != nil {
		logger.Fatal(err)
	}

	// get the nats server address
	serverAddress := server.Addr().String()

	// create the nats discovery provider
	config := &nats.Config{
		Server:        fmt.Sprintf("nats://%s", serverAddress),
		Subject:       "example",
		Host:          host,
		DiscoveryPort: discoveryPort,
	}

	// create an instance of the discovery provider
	discovery := nats.NewDiscovery(config)

	// create an instance of the cluster config
	clusterConfig := gokv.NewConfig().
		WithPort(clientPort).
		WithDiscoveryPort(discoveryPort).
		WithDiscoveryProvider(discovery).
		WithHost(host).
		WithSyncInterval(time.Second).
		WithLogger(logger)

	// create an instance of a node
	node, err := gokv.NewNode(clusterConfig)
	if err != nil {
		logger.Fatal(err)
	}

	// start the node
	if err := node.Start(ctx); err != nil {
		logger.Fatal(err)
	}

	// get the cluster client
	client := node.Client()

	key := "key"
	value := []byte("contoso.com")

	// let us distribute the key in the cluster. At the moment we only have one node
	// Put will override an existing key in the cluster
	if err := client.Put(ctx, &gokv.Entry{Key: key, Value: value}, gokv.NoExpiration); err != nil {
		logger.Fatal(err)
	}

	// since the distribution is eventual consistent let us wait a while for the distribution to happen
	// note: one can control how often and quickly keys are replicated by setting the StateSyncInterval (see cluster config setting)
	lib.Pause(time.Second)

	// let us check the existence of the key
	exist, err := client.Exists(ctx, key)
	if err != nil {
		logger.Fatal(err)
	}

	if !exist {
		logger.Infof("key %s not exist", key)
	}
	logger.Infof("key %s exist", key)

	// let us get the key value back from the cluster
	entry, err := client.Get(ctx, key)
	if err != nil {
		logger.Fatal(err)
	}

	if !bytes.Equal(entry.Value, value) {
		logger.Fatal("failed to get key value back from the cluster")
	}
	logger.Infof("key %s value %s successfully retrieved from the cluster", key, string(entry.Value))

	// capture ctrl+c
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal

	// stop the node
	_ = node.Stop(ctx)
	os.Exit(0)
}

func startNatsServer() (*natsserver.Server, error) {
	serv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1",
		Port: -1,
	})

	if err != nil {
		return nil, err
	}

	ready := make(chan bool)
	go func() {
		ready <- true
		serv.Start()
	}()
	<-ready

	if !serv.ReadyForConnections(2 * time.Second) {
		return nil, fmt.Errorf("nats server not ready")
	}

	return serv, nil
}
