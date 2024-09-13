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
	nethttp "net/http"

	"connectrpc.com/connect"
	"go.uber.org/atomic"

	"github.com/tochemey/gokv/internal/http"
	"github.com/tochemey/gokv/internal/internalpb"
	"github.com/tochemey/gokv/internal/internalpb/internalpbconnect"
)

// Client defines the cluster client
type Client struct {
	// http client
	httpClient *nethttp.Client
	// host defines the host address
	host string
	// port defines the gRCP port for client connections
	port      int
	kvService internalpbconnect.KVServiceClient

	connected *atomic.Bool
}

// Put distributes the key/value pair in the cluster
func (client *Client) Put(ctx context.Context, key string, value []byte) error {
	if !client.connected.Load() {
		return ErrClientNotConnected
	}
	_, err := client.kvService.Put(ctx, connect.NewRequest(&internalpb.PutRequest{
		Key:   key,
		Value: value,
	}))
	return err
}

// Get retrieves the value of the given key from the cluster
func (client *Client) Get(ctx context.Context, key string) ([]byte, error) {
	if !client.connected.Load() {
		return nil, ErrClientNotConnected
	}
	response, err := client.kvService.Get(ctx, connect.NewRequest(&internalpb.GetRequest{
		Key: key,
	}))

	if err != nil {
		code := connect.CodeOf(err)
		if code == connect.CodeNotFound {
			return nil, nil
		}
		return nil, err
	}

	return response.Msg.GetKv().GetValue(), nil
}

// Close closes the client connection to the cluster
func (client *Client) Close() error {
	// no-op when the client is not connected
	if !client.connected.Load() {
		return nil
	}
	client.connected.Store(false)
	client.httpClient.CloseIdleConnections()
	return nil
}

// newClient creates an instance of the cluster Client
func newClient(host string, port int) *Client {
	httpClient := http.NewClient()
	kvService := internalpbconnect.NewKVServiceClient(
		httpClient,
		http.URL(host, port),
		// TODO: add observability options
	)
	return &Client{
		httpClient: httpClient,
		kvService:  kvService,
		host:       host,
		port:       port,
		connected:  atomic.NewBool(true),
	}
}
