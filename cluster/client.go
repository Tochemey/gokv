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
	"time"

	"connectrpc.com/connect"
	"go.uber.org/atomic"

	"github.com/tochemey/gokv/internal/http"
	"github.com/tochemey/gokv/internal/internalpb"
	"github.com/tochemey/gokv/internal/internalpb/internalpbconnect"
)

// Client defines the cluster client
// This client can connect to any Go-KV cluster node and retrieve data from other
// of the cluster.
type Client struct {
	// http client
	httpClient *nethttp.Client
	// host defines the host discoveryAddress
	kvService internalpbconnect.KVServiceClient
	connected *atomic.Bool
}

// Put distributes the key/value pair in the cluster
func (client *Client) Put(ctx context.Context, entry *Entry, expiration time.Duration) error {
	if !client.connected.Load() {
		return ErrClientNotConnected
	}

	_, err := client.kvService.Put(ctx, connect.NewRequest(
		&internalpb.PutRequest{
			Key:    entry.Key,
			Value:  entry.Value,
			Expiry: setExpiry(expiration),
		}))
	return err
}

// Get retrieves the value of the given key from the cluster
func (client *Client) Get(ctx context.Context, key string) (*Entry, error) {
	if !client.connected.Load() {
		return nil, ErrClientNotConnected
	}
	response, err := client.kvService.Get(ctx, connect.NewRequest(
		&internalpb.GetRequest{
			Key: key,
		}))

	if err != nil {
		code := connect.CodeOf(err)
		if code == connect.CodeNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	return fromNode(response.Msg.GetEntry()), nil
}

// List returns the list of entries at a point in time
func (client *Client) List(ctx context.Context) ([]*Entry, error) {
	if !client.connected.Load() {
		return nil, ErrClientNotConnected
	}

	response, err := client.kvService.List(ctx, connect.NewRequest(&internalpb.ListRequest{}))
	if err != nil {
		return nil, err
	}

	entries := make([]*Entry, 0, len(response.Msg.GetEntries()))
	for _, entry := range response.Msg.GetEntries() {
		entries = append(entries, fromNode(entry))
	}
	return entries, nil
}

// Delete deletes a given key from the cluster
// nolint
func (client *Client) Delete(ctx context.Context, key string) error {
	if !client.connected.Load() {
		return ErrClientNotConnected
	}
	_, err := client.kvService.Delete(ctx, connect.NewRequest(
		&internalpb.DeleteRequest{
			Key: key,
		}))

	return err
}

// Exists checks the existence of a given key in the cluster
func (client *Client) Exists(ctx context.Context, key string) (bool, error) {
	if !client.connected.Load() {
		return false, ErrClientNotConnected
	}

	response, err := client.kvService.KeyExists(ctx, connect.NewRequest(
		&internalpb.KeyExistsRequest{
			Key: key,
		}))

	if err != nil {
		return false, err
	}

	return response.Msg.GetExists(), nil
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

// NewClient creates an instance of the cluster Client
// host and port are a Go-KV cluster node host and port
func NewClient(host string, port int) *Client {
	httpClient := http.NewClient()
	kvService := internalpbconnect.NewKVServiceClient(
		httpClient,
		http.URL(host, port),
		// TODO: add observability options
	)
	return &Client{
		httpClient: httpClient,
		kvService:  kvService,
		connected:  atomic.NewBool(true),
	}
}
