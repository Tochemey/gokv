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

package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// NewClient creates a http client use h2c
func NewClient() *http.Client {
	// Speak unencrypted HTTP/2 (h2c) with prior knowledge. Enabling only
	// UnencryptedHTTP2, and not HTTP1, makes the standard library transport
	// use h2c for http:// URLs without a TLS handshake.
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Client{
		// Most RPC servers don't use HTTP redirects
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			Protocols: protocols,
			HTTP2: &http.HTTP2Config{
				// Send a health-check ping when no frame has been received for this long.
				SendPingTimeout: 30 * time.Second,
				// Close the connection when a ping goes unanswered for this long.
				PingTimeout: 30 * time.Second,
			},
		},
	}
}

// NewServer returns an instance of an http server
func NewServer(ctx context.Context, host string, port int, mux *http.ServeMux) *http.Server {
	// For gRPC clients, it's convenient to support HTTP/2 without TLS. Clients
	// connect with prior knowledge (see NewClient), which the standard library
	// supports natively via the Protocols field.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	// TODO revisit the timeouts
	// reference: https://adam-p.ca/blog/2022/01/golang-http-server-timeouts/
	return &http.Server{
		Addr: net.JoinHostPort(host, strconv.Itoa(port)),
		// The maximum duration for reading the entire request, including the body.
		// It’s implemented in net/http by calling SetReadDeadline immediately after Accept
		// ReadTimeout := handler_timeout + ReadHeaderTimeout + wiggle_room
		ReadTimeout: 3 * time.Second,
		// ReadHeaderTimeout is the amount of time allowed to read request headers
		ReadHeaderTimeout: time.Second,
		// WriteTimeout is the maximum duration before timing out writes of the response.
		// It is reset whenever a new request’s header is read.
		// This effectively covers the lifetime of the ServeHTTP handler stack
		WriteTimeout: time.Second,
		// IdleTimeout is the maximum amount of time to wait for the next request when keep-alive are enabled.
		// If IdleTimeout is zero, the value of ReadTimeout is used. Not relevant to request timeouts
		IdleTimeout: 1200 * time.Second,
		Handler:     mux,
		Protocols:   protocols,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}
}

// URL create a http connection address
func URL(host string, port int) string {
	return fmt.Sprintf("http://%s", net.JoinHostPort(host, strconv.Itoa(port)))
}
