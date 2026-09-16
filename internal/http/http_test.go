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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/go-dynaport"
)

func TestNewClient(t *testing.T) {
	cl := NewClient()
	assert.IsType(t, new(http.Client), cl)
	assert.IsType(t, new(http.Transport), cl.Transport)
	tr := cl.Transport.(*http.Transport)
	assert.NotNil(t, tr.Protocols)
	assert.True(t, tr.Protocols.UnencryptedHTTP2())
	assert.False(t, tr.Protocols.HTTP1())
	assert.NotNil(t, tr.HTTP2)
	assert.Equal(t, 30*time.Second, tr.HTTP2.PingTimeout)
	assert.Equal(t, 30*time.Second, tr.HTTP2.SendPingTimeout)
}

func TestNewServer(t *testing.T) {
	host := "127.0.0.1"
	port := dynaport.Get(1)[0]
	mux := http.NewServeMux()
	ctx := context.TODO()

	server := NewServer(ctx, host, port, mux)
	assert.NotNil(t, server)
	assert.IsType(t, new(http.Server), server)
}

func TestURL(t *testing.T) {
	host := "127.0.0.1"
	port := 123

	url := URL(host, port)
	assert.Equal(t, "http://127.0.0.1:123", url)
}
