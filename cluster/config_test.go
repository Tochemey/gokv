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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tochemey/gokv/log"
	mocks "github.com/tochemey/gokv/mocks/discovery"
)

func TestConfig(t *testing.T) {
	t.Run("With valid config", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost("127.0.0.1").
			WithSyncInterval(time.Second).
			WithLogger(log.DiscardLogger).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		assert.NoError(t, config.Validate())
	})
	t.Run("With invalid host", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost(""). // host not provided will return an error
			WithSyncInterval(time.Second).
			WithLogger(log.DiscardLogger).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		assert.Error(t, config.Validate())
	})
	t.Run("With invalid host", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost(""). // host not provided will return an error
			WithSyncInterval(time.Second).
			WithLogger(log.DiscardLogger).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		assert.Error(t, config.Validate())
	})
	t.Run("With invalid host", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost(""). // host not provided will return an error
			WithSyncInterval(time.Second).
			WithLogger(log.DiscardLogger).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		assert.Error(t, config.Validate())
	})
	t.Run("With invalid host", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost(""). // host not provided will return an error
			WithSyncInterval(time.Second).
			WithLogger(log.DiscardLogger).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		assert.Error(t, config.Validate())
	})
	t.Run("With empty host", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost("").
			WithSyncInterval(time.Second).
			WithLogger(log.DiscardLogger).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)

		err := config.Validate()
		assert.Error(t, err)
		assert.EqualError(t, err, "the [host] is required")
	})
	t.Run("With provider not set", func(t *testing.T) {
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(nil).
			WithHost("127.0.0.1").
			WithSyncInterval(time.Second).
			WithLogger(log.DiscardLogger).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		err := config.Validate()
		assert.Error(t, err)
		assert.EqualError(t, err, "discovery provider is not set")
	})
	t.Run("With invalid join retry interval", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost("127.0.0.1").
			WithLogger(log.DiscardLogger).
			WithSyncInterval(time.Second).
			WithJoinRetryInterval(-1).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		err := config.Validate()
		assert.Error(t, err)
		assert.EqualError(t, err, "join retry interval is invalid")
	})
	t.Run("With invalid sync retry interval", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost("127.0.0.1").
			WithLogger(log.DiscardLogger).
			WithSyncInterval(-1).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithReadTimeout(time.Second)
		err := config.Validate()
		assert.Error(t, err)
		assert.EqualError(t, err, "stateSync interval is invalid")
	})
	t.Run("With invalid shutdown timeout", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost("127.0.0.1").
			WithLogger(log.DiscardLogger).
			WithSyncInterval(time.Second).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(-1).
			WithReadTimeout(time.Second)
		err := config.Validate()
		assert.Error(t, err)
		assert.EqualError(t, err, "shutdown timeout is invalid")
	})
	t.Run("With invalid max join attempts", func(t *testing.T) {
		discovery := new(mocks.Provider)
		config := NewConfig().
			WithPort(1234).
			WithDiscoveryPort(1235).
			WithDiscoveryProvider(discovery).
			WithHost("127.0.0.1").
			WithLogger(log.DiscardLogger).
			WithSyncInterval(time.Second).
			WithJoinRetryInterval(time.Second).
			WithShutdownTimeout(time.Second).
			WithMaxJoinAttempts(-1).
			WithReadTimeout(time.Second)
		err := config.Validate()
		assert.Error(t, err)
		assert.EqualError(t, err, "max join attempts is invalid")
	})
}
