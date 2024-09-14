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
	"os"
	"time"

	"github.com/tochemey/gokv/discovery"
	"github.com/tochemey/gokv/internal/validation"
	"github.com/tochemey/gokv/log"
)

// Config defines the cluster config
type Config struct {
	// specifies the maxJoinAttempts
	maxJoinAttempts int
	// specifies the join retry interval
	joinRetryInterval time.Duration
	// specifies the discovery provider
	provider discovery.Provider
	// specifies the node client port
	port uint16
	// specifies the node discovery port
	discoveryPort uint16
	// specifies the shutdown timeout
	shutdownTimeout time.Duration
	// specifies the logger
	logger log.Logger
	// specifies the host
	host string
	// specifies the state sync interval
	stateSyncInterval time.Duration
}

// enforce compilation error
var _ validation.Validator = (*Config)(nil)

// NewConfig creates an instance of Config
// with the required default values
func NewConfig() *Config {
	return &Config{
		host:              "0.0.0.0",
		maxJoinAttempts:   10,
		joinRetryInterval: time.Second,
		shutdownTimeout:   3 * time.Second,
		stateSyncInterval: time.Minute,
		logger:            log.New(log.ErrorLevel, os.Stderr),
	}
}

// WithDiscoveryProvider sets the discovery provider
func (config *Config) WithDiscoveryProvider(provider discovery.Provider) *Config {
	config.provider = provider
	return config
}

// WithDiscoveryPort sets the discovery port
func (config *Config) WithDiscoveryPort(gossipPort uint16) *Config {
	config.discoveryPort = gossipPort
	return config
}

// WithPort sets the client port
func (config *Config) WithPort(port uint16) *Config {
	config.port = port
	return config
}

// WithLogger sets the logger
func (config *Config) WithLogger(logger log.Logger) *Config {
	config.logger = logger
	return config
}

// WithShutdownTimeout sets the timeout
func (config *Config) WithShutdownTimeout(timeout time.Duration) *Config {
	config.shutdownTimeout = timeout
	return config
}

// WithMaxJoinAttempts sets the max join attempts
func (config *Config) WithMaxJoinAttempts(max int) *Config {
	config.maxJoinAttempts = max
	return config
}

// WithJoinRetryInterval sets the join retry interval
func (config *Config) WithJoinRetryInterval(interval time.Duration) *Config {
	config.joinRetryInterval = interval
	return config
}

// WithHost specifies the config host
func (config *Config) WithHost(host string) *Config {
	config.host = host
	return config
}

// WithStateSyncInterval sets the state sync interval
func (config *Config) WithStateSyncInterval(interval time.Duration) *Config {
	config.stateSyncInterval = interval
	return config
}

// Validate implements validation.Validator.
func (config *Config) Validate() error {
	return validation.
		New(validation.AllErrors()).
		AddAssertion(config.provider != nil, "discovery provider is not set").
		AddAssertion(config.discoveryPort > 0, "gossip port is invalid").
		AddAssertion(config.port > 0, "client port is invalid").
		AddAssertion(config.joinRetryInterval > 0, "join retry interval is invalid").
		AddAssertion(config.shutdownTimeout > 0, "shutdown timeout is invalid").
		AddAssertion(config.maxJoinAttempts > 0, "max join attempts is invalid").
		AddAssertion(config.stateSyncInterval > 0, "stateSync interval is invalid").
		AddValidator(validation.NewEmptyStringValidator("host", config.host)).
		Validate()
}
