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

package nats

import (
	"fmt"
	"strings"
	"time"

	"github.com/tochemey/gokv/internal/validation"
)

// Config represents the nats provider discoConfig
type Config struct {
	// Server defines the nats server in the format nats://host:port
	Server string
	// Subject defines the custom NATS subject
	Subject string
	// Timeout defines the nodes discovery timeout
	Timeout time.Duration
	// MaxJoinAttempts denotes the maximum number of attempts to connect an existing NATs server
	// Default to 5
	MaxJoinAttempts int
	// ReconnectWait sets the time to backoff after attempting a reconnect
	// to a server that we were already connected to previously.
	// Defaults to 2s.
	ReconnectWait time.Duration
	// Host specifies the host
	Host string
	// DiscoveryPort specifies the node discovery port
	DiscoveryPort uint16
}

// Validate checks whether the given discovery configuration is valid
func (config Config) Validate() error {
	return validation.New(validation.FailFast()).
		AddValidator(validation.NewEmptyStringValidator("Server", config.Server)).
		AddValidator(NewServerAddrValidator(config.Server)).
		AddValidator(validation.NewEmptyStringValidator("Subject", config.Subject)).
		AddValidator(validation.NewEmptyStringValidator("Host", config.Host)).
		AddAssertion(config.DiscoveryPort > 0, "DiscoveryPort is invalid").
		Validate()
}

// ServerAddrValidator helps validates the NATs server address
type ServerAddrValidator struct {
	server string
}

// NewServerAddrValidator validates the nats server address
func NewServerAddrValidator(server string) validation.Validator {
	return &ServerAddrValidator{server: server}
}

// Validate execute the validation code
func (x *ServerAddrValidator) Validate() error {
	// make sure that the nats prefix is set in the server address
	if !strings.HasPrefix(x.server, "nats") {
		return fmt.Errorf("invalid nats server address: %s", x.server)
	}

	hostAndPort := strings.SplitN(x.server, "nats://", 2)[1]
	return validation.NewTCPAddressValidator(hostAndPort).Validate()
}

// enforce compilation error
var _ validation.Validator = (*ServerAddrValidator)(nil)
