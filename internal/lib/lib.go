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

package lib

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/tochemey/gokv/internal/types"
)

// Pause pauses the running process for some time period
func Pause(duration time.Duration) {
	stopCh := make(chan types.Unit, 1)
	timer := time.AfterFunc(duration, func() {
		stopCh <- types.Unit{}
	})
	<-stopCh
	timer.Stop()
}

// HostPort returns the combination of host:port
func HostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// Ptr creates a pointer of a primitive
func Ptr[T any](v T) *T {
	return &v
}

// MapKeyExists return an error when the given key is not found in the map
func MapKeyExists(m map[string]any, key string) error {
	if _, ok := m[key]; ok {
		return nil
	}
	return fmt.Errorf("key %s does not exist", key)
}
