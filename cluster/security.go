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
	"errors"

	"github.com/tochemey/gokv/internal/validation"
)

// Security defines measures needed to protect and guarantee
// the authenticity of data shared in the cluster
type Security struct {
	// cookie is a set of bytes to use as authentication label
	// This has to be the same within the cluster to ensure smooth GCM authenticated data
	// reference: https://en.wikipedia.org/wiki/Galois/Counter_Mode
	cookie string
	// encryptionKey is used to encrypt messages and decrypt messages. Providing a
	// value for this will enable message-level encryption and
	// verification.
	//
	// The value should be either 16, 24, or 32 bytes to select AES-128,
	// AES-192, or AES-256.
	encryptionKey []byte
}

// NewSecurity creates an instance of Security by providing the cookie and the encryption key
//
// cookie is a set of bytes to use as authentication label
// encryptionKey is used to encrypt messages and decrypt messages. Providing a
// value for this will enable message-level encryption and
// verification. The cookie has to be the same within the cluster to ensure smooth GCM authenticated data
// reference: https://en.wikipedia.org/wiki/Galois/Counter_Mode
//
// The value should be either 16, 24, or 32 bytes to select AES-128,
// AES-192, or AES-256.
func NewSecurity(cookie string, privateKey []byte) *Security {
	return &Security{cookie: cookie, encryptionKey: privateKey}
}

// enforce compilation error
var _ validation.Validator = (*Security)(nil)

// Validate validates the security object
func (sec *Security) Validate() error {
	if l := len(sec.encryptionKey); l != 16 && l != 24 && l != 32 {
		return errors.New("key size must be 16, 24 or 32 bytes")
	}
	return nil
}
