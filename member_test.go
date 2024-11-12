/*
 * MIT License
 *
 * Copyright (c) 2024 Arsene Tochemey Gandote
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

package gokv

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/gokv/internal/internalpb"
	"github.com/tochemey/gokv/internal/lib"
)

func TestMember(t *testing.T) {
	member := &Member{
		Name:          "name",
		Host:          "host",
		Port:          1234,
		DiscoveryPort: 1235,
	}
	expected := lib.HostPort("host", 1235)
	assert.Equal(t, expected, member.DiscoveryAddress())
}

func TestMemberFromMeta(t *testing.T) {
	// caution make sure to set the time to UTC
	// then the reflect.DeepEqual will work
	creationTime := time.Now().UTC()
	meta := &internalpb.NodeMeta{
		Name:          "name",
		Host:          "host",
		Port:          1234,
		DiscoveryPort: 1235,
		CreationTime:  timestamppb.New(creationTime),
	}

	bytea, err := proto.Marshal(meta)
	require.NoError(t, err)
	require.NotEmpty(t, bytea)

	expected := &Member{
		Name:          "name",
		Host:          "host",
		Port:          1234,
		DiscoveryPort: 1235,
		CreatedAt:     creationTime,
	}

	actual, err := memberFromMeta(bytea)
	require.NoError(t, err)
	require.NotNil(t, actual)
	assert.IsType(t, new(Member), actual)
	assert.True(t, reflect.DeepEqual(expected, actual))
}
