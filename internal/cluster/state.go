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
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/gokv/internal/internalpb"
)

// State defines a node state
type State struct {
	*sync.RWMutex
	// node metadata shared in the cluster
	// for instance the IP address of the node, the name of the node
	// relevant information that can be known by the other peers in the cluster
	nodeMeta *internalpb.NodeMeta
	//  node internal state - this is the actual config being gossiped
	nodeState *internalpb.NodeState
}

// enforce compilation error
var _ memberlist.Delegate = (*State)(nil)

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
// nolint
func (state *State) NodeMeta(limit int) []byte {
	state.Lock()
	// no need to check the error
	bytea, _ := proto.Marshal(state.nodeMeta)
	state.Unlock()
	return bytea
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
// nolint
func (state *State) NotifyMsg(bytes []byte) {
	// push/pull sync all the way
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
// nolint
func (state *State) GetBroadcasts(overhead, limit int) [][]byte {
	// nothing to broadcast
	return nil
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
// nolint
func (state *State) LocalState(join bool) []byte {
	state.Lock()
	// no need to check the error
	bytea, _ := proto.Marshal(state.nodeState)
	state.Unlock()
	return bytea
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
// nolint
func (state *State) MergeRemoteState(buf []byte, join bool) {
	state.Lock()
	remoteState := new(internalpb.NodeState)
	// ignore the error for the meantime
	// TODO: check it when necessary
	_ = proto.Unmarshal(buf, remoteState)

	currentState := state.nodeState.GetState()
	for key, value := range remoteState.GetState() {
		currentValue := currentState[key]
		if !proto.Equal(currentValue, value) {
			state.nodeState.State[key] = value
		}
	}

	state.Unlock()
}

// Put adds the key/value to the store
func (state *State) Put(key string, value []byte) {
	state.Lock()
	state.nodeState.State[key] = &internalpb.KV{
		Key:       key,
		Value:     value,
		CreatedAt: timestamppb.New(time.Now().UTC()),
	}
	state.Unlock()
}

// Get returns the value of the given key
func (state *State) Get(key string) *internalpb.KV {
	state.RLock()
	kv := state.nodeState.GetState()[key]
	state.RUnlock()
	return kv
}

// Delete deletes the given key from the cluster
func (state *State) Delete(key string) {
	state.RLock()
	internalState := state.nodeState.GetState()
	delete(internalState, key)
	state.nodeState.State = internalState
	state.RUnlock()
}

// newState creates a new State
func newState(meta *internalpb.NodeMeta) *State {
	return &State{
		RWMutex:   &sync.RWMutex{},
		nodeMeta:  meta,
		nodeState: &internalpb.NodeState{},
	}
}
