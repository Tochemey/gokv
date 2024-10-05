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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/gokv/internal/internalpb"
)

// NodeFSM defines the given node finite state machine
// in the cluster
type NodeFSM struct {
	sync.RWMutex
	self string

	// node metadata shared in the cluster
	// for instance the IP discoveryAddress of the node, the name of the node
	// relevant information that can be known by the other peers in the cluster
	nodeMeta *internalpb.NodeMeta

	// localState holds the node local state
	// this will be replicated on other peer nodes
	// via the gossip protocol
	localState *internalpb.NodeState

	// peersState holds all the peers state
	// this will be used when merging other node state
	// during merge each node will unmarshal the incoming bytes array into
	// internalpb.NodeState and try to find out whether the given entry exists in its peer
	// state and add it.
	peersState *internalpb.PeersState
}

// enforce compilation error
var _ memberlist.Delegate = (*NodeFSM)(nil)

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
// nolint
func (fsm *NodeFSM) NodeMeta(limit int) []byte {
	fsm.Lock()
	// no need to check the error
	bytea, _ := proto.Marshal(fsm.nodeMeta)
	fsm.Unlock()
	return bytea
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
// nolint
func (fsm *NodeFSM) NotifyMsg(bytes []byte) {}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
// nolint
func (fsm *NodeFSM) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
// nolint
func (fsm *NodeFSM) LocalState(join bool) []byte {
	fsm.Lock()
	bytea, _ := proto.Marshal(fsm.localState)
	fsm.Unlock()
	return bytea
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// delegate received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
// nolint
func (fsm *NodeFSM) MergeRemoteState(buf []byte, join bool) {
	fsm.Lock()
	incomingState := new(internalpb.NodeState)
	_ = proto.Unmarshal(buf, incomingState)
	incomingNodeID := incomingState.GetNodeId()

	// override the existing peer state if already exists
	fsm.peersState.GetRemoteStates()[incomingNodeID] = incomingState
	fsm.Unlock()
}

// Put adds the key/value to the node local state
func (fsm *NodeFSM) Put(key string, value []byte, expiration time.Duration) {
	fsm.Lock()
	localState := fsm.localState
	newEntry := &internalpb.Entry{
		Key:             key,
		Value:           value,
		LastUpdatedTime: timestamppb.New(time.Now().UTC()),
		Expiry:          setExpiry(expiration),
	}
	localState.GetEntries()[key] = newEntry
	fsm.Unlock()
}

// Get returns the value of the given key
// This can return a false negative meaning that the key may exist but at the time of checking it
// is having yet to be replicated in the cluster
func (fsm *NodeFSM) Get(key string) (*internalpb.Entry, error) {
	fsm.RLock()
	localState := fsm.localState

	// first let us lookup our local state and see whether the given is in there
	// if exists return it otherwise check the peer state maybe a node has it
	if entry, exists := localState.GetEntries()[key]; exists {
		fsm.RUnlock()
		if expired(entry) {
			return nil, ErrKeyNotFound
		}
		return entry, nil
	}

	// this node does not have the given, let us check our current peer states
	peerStates := fsm.peersState.GetRemoteStates()
	for _, peerState := range peerStates {
		if entry, exists := peerState.GetEntries()[key]; exists {
			fsm.RUnlock()
			if expired(entry) {
				return nil, ErrKeyNotFound
			}
			return entry, nil
		}
	}
	fsm.RUnlock()
	return nil, ErrKeyNotFound
}

// Delete deletes the given key from the cluster
// One can only delete a key if the given node is the owner
func (fsm *NodeFSM) Delete(key string) {
	fsm.Lock()
	localState := fsm.localState
	if _, exists := localState.GetEntries()[key]; exists {
		delete(localState.GetEntries(), key)
		fsm.Unlock()
		return
	}
	fsm.Unlock()
}

// Exists checks whether a given exists
// This can return a false negative meaning that the key may exist but at the time of checking it
// is having yet to be replicated in the cluster
func (fsm *NodeFSM) Exists(key string) bool {
	fsm.RLock()
	localState := fsm.localState

	// first let us lookup our local state and see whether the given is in there
	// if exists return it otherwise check the peer state maybe a node has it
	if entry, exists := localState.GetEntries()[key]; exists {
		fsm.RUnlock()
		return !expired(entry)
	}

	// this node does not have the given, let us check our current peer states
	peerStates := fsm.peersState.GetRemoteStates()
	for _, peerState := range peerStates {
		if entry, exists := peerState.GetEntries()[key]; exists {
			fsm.RUnlock()
			return !expired(entry)
		}
	}
	fsm.RUnlock()
	return false
}

// List returns the list of entries in the cluster
// It returns a combined list of entries in the given node and its peers
// at a given point in time.
func (fsm *NodeFSM) List() []*internalpb.Entry {
	fsm.RLock()
	localState := fsm.localState
	var entries []*internalpb.Entry

	for _, entry := range localState.GetEntries() {
		if !expired(entry) {
			entries = append(entries, entry)
		}
	}

	for _, peerState := range fsm.peersState.GetRemoteStates() {
		for _, peerState := range peerState.GetEntries() {
			if !expired(peerState) {
				entries = append(entries, peerState)
			}
		}
	}

	fsm.RUnlock()
	return entries
}

// removeExpired removes all entries that are expired
func (fsm *NodeFSM) removeExpired() {
	fsm.Lock()
	localState := fsm.localState
	for key, entry := range localState.GetEntries() {
		if !expired(entry) {
			delete(localState.GetEntries(), key)
		}
	}
	fsm.Unlock()
}

// nodeFSM creates an instance of NodeFSM
func nodeFSM(name string, meta *internalpb.NodeMeta) *NodeFSM {
	return &NodeFSM{
		RWMutex:  sync.RWMutex{},
		nodeMeta: meta,
		self:     name,
		localState: &internalpb.NodeState{
			NodeId:  name,
			Entries: make(map[string]*internalpb.Entry, 10),
		},
		peersState: &internalpb.PeersState{
			RemoteStates: make(map[string]*internalpb.NodeState, 100),
		},
	}
}

// expired returns true if the item has expired.
func expired(entry *internalpb.Entry) bool {
	if entry.GetExpiry() == nil {
		return false
	}
	expiration := entry.GetLastUpdatedTime().AsTime().Unix()
	if expiration <= 0 {
		return false
	}
	return time.Now().UTC().Unix() > expiration
}

// setExpiry sets the expiry time
func setExpiry(expiration time.Duration) *durationpb.Duration {
	var expiry *durationpb.Duration
	if expiration > 0 {
		expiry = durationpb.New(expiration)
	}
	return expiry
}
