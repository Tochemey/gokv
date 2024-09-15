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
	"maps"
	"sync"

	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/gokv/internal/internalpb"
	"github.com/tochemey/gokv/internal/lib"
)

// Delegate defines a node delegate
type Delegate struct {
	*sync.RWMutex
	// node metadata shared in the cluster
	// for instance the IP discoveryAddress of the node, the name of the node
	// relevant information that can be known by the other peers in the cluster
	nodeMeta *internalpb.NodeMeta
	// holds the peers state
	fsm *internalpb.FSM

	me string
}

// enforce compilation error
var _ memberlist.Delegate = (*Delegate)(nil)

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
// nolint
func (d *Delegate) NodeMeta(limit int) []byte {
	d.Lock()
	// no need to check the error
	bytea, _ := proto.Marshal(d.nodeMeta)
	d.Unlock()
	return bytea
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
// nolint
func (d *Delegate) NotifyMsg(bytes []byte) {
	// push/pull sync all the way
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
// nolint
func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	// nothing to broadcast
	return nil
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
// nolint
func (d *Delegate) LocalState(join bool) []byte {
	d.Lock()
	bytea, _ := proto.Marshal(d.fsm)
	d.Unlock()
	return bytea
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// delegate received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
// nolint
func (d *Delegate) MergeRemoteState(buf []byte, join bool) {
	d.Lock()
	defer d.Unlock()

	remoteFSM := new(internalpb.FSM)
	_ = proto.Unmarshal(buf, remoteFSM)
	localFSM := d.fsm

	// build the map of all entries in the node local state
	entries := make(map[string]map[string]*internalpb.Entry)
	for _, nodeState := range localFSM.GetNodeStates() {
		entries[nodeState.GetNodeId()] = nodeState.GetEntries()
	}

	// iterate all the entries coming from the remote node
	// 1. if there is corresponding node ID in the node local state, combine the local state entries for that nodeID with the remote node entries
	// 2. if there is no corresponding node ID in the node local state, set the entries with the remote entries
	for _, nodeState := range remoteFSM.GetNodeStates() {
		localEntries, ok := entries[nodeState.GetNodeId()]
		if !ok {
			entries[nodeState.GetNodeId()] = nodeState.GetEntries()
			continue
		}

		if len(localEntries) == 0 {
			localEntries = make(map[string]*internalpb.Entry)
		}

		maps.Copy(localEntries, nodeState.GetEntries())
		entries[nodeState.GetNodeId()] = localEntries
	}

	// iterate the entries and build the new nodeState list
	nodeStates := make([]*internalpb.NodeState, 0, len(entries))
	for k, v := range entries {
		nodeStates = append(nodeStates, &internalpb.NodeState{
			NodeId:  k,
			Entries: v,
		})
	}

	// set the local node state with the new nodeState list
	d.fsm.NodeStates = nodeStates
}

// Put adds the key/value to the store
func (d *Delegate) Put(key string, value []byte) {
	d.Lock()
	defer d.Unlock()

	localState := d.fsm
	keyExists := false

	// first check the key existence and overwrite when found
	for _, nodeState := range localState.GetNodeStates() {
		for k := range nodeState.GetEntries() {
			if k == key {
				nodeState.Entries[k] = &internalpb.Entry{
					Value:    value,
					Archived: nil,
				}
				keyExists = true
				break
			}
		}
	}

	// key has been found which means it has been overwritten
	// just return
	if keyExists {
		return
	}

	// key does not exist which means the given node is adding it as
	// part of its key/value pair
	for _, nodeState := range localState.GetNodeStates() {
		if nodeState.GetNodeId() == d.me {
			if len(nodeState.GetEntries()) == 0 {
				nodeState.Entries = map[string]*internalpb.Entry{
					key: {
						Value:    value,
						Archived: nil,
					},
				}
				return
			}

			nodeState.GetEntries()[key] = &internalpb.Entry{
				Value:    value,
				Archived: nil,
			}
			return
		}
	}
}

// Get returns the value of the given key
func (d *Delegate) Get(key string) []byte {
	d.RLock()
	defer d.RUnlock()
	localState := d.fsm
	for _, nodeState := range localState.GetNodeStates() {
		for k, entry := range nodeState.GetEntries() {
			if k == key {
				return entry.GetValue()
			}
		}
	}
	return nil
}

// Delete deletes the given key from the cluster
// One can only delete a key if the given node is the owner
func (d *Delegate) Delete(key string) {
	d.Lock()
	defer d.Unlock()

	for index, nodeState := range d.fsm.GetNodeStates() {
		for k := range nodeState.GetEntries() {
			if k == key && nodeState.GetNodeId() == d.me {
				nodeState.Entries[key].Archived = lib.Ptr(true)
				d.fsm.NodeStates[index] = nodeState
				return
			}
		}
	}
}

// Exists checks whether a given exists
func (d *Delegate) Exists(key string) bool {
	d.RLock()
	defer d.RUnlock()
	localState := d.fsm
	for _, nodeState := range localState.GetNodeStates() {
		for k, entry := range nodeState.GetEntries() {
			if k == key {
				return !entry.GetArchived() && len(entry.GetValue()) > 0
			}
		}
	}
	return false
}

// newDelegate creates a new Delegate
func newDelegate(name string, meta *internalpb.NodeMeta) *Delegate {
	return &Delegate{
		RWMutex:  &sync.RWMutex{},
		nodeMeta: meta,
		me:       name,
		fsm: &internalpb.FSM{
			NodeStates: []*internalpb.NodeState{
				{
					NodeId:  name,
					Entries: make(map[string]*internalpb.Entry, 10),
				},
			},
		},
	}
}
