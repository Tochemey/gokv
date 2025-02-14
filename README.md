# Go-KV

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/Tochemey/gokv/build.yml)]((https://github.com/Tochemey/gokv/actions/workflows/build.yml))

Simple Distributed in-memory key/value store. 
GoKV provides high availability and fault tolerance which makes it suitable large-scale applications system without sacrificing performance and reliability. 
With GoKV, you can instantly create a fast, scalable, distributed system  across a cluster of computers. 

## 👋 Table Of Content
- [Installation](#-installation)
- [Design](#-design)
- [Features](#-features)
- [Use Cases](#-use-cases)
- [Trade-offs](#-trade-offs)
- [Example](#-example)
- [Discovery](#-builtin-discovery)

## 🖥️ Installation

```bash
go get github.com/tochemey/gokv
```

## 🏛️ Design

Go-KV is a distributed key/value store designed to synchronize data across a cluster of computers using the push-pull anti-entropy method.
This ensures that updates made to a node are eventually propagated to all other nodes, maintaining eventual consistency within the system.
Whenever a key/value entry is modified on any node, the entire state of that entry is replicated across the cluster.
While Go-KV does not guarantee immediate consistency, it ensures that all nodes will converge to the same state over time.
To control the frequency of synchronization, Go-KV provides a configurable parameter:`syncInterval` that defines how often nodes exchange data to reconcile differences. 
- Lower values lead to faster synchronization but increase network traffic. 
- Higher values reduce network overhead but may lead to longer delays in data propagation.

## 🎯 Features
- Built-in [client](./cluster/client.go) to interact with the cluster via the following apis:
  - `Put`: create key/value pair that is eventually distributed in the cluster of nodes. The `key` is a string and the `value` is a byte array. One can set an expiry to the key.
  - `PutProto`: to create a key/value pair where the value is a protocol buffer message
  - `PutString`: to create a key/value pair where the value is a string
  - `PutAny`: to create a key/value pair with a given [`Codec`](./codec.go) to encode the value type.
  - `Get`: retrieves the value of a given `key` from the cluster of nodes. This can return a false negative meaning that the key may exist but at the time of checking it is having yet to be replicated in the cluster.
  - `GetProto`: retrieves a protocol buffer message for a given `key`. This requires `PutProto` or `Put` to be used to set the value.
  - `GetString`: retrieves a string value for a given `key`. This requires `PutString` or `Put` to be used to set the value.
  - `GetAny`: retrieves any value type for a given `key`. This requires `PutAny` to be used to set the value.
  - `List`: retrieves the list of key/value pairs in the cluster at a point in time
  - `Exists`: check the existence of a given `key` in the cluster. This can return a false negative meaning that the key may exist but at the time of checking it is having yet to be replicated in the cluster.
  - `Delete`: delete a given `key` from the cluster. Node only deletes the key they own
- Built-in janitor to remove expired entries. One can set the janitor execution interval. Bearing in mind of the eventual consistency of the Go-KV, one need to set that interval taking into consideration the [`syncInterval`](./cluster/config.go)
- Discovery API to implement custom nodes discovery provider. See: [Discovery](./discovery/provider.go)
- Data encryption using the `cookie` and the set of `secrets` via the [Config](./config.go)
- Configuration can be customized. See [Config](./config.go)
- TLS Support. See [Config](./config.go)
- Comes bundled with some discovery providers that can help you hit the ground running:
    - [kubernetes](https://kubernetes.io/docs/home/) [api integration](./discovery/kubernetes) is fully functional
    - [nats](https://nats.io/) [integration](./discovery/nats) is fully functional
    - [static](./discovery/static) is fully functional and for demo purpose
    - [dns](./discovery/dnssd) is fully functional

## 💡 Use Cases

Go-KV is suitable for scenarios where high availability and fault tolerance are more critical than strict consistency, such as:

- Service Discovery
- Configuration Management
- Decentralized Coordination
- Distributed Caching

## ⚖️ Trade-offs

- ✅ Highly Available – Nodes remain operational even in the presence of network partitions. 
- ✅ Fault Tolerant – Can recover from node failures without losing data permanently. 
- ❌ Eventual Consistency – No immediate guarantees that all nodes have the same data at any given moment. 
- ❌ Increased Network Traffic – Lower syncInterval values can lead to higher bandwidth usage.

## 📋 Example

There is an example on how to use it with NATs [here](./example/example.go)

## 🌐 Builtin Discovery

### nats

To use the [nats](https://nats.io/) discovery provider one needs to provide the following:

- `Server`: the NATS Server address
- `Subject`: the NATS subject to use
- `Timeout`: the nodes discovery timeout
- `MaxJoinAttempts`: the maximum number of attempts to connect an existing NATs server. Defaults to `5`
- `ReconnectWait`: the time to backoff after attempting a reconnect to a server that we were already connected to previously. Default to `2 seconds`
- `DiscoveryPort`: the discovery port of the running node
- `Host`: the host address of the running node

### dns

This provider performs nodes discovery based upon the domain name provided. This is very useful when doing local development
using docker.

To use the DNS discovery provider one needs to provide the following:

- `DomainName`: the domain name
- `IPv6`: it states whether to lookup for IPv6 addresses.

### static

This provider performs nodes discovery based upon the list of static hosts addresses.
The address of each host is the form of `host:port` where `port` is the discovery port.

### kubernetes

To get the [kubernetes](https://kubernetes.io/docs/home/) discovery working as expected, the following need to be set in the manifest files:

- `Namespace`: the kubernetes namespace
- `DiscoveryPortName`: the discovery port name
- `PortName`: the client port name. This port is used by the built-in cluster client for the various operations on the key/value pair distributed store
- `PodLabels`: the POD labels

Make sure to provide the right RBAC settings to be able to access the pods.
