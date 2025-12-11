## Overview

The spec supports a peer discovery algorithm. Full nodes and validators have different goals in the peer discovery protocol.

For a full node, its goals are:
- To discover the name records of the entire active validator set so that it can forward transactions directly to these validators for transaction inclusion
- To advertise its own name record to a few validators running secondary raptorcast so that these validators include it when forming a secondary raptorcast group

For a validator, its goals are:
- To discover the name records of the entire active validator set so that it can send consensus messages to the validators

As such, the peer discovery protocol is in a way validator-centric, i.e. all nodes in the network will aim to discover the name records of the active validator set only, without needing to actively discover the name records of full nodes. Full nodes themselves are responsible for advertising their own name records to validators for secondary raptorcast group inclusion.

Peer discovery messages are currently part of raptorcast messages. All raptorcast messages are replay protected and signed to ensure authenticity of the message in regards to the sender's node ID. Peer discovery messages itself do not carry standalone signatures. Name records are independently signed. 


### Base types

```rust
struct MonadNameRecord {
  address: std::net::SocketAddrV4,
  seq: u64,
  signature: SecpSignature,
}

enum PeerDiscoveryMessage {
  Ping(Ping),
  Pong(Pong),
  PeerLookupRequest(PeerLookupRequest),
  PeerLookupResponse(PeerLookupResponse),
  FullNodeRaptorcastRequest,
  FullNodeRaptorcastResponse,
}
```

### Ping

---

Checks if peer is alive, and advertises local name record. Sender must have receiver's name record in order to be able to deliver the ping message to the receiver

```rust
struct Ping {
  id: u32,
  local_name_record: MonadNameRecord,
}
```

### Pong

---

Response to ping message. Name records are only added to the local routing table upon a successful ping pong round trip

```rust
struct Pong {
  ping_id: u32,
  local_record_seq: u64,
}
```

### PeerLookupRequest

---

Request to look up name record of target peer. Client can request more peers returned by setting `open_discovery`. Server can choose to ignore `open_discovery`

```rust
struct PeerLookupRequest {
  lookup_id: u32,
  target: NodeId,
  open_discovery: bool,
}
```

### PeerLookupResponse

---

Response to PeerLookupRequest message. Server should respond with target’s name record if it’s known locally. lookup_id must match the request

```rust
struct PeerLookupResponse {
  lookup_id: u32,
  target: NodeId,
  name_records: Vec<MonadNameRecord>,
}
```

### FullNodeRaptorcastRequest

For a full node wishing to participate in secondary raptorcast, it needs to reach out to validators running as a publisher in secondary raptorcast for two purposes:
1. Indicate interest for joining the secondary raptorcast group
2. Advertises its name record

`FullNodeRaptorcastRequest` is the message type to fulfill the first purpose. A full node tries to connect to 3 validators by sending this message type. At the same time, the full node sends a ping message to fulfill the second purpose. If the full node does not get a response from the validator, it retries the `FullNodeRaptorcastRequest` message for a few times before reaching out to another validator.


### FullNodeRaptorcastResponse

If a validator running as a publisher in secondary raptorcast still has capacity to accommodate more full nodes (i.e. current group size less than `max_group_size`), it sends a `FullNodeRaptorcastResponse` back to the full node that contacted it.


## Operations

### connect

- A ping pong round trip has to be completed before a node will add a peer's name record into its routing table
- X receives a new name record signed by Y that it has not seen (this can happen when X receives a name record from a ping message or from a peer lookup response)
- X sends a **Ping** to Y, also advertising its own name record
- Y responds with **Pong**
- X knows that Y is alive and inserts the name record into its routing table
- If Y does not respond with a pong, X retries for a few times before dropping the name record

### discover(target)

- A node always tries to have the name records of the active validator set
- If there is any missing name record of validators, X sends a **PeerLookupRequest** message to a peer it knows
- Server responds with
    - target’s latest name record if known to the server
    - empty if unknown
    - (optional) a sample of known validators if target is unknown and if the server chooses to honor a set open_discovery bit. This is necessary to bootstrap nodes, but vulnerable to amplification attack. We currently limit the number of peers in a response to 16. Current implementation always honor the open_discovery but this is not enforced by the protocol

### bootstrapping

- A few bootstrap addresses is made known to new joining nodes. These can be normal nodes or specialized peer discovery servers that serves as the network entrypoint. Nodes always try to `discover(validator)`.
- This process can be repeated to discover more validators.

### pruning

- Peer pruning is performed periodically and triggered by high watermark to keep peers manageable
- Nodes that have not participated in secondary raptorcast beyond a threshold are pruned
- Public full nodes are pruned if we're still above high watermark
- Currently validators for the current and next epoch, dedicated and prioritized full nodes are not pruned even if unresponsive or total number of peers is above high watermark

### persisting name records

- Name records in the local routing table are persisted to disk periodically
- This speeds up peer discovery time during restarts and reduces surface area for eclipse attack


## Security goals

The security goals of the peer discovery protocol are listed as follows.

**Peer discovery message replay protection**

Replayed peer discovery messages should not corrupt the state of the victim. The protocol enforces matching ping id and node id when handling a corresponding pong, and a ping that has been matched with a pong is removed from the pending queue immediately.

**Name record replay protection**

Replayed name records that have been outdated should not be added to the routing table of the victim. An outdated name record is where the name record signer does not have access to the socket address anymore. The protocol enforces a ping pong round trip where a ping message is sent **to** and a pong message received **from** the socket address in the name record before inserting the name record into the routing table, therefore socket address that is outdated will result in a round trip not being completed and thus not being inserted into the routing table.

**Distributed denial of service protection**

An adversary should not be able to direct unreasonable traffic from honest nodes to other validators. The protocol enforces a one node id per socket address, in order to prevent an adversary from advertising a socket address of another node to force ping/pong messages towards that node.

**Sybil and eclipse attack protection**

An adversary should not be able to prevent a node from connecting to other nodes and participate in the network. Since the protocol enforces a ping pong round trip before inserting a name record, an adversary can only fill up the routing table by actually owning the IPs that are advertised. This also means that the adversary cannot falsely claim a socket address that it doesn't have access to.

Additionally, the protocol gives priority to validators, i.e. a validator name record is still inserted even if routing table is full, and other full nodes exceeding the limit are pruned later. This is to ensure that validators can always get connected to other validators for consensus participation, and that full nodes can get connected to validators for transaction forwarding and secondary raptorcast group participation.


## Security assumptions
On startup, the node ids of the active validator set is provided out of band which is assumed to be correct. The name records of the active validator set is then discoverable through peer discovery, therefore unlike Ethereum, anonymity of validators' name records is not a design goal.

Name record poisoning, i.e. someone claiming a socket address that it doesn't have access to is possible if the adversary is able to carry out a man in the middle attack during the ping pong round trip.
