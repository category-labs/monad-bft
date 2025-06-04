## Overview

The spec supports a simple peer discovery algorithm. Peers maintain connections by sending periodic pings to each other. A node can specify the desired minimum active connections and maximum active connections, where the node will actively look for new peers or prune unresponsive peers depending on size of current peer list

## Message types

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
}
```

### Ping

---

Checks if peers is alive, and advertises local name record. Sender must have receiver's name record in order to send a ping

```rust
struct Ping {
  id: u32,
  local_name_record: Option<MonadNameRecord>,
}
```

### Pong

---

Response to ping message. Sender may update peer name record if one is attached to ping message. ping_id must match the request

```rust
struct Pong {
  ping_id: u32,
  local_record_seq: u64,
}
```

### PeerLookupRequest

---

Request to look up name record of target peer. Client can request more peers returned by setting `open_discovery`, when `target` is not found at the server. Server can choose to ignore `open_discovery`

```rust
struct PeerLookupRequest {
  lookup_id: u32,
  target: NodeId,
  open_discovery: bool,
}
```

### PeerLookupResponse

---

Response to PeerLookupRequest message. Server should respond with target’s name record if it’s known locally. Otherwise it can either send an empty response, or refer the client to other endpoints. lookup_id must match the request

```rust
struct PeerLookupResponse {
  lookup_id: u32,
  target: NodeId,
  name_records: Vec<MonadNameRecord>,
}
```

## Operations

### connect

- A is assumed to have B’s name record
- A sends a **Ping** to B, optionally advertising its own name record
- B responds with **Pong**
    - If advertised local_name_record is newer (higher sequence number) than B’s local record, B should update A's name record
- A knows that B is alive

### discover(target)

- A sends a **PeerLookupRequest** message to a peer it knows when the number of peers is below minimum active connections
- Server responds with
    - target’s latest name record if known to the server
    - empty if unknown
    - (optional) a sample of known peers if target is unknown and if server choose to honor a set open_discovery bit. This is necessary to bootstrap nodes, but vulnerable to amplification attack. We currently limit the number of peers in a response to 16

### liveness check

- Occasionally, nodes checks if peer is alive by **connecting** to its peer
- If peer doesn’t respond to consecutive pings, it’s considered offline and removed from peer list

### bootstrapping

- A few bootstrap addresses is made known to new joining nodes. These can be normal nodes or specialized peer discovery servers that serves as the network entrypoint
    - If validator set is known to node, it can **discover(validator)**
    - If validator set is unknown, it **discover(random_node_id)**, hoping that server will respond with new nodes to query
- This process can be repeated to discover more nodes

### pruning

- Peer pruning is performed periodically/triggered by high watermark to keep peers manageable
- Nodes that do not respond to consecutive pings beyond a threshold are pruned
- Random full nodes are pruned if we're still above high watermark
- Currently validators for the current and next epoch, and dedicated full nodes are not pruned even if unresponsive or total number of peers is above high watermark
