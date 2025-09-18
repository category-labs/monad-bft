## Overview

The spec supports a peer discovery algorithm. A node can specify the desired minimum active connections and maximum active connections, where the node will actively look for new peers when below the minimum active connections, and stop accepting connections when above the maximum active connections.

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
  local_name_record: MonadNameRecord,
}
```

### Pong

---

Response to ping message. Sender may update peer name record if the attached name record is more recent. ping_id must match the request

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

- A ping pong round trip has to be completed before a node will add a peer's name record into its routing table
- X receives a new name record signed by Y that it has not seen (this can happen when X receives a name record from a ping message or from a peer lookup response)
- X sends a **Ping** to Y, also advertising its own name record
- Y responds with **Pong**
- X knows that Y is alive and inserts the name record into its routing table
- If Y does not respond with a pong, X retries for a few times before dropping the name record

### discover(target)

- X sends a **PeerLookupRequest** message to a peer it knows when the number of peers is below minimum active connections
- Server responds with
    - target’s latest name record if known to the server
    - empty if unknown
    - (optional) a sample of known peers if target is unknown and if server choose to honor a set open_discovery bit. This is necessary to bootstrap nodes, but vulnerable to amplification attack. We currently limit the number of peers in a response to 16

### bootstrapping

- A few bootstrap addresses is made known to new joining nodes. These can be normal nodes or specialized peer discovery servers that serves as the network entrypoint
    - If validator set is known to node, it can **discover(validator)**
    - If validator set is unknown, it **discover(random_node_id)**, hoping that server will respond with new nodes to query
- This process can be repeated to discover more nodes

### pruning

- Peer pruning is performed periodically and triggered by high watermark to keep peers manageable
- Nodes that have not participated in secondary raptorcast beyond a threshold are pruned
- Public full nodes are pruned if we're still above high watermark
- Currently validators for the current and next epoch, dedicated and prioritized full nodes are not pruned even if unresponsive or total number of peers is above high watermark
