# monad-sim-swarm — design

The protocol-agnostic network + swarm layer between the
[`monad-sim`](../../monad-sim) engine and protocol-specific simulation
harnesses. This document covers the design of its pieces and the seams they
expose; the [README](../README.md) is the quick tour, and
[`monad-sim/docs/design.md`](../../monad-sim/docs/design.md) describes the
underlying engine.

## Position in the stack

Everything in this crate is parameterized only by a node address type `Addr`
and a wire message type `Msg`; it carries no Monad or consensus knowledge
(dependencies: `monad-sim`, `rand`, `rand_chacha`). Protocol-specific
harnesses layer on top — the Monad BFT mock-swarm in this repository is one
consumer, other protocols build on it elsewhere, and further harnesses are
expected. Anything that requires knowing a protocol's events, rounds, or
assertions belongs in those harnesses, not here.

## The network as a process

The network is just another `monad-sim` process — no dedicated framework
concept. `Net<Addr, Msg>`'s state is a routing table plus a `NetworkModel`; a
node sends by scheduling a routing step on the `Net` handle, and the net
samples the model and schedules the surviving deliveries on the recipients.
This makes the network model modular and swappable, including mid-run
(`set_model`).

A `NetworkModel` answers one question per message: at which delays are copies
of it delivered? An empty answer drops the message, more than one duplicates
it, and reordering needs no special handling — a smaller delay simply arrives
first. The declarative `Network` builder covers the common cases (constant or
sampled latency, independent loss, time-bounded partitions, predicate drops);
`Network::custom` accepts a full `NetworkModel` implementation.

The net keeps its own ChaCha RNG, seeded independently of the engine's
scheduling RNG, so network randomness is reproducible on its own.

### Topology and uplinks

Latency can depend on the link. `Network::with_link_latency` takes a function
of the link (sender, receiver, send time) and the RNG.
`Network::with_matrix` builds on it with a `LatencyMatrix`: one-way base
latencies between regions, a placement of addresses into regions, and a
jitter distribution added per message. Rows are senders, so asymmetric paths
are allowed. One region per node gives a full node-to-node matrix; a
geographic topology places many nodes per region and keeps the matrix small.

`Network::egress` gives every sender a serial uplink of a given bit rate, with
per-sender overrides through `egress_rate`. The caller supplies the size of a
message. A message leaves after everything the same sender sent before it and
occupies the uplink for its size divided by the rate; its delivery delay is
that wait and transmission time plus the link latency.

- A message the model then drops (loss, partition, predicate) was still
  transmitted, so it still occupies the uplink.
- A node's messages to itself bypass the uplink.
- Unicast fan-out serializes, as on a real uplink: a broadcast to n nodes
  takes n transmission times.

Sequencing is per message. That fits transports whose messages each fit one
datagram. Splitting a large message into frames that interleave with other
traffic is out of scope. Scheduling *policy*, such as priority queues, belongs
to the node: a node decides in which order it hands messages to the net, and
the net transmits them in that order.

### Topology presets

`topology.rs` builds `LatencyMatrix` values; the network model itself does
not know about it.

A `RegionTable` is a square table of one-way latencies between named
regions, plus a weight per region and a `Provenance`: a name, the source of
the numbers, and the date they were measured or entered. Every table records
where its numbers come from, and says so when they are approximations.

Every entry must be positive, the diagonal included: nodes in the same
region still have a positive one-way latency. `with_matrix` adds jitter as a
`Duration`, so never negative, which makes each entry a true per-link lower
bound. A scheduler that needs a lower bound on link delays can take
`min_latency()` or the entries themselves. A measured RTT matrix usually has
a zero diagonal; the caller replaces it before building a table.

The crate ships one approximate geographic table and a few synthetic shapes
for tests (one region, two clusters, ring, star). Measured matrices of
specific deployments are project data: the consumer that owns them builds a
`RegionTable` with `new` or `from_rtt`, or loads one from its own files.

A `Placer` turns a table into a node-level topology:

- Region counts are apportioned to the weights by largest remainder (ties go
  to the lower region index), so 100 nodes over weights summing to 100 match
  them exactly. A shuffle then decides which address goes where.
- An optional last mile per node, uniform over a range, is added at both
  ends of every link. Without it the matrix stays region-level and small;
  with it, it has one row per node.
- Stake is equal, or log-normal and normalized to sum to 1.

Randomness comes only from the RNG passed to `place`, drawn in a fixed order
(shuffle, last miles, stakes), so a seed reproduces the placement. The
placer holds no state between calls, and the matrix it yields is immutable
data.

## The type-erased delivery boundary

`Net` routes to nodes through `Deliver<Addr, Msg>` thunks (built by
`deliver_to`): each thunk captures a recipient's *concrete* `Handle<N>` and
monomorphizes the `ctx.schedule` call, so the network itself only ever names
`Addr` and `Msg`. A single network can therefore carry *heterogeneous* node
process types as long as they share the wire type — the seam for simulating
different protocol versions, or a Byzantine node, in one swarm.

## The swarm harness

`Swarm<N>` is a minimal builder over the `SimClient` seam: spawn a set of
nodes plus a `Net`, wire them together (each node learns its own handle and
the net handle once, via `SimClient::wire`), and forward run control to the
engine. Inspection and lifecycle go through the harness: `with_node` /
`with_node_mut` between steps, `add_node` / `remove_node` for restart
scenarios (in-flight messages to a removed node are dropped on arrival), and
`set_network` for mid-run reconfiguration.

The harness is deliberately homogeneous for now (one node type `N`); the
`Net` delivery boundary is already type-erased, so a future heterogeneous
swarm builder reuses the network unchanged.

Protocol-specific inspection (finalized blocks, current round, agreement
assertions) deliberately does *not* live here; it belongs to each protocol's
harness, layered over `Swarm::with_node`.

## Timers

`Timers<K>` is the re-arming timer set every protocol node needs for
pacemaker-style timeouts: at most one armed timer per key, and arming a key
cancels the previous one. It holds the engine's `CancelToken`s; the caller
schedules the step and hands the token to `arm`. `reset` cancels a pending
timer; `clear` forgets one that has just fired.

## Future work

- **A heterogeneous swarm builder** (mixed node process types over one `Net`).
  The delivery boundary already supports it; only the builder is missing.
