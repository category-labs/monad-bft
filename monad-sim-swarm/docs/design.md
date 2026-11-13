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

## The network as processes

The network is made of ordinary `monad-sim` processes — no dedicated framework
concept. Each node has its own network stack, a `Net<Addr, Msg>` process. A
node sends by scheduling a step on its stack (a step can only reach its own
process's state, so sending is a scheduled step); the stack samples the model
and schedules the surviving deliveries on the recipients.

All stacks share one read-only description: the routing table and the
`NetworkModel`. What changes as messages are sent is per stack: the sender's
part of the model (its uplink queue) and its random generator. That keeps the
stacks independent of each other, so they run in parallel when their nodes'
domains do (see [parallel.md](parallel.md)).

A `NetworkModel` is shared by all stacks. It creates each sender's part, a
`SenderModel`, which answers one question per message: at which delays are
copies of it delivered? An empty answer drops the message, more than one
duplicates it, and reordering needs no special handling — a smaller delay
simply arrives first. The model also declares a lower bound on each link's
delay (`min_delay`), which a parallel swarm uses as its lookahead. The
declarative `Network` builder covers the common cases (constant or sampled
latency, independent loss, time-bounded partitions, predicate drops, uplinks);
`Network::custom` accepts a full `NetworkModel` implementation.

`Wiring` assembles a network: register each node's delivery, then spawn each
node's stack from it. After a change (a registration, a new model) it brings
the live stacks up to date, which is how the model stays swappable mid-run.
`Swarm` does this for a homogeneous swarm; tests build heterogeneous ones by
hand.

Each stack has its own ChaCha generator: one seed for the network, and a
stream per address in order of first registration. Network randomness is
therefore reproducible on its own, and does not depend on how nodes are
grouped for execution or on what other senders draw.

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

Each lower bound ignores the uplink: queueing only adds delay. `reliable`
and `with_matrix` know their bounds; a sampled latency has bound 0 unless the
scenario declares one with `Network::min_latency` (one bound for every
link) or `Network::link_min_latency` (a bound per link, for example a latency
matrix under a time-varying delay), which every sample is checked against.

Sequencing is per message. That fits transports whose messages each fit one
datagram. Splitting a large message into frames that interleave with other
traffic is out of scope.

#### Priority classes

`Network::priority(class_of, depth)` turns the uplink into a transmit ring of
`depth` messages fed by one queue per class (0 the most urgent), as on a NIC
whose TX ring is fed by priority queues:

- class 0 goes on the wire at once, behind what is on it already;
- any other class goes on only while fewer than `depth` messages are on the
  wire, after every waiting message of a more urgent class and the older ones
  of its own.

A class-0 message therefore waits behind at most `depth` messages of other
classes.

Without priority, a message's departure depends only on the bytes its sender
handed over before it. It is known when the message is sent, and the stack
adds the transmission and the link latency into one delay. With priority, the
departure of a waiting message also depends on more urgent messages not sent
yet. So the sender's part of the model keeps waiting messages
(`SenderModel::submit`), and the stack wakes itself when the ring has room
(`wake_at`, `release`). A waiting message's fate (loss, latency) is decided
when it goes on the wire.

The cost is one local step per wake-up. A class-0 message never waits, so
bursts of it cost what they cost without priority. A node only states each
message's class (through `class_of` on its message type); it needs no model
of the uplink's timing. The send port is the node's own, but the network
models it.

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

The crate ships one geographic table, derived from a public ping dataset (see
its rustdoc for the sources and method), and a few synthetic shapes
for tests (one region, two clusters, ring, star). Measured matrices of
specific deployments are project data: the consumer that owns them builds a
`RegionTable` with `new` or `from_rtt`, or loads one from its own files.

A `Placer` turns a table into a node-level topology:

- By default (`Regions::Apportioned`), region counts are apportioned to the
  weights by largest remainder (ties go to the lower region index), so 100
  nodes over weights summing to 100 match them exactly. A shuffle then
  decides which address goes where. Every seed has the same geographic
  composition, which suits tests and comparisons between protocol variants.
- `Regions::Sampled` draws each node's region independently by weight, so
  the seed varies the composition too: the right choice for Monte Carlo over
  random node sets, and for comparing with models that sample the same way.
  Small regions may end up empty under either method.
- An optional last mile per node, uniform over a range, is added at both
  ends of every link.
- An optional route spread scales the region latency of each node pair by a
  factor drawn once, the same in both directions. An optional asymmetry
  scales one direction by `1 + a` and the other by `1 - a`, keeping the
  round trip. Both are persistent per pair, unlike jitter, which is drawn
  per message: without them, all nodes in two regions see the same base
  latency, and arrival times cluster by region. Asymmetry also matters to
  anything that estimates one-way delay as half the round trip, such as
  clock-offset estimation, since an asymmetric path looks like an offset.
  Factors stay positive (the spread range is positive and `|a| < 1`), so
  entries remain positive lower bounds.
- Stake is equal, or log-normal and normalized to sum to 1.

Without last mile, spread and asymmetry, the matrix stays region-level and
small; with any of them, it has one row per node.

Randomness comes only from the RNG passed to `place`, drawn in a fixed order
(regions, last miles, stakes, then per pair spread and asymmetry), so a seed
reproduces the placement, and turning on the route options does not change
the earlier draws. The
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

- **An inbound hook** in the receiver's stack (downlink bandwidth, ingress
  queues, demultiplexing to several node processes); see
  [parallel.md](parallel.md).

- **A heterogeneous swarm builder** (mixed node process types over one `Net`).
  The delivery boundary already supports it; only the builder is missing.
