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
