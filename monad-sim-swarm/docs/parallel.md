# monad-sim-swarm — per-node network stacks for parallel runs (design)

**Status: implemented as described below, except the inbound hook
(deliveries go to the node directly).** `Swarm::build_parallel`
places nodes in domains (`Parallel::domains`, default one per node), and
`Wiring` assembles the stacks; see [design.md](design.md). The engine side (domains, lookahead, windows, determinism) is in
[`monad-sim/docs/parallel.md`](../../monad-sim/docs/parallel.md). The network
is defined at this level; `monad-sim` knows only processes, domains, and
lower bounds on causal links.

## Problem

Today a node sends by scheduling a zero-delay step on the single `Net`
process, and the net samples the model and schedules the delivery. The extra
step exists because of the lent-state model: a step only has `&mut` to its own
process, so a node cannot call into the network's state. It has to schedule a
step on it. That also kept the routing table, the model and the network RNG in
one place, so that `set_model` swaps them at once.

For parallel runs, that one process is a sequencing point twice over:

- every send is a step on it, which puts it on the critical path (measured
  `W / D` 2.2–6.5 as built against 19.5–151 without it, in the MCP scenarios at
  n = 16–128);
- the zero-delay step from a node to the net puts the net and every node in
  one domain, which leaves nothing to run in parallel.

## A network stack per node

Each node gets its own network stack: a process in the node's domain. The
node (or any of the node's components) sends with a zero-delay step on its
stack, which is local because it stays in the same domain. The stack samples
the model and schedules the delivery on the receiver's inbound side at
`now + delay`. That delivery is the only cross-domain schedule, and its delay
is at least the lookahead.

This keeps the network a separate component, as today, but per node:

- **Nodes made of several processes** (consensus, mempool, execution, …)
  share one stack, as a real host shares one network stack. The node is then a
  set of processes in one domain; nothing about the network forces a node to
  be a single process.
- **Simple nodes** keep the shape they have now: one node process plus its
  stack.
- The extra step per send (not per recipient: a broadcast is one step) stays.
  It is cheap at 2–3 µs and keeps the lent-state boundary between node and
  network.

The network splits into two parts:

- **A description, shared and read-only during a run** (`Arc`, `Send + Sync`):
  - the routing table (address → delivery thunk);
  - the latency model;
  - loss probability, time-bounded partitions, and `drop_if`;
  - egress rates and `size_of`.

  Every stack holds the same `Arc`, which is the global routing table
  available at every instance, without copying.
- **Per-stack state**:
  - egress `busy_until`;
  - the stack's RNG, seeded from `(net seed, address)`, so loss and jitter draws
    do not depend on grouping or on other senders' traffic.

Every condition in today's `Conditions` fits this split. Latency, partitions
and `drop_if` are pure functions of `(link, message, rng)`, and egress state
is per sender. The latency closure changes from `FnMut` to
`Fn + Send + Sync`; `with_latency` and `with_matrix` already satisfy that.

### Inbound side

Delivery schedules on the receiver's stack, or directly on the node when the
stack has nothing to do inbound. That is the place for receiver-side network
logic: downlink bandwidth, an ingress queue, or demultiplexing to several
components of a multi-process node. Inbound logic may only postpone the
hand-off, and it runs in the receiver's domain, so it does not affect the
lookahead. The first version delivers directly to the node and leaves the
inbound hook as structure for later.

### Seam change

Before:

```rust
fn wire(&mut self, me: Handle<Self>, net: Handle<Net<Addr, Msg>>);
ctx.schedule(net, ctx.now(), label, move |net, ctx| net.send(ctx, from, to, msg));
```

After: the handle points to the node's own stack, so node code does not change
beyond the type.

```rust
fn wire(&mut self, me: Handle<Self>, net: Handle<Net<Addr, Msg>>); // the node's own stack
ctx.schedule(net, ctx.now(), label, move |net, ctx| net.send(ctx, to, msg));
```

The declarative `Network` builder stays the configuration surface, unchanged
for scenario code. `Swarm::build` turns it into the shared description plus
one stack per node. Sequential runs use the same code, so there is one network
implementation.

## Network latency model vs. domain lookahead

The two are related but different, in meaning and in granularity:

- The **latency model** says what delay a message gets: a node × node or
  region matrix of typical one-way latencies, plus jitter, egress queueing and
  so on.
- The **lookahead** is a lower bound on any causal link between two
  *domains*, used only to synchronize safely. Its granularity is the domain
  placement, which is a scenario choice: one domain per node by default, or
  one per region, with several nodes per domain.

The lookahead is derived from the model:

```
L(a, b) = min over nodes i in a, j in b, i ≠ j, of min_delay(i, j)
```

`min_delay` is the network's lower bound per link, not its average:

- `reliable(d)`: `d`.
- `with_matrix(m, jitter)`: the base entry `m[i][j]`; jitter is a `Duration`,
  so it is never negative.
- `with_latency` / `with_link_latency`: unknown, so 0, unless the scenario
  declares `.min_latency(d)`, or a bound per link with
  `.link_min_latency(f)`. The stack checks every sample against the
  declared bound and panics on a violation.
- Egress only adds delay, and loss, partitions and `drop_if` only remove
  messages, so none of them lowers the bound.

Within a domain, the lookahead is 0 by definition, whatever the latencies
between its nodes. Between domains it must be positive, and the build fails
otherwise. A latency matrix should not use 0 on its diagonal: nodes in the same
region still have a positive one-way latency. With one domain per node, a 0
diagonal would be rejected; the scenario would have to place each region in one
domain. The topology presets (`RegionTable`, see [design.md](design.md))
have a positive diagonal.

## Mid-run changes

All of these happen between runs, when the driver owns every process.

- `set_network` builds a new description and installs it in every stack. As
  with `set_model` today, per-stack state starts fresh.
- `add_node` / `remove_node` replace the routing table (a new `Arc`) in every
  stack.
- Messages in flight to a removed node are dropped on arrival, as today.
- The lookahead is fixed at build. A model change that lowers `min_delay`
  below the built lookahead is rejected.

## Custom models

`Network::custom` needs an explicit sharding contract. The model is a shared
description that creates one per-stack part:

```rust
pub trait NetworkModel<Addr, Msg>: Send + Sync {
    fn sender(&self, from: &Addr) -> Box<dyn SenderModel<Addr, Msg>>;
    fn min_delay(&self, from: &Addr, to: &Addr) -> Duration { Duration::ZERO }
}

pub trait SenderModel<Addr, Msg>: Send {
    fn deliveries(&mut self, link: &Link<Addr>, message: &Msg, rng: &mut ChaChaRng)
        -> Vec<Duration>;
}
```

All mutable state lives in the `SenderModel`; anything shared is immutable.
The default `min_delay` of 0 is correct but sequential. Effects that depend on
several senders' traffic (shared links, switches, contention) need a component
in the network itself: its own process in its own domain, with lookahead from
the links on both sides. The engine supports that as is. It is out of scope
until a scenario needs it.

## Implementation (step 7 of the engine plan)

1. Split `Network` into the description (`NetworkModel`, assembled by
   `Wiring`) and the per-node `Net` with its `SenderModel`.
   Per-stack RNG, `min_delay`. Port the `monad-sim-swarm` tests, including
   those of the latency matrix and egress.
2. `Swarm`: one stack and one domain per node by default, an optional
   placement function `Addr → Domain`, and lookahead from `min_delay`.
3. Harness ports: the MCP node scenarios and `monad-mcp-cadence-sim` get the
   stack handle type, and `Box<dyn Component>` becomes `+ Send` (D1).
   Re-measure `W / D`.
