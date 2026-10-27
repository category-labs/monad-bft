# monad-sim-swarm

Generic network + swarm infrastructure on top of the
[`monad-sim`](../monad-sim) discrete-event engine. Still protocol-agnostic:
everything here is parameterized only by a node address type `Addr` and a wire
message type `Msg`, with no Monad or consensus knowledge. Protocol-specific
harnesses sit on top — the Monad BFT mock-swarm in this repository is one
consumer. See [`docs/design.md`](docs/design.md) for the design, and
[`monad-sim/docs/design.md`](../monad-sim/docs/design.md) for the underlying
engine.

## Pieces

- **`Net<Addr, Msg>`** — a node's network stack, a simulation *process*. A
  node sends by scheduling a step on its `Net`; the stack samples the model and
  schedules the surviving deliveries on the recipients. All stacks share the
  routing table and a swappable `NetworkModel`; each has its own uplink state
  and random generator. `Wiring` assembles them.
- **`Network`** — the declarative model builder:

  ```rust
  Network::reliable(delta)                    // constant latency, lossless
  Network::with_latency(distribution)         // per-message sampled latency
  Network::with_link_latency(|link, rng| ..)  // latency per sender/receiver
  Network::with_matrix(matrix, jitter)        // region latency matrix + jitter
      .egress(bits_per_sec, |msg| size)       // serial per-sender uplink
      .egress_rate(addr, bits_per_sec)        // per-sender uplink override
      .loss(p)                                // independent drop probability
      .partition(window, [group_a, group_b])  // time-bounded network split
      .drop_if(|link, msg| ...)               // predicate drop
      .min_latency(bound)                     // declared lower bound (sampled latency)
  Network::custom(model)                      // a full NetworkModel impl
  ```

  A model returns the delays at which *copies* of a message are delivered: an
  empty result drops it, more than one duplicates it, and reordering needs no
  special handling — a smaller delay simply arrives first.
- **`RegionTable` / `Placer`** — predefined latency topologies (see
  [Topologies](#topologies)).
- **`Deliver` / `deliver_to`** — a *type-erased* per-node delivery thunk, so
  the network only ever names `Addr` and `Msg`. One net can therefore carry
  heterogeneous node process types (different protocol versions, a Byzantine
  node) as long as they share the wire type.
- **`Swarm<N>` + `SimClient`** — the minimal harness: implement `SimClient`
  for your node type (a `receive` handler, plus an optional `wire` hook that
  hands the node its own and its stack's handles), and `Swarm::build` spawns
  the nodes and their stacks, wires them together, and forwards run control to
  the engine. `Swarm::build_parallel` places the nodes in domains (one per
  node, or by `Parallel::domains`) and runs them on worker threads, with the
  lookahead taken from the network's lower bounds on link delay; results are
  the same for every thread count.
- **`Timers<K>`** — at most one armed timeout per key, re-arming resets — the
  pacemaker pattern every node needs.

## Example

```rust
use monad_sim::{time::millis, Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{Net, Network, SimClient, Swarm};

struct Counter { net: Option<Handle<Net<u32, u64>>>, got: u32 }

impl SimClient for Counter {
    type Addr = u32;
    type Message = u64;
    fn wire(&mut self, _me: Handle<Self>, net: Handle<Net<u32, u64>>) {
        self.net = Some(net);
    }
    fn receive(&mut self, _from: u32, _msg: u64, _ctx: &mut Ctx) {
        self.got += 1;
    }
}

let nodes = (0..3).map(|a| (a, Counter { net: None, got: 0 }));
let mut swarm = Swarm::build(0, Network::reliable(millis(5)), nodes);

// Kick node 0: broadcast once. Sending = scheduling a step on its stack.
let h = swarm.handle(&0).unwrap();
swarm.sim().schedule(h, Time(0), StepLabel::source("start"), |c, ctx| {
    let net = c.net.unwrap();
    ctx.schedule(net, ctx.now(), StepLabel::source("route"), move |net, ctx| {
        net.broadcast(ctx, 7)
    });
});

swarm.run_to_completion();
for a in 0..3 {
    assert_eq!(swarm.with_node(&a, |c| c.got), 1); // everyone, one hop later
}
```

Determinism comes from the engine; each stack additionally keeps its own
generator (one network seed, a stream per address), so network randomness is
reproducible on its own and independent of how nodes are grouped.

## Topologies

A `RegionTable` holds one-way latencies between named regions, a weight per
region, and its provenance (name, source, date). Every entry, including the
diagonal, is positive. Presets:

| preset | regions | source |
|---|---|---|
| `RegionTable::public_internet()` | FRA, AMS, LON, NYC, CHI, LAX, SGP, TYO, SYD, SAO | median RTTs from the [WonderNetwork](https://wondernetwork.com/pings) public dataset of 2020-07-19/20, halved |
| `RegionTable::single_region(local)` | 1 | synthetic |
| `RegionTable::two_clusters(near, far)` | 2 | synthetic |
| `RegionTable::ring(n, local, hop)` | n | synthetic |
| `RegionTable::star(leaves, local, spoke)` | hub + leaves | synthetic |

`RegionTable::new` and `RegionTable::from_rtt` build your own, for example
from a measured matrix of a specific deployment; such data belongs with the
code that uses it, not in this crate.

A `Placer` spreads nodes over a table's regions by weight, with fixed
(apportioned) or sampled counts, optionally with a per-node last mile,
persistent per-pair route spread and asymmetry, and stake weights. It draws
only from the RNG it is given:

```rust
let table = RegionTable::public_internet();
let placement = Placer::new(&table)
    .regions(Regions::Apportioned)          // or Regions::Sampled
    .last_mile(micros(300)..=millis(4))
    .route_spread(0.95..=1.10)              // per pair, both directions
    .asymmetry(0.04)                        // per pair, 1 + a there, 1 - a back
    .stake(Stake::LogNormal { sigma: 1.0 })
    .place(0..100u32, &mut ChaChaRng::seed_from_u64(seed));
let network = Network::with_matrix(placement.matrix(), jitter);
// placement.nodes(): region, last mile and stake share per address
```

## Inspection & lifecycle

`with_node` / `with_node_mut` inspect or mutate a node between steps;
`add_node` / `remove_node` support restart scenarios (in-flight messages to a
removed node are dropped); `set_network` swaps the model mid-run; and
`swarm.sim()` exposes the raw engine for kickoff steps and custom run loops.

## Running

```sh
cargo test -p monad-sim-swarm
```

`tests/swarm.rs` demonstrates heterogeneous node types sharing one network,
the swarm broadcast, and predicate drops. `tests/network.rs` covers the
latency matrix and egress, and `tests/topology.rs` the presets and the placer.
