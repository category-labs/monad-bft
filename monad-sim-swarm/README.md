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

- **`Net<Addr, Msg>`** — the network as a simulation *process*: a routing
  table plus a swappable `NetworkModel`. A node sends by scheduling a step on
  the `Net` handle; the net samples the model and schedules the surviving
  deliveries on the recipients.
- **`Network`** — the declarative model builder:

  ```rust
  Network::reliable(delta)                    // constant latency, lossless
  Network::with_latency(distribution)         // per-message sampled latency
      .loss(p)                                // independent drop probability
      .partition(window, [group_a, group_b])  // time-bounded network split
      .drop_if(|link, msg| ...)               // predicate drop
  Network::custom(model)                      // a full NetworkModel impl
  ```

  A model returns the delays at which *copies* of a message are delivered: an
  empty result drops it, more than one duplicates it, and reordering needs no
  special handling — a smaller delay simply arrives first.
- **`Deliver` / `deliver_to`** — a *type-erased* per-node delivery thunk, so
  the network only ever names `Addr` and `Msg`. One net can therefore carry
  heterogeneous node process types (different protocol versions, a Byzantine
  node) as long as they share the wire type.
- **`Swarm<N>` + `SimClient`** — the minimal harness: implement `SimClient`
  for your node type (a `receive` handler, plus an optional `wire` hook that
  hands the node its own and the network's handles), and `Swarm::build` spawns
  the nodes and the net, wires them together, and forwards run control to the
  engine.
- **`Timers<K>`** — at most one armed timeout per key, re-arming resets — the
  pacemaker pattern every node needs.

## Example

```rust
use monad_sim::{time::millis, Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{Net, Network, SimClient, Swarm};

struct Counter { addr: u32, net: Option<Handle<Net<u32, u64>>>, got: u32 }

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

let nodes = (0..3).map(|a| (a, Counter { addr: a, net: None, got: 0 }));
let mut swarm = Swarm::build(0, Network::reliable(millis(5)), nodes);

// Kick node 0: broadcast once. Sending = scheduling a step on the net.
let h = swarm.handle(&0).unwrap();
swarm.sim().schedule(h, Time(0), StepLabel::source("start"), |c, ctx| {
    let (net, from) = (c.net.unwrap(), c.addr);
    ctx.schedule(net, ctx.now(), StepLabel::source("route"), move |net, ctx| {
        net.broadcast(ctx, from, 7)
    });
});

swarm.run_to_completion();
for a in 0..3 {
    assert_eq!(swarm.with_node(&a, |c| c.got), 1); // everyone, one hop later
}
```

Determinism comes from the engine; the net additionally keeps its own
independently-seeded RNG, so network randomness is reproducible on its own.

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
the swarm broadcast, and predicate drops.
