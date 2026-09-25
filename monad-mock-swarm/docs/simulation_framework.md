# The Monad BFT simulation harness

Deterministic, single-threaded simulation of Monad BFT consensus. The
simulation drives the **real** production consensus code — `MonadState::update`
plus the `Executor` / `Command` / `MonadEvent` contract — so tests exercise the
same logic that runs in production; only the environment (clock, network,
ledger / mempool / state-sync) is mocked.

The harness is the Monad-specific top layer of a generic simulation stack. The
generic layers are separate crates with their own documentation:

- [`monad-sim/docs/design.md`](../../monad-sim/docs/design.md) — the
  discrete-event engine: the step/process model, determinism, the concurrency
  metric, and the engine rationale.
- [`monad-sim-swarm/docs/design.md`](../../monad-sim-swarm/docs/design.md) —
  the protocol-agnostic network + swarm layer.

This document covers the integration: how a Monad node runs on that stack,
what the simulation guarantees, what it does and does not model, and the
harness/test surface. The trade-off analysis behind the integration choices is
collected in [Design notes & rationale](#design-notes--rationale).

## Architecture

```
+---------------------------------------------------------------------+
| GENERIC engine                                       (monad-sim)     |
|   Time, scheduler + queue, process handles + lent-state steps        |
|   (Handle<S> / Ctx), run control, RNG/distributions, metrics.        |
|   ZERO knowledge of Monad / consensus / MonadEvent / Command.        |
+---------------------------------------------------------------------+
        ^  Handle<S> / Ctx scheduler API
+---------------------------------------------------------------------+
| GENERIC network + swarm layer                  (monad-sim-swarm)     |
|   Net<Addr, Msg> network process + declarative Network model;        |
|   type-erased delivery (Deliver / deliver_to); SimClient seam;       |
|   re-arming Timers. Parameterized only by Addr / Msg.                |
+---------------------------------------------------------------------+
        ^  Net / SimClient / deliver_to
+---------------------------------------------------------------------+
| MONAD-SPECIFIC sim adapters & mocks        (monad-mock-swarm::sim)   |
|   SimNode = framework-owned node state; in-process Command dispatch; |
|   mock/prod executors driven via exec()+drain; SimNet = the generic  |
|   Net at (NodeId, TransportMessage); SimSwarm / SimVerifier.         |
+---------------------------------------------------------------------+
        ^  the production contract:  Executor + Command + MonadEvent
+---------------------------------------------------------------------+
| SHARED production logic                              (unchanged)     |
|   MonadState::update, Command/MonadEvent, split_commands,            |
|   consensus / blocksync / mempool / statesync logic.                 |
+---------------------------------------------------------------------+
```

In the engine, a simulation is a time-ordered queue of *steps* over
framework-owned *processes*: any `S: 'static` spawned into the `Simulation`,
addressed by a typed `Handle<S>`. A step scheduled against a handle is lent
`&mut S` exclusively while it runs, plus a `Ctx` that can read time, sample
randomness, and schedule further steps — but cannot reach any other process's
state, so cross-process interaction is always "schedule a step on that
handle". `monad-sim-swarm` adds the network as just another process, routing
to nodes through type-erased delivery thunks. Concrete Monad types
(`MonadEvent`, `Command`) appear only inside this crate's closures, never in
a generic-layer signature.

### A Monad node on the scheduler (`sim.rs`)

Each node is a `SimNode<S>` process (`S: SwarmRelation`). A scheduled step
feeds one event to `state.update`, then dispatches the resulting commands
in-process:

- **Router / network** → outbound messages go to `SimNet<S>` — the generic
  `monad_sim_swarm::Net` instantiated at this relation's `NodeId` and
  `TransportMessage` — which applies the network model and schedules delivery
  steps on the recipients. `SimNode` plugs into the network via the
  `monad_sim_swarm::SimClient` seam, and the crate's `Network<S>` is a thin
  `SwarmRelation`-flavored facade over the generic declarative builder:

  ```rust
  Network::reliable(delta)                       // constant latency, lossless
  Network::with_latency(distribution)            // per-message sampled latency
      .loss(p)                                    // independent drop probability
      .partition(window, [group_a, group_b])      // time-bounded network split
      .drop_if(|link, msg| ...)                   // predicate drop (sees link.now)
  Network::custom(model)                          // a full NetworkModel impl
  ```

  An empty set of delivery delays drops a message, more than one duplicates
  it, and reordering needs no special handling (a smaller delay simply
  arrives first).
- **Timers** → `TimerCommand::Schedule` arms a `CancelToken`;
  `ScheduleReset` cancels it.
- **Ledger / TxPool / ValSet / StateSync / Loopback** → the existing
  executors (production `LoopbackExecutor`, or the mocks shared with
  `monad-testground` / `monad-eth-ledger`) are reused unmodified: call
  `exec()`, then drain their buffered events (`pop` / `next`) and re-inject
  each as a zero-delay step. Their `ready()` / `Stream` / `pop` surface is
  used purely as a drain, never polled.

Routing internal reactions through the scheduler as zero-delay steps (rather
than draining them synchronously) is what lets a same-tick network delivery
interleave between a node's internal reactions, and is the hook a future
compute-cost model would use (see [Future work](#future-work) and
[Event ordering & interleaving](#event-ordering--interleaving)).

### The production contract

`state.update(event) -> Vec<Command>`, `Command::split_commands`, and the
`Executor` trait are used exactly as production defines them; this is what
guarantees the simulation exercises production logic. The simulation imposes
no changes on production code — the one trait-level accommodation is
`SwarmRelation::TransportMessage: Clone` (needed for message duplication) —
and genuine production executors (e.g. `LoopbackExecutor`, the same struct
`monad-node` runs) run unmodified inside the simulation.

### The harness

`SimSwarm<S>` wraps a `Simulation` and its node handles, exposing what tests
need:

- construction — `SimSwarm::from_builders(seed, builders, network)` (generic
  over any `SwarmRelation`); `build_swarm` / `build_swarm_with` are
  `NoSerSwarm` conveniences.
- bounded run control — `run_until`, `run_until_blocks` (all nodes),
  `run_until_any_blocks` (some node), `run_until_round`.
- inspection / mutation between steps — `with_node`, `with_node_mut`,
  `finalized_blocks`, `current_rounds`, `send_transaction`,
  `assert_agreement`.
- node lifecycle — `remove_node` / `add_node` (restart-from-forkpoint
  scenarios), `set_network` (mid-run reconfiguration).

`SimVerifier` (`sim_verify.rs`) accumulates tick / ledger-length /
per-node-metric expectations and checks them through `SimSwarm`'s public
surface; metric selectors are written with the `sim_metric!` macro.

## Determinism & event ordering

The engine orders steps by the total key `(time, tie_break, priority, seq)`
(see the [engine design doc](../../monad-sim/docs/design.md)). The
integration maps it to production behaviour:

- **`priority`** classes mirror production's `ParentExecutor::poll_next`
  order, so a node drains ready internal work before it services a same-tick
  network message — production's anti-starvation direction.
- The default `TieBreak::Fifo` is fully reproducible, which is why the test
  suite can assert *exact* ticks with no tolerance window.
  `TieBreak::Randomized` (a seeded same-tick permutation, for
  order-dependence fuzzing) is implemented in the engine but not yet wired
  through the `SimSwarm` harness — see [Future work](#future-work).

Randomized scenario exploration is Monte-Carlo over the simulation seed:
seeded latency jitter in the network model shifts events onto different
ticks. The fidelity trade-offs are analysed in
[Event ordering & interleaving](#event-ordering--interleaving).

## What the simulation does (and does not) model

Stated exactly so results are not over-read:

- **Network behaviour is modeled**: latency (constant or sampled), loss,
  partitions, duplication, and reordering, per the `Network` model.
- **No simulated-time latency for local processing.** Component executors
  emit at the current tick; only timers, the timestamper period, and network
  latency carry simulated-time delays.
- **No backpressure.** Every component buffer is unbounded; `ready()` is
  never false due to a full queue.
- **Deferred execution/finalization *is* modeled, but as a fixed block
  count, not a duration** (`execution_delay` / `state_root_delay` /
  `finalization_delay` are `SeqNum`s). A configured, deterministic,
  structural execution lag is in scope (and its consequences, e.g. the
  statesync threshold), but emergent, *compute-induced* slowness (CPU/DB/GC)
  is not.

So the simulation produces "a node falls behind / triggers blocksync / times
out" from network-induced lag or a configured block-count offset, never from
compute. Growing into a compute-cost model is adapter-only: internal
reactions already flow through the scheduler, so a per-operation cost hook
would schedule them at `now + cost`; bounded queues plus a feedback path
would add backpressure, which then *emerges* from per-node cost plus the
network model. No engine rewrite would be needed.

## Concurrency of a simulated swarm

The engine's deterministic concurrency metric (critical path `D`, total
steps `W`, `parallelism() = W / D` — see the
[engine design doc](../../monad-sim/docs/design.md)), measured on the
happy-path `NoSerSwarm` (reliable 1 ms network, 100 committed blocks):

| nodes | steps `W` | critical path `D` | parallelism `W/D` |
|------:|----------:|------------------:|:-----------------:|
| 2     | 2,836     | 1,470             | 1.93              |
| 4     | 5,608     | 2,196             | 2.55              |
| 16    | 22,710    | 7,951             | 2.86              |
| 64    | 99,750    | 34,463            | 2.89              |

Available parallelism rises then **plateaus around ~2.9× by 16 nodes** and
does not grow with network size: both `W` and `D` scale ≈ linearly in N
(each round the leader handles O(N) votes as a serial chain — the fan-in
spine — so per-round depth ≈ O(N)). This is why *analyzing* a simulated
system's concurrency is currently more valuable than parallelizing a single
run; see
[Concurrency, and why not to parallelize yet](#concurrency-and-why-not-to-parallelize-yet).

## Test suite

The suite lives in `tests/sim_*.rs`, with shared helpers in
`tests/sim_common` (the `NoSerConfig` builder and reusable custom network
models) and `tests/eth_swarm_common` (the Eth relation and its `SimSwarm`
builders). Coverage: two-node and multi-node consensus + agreement; BLS
signature collection; message delays and random latency; many-node (40)
swarms; metrics; Eth execution with real transactions; nonces / NEC; reserve
balance; epoch validator switching and boundary handling; blocksync
(blackout / delay / timeout recovery); message-reordering recovery;
forkpoint restart/recovery.

Scenarios are expressed *declaratively*: a blackout is a time-bounded
`Network::partition`, message filtering a `drop_if`, and
restart-from-forkpoint uses the `remove_node` / `add_node` lifecycle API.

`tests/sim_epoch_missing_valset.rs` is a regression gate (`#[should_panic]`)
for an open production `todo!()` reachable at an epoch boundary under
outage; it turns red when the production case is handled.

## Legacy engine

The crate still contains the previous simulation engine (`mock`,
`mock_swarm`, `node`, `verifier`, `swarm`, `transformer`, `terminator`) and
its tests; it is deprecated and slated for removal. While it remains,
`benches/sim_engine.rs` compares engine throughput (criterion, 100 committed
blocks, build excluded, both engines routing internal events through their
scheduler):

| nodes | legacy serial | new serial | new vs legacy |
|------:|--------------:|-----------:|:-------------:|
| 4     | 17.0 ms       | 14.1 ms    | 1.21×         |
| 16    | 112.8 ms      | 81.2 ms    | 1.39×         |
| 40    | 488.9 ms      | 326.8 ms   | 1.50×         |
| 64    | 1122.7 ms     | 723.0 ms   | 1.55×         |

Most of the margin comes from the engine's `O(log E)` heap replacing the
legacy engine's nested `peek` scans; see
[Performance attribution](#performance-attribution). Before the legacy
engine is deleted, the per-event `MonadEvent` RLP/serde round-trips embedded
in its `two_nodes_bls` / `nonces` tests (engine-independent serialization
coverage) need extracting into standalone serialization tests.

## Future work

Engine-side items — per-process local clocks, the offline concurrency
analyzer, parallel execution of a single run — are tracked in the
[engine design doc](../../monad-sim/docs/design.md) ("Future work").
Integration-side:

- **Wire `TieBreak::Randomized` through the `SimSwarm` harness**, then add a
  Monte-Carlo-over-seed test asserting agreement + liveness on a low-latency
  cluster (and ideally that both orders of a same-instant
  internal-vs-external race are reached). This turns the same-tick
  permutation capability into actual bug-finding leverage.
- **Compute-cost / backpressure model** (see
  [fidelity](#what-the-simulation-does-and-does-not-model)); both are
  adapter-level additions with existing injection points.
- **Timestamp drift.** A drifted-clock scenario needs the engine's
  per-process local clocks; blocked on that.
- **Mock relocation (optional, separable).** Moving the sim-only mocks out
  of the production crates (`monad-updaters`, `monad-router-scheduler`) into
  a test-support crate. More entangled than it looks — `monad-testground`
  and `monad-eth-ledger` consume the mocks / the `Mockable*` contract.
- **Port other swarm/sim consumers.** `monad-debugger`, `monad-twins`,
  `monad-peer-disc-swarm`, and `monad-randomized-tests` are separate one-off
  drivers today; each could model its components as processes over
  `monad-sim`. The `monad-debugger` port must preserve its GraphQL schema
  for the external UI.

---

## Design notes & rationale

Detail behind the integration choices. Not needed to use the harness; kept
for whoever revisits the design. Generic-engine rationale (the lent-state
model, the single-threaded concurrency stance) lives in the
[engine design doc](../../monad-sim/docs/design.md).

### Event ordering & interleaving

Same-instant event ordering in the simulation does not exactly reproduce
production's; two axes matter, and they should not be conflated.

**1. Priority direction (internal vs. network).** Production
(`ParentExecutor::poll_next`) polls the network *nearly last* — draining
timers / ledger / txpool first — a deliberate anti-starvation design. Under
`Fifo` the harness's `priority` classes finish a node's ready internal work
before the engine services the same-tick network `rx`, matching production's
intent, enforced by the scheduler key rather than by an atomic drain.

**2. Granularity (can a network event interleave *between* two internal
reactions).** Yes — internal reactions are individual zero-delay steps, so a
same-tick delivery can weave between them, as it can in production.

The two axes do not conflict because `tie_break` precedes `priority` in the
engine's ordering key: `Fifo` honours the production poll-order while
`Randomized` fully shuffles across classes — a direct same-tick permutation,
strictly more than timing-jitter fuzzing alone. This only ever concerns
events at the *exact same tick*; with nonzero link latency a node's cascade
and a delivery are strictly time-ordered, so the question is narrow — and
consensus is designed to be robust to event order regardless. Deliberately
*not* done: priority-respecting fuzzing (shuffle only within a class) — a
clean future toggle in the engine, currently unneeded.

### Concurrency, and why not to parallelize yet

The [concurrency measurements](#concurrency-of-a-simulated-swarm) bound what
parallelizing a single run could buy, and for this protocol the bound is
both small and non-scaling:

- **`W/D` plateaus at ~2.9× and is flat in N.** Total work and critical path
  both grow ≈ linearly with node count — `W` because per-node work is
  roughly constant, `D` because each round the leader handles O(N) votes as
  a serial chain (the fan-in spine). Their ratio stops rising once the
  fan-in dominates (~16 nodes here), so more nodes or cores buy no more
  intra-run speedup.
- **The achievable gain is a fraction of that ceiling.** Conservative
  windowing is bounded by the lookahead (min network latency) plus barrier /
  load-imbalance overhead.
- **The parallelism that scales is already exploited.** Independent
  simulations run near-linearly across cores (seed sweeps, forkpoint and
  randomized tests) — the dominant workload. Intra-run parallelism only
  helps a single, un-shardable simulation.
- **It is also the wrong lever for larger networks.** A flat ~3× extends the
  reachable node count by only ~2-3× (less under O(N²) messaging). Going
  materially bigger means reducing `W` — cheaper per-event handling,
  same-tick batching, abstracting all-to-all traffic — not a constant-factor
  parallel speedup.

So the priority is to *analyze* a simulated system's concurrency (the
metric, then the offline analyzer) rather than to parallelize the engine;
the metric is the trigger to revisit. `W/D` here is unit-cost; the
cost-weighted ceiling may be somewhat higher but, being flat in N, does not
change the conclusion.

### Performance attribution

Routing a node's internal reactions through the scheduler as zero-delay
steps (instead of draining them synchronously) costs ~8% at N=4 rising to
~14% at N=64 — paid deliberately for interleaving fidelity and the
compute-cost hook. Even including that cost, the engine outperforms the
legacy engine at every size (see [Legacy engine](#legacy-engine)); the
`O(log E)` heap is the majority of the win and all of the scaling advantage.
Batching same-tick steps could recover most of the ~14% later.
