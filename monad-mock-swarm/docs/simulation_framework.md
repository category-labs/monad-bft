# Monad Simulation Framework

## Status

Implemented on branch `lars/mock-swarm-for-mcp`. The framework reproduces at
least the coverage of the legacy `monad-mock-swarm` engine (every category of
legacy test runs on it; see [Test coverage](#test-coverage)). The legacy engine
remains in place until this is reviewed, then is removed.

This document describes the architecture, the public seams, performance, and the
work left for later. The deeper trade-off analysis behind the design choices is
collected in [Design notes & rationale](#design-notes--rationale) so it does not
interrupt the main read.

## Motivation

A simulation is valuable only if it exercises the *real* consensus code. The
legacy engine does, but the machinery around it had grown hard to extend:

- **Three nested event queues.** "What happens next?" was recomputed by scanning
  a tree top to bottom every step: a swarm (`Nodes`) took the minimum over all
  nodes (`O(N)`), each node (`Node`) took the minimum of its executor and a
  per-node inbound queue, and each node's `MockExecutor` took the minimum over
  eight heterogeneous component sources with two different readiness semantics,
  glued together by an enum ordering and a per-node RNG.
- The same simulated time was threaded through four data structures, and finding
  the next event cost `O(N · components)` per step.
- Adding a new component, protocol variant, or process kind (network, adversary)
  meant touching all three layers.

The framework separates three concerns cleanly and collapses the three queues
into one:

1. **Generic simulation infrastructure** (Layer A, `monad-sim`) — a
   domain-agnostic discrete-event scheduler. No knowledge of Monad, consensus,
   `MonadEvent`, or `Command`.
2. **Application mocks & adapters** (Layer B, `monad-mock-swarm`) — the glue that
   drives the real consensus logic inside the generic framework.
3. **Shared production logic** (Layer C, unchanged) — `MonadState` and the
   consensus / blocksync / mempool / statesync implementation.

Secondary goals, both achieved: **simplify** (one heap instead of three nested
scans) and, for the common serial case, **speed up** (`O(log E)` event selection;
see [Performance](#performance)).

**Hard invariant — no production-logic change.** Layer C, the seam-2 contract,
and genuine production executors (e.g. `LoopbackExecutor`, the same struct
`monad-node` runs) are untouched; that is what guarantees the simulation tests
production code. The one trait-level change required was adding `+ Clone` to
`SwarmRelation::TransportMessage` (needed for message duplication; every relation
already satisfied it). Test/simulation code — including the mock executors — may
change freely.

## Architecture

```
+---------------------------------------------------------------------+
| Layer A - GENERIC simulation infrastructure          (monad-sim)    |
|   Time, scheduler + queue, process handles + lent-state steps        |
|   (Handle<S> / Ctx), run control, RNG/distributions, metrics.        |
|   Agnostic about event types and component state.                    |
|   ZERO knowledge of Monad / consensus / MonadEvent / Command.        |
+---------------------------------------------------------------------+
        ^  seam 1:  Handle<S> / Ctx scheduler API
+---------------------------------------------------------------------+
| Layer B - APP-SPECIFIC sim adapters & mocks      (monad-mock-swarm) |
|   SimNode = framework-owned node state; in-process Command dispatch; |
|   mock/prod executors driven via exec()+drain; SimNet network        |
|   process with a declarative Network model; SimSwarm/SimVerifier.    |
+---------------------------------------------------------------------+
        ^  seam 2:  Executor + Command + MonadEvent contract (unchanged)
+---------------------------------------------------------------------+
| Layer C - SHARED production logic                     (unchanged)    |
|   MonadState::update, Command/MonadEvent, split_commands,            |
|   consensus / blocksync / mempool / statesync logic.                |
+---------------------------------------------------------------------+
```

### Layer A — the generic engine (`monad-sim`)

A simulation is a time-ordered queue of *steps*. A step is a closure that runs at
a specific `Time` and may schedule further steps. State lives in *processes*: any
value `S: 'static` registered with `Simulation::spawn`, addressed by a typed
`Handle<S>`. When a step scheduled against a handle runs, the framework lends it
`&mut S` **exclusively** for that step, via a `Ctx` that can read time, sample
randomness, and schedule more work — but cannot reach any other process's state.

```rust
impl Simulation {
    pub fn new(seed: u64) -> Self;                 // fresh, isolated; the unit of reset
    pub fn now(&self) -> Time;

    pub fn spawn<S: 'static>(&mut self, state: S) -> Handle<S>;
    pub fn schedule<S: 'static>(&mut self, who: Handle<S>, at: Time, label: StepLabel,
                                step: impl FnOnce(&mut S, &mut Ctx) + 'static);
    pub fn schedule_global(&mut self, at: Time, label: StepLabel,
                           step: impl FnOnce(&mut Ctx) + 'static);

    // run control — each returns why it stopped (RunOutcome)
    pub fn run_to_completion(&mut self) -> RunOutcome;
    pub fn run_until_time(&mut self, t: Time) -> RunOutcome;
    pub fn run_steps(&mut self, n: u64) -> RunOutcome;
    pub fn run_while(&mut self, pred: impl FnMut(&Simulation) -> bool) -> RunOutcome;

    // between-step inspection / lifecycle
    pub fn with<S: 'static, R>(&self, who: Handle<S>, f: impl FnOnce(&S) -> R) -> R;
    pub fn with_mut<S: 'static, R>(&mut self, who: Handle<S>, f: impl FnOnce(&mut S) -> R) -> R;
    pub fn despawn<S: 'static>(&mut self, who: Handle<S>) -> S;
}
```

`Ctx` is the only capability a running step has besides its own `&mut S`: it can
read time, `sample` randomness, and `schedule` / `schedule_global` / `spawn` more
work. Cross-process interaction is therefore always "schedule a step on that
handle." This keeps the engine agnostic about event and state types, and makes
reentrancy impossible by construction (within a step you hold the only `&mut S`,
and there is no `borrow()` to call) — see
[Why the lent-state model](#why-the-lent-state-model).

Every step carries a `StepLabel` (`source`, `kind`, `priority`) and is owned by a
`ProcessId`, so steps are introspectable rather than opaque closures. Timers are
expressed with `CancelToken` (schedule = arm, drop/cancel = reset).

### Seam 1 — the scheduler API

`Handle<S>` / `Ctx` (above) *is* seam 1. It is the boundary the legacy engine
lacked: there was no abstraction between "the scheduler" and "a node," only
ad-hoc closures capturing shared state. Concrete event types (`MonadEvent`) and
component internals appear **only** inside Layer-B closures, never in a Layer-A
signature. Layer A defines no `Process`, `Network`, or `EffectHandler` trait — a
process is just some `S: 'static`, and command/effect dispatch is Layer B's job.

### Layer B — a Monad node on the scheduler (`monad-mock-swarm::sim`)

Each node is a `SimNode<S>` process. A scheduled step feeds one event to
`state.update`, then dispatches the resulting commands in-process:

- **Router / network** → outbound messages go to a `SimNet` network *process*
  (seam 1b, below), which applies the network model and schedules delivery steps
  on recipients.
- **Timers** → `TimerCommand::Schedule` arms a `CancelToken`; `ScheduleReset`
  cancels it.
- **Ledger / TxPool / ValSet / StateSync / Loopback** → the existing executors
  (production `LoopbackExecutor`, or mocks shared with `monad-testground` /
  `monad-eth-ledger`) are reused unmodified: call `exec()`, then drain their
  buffered events (`pop` / `next`) and re-inject each as a zero-delay step. Their
  `ready()` / `Stream` / `pop` surface is used purely as a drain, never polled.

`MonadEvent` and `Command` live only inside these closures. Routing internal
reactions through the scheduler as zero-delay steps (rather than draining them
synchronously) is what lets a same-tick network delivery interleave between a
node's internal reactions, and is the hook a future compute-cost model would use
(see [Future work](#future-work) and
[Event ordering & interleaving](#event-ordering--interleaving)).

### Seam 1b — the network as a process

The network is just another process. `SimNet`'s state is a routing table plus a
`NetworkModel`; a node's send schedules a routing step on `SimNet`, which samples
the model and schedules deliver steps on the recipients. This makes the network
model modular and swappable with no extra framework concept. The default is a
declarative `Network` builder:

```rust
Network::reliable(delta)                       // constant latency, lossless
Network::with_latency(distribution)            // per-message sampled latency
    .loss(p)                                    // independent drop probability
    .partition(window, [group_a, group_b])      // time-bounded network split
    .drop_if(|link, msg| ...)                   // predicate drop (sees link.now)
Network::custom(model)                          // a full NetworkModel impl
```

A `NetworkModel` returns the delays at which copies of a message are delivered:
an empty result drops it, more than one duplicates it, and reordering needs no
special handling (a smaller delay simply arrives first).

### Seam 2 — the shared production contract (unchanged)

`state.update(event) -> Vec<Command>`, `Command::split_commands`, and the
`Executor` trait. Defined in Layer C, preserved exactly. This is what guarantees
the simulation exercises production logic, and it is why the migration was
possible without touching consensus.

### Harness

`SimSwarm<S>` wraps a `Simulation` and its node handles, exposing what tests need:

- construction — `SimSwarm::from_builders(seed, builders, network)` (generic over
  any `SwarmRelation`); `build_swarm` / `build_swarm_with` are `NoSerSwarm`
  conveniences.
- bounded run control — `run_until`, `run_until_blocks` (all nodes),
  `run_until_any_blocks` (some node), `run_until_round`.
- inspection / mutation between steps — `with_node`, `with_node_mut`,
  `finalized_blocks`, `current_rounds`, `send_transaction`, `assert_agreement`.
- node lifecycle — `remove_node` / `add_node` (restart-from-forkpoint tests),
  `set_network` (mid-run reconfiguration).

`SimVerifier` (`sim_verify.rs`) accumulates tick / ledger-length / per-node-metric
expectations and checks them through `SimSwarm`'s public surface; metric selectors
are written with the `sim_metric!` macro.

## Determinism & event ordering

The scheduler is deterministic. Steps are ordered by a total key
`(time, tie_break, priority, seq)`:

- **`priority`** classes mirror production's `ParentExecutor::poll_next` order, so
  a node drains ready internal work before it services a same-tick network message
  — production's anti-starvation direction.
- **`tie_break`** selects the policy. The default `Fifo` makes `tie_break` a
  constant, so ordering reduces to `(time, priority, seq)` — fully reproducible,
  and the reason the test suite can assert *exact* ticks with no tolerance window.
  `Randomized` shuffles same-tick steps across priority classes for fuzzing
  (implemented in `monad-sim`; not yet wired through the `SimSwarm` harness — see
  [Future work](#future-work)).

Fuzzing in the legacy `monad-randomized-tests` comes from seeded transformers
(latency jitter), which the network model preserves; the framework is therefore
fuzzed Monte-Carlo over the simulation seed, not per-event. The fidelity
trade-offs are analysed in
[Event ordering & interleaving](#event-ordering--interleaving).

## Performance

`benches/sim_engine.rs` (criterion, 100 committed blocks, build excluded),
comparing the legacy serial engine and the new serial engine, both routing
internal events through the scheduler:

| nodes | legacy serial | new serial | new vs legacy |
|------:|--------------:|-----------:|:-------------:|
| 4     | 17.0 ms       | 14.1 ms    | 1.21×         |
| 16    | 112.8 ms      | 81.2 ms    | 1.39×         |
| 40    | 488.9 ms      | 326.8 ms   | 1.50×         |
| 64    | 1122.7 ms     | 723.0 ms   | 1.55×         |

The new serial engine is faster than the legacy serial engine, the margin
widening with node count — mostly from the `O(log E)` heap replacing the legacy's
three nested `peek` scans. The legacy *parallel* (`rayon`) path was faster on
large swarms, but it was used by exactly one test, was fragile (a safe-window
bound sound only under uniform latency), and was non-deterministic; dropping it in
favour of determinism + a single heap is deliberate. The step-metadata schema
leaves room to parallelize the new queue later if large-swarm throughput ever
matters (see [Future work](#future-work)). More detail in
[Performance attribution](#performance-attribution).

## Concurrency

The engine maintains a deterministic **concurrency metric** in `SimMetrics`: the
critical-path length `D` (`critical_path_len` — the longest chain of
causally-dependent steps), the total step count `W` (`steps_executed`), and
`parallelism() = W / D` (the average available parallelism). A step depends only
on the step that scheduled it and the previous step on its own process, so the
lent-state model makes this exact. It is an *upper bound*, in ticks, on the
speedup any parallel execution of a run could reach.

Measured on the happy-path `NoSerSwarm` (reliable 1 ms network, 100 committed
blocks):

| nodes | steps `W` | critical path `D` | parallelism `W/D` |
|------:|----------:|------------------:|:-----------------:|
| 2     | 2,836     | 1,470             | 1.93              |
| 4     | 5,608     | 2,196             | 2.55              |
| 16    | 22,710    | 7,951             | 2.86              |
| 64    | 99,750    | 34,463            | 2.89              |

Available parallelism rises then **plateaus around ~2.9× by 16 nodes** and does
not grow with network size: both `W` and `D` scale ≈ linearly in N (each round the
leader handles O(N) votes as a serial chain — the fan-in spine — so per-round
depth ≈ O(N)). This is why *analyzing* the simulated system's concurrency is
currently more valuable than parallelizing a single run; see
[Concurrency, and why not to parallelize yet](#concurrency-and-why-not-to-parallelize-yet)
and [Future work](#future-work).

## Test coverage

Tests for the new framework live in `tests/sim_*.rs`, added *additively* — the
legacy tests stay in place and untouched until the legacy engine is removed, which
minimizes merge conflicts and keeps a reference for parity. Shared helpers:
`tests/sim_common` (the `NoSerConfig` builder and reusable custom network models)
and `tests/eth_swarm_common` (the Eth relation and its `SimSwarm` builders).

Ported categories: two-node and multi-node consensus + agreement; BLS signature
collection; message delays and random latency; many-node (40) swarms; metrics;
Eth execution with real transactions; nonces / NEC; reserve balance; epoch
validator switching and boundary handling; blocksync (blackout / delay / timeout
recovery); message-reordering recovery; forkpoint restart/recovery.

Notable scenarios are expressed *declaratively* where the legacy mutated the
transformer pipeline mid-run: a blackout becomes a time-bounded
`Network::partition`, message filtering a `drop_if`, and restart-from-forkpoint
uses the `remove_node` / `add_node` lifecycle API.

`tests/sim_epoch_missing_valset.rs` is a regression gate (`#[should_panic]`) for
an open production `todo!()` reachable at an epoch boundary under outage; it turns
red when the production case is handled.

Coverage not yet ported is listed in [Future work](#future-work).

## Future work

- **Per-node local / NTP clocks** and the clock-scheduling strategy that orders
  steps across them. Prototyped privately but not extracted into `monad-sim`;
  the engine runs a single global clock today. Porting the legacy
  `timestamp_drift` test depends on this.
- **Wire `TieBreak::Randomized` through the `SimSwarm` harness**, then add a
  Monte-Carlo-over-seed test asserting agreement + liveness on a low-latency
  cluster (and ideally that both orders of a same-instant internal-vs-external
  race are reached). This turns the same-tick-permutation capability into actual
  bug-finding leverage.
- **Offline concurrency analyzer (higher priority).** Building on the shipped
  critical-path metric, log per-step `{id, parent_id, process, time, cost}` and
  analyze the run's dependency DAG: cost-weighted parallelism, the
  parallelism-over-time profile, a `speedup(P)` curve, a conservative-window
  replay (sweep lookahead `l`), and sub-process re-attribution (relabel processes
  to test decomposition). This measures *whether* a protocol or scenario has
  parallelism worth exploiting — and is the trigger for the next item. Its
  per-source cost model is the same hook as the compute-cost model.
- **Parallelize a single simulation run (lower priority for now).** Conservative
  windowing (lookahead = min network latency); needs the per-step
  `min_propagation` metadata (`StepLabel` reserves room) and per-process RNG. The
  measured parallelism ceiling is only ~2.9× and **flat in N** (see
  [Concurrency](#concurrency)), so real gains would be a fraction of that and
  would not scale with network size — while independent simulations already
  parallelize near-linearly. Revisit only if a future protocol lifts `W/D` (e.g.
  pipelined / overlapping rounds — the analyzer will show it) or a single
  un-shardable large run becomes a wall-clock bottleneck. Batching same-tick steps
  is a cheaper, deterministic intermediate win. See
  [Concurrency, and why not to parallelize yet](#concurrency-and-why-not-to-parallelize-yet).
- **Compute-cost / backpressure model.** Today component executors emit at the
  current tick (zero local-processing latency) and buffers are unbounded. A
  per-operation cost hook would schedule internal reactions at `now + cost`
  instead of `now + 0` (the injection point already exists); bounded queues +
  push-back would model backpressure. Both are Layer-B additions; no engine
  rewrite. See [Compute-time fidelity](#compute-time-fidelity).
- **Mock relocation (optional, separable).** Moving the sim-only mocks out of the
  production crates (`monad-updaters`, `monad-router-scheduler`) into a
  test-support crate. More entangled than it looks — `monad-testground` and
  `monad-eth-ledger` consume the mocks / the `Mockable*` contract — so it is kept
  off the critical path.
- **Port other swarm/sim consumers.** `monad-debugger`, `monad-twins`,
  `monad-peer-disc-swarm`, and `monad-randomized-tests` are separate one-off
  drivers today; each could model its components as processes over `monad-sim`.
  The `monad-debugger` port must preserve its GraphQL schema for the external UI.
- **Relocate engine-independent coverage before deleting the legacy engine.** The
  per-event `MonadEvent` RLP/serde round-trips in `two_nodes_bls` / `nonces` are
  serialization coverage independent of the engine; extract them into standalone
  serialization tests.

---

## Design notes & rationale

Detail behind the choices above. Not needed to use or review the framework; kept
for whoever revisits the design.

### Why the lent-state model

The alternative was a shared `Rc<RefCell>` / `Arc<Mutex>` node captured by
closures. That converts aliasing bugs into *runtime* borrow panics / deadlocks on
reentrancy — self-send, broadcast-to-self, synchronous signal handlers. With the
framework owning node state and lending `&mut` exclusively per step, reentrancy is
**non-representable**: within a step you hold the only `&mut S` and there is no
`borrow()` to fail, and you cannot reach another process's state at all. Work that
must happen "now" elsewhere is a zero-delay scheduled step. Every queued step also
names its `ProcessId`, which is the basis for debugging and any future
cross-handle parallelism. The migration cost was real (every `&mut Node` site
became a scheduled step), but the bug class is designed out rather than merely
discouraged.

### Event ordering & interleaving

Same-instant event ordering differs across the three engines, and *none* exactly
reproduces production's priority. Two axes matter; do not conflate them.

**1. Priority direction (internal vs. network).** Production
(`ParentExecutor::poll_next`) polls the network *nearly last* — draining timers /
ledger / txpool first — a deliberate anti-starvation design. The legacy sim
*inverted* this (router first in its tie-break), exploring orderings production
rules out. The framework is on the correct side: under `Fifo` the `priority`
classes finish a node's ready internal work before the engine services the
same-tick network `rx`, matching production's intent, enforced by the scheduler
key rather than by an atomic drain.

**2. Granularity (can a network event interleave *between* two internal
reactions).** The legacy could (per-event peek-min). An early version of the
framework drained a node's internal cascade synchronously and could *not* — a
potential coverage gap. That is why internal reactions are now individual
zero-delay steps: a same-tick delivery can again weave between them.

Both axes are now covered, and they no longer conflict because `tie_break`
precedes `priority` in the key: `Fifo` honours the production poll-order while
`Randomized` fully shuffles across classes. `Randomized` is in fact a capability
the legacy never had — the legacy fuzzed *timing* (latency jitter shifting events
onto different ticks) and resolved genuinely same-tick collisions with fixed
tie-breaks; a direct same-tick permutation is strictly more. This only ever
concerns events at the *exact same tick*; with nonzero link latency a node's
cascade and a delivery are strictly time-ordered, so the question is narrow, and
consensus is designed to be robust to event order regardless. Deliberately *not*
done: priority-respecting fuzzing (shuffle only within a class) — a clean future
toggle (`priority` before `tie_break`), currently unneeded.

### Compute-time fidelity

A distinct axis from ordering, stated exactly so the sim's results are not
over-read:

- **No simulated-time latency for local processing.** Component executors emit at
  the current tick; only timers, the timestamper period, and network latency
  carry simulated-time delays. (Same in the legacy engine.)
- **No backpressure.** Every component buffer is unbounded; `ready()` is never
  false due to a full queue.
- **Deferred execution/finalization *is* modeled, but as a fixed block count, not
  a duration** (`execution_delay` / `state_root_delay` / `finalization_delay` are
  `SeqNum`s). So a configured, deterministic, structural execution lag is in scope
  (and its consequences, e.g. the statesync threshold), but emergent,
  *compute-induced* slowness (CPU/DB/GC) is not.

So the sim produces "a node falls behind / triggers blocksync / times out" from
network-induced lag or a configured block-count offset, never from compute. The
A/B/C separation makes growing into this safe and Layer-B-only: internal reactions
already flow through the scheduler, so a per-operation cost hook would schedule
them at `now + cost`; bounded queues + a feedback path would add backpressure, and
global backpressure then *emerges* from per-node cost plus the network model. No
engine rewrite — that would only be needed for real preemption/threads, which
this does not require. (The legacy engine does not model this either, so it is not
a cutover gate.)

### Concurrency stance

The simulation is single-threaded and deterministic. Internal parallelism in pure
Layer-C computation that never touches the simulation environment (e.g.
cryptographic primitives) is allowed and invisible — it produces no observable
scheduling effect. Intra-simulation parallelism (the legacy `rayon`
`batch_step_until`) was dropped on purpose (see [Performance](#performance)).
Independent simulations still run in parallel across threads — the per-simulation
isolation (`Simulation::new` owns all its state; no shared `thread_local`) is what
makes that safe, which also unblocks the heavily-used independent-swarm
parallelism in `forkpoint` / `randomized-tests`.

### Concurrency, and why not to parallelize yet

The [Concurrency](#concurrency) metric bounds what parallelizing a single run
could buy, and for this protocol the bound is both small and non-scaling:

- **`W/D` plateaus at ~2.9× and is flat in N.** Total work and critical path both
  grow ≈ linearly with node count — `W` because per-node work is roughly constant,
  `D` because each round the leader handles O(N) votes as a serial chain (the
  fan-in spine). Their ratio stops rising once the fan-in dominates (~16 nodes
  here), so more nodes or cores buy no more intra-run speedup.
- **The achievable gain is a fraction of that ceiling.** Conservative windowing is
  bounded by the lookahead (min network latency) plus barrier / load-imbalance
  overhead, and is a net loss below ~8 nodes. The legacy rayon experiment (~3× at
  large N) is the empirical anchor and matches the ~2.9 ceiling.
- **The parallelism that scales is already exploited.** Independent simulations run
  near-linearly across cores (seed sweeps, `forkpoint`, `randomized-tests`) — the
  dominant workload. Intra-run parallelism only helps a single, un-shardable
  simulation.
- **It is also the wrong lever for larger networks.** A flat ~3× extends the
  reachable node count by only ~2-3× (less under O(N²) messaging). Going
  materially bigger means reducing `W` — cheaper per-event handling, same-tick
  batching, abstracting all-to-all traffic — not a constant-factor parallel speedup.

So the priority is to *analyze* the simulated system's concurrency (the metric,
then the offline analyzer) rather than to parallelize the engine; the metric is
the trigger to revisit. `W/D` here is unit-cost; the cost-weighted ceiling may be
somewhat higher but, being flat in N, does not change the conclusion.

### Performance attribution

The speedup has two sources, now measured separately. Converting the internal
drain from synchronous calls to zero-delay scheduled steps (so internal events go
through the heap in *both* engines, an apples-to-apples comparison) cost ~8% at
N=4 rising to ~14% at N=64. Even after that, the new engine is still faster than
legacy serial at every size (see [Performance](#performance)), the margin widening
with N. So the **data structure** (the
`O(log E)` heap replacing three nested `peek` scans) is the majority of the win
and all of the scaling advantage; the drain conversion is a modest top-up. Batching
same-tick steps could recover most of that ~14% later, alongside parallelization.
