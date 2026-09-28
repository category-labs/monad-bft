# monad-sim — engine design

The generic, deterministic discrete-event simulation engine. This document
covers the architecture, the seam consumers build on, determinism, the
concurrency metric, and the rationale behind the main design choices. For a
quick tour of the API, start with the [README](../README.md).

## Purpose & scope

`monad-sim` is the domain-agnostic bottom layer for deterministic protocol
simulations: a time-ordered queue of scheduled steps over framework-owned
process state, with bounded run control, seeded randomness, cancellable
timers, and metrics. It knows nothing about Monad, consensus, or any
particular event or message type.

The engine is written for multiple consumers, present and future. In this
repository, [`monad-sim-swarm`](../../monad-sim-swarm) layers a generic
network + swarm harness on top, and the Monad BFT mock-swarm harness drives
the production consensus code in simulation; the engine is also used by other
protocol simulations outside this repository, and further harnesses are
expected. The rule that keeps this possible: nothing in this crate may name a
protocol, an event type, or a component layout — anything protocol-specific
belongs in a consumer crate.

## The model

A simulation is a time-ordered queue of *steps*. A step is a closure that runs
at a specific `Time` and may schedule further steps. State lives in
*processes*: any value `S: 'static` registered with `Simulation::spawn`,
addressed by a typed `Handle<S>`. When a step scheduled against a handle runs,
the framework lends it `&mut S` **exclusively** for that step, via a `Ctx`
that can read time, sample randomness, and schedule more work — but cannot
reach any other process's state.

```rust
impl Simulation {
    pub fn new(seed: u64) -> Self;                 // fresh, isolated; the unit of reset
    pub fn now(&self) -> Time;

    pub fn spawn<S: 'static>(&mut self, state: S) -> Handle<S>;
    pub fn schedule<S: 'static>(&mut self, who: Handle<S>, at: Time, label: StepLabel,
                                step: impl FnOnce(&mut S, &mut Ctx) + 'static) -> CancelToken;
    pub fn schedule_global(&mut self, at: Time, label: StepLabel,
                           step: impl FnOnce(&mut Ctx) + 'static) -> CancelToken;

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

`Ctx` is the only capability a running step has besides its own `&mut S`: it
can read time, `sample` randomness, and `schedule` / `schedule_global` /
`spawn` more work. Cross-process interaction is therefore always "schedule a
step on that handle". This keeps the engine agnostic about event and state
types, and makes reentrancy impossible by construction (within a step you hold
the only `&mut S`, and there is no `borrow()` to call) — see
[Why the lent-state model](#why-the-lent-state-model).

Every step carries a `StepLabel` (`source`, `kind`, `priority`) and is owned
by a `ProcessId`, so steps are introspectable rather than opaque closures.
Timers are expressed with `CancelToken` (schedule = arm, cancel = reset;
cancellation is lazy — a cancelled step is skipped when popped).

## The consumer seam

`Handle<S>` / `Ctx` *is* the engine's public seam. The engine defines no
`Process`, `Network`, or `EffectHandler` trait — a process is just some
`S: 'static`, and event/effect dispatch is the consumer's job. Concrete event
types (for the Monad BFT harness: `MonadEvent`, `Command`) appear only inside
consumer closures, never in an engine signature. A new consumer brings its own
state, address, and message types and only ever talks to the scheduler; that
is what makes the engine reusable across protocols and protocol versions.

## Determinism & event ordering

The scheduler is deterministic: a run is exactly reproducible from
`(configuration, seed)`. Steps are ordered by the total key
`(time, tie_break, priority, seq)`:

- **`priority`** is a same-time ordering class (lower runs first); consumers
  use it to encode policies such as "drain a node's ready internal work before
  servicing a same-tick network message".
- **`tie_break`** selects the policy. The default `TieBreak::Fifo` makes
  `tie_break` a constant, so ordering reduces to `(time, priority, seq)` —
  fully reproducible, which is what lets test suites assert *exact* ticks with
  no tolerance window. `TieBreak::Randomized` shuffles same-time steps with a
  seeded permutation (still deterministic per seed); sweeping the seed
  explores different same-instant orderings — fuzzing for order-dependence
  bugs.

`tie_break` deliberately precedes `priority` in the key: under `Randomized`
the per-step tie-break dominates, so same-time steps are shuffled *across*
priority classes. Priority-respecting fuzzing (shuffle only within a class)
would put `priority` first instead; it is a clean future toggle, not
implemented until a consumer needs it.

All randomness flows through the simulation's seeded generator
(`Simulation::sample` / `Ctx::sample`); the `dist` module provides latency and
clock-drift distributions to sample through it.

## Concurrency metric

The engine maintains a deterministic concurrency metric in `SimMetrics`: the
critical-path length `D` (`critical_path_len`, the longest chain of causally
dependent steps), the total step count `W` (`steps_executed`), and
`parallelism() = W / D`, the average available parallelism. A step depends
only on the step that scheduled it and on the previous step on its own process
(processes are serial), so the lent-state model makes the metric exact.

`W / D` is an *upper bound*, in steps, on the speedup any parallel execution
of the same run could achieve — a property of the simulated system, not of the
engine. It is deterministic for a given `(configuration, seed)`, so it doubles
as a regression metric, and it is the measurement that decides whether
parallel execution is worth building (see [Future work](#future-work)).

## Design rationale

### Why the lent-state model

The alternative was a shared `Rc<RefCell>` / `Arc<Mutex>` process captured by
closures. That converts aliasing bugs into *runtime* borrow panics or
deadlocks on reentrancy — self-send, broadcast-to-self, synchronous signal
handlers. With the framework owning process state and lending `&mut`
exclusively per step, reentrancy is **non-representable**: within a step you
hold the only `&mut S` and there is no `borrow()` to fail, and you cannot
reach another process's state at all. Work that must happen "now" elsewhere is
a zero-delay scheduled step. Every queued step also names its `ProcessId`,
which is the basis for debugging, the critical-path metric, and any future
cross-process parallelism.

### Concurrency stance

A simulation is single-threaded and deterministic. Internal parallelism in
pure consumer computation that never touches the simulation environment (e.g.
cryptographic primitives) is allowed and invisible — it produces no observable
scheduling effect. Independent simulations run in parallel across threads —
per-simulation isolation (`Simulation::new` owns all its state; no shared
`thread_local`) is what makes that safe, and that is the parallelism that
scales (seed sweeps, independent scenarios).

Parallelizing a *single* run is deliberately deferred. The measured ceiling on
the first consumer is low and flat: the Monad BFT harness measured `W / D`
plateauing around ~2.9× regardless of swarm size (see that harness's
documentation), and conservative-window execution would realize only a
fraction of the ceiling. The `StepLabel` schema reserves the metadata a
parallel executor would need (`min_propagation`).

## Future work

- **Per-process local clocks** (NTP-style drift) and the clock-scheduling
  strategy that orders steps across them; the engine runs a single global
  clock today. `Time` / `ClockDiff` already support negative offsets.
  Synchronous signal fan-out is deferred with it.
- **Offline concurrency analyzer.** Building on the critical-path metric: log
  per-step `{id, parent_id, process, time, cost}` and analyze the run's
  dependency DAG — cost-weighted parallelism, a parallelism-over-time profile,
  a `speedup(P)` curve, and sub-process re-attribution (relabel processes to
  evaluate a finer decomposition). This measures *whether* a protocol or
  scenario has parallelism worth exploiting, and is the trigger for the next
  item.
- **Parallel execution of a single run** — conservative windowing with
  lookahead bounded by the minimum event latency; needs the per-step
  `min_propagation` metadata and a per-process RNG. Revisit when a consumer's
  measured `W / D` makes it worthwhile. Batching same-tick steps is a cheaper,
  deterministic intermediate win.
