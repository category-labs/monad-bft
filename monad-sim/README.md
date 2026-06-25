# monad-sim

A generic, single-threaded, deterministic discrete-event simulation engine.
Standalone and domain-agnostic: it knows nothing about Monad, consensus, or any
particular message type. Protocol simulations layer on top — for instance
[`monad-sim-swarm`](../monad-sim-swarm) (generic network + swarm harness) and
the Monad BFT mock-swarm harness. See
[`docs/design.md`](docs/design.md) for the architecture and rationale.

## Model

A simulation is a time-ordered queue of **steps** over a set of
framework-owned **processes**:

- **Process** — any value `S: 'static`, registered with `Simulation::spawn` and
  addressed by a typed, `Copy` `Handle<S>`.
- **Step** — a closure scheduled against a handle to run at a `Time`. While it
  runs, the framework lends it `&mut S` *exclusively*, plus a `Ctx` that can
  read the clock, sample randomness, and schedule further steps — but cannot
  reach any other process's state.

Cross-process interaction is therefore always "schedule a step on that
handle". This keeps the engine agnostic to event and state types, and makes
reentrancy (self-send, broadcast-to-self) non-representable: within a step you
hold the only `&mut S`, and there is no `RefCell::borrow()` to panic.

```rust
use monad_sim::{time::millis, Simulation, StepLabel, Time};

let mut sim = Simulation::new(7); // the seed; the unit of isolation & reset
let a = sim.spawn(0u64);          // any S: 'static is a process
let b = sim.spawn(Vec::<u64>::new());

sim.schedule(a, Time(0), StepLabel::source("kick"), move |count, ctx| {
    *count += 1;
    // interact with b by scheduling a step on it, one hop later
    ctx.schedule_after(b, millis(5), StepLabel::source("deliver"), |log, _| {
        log.push(42);
    });
});

sim.run_to_completion();
assert_eq!(sim.now(), Time(0) + millis(5));
sim.with(b, |log| assert_eq!(*log, vec![42])); // inspect between steps
```

## Determinism

A run is exactly reproducible from `(configuration, seed)`. Steps execute in
the total order `(time, tie_break, priority, seq)`:

- The default `TieBreak::Fifo` orders same-time steps by priority class, then
  creation order — stable enough that tests can assert exact ticks.
- `TieBreak::Randomized` shuffles same-time steps with a seeded permutation
  (still fully reproducible); sweep the seed to fuzz for order-dependence bugs.

All randomness flows through the seeded generator, via `sim.sample(..)` /
`ctx.sample(..)`.

## Toolbox

| | |
|---|---|
| `schedule` / `schedule_after` / `schedule_global` | timed steps on a process, or process-less (setup, fan-out) |
| `CancelToken` | cancellable steps — schedule = arm a timer, cancel = reset it |
| `run_to_completion` / `run_until_time` / `run_steps` / `run_while` | bounded run control, each returning why it stopped (`RunOutcome`) |
| `with` / `with_mut` / `despawn` | inspect, inject, or remove process state between steps |
| `on_step` / `StepInfo` | observe every executed step (source/kind/priority labels) |
| `SimMetrics` | step counts, queue depth, critical-path length, available `parallelism()` |
| `time` | `Time` (ns-resolution simulation clock), `ClockDiff`, `millis`/`secs`/… shorthands |
| `dist` | latency distributions (`uniform_duration`, `lognormal_duration2`, `global_udp_duration`, …) |

`tests/processes.rs` is a worked example with toy processes: nodes exchanging
messages through a swappable network process, sampled latency, and a
resettable watchdog timer.

## Running

```sh
cargo test -p monad-sim
```
