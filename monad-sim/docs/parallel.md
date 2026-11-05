# monad-sim — parallel execution of a single run (design)

**Status: implemented** (see [Implementation plan](#implementation-plan)).
It extends [design.md](design.md). The network side is in
[`monad-sim-swarm/docs/parallel.md`](../../monad-sim-swarm/docs/parallel.md).

## Goal

Run one simulation on several threads, with results that are bit-identical for
every thread count, using conservative synchronization (no rollback).
Optimistic execution (Time Warp) is out of scope: protocol state is large and
grows without bound, so per-step snapshots are too expensive.

The motivation is measured. The MCP node scenario harness runs Cadence, which
has no leader spine. There, the unit-cost critical-path metric `W / D` is
2.2–6.5 as built, because every send is a step on the single network process.
With routing not serialized through that process, it is 19.5 / 77 / 151 at
n = 16 / 64 / 128. Parallel execution therefore needs two changes: the engine
below, and per-node network stacks in the network layer.

`monad-sim` stays free of any notion of a network. It knows processes,
*domains* of processes, and a positive lower bound on the delay of causal links
between domains.

Throughout, the machinery runs only what a run actually uses. A sequential
run pays for nothing parallel, and a parallel run pays for observation only
when it observes.

## Concepts

```
process   state + steps, as today
domain    a set of processes that never runs split; fixed at spawn
L(a, b)   lookahead: the declared minimum delay of any step a process in
          domain a schedules on a process in domain b
group     a set of whole domains with one event queue; the unit of execution
threads   a rayon pool that runs the groups of a window as tasks
```

**Lookahead.**

- `L(a, a) = 0`. Within a domain, zero-delay scheduling and cancellation are
  allowed.
- `L(a, b) > 0` for `a ≠ b`, validated when the simulation is built. Processes
  that need a zero-delay link, or cancellation across each other, belong in
  one domain.
- Every cross-domain schedule checks `at ≥ now + L(a, b)` and panics with both
  processes, both domains, and the delay. The check is per domain pair, not per
  group, so a violation fails on one thread too, never depending on the
  grouping.
- The lookahead is a lower bound on causality, not a latency model. A network
  layer derives it from its latency model, but they differ in meaning and
  granularity; see the swarm document.

**Domains** come from the consumer, who chooses the granularity: one per node,
one per region, a domain per switch in a hardware model. A process spawned from
a step inherits the spawner's domain.

**Groups** are a performance choice. The default grouping is computed from the
lookahead and domain weights, not from the core count; see
[Grouping](#grouping). It can be overridden.

## One engine

The sequential simulation is the special case of one domain (and so one
group). There is one `Simulation` type.

- `Simulation::new(seed)` keeps its API. Everything runs in one domain, on the
  calling thread. There is no cross-domain check, no window, no barrier, and no
  thread.
- `Simulation::builder(seed).lookahead(l).grouping(g).build()` opts into
  domains, and `spawn_in(domain, state)` places a process.
- Every run-control method is available in both modes; see
  [Run control](#run-control).

```rust
pub trait Lookahead: Send + Sync {
    fn domains(&self) -> usize;
    fn min_delay(&self, from: Domain, to: Domain) -> Duration; // > 0 for from != to
}
```

`Domain` is a dense `u32`. Placing it in the `ProcessId` makes the
cross-domain test a field compare on the schedule path; the domain-to-group map
is a small `Vec`.

## Determinism and the canonical step order

Today's order key `(time, tie_break, priority, seq)` uses a global sequence
number, which no parallel execution can reproduce. The proposal is:

```
(time, domain, tie_break, priority, created_at, creator, counter)
```

- `domain`: the target process's domain.
- `created_at`: the simulation time at which the step was scheduled. It keeps
  today's "earlier sent arrives first" for same-instant steps.
- `creator`: the scheduling process (`ROOT` for the driver).
- `counter`: the number of steps that process has scheduled so far.
- Under `TieBreak::Randomized`, `tie_break = hash(seed, creator, counter)`.

**Why `domain` is in the key.** `(creator, counter)` alone already makes the
key unique, so the keys are totally ordered. But execution order is the heap's
*pop* order, not sorted key order: a zero-delay child can sort before steps that
already ran. Putting `domain` right after `time` makes the engine run each
instant domain by domain. Only a domain itself can add steps to its own
instant, so each domain's run of an instant is the same whichever group it is
in. That is what lets per-group logs be merged into the single-thread order
(Claim 2).

The cost is small. The domain is read from the target's `ProcessId`, it is
compared only when times tie, and it is constant in a sequential run. Compared
with today, an entry grows by about 28 bytes (`created_at`, `creator`,
`domain`; `counter` replaces `seq`), against a boxed closure and 2–3 µs per
step.

Further requirements:

- **Per-process RNG.** `Ctx::sample` draws from the running process's own
  generator, seeded from `(seed, process id)`, created on first use and stored
  in the process slot (no lookup).
- **Deterministic process ids.** Processes spawned during a run get ids
  derived from the spawner, not from a global counter.
- **Global steps** (`schedule_global`) run on the driver at a forced barrier at
  their time, before all other steps of that instant. No consumer on mcp-dev
  uses them. A step of domain `a` must schedule one at least
  `max over b ≠ a of L(a, b)` ahead, the largest lookahead from its domain: a
  global step synchronizes every domain, and a window is never wider than the
  lookahead from `a` to some domain in another group. (The proposal said the
  smallest lookahead; the property test found that a global step could then
  reach the driver after the groups had passed its time.)

**Claim 1: every process executes the same sequence of steps under every valid
grouping and thread count.** Proof sketch:

- A step for a later instant is in its target's queue before that instant
  starts. Within a group that is trivial; across groups it holds because
  delivery happens at a barrier that is no later.
- Only zero-delay steps arrive "late", and they stay inside a domain.
- A heap that interleaves independent subsets pops each subset in the order it
  would pop that subset alone.
- Keys depend only on per-process histories.

**Claim 2: the one-group execution order is a canonical, causally consistent
total order.** Each group executes a subsequence of it. A k-way merge of the
groups' executed steps by `(time, domain)`, keeping each group's own order
within a `(time, domain)` run, reproduces it exactly.

**Checks.** The kickoff tested both claims on a throwaway model of the
scheduler: 300 random lookahead matrices, 6 domains, 12 processes, zero-delay
chains, per-process RNG, both tie-break modes, and 4 random groupings each,
for 2,400 runs and about 430k steps.

- With the proposed key, every per-process history and final state matched the
  one-group run.
- The merged stream matched the one-group execution order step for step.
- Controls:
  - with today's global sequence number in the key, 105 of the 2,400 runs
    changed some per-process history;
  - merging by time alone, without the domain, changed 245 of the 2,400 merged
    streams.

The model also allowed `L = 0` between domains, treating zero-linked domains
as one unit; that is the same thing as merging them into one domain. The
implementation turns these checks into a randomized property test.

**The reference is the new engine with one group, not today's order.**
Bit-identity with the current global FIFO is impossible in parallel, and
keeping the old key for sequential runs would give two semantics. Tests pinned
to exact ticks are re-baselined once. One known case is the Monad BFT golden
`xor_latency_four_nodes` on the mock-swarm branch.

The per-domain lookahead makes `StepLabel::min_propagation` (reserved for a
per-step lookahead) unnecessary; it can be removed.

## Top-level API and monitoring

The simulation remains a single global view. `now()`, `with` / `with_mut`,
`spawn_in`, `despawn`, `metrics()` and `on_step` act on the whole simulation.
Between run-control calls the driver owns every group, so inspection and
mutation are as today.

There is one event stream, in the canonical order (Claim 2). `on_step` sees
exactly the sequence the single-thread run executes, for every thread count.
Each `StepInfo` names its parent `(creator, counter)`, so the causal partial
order is available too; that is the input to the offline concurrency
analyzer.

Observation is paid for only when used:

- **No observer:** groups record nothing and there is no merge.
- **With an observer:** groups buffer `StepInfo` records, and at each barrier
  the driver merges them and calls the observer on its own thread. The
  observer therefore needs no `Send`; the `Rc<Cell>` counter in
  `monad-mcp-cadence-sim/tests/fallback_mvba.rs` keeps working.
- **Filtered observer** (`on_step_filtered(filter, observer)`): the filter
  (`Fn(&StepInfo) -> bool + Send + Sync`, for example a label source or a set
  of processes) runs in the workers, so only matching steps are buffered and
  merged. The filtered stream is the canonical order restricted to the matches.

The merge is a `G`-way merge, tens of nanoseconds per record against
microseconds per step, so it is unlikely to matter. The filter is there for
runs that observe a small fraction of their steps.

Metrics are always on, because they are cheap and deterministic. `steps_*` are
summed, and `critical_path_len` is the maximum over groups. `max_queue_len` is
per group and is the only metric that depends on the grouping.

## Run control

| method | behaviour |
|---|---|
| `next_step()`, `run_steps(n)` | exact: pick the smallest head key across the group queues (linear search), on the calling thread |
| `run_until_time(t)`, `run_to_completion()` | windowed, parallel; windows clipped at `t` |
| `run_while(pred)`, `Swarm::run_until` | windowed, parallel; `pred` checked at every synchronization point |
| `run_at_least_steps(n)` | windowed, parallel; stops at the first synchronization point after `n` steps |

- **Exact stepping** yields the canonical order, so stepping a parallel
  simulation is identical to stepping a sequential one. Linear search over the
  `G` heads is fine for tests and debugging; tight per-step loops belong in a
  single-domain simulation.
- **Synchronization-point checks.** `run_while` runs all groups to the end of
  the window and checks the predicate at the barrier. With one group every step
  is a synchronization point, which is exactly today's behaviour.
  `run_at_least_steps(n)` is `run_while(|sim| sim.step_count() < start + n)`,
  provided as a method: the parallel counterpart of `run_steps` for large `n`.
- **Fully deterministic stop points.** The window schedule (width, starts,
  clipping) is a function of the configuration (lookahead and grouping), never
  of the thread count. So the state at which `run_while` or
  `run_at_least_steps` stops is the same on 1 or 14 threads. Changing the
  grouping can move the stop point, but never changes the underlying run
  (Claim 1).
- **Switching modes mid-run is safe.** After exact stepping, the next window
  starts at the current time; every pending step is at or after it, and every
  later cross-group schedule respects `L`.
- Scenarios that need an exact instant should record it in node state rather
  than read `now()` after stopping, as the MCP and chorus harnesses already do.

## Cancellation

A token cancels only a step in the domain it is cancelled from, which is where
`L = 0`.

- The token records its target's domain. `cancel()` is allowed while a step of
  that domain runs, which costs one thread-local read per cancel, or from the
  driver between runs.
- A cross-domain cancel panics.
- A remote cancel is a message: schedule a step in the target domain, which
  pays at least `L` and then cancels locally. That is honest about causality,
  because nothing affects another domain faster than its causal link allows.

Today's uses fit: timers are armed and reset by their own process, and delivery
tokens are discarded. The flag becomes an `Arc<AtomicBool>` (relaxed), since
tokens move between threads with their process state.

## Synchronization

One barrier per window, no locks on queues. Fine-grained or pairwise schemes
(Chandy–Misra–Bryant null messages, per-pair horizons) add synchronization per
message or per pair to save idle time only under strongly asymmetric
lookahead. They are not worth the complexity here.

Each window:

1. **Driver.** Compute `[s, e)`:
   - `s` is the smallest group head after the last exchange, which skips empty
     stretches;
   - `e` is the minimum of `s + w`, the run's stop time, and the next global
     step;
   - `w` is the smallest `L` between domains in different groups.
2. **Parallel phase.** `groups.par_iter_mut()` (rayon). Each group owns its
   heap, its processes, its outbox and its observer buffer exclusively, and pops
   while its head is before `e`. A step for another group goes into that
   group's slot of this group's outbox. There is no shared mutable state and
   there are no locks.
3. **Join.** The end of the rayon call is the barrier.
4. **Exchange.** Move outbox entries into their target heaps (order does not
   matter, since keys are total), merge observer buffers if any, fold metrics,
   and evaluate a pending predicate. The exchange is linear in cross-group
   traffic; it can itself run per target group in parallel if profiles ever
   show it.

The window is safe: a cross-group step created at `t ≥ s` lands at
`≥ t + L ≥ s + w ≥ e`.

**rayon** (D3): fork-join per window, and work stealing for balance (which does
not affect results). It is already a workspace dependency. Fork-join costs
microseconds, and a window holds thousands of steps (about 700 steps per
simulated millisecond at n = 64, windows of 3–10 ms). Revisit only if profiles
show barrier overhead.

## Grouping

`Grouping::Auto { groups: G }` (default `G = DEFAULT_GROUPS = 128`, a
constant, not the core count, so the window schedule is machine-independent;
tasks then balance across threads). Many more groups than threads matter:
groups are rayon tasks, and uneven domains (busy proposers) are only evened
out between tasks. On the MCP scenarios at `n = 190` (400 KB proposals, 10
threads), 16 / 32 / 64 / 128 / 190 groups took 1.23 / 1.01 / 0.85 / 0.83 /
0.82 s per simulated slot at windows of 1.22 to 1.11 ms; the workers sat idle
35–40% of the time with 32 groups. `Grouping::Explicit` and `Grouping::Single` override
it. The grouping is applied when the simulation first runs, so `Auto` weighs
each domain by the processes spawned by then.

Algorithm:

1. Sort the domain pairs by distance `min(L(a, b), L(b, a))`.
2. Walk them in increasing order, merging components (single-linkage
   clustering). Before each merge, the current components allow a window as
   wide as the pair's distance.
3. For each such component structure, pack the components into `G` bins
   (heaviest first, each into the lightest bin). The achievable speedup is the
   total weight over the heaviest bin.
4. The first structure (all domains separate) gives the best balance. Take the
   widest window whose speedup is at least 75% of that.

That is `n − 1` packings, `O(n² log n)` for `n` domains. With uniform
lookahead it is pure load balancing; with a region-shaped one the components
are the regions. `Simulation::groups()` and `window()` report the result.

Regrouping between windows from measured load is possible, because results do
not depend on the grouping, but it would change the window schedule and so the
`run_while` stop points. It is opt-in and later.

## Cost

Sequential (`Simulation::new`):

| item | cost |
|---|---|
| `Send` bound on process state and step closures | compile time only; existing code: one engine unit test |
| `Arc<AtomicBool>` cancel flag (today `Rc<Cell>`) | same allocation per schedule; one uncontended atomic increment and decrement |
| order key | packed into `(time, class: u128, created_at, id: u128)`, compared lazily |
| per-process RNG | created lazily in the process slot |
| process lookup | two `Vec` indexes instead of a `HashMap` |
| cross-domain branch | a never-taken compare |
| windows, barriers, threads, outboxes, observer buffers | none |

Measured on a pure scheduler benchmark (64 processes passing tokens,
`Simulation::new`, median of 7 runs, Apple M-series), the sequential engine is
as fast as before. The first version was about 20% slower with re-armed timers
(about 34k lazily cancelled entries in the heap); the cause was the comparator,
which evaluated every key component eagerly. Packing the key and comparing
lazily fixed it:

| workload | before (`07f91d53a`) | after |
|---|---|---|
| tokens and re-armed timers | 2.55–2.78 M steps/s | 2.51–2.69 M steps/s |
| tokens only | 5.44–5.58 M steps/s | 5.64–5.65 M steps/s |

Parallel, per step: a group-local heap operation as today; a cross-group
schedule is a `Vec` push into an outbox instead of a heap push; observer
buffering only with an observer. Per window: one fork-join, the exchange, and
the head scan.

`examples/throughput.rs` measures parallel speedup on a synthetic load: 64
processes in 64 domains, 1 ms lookahead, 16 tokens each (about 680 steps per
simulated millisecond, the density of the MCP harness at n = 64), and about
2.4 µs of work per step (the MCP per-step cost). The final digest is identical
for every thread count.

| threads | 1 | 2 | 4 | 8 | 10 |
|---|---|---|---|---|---|
| wall time | 1.65 s | 0.93 s | 0.54 s | 0.37 s | 0.36 s |
| speedup | 1.0× | 1.8× | 3.1× | 4.5× | 4.6× |

At 1 ms windows each group runs only a few dozen steps per window, so the
barrier and imbalance limit scaling beyond 8 threads. The MCP scenarios use
10 ms latency, which gives windows ten times as long.

Acceptance includes: the sequential benchmark (`monad-sim` bench, MCP scenario
at n = 64) within noise of today.

## Consumer obligations

- Process state, step closures and messages are `Send`.
- State shared between processes is read-only during a run, or written by one
  process and read by the driver only between runs. The per-node logs and
  planner in `monad-mcp-cadence-sim` satisfy this.
- No unseeded randomness or wall-clock reads in simulated code (see
  `monad-mcp-node/docs/determinism.md` for one consumer's rules).
- Log lines from workers interleave; node spans keep them attributable, and
  log order is not part of the result.

## Decisions

- **D1.** Require Rust's `Send` on process state and step closures, always:
  `S: Send` in `spawn`, `F: Send` on schedule. One `Ctx` type, no mode
  parameter. Observers stay unbounded.
- **D2.** One `Simulation` type. Every run-control method works in every mode,
  and predicates are checked at synchronization points.
- **D3.** rayon.
- `L(a, b) > 0` between distinct domains; zero-delay links and cancellation
  only within a domain.

## Implementation plan

Each step kept all tests green.

1. Done. Order key, per-process RNG. No tick-pinned test needed re-baselining
   on this line.
2. Done. `Send` bounds, `Arc` cancel tokens.
3. Done (with 4–6). Domains, `Lookahead` (validated `> 0` between domains), the
   cross-domain check, domain-local cancellation, per-domain process ids.
4. Done. Windowed driver with `G` groups; exact stepping by linear search;
   merged and filtered observer streams; `run_at_least_steps`.
   `tests/parallel.rs`: random process graphs (zero-delay chains, re-armed and
   cancelled timers, spawns, global steps, both tie-break modes) give identical
   per-process histories, observer streams and metrics for random groupings,
   `Auto`, and 1/2/4 threads, also when a run stops at synchronization points
   and switches between exact stepping and windows. Comparing against
   `Simulation::new` is not possible: process ids, and so per-process
   generators, encode the domain. Set `MONAD_SIM_PARALLEL_CASES` for a longer
   sweep (400 cases pass). Removing the domain from the order key makes it
   fail.
5. Done. rayon execution.
6. Done. Automatic grouping; `groups()` and `window()` report it.
7. Done. Network side (swarm document): per-node network stacks, domains and
   lookahead from the network, `Swarm::build_parallel`.
8. Done. A consumer: the MCP node scenario harness runs on the windowed
   engine (`monad-mcp-node/tests/scenarios`), with results checked identical
   across thread counts and a scaling bench.

**Measurements on a consumer.** The MCP node scenarios record their
measurements in
[`monad-mcp-node/docs/parallel-scenarios.md`](../../monad-mcp-node/docs/parallel-scenarios.md).
In summary: with one domain per node, `W / D` grows with the swarm (64.6 at
`n = 64`, 127.4 at `n = 128`); on 10 threads, runs are 5–7 times faster than
sequential at `n` of 128 and up, and 3–5 times at `n = 64`, where steps are
cheap enough that the fixed cost of each window weighs more.

## Future work

- Parallel efficiency with cheap steps or few domains: regrouping between
  windows from measured load (opt-in, since it moves `run_while` stop points),
  per-pair horizons instead of global windows, and a cheaper exchange between
  windows.
- Re-measure the Monad BFT harness's `W / D` (a ~2.9× plateau with the single
  network process) with per-node network stacks.
