// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! The discrete-event scheduler: [`Simulation`], process [`Handle`]s, and the
//! per-step [`Ctx`].

use std::{
    any::Any,
    cell::Cell,
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    rc::Rc,
    time::Duration,
};

use rand::{distributions::Distribution, rngs::StdRng, SeedableRng};

use crate::time::Time;

/// Identifies a process registered in a [`Simulation`]. Stable for the lifetime
/// of that process; ids are never reused.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ProcessId(u64);

/// The reserved root process that owns process-less ([`Simulation::schedule_global`])
/// steps. Its state is `()`.
const ROOT: ProcessId = ProcessId(0);

/// A typed reference to a process's state owned by the [`Simulation`].
///
/// `Copy` regardless of `S`; cheap to capture in closures. A step scheduled
/// against a handle is lent `&mut S` while it runs.
///
/// The `id`→`S` binding is fixed at [`spawn`](Simulation::spawn) — ids are never
/// reused and a process's state type never changes — and handles are minted only
/// there. That is the invariant that makes the erased `Action` downcast
/// infallible.
pub struct Handle<S> {
    id: ProcessId,
    _pd: PhantomData<fn() -> S>,
}

impl<S> Clone for Handle<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Handle<S> {}

impl<S> Handle<S> {
    /// The id of the referenced process.
    pub fn id(&self) -> ProcessId {
        self.id
    }
}

/// Optional metadata attached to a scheduled step, surfaced in [`StepInfo`] for
/// observability. `min_propagation` is reserved for future causal-independence
/// inference and is otherwise ignored.
///
/// `priority` is a same-time ordering class (lower runs first); see [`TieBreak`]
/// and `Entry`'s ordering for how it interacts with the tie-break. The default
/// `0` leaves a step in the highest-priority class.
#[derive(Clone, Debug, Default)]
pub struct StepLabel {
    pub source: Option<&'static str>,
    pub kind: Option<&'static str>,
    pub min_propagation: Option<Duration>,
    pub priority: u32,
}

impl StepLabel {
    /// A label tagged with the component that scheduled the step.
    pub fn source(source: &'static str) -> Self {
        Self {
            source: Some(source),
            ..Self::default()
        }
    }

    /// Set the step kind (builder style).
    pub fn kind(mut self, kind: &'static str) -> Self {
        self.kind = Some(kind);
        self
    }

    /// Set the same-time priority class (builder style); lower runs first at the
    /// same simulated time under `TieBreak::Fifo`.
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

/// Description of an executed step, passed to the observer and available for
/// run-control predicates.
#[derive(Clone, Debug)]
pub struct StepInfo {
    pub time: Time,
    pub seq: u64,
    pub process: ProcessId,
    pub label: StepLabel,
    /// This step's critical-path depth (see [`SimMetrics::critical_path_len`]);
    /// `0` if the step was skipped because its process had been despawned.
    pub depth: u64,
}

/// Why a bounded run returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    /// The queue emptied.
    Drained,
    /// Reached the requested time; later steps remain queued.
    ReachedTime(Time),
    /// Executed the requested number of steps; more remain.
    ReachedStepLimit,
    /// A `run_while` predicate asked to stop.
    Stopped,
}

/// Handle to a scheduled step that lets it be cancelled before it runs.
///
/// Cancellation is lazy: the step stays in the queue and is skipped when popped.
/// Cancelling after the step has already run is a no-op.
#[derive(Clone)]
pub struct CancelToken(Rc<Cell<bool>>);

impl CancelToken {
    fn new() -> Self {
        CancelToken(Rc::new(Cell::new(false)))
    }

    /// Prevent the associated step from running.
    pub fn cancel(&self) {
        self.0.set(true);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.get()
    }
}

/// Counters describing what a [`Simulation`] has done. Purely informational.
#[derive(Clone, Debug, Default)]
pub struct SimMetrics {
    pub steps_executed: u64,
    pub steps_scheduled: u64,
    /// Steps skipped because they were cancelled before being popped.
    pub steps_cancelled: u64,
    pub max_queue_len: usize,
    /// Longest chain of causally-dependent steps, in steps. A step depends on
    /// the step that scheduled it and on the previous step on its own process
    /// (processes are serial). This is the run's critical path: the fewest
    /// sequential steps any execution — parallel or not — must take. Together
    /// with `steps_executed` (the total work) it bounds the available
    /// parallelism (see [`parallelism`](Self::parallelism)). Deterministic for a
    /// given `(config, seed)`, so it is usable as a regression metric.
    pub critical_path_len: u64,
}

impl SimMetrics {
    /// Average available parallelism: total steps over the critical path. `1.0`
    /// for a fully sequential run; higher means more steps could, in principle,
    /// run concurrently. An upper bound on achievable speedup, not a measurement
    /// of any actual parallel run.
    pub fn parallelism(&self) -> f64 {
        self.steps_executed as f64 / self.critical_path_len.max(1) as f64
    }
}

/// A queued step with its state type erased, so one heap can hold steps for
/// heterogeneous processes. The closure (built by `enqueue::<S>`) downcasts the
/// erased `&mut dyn Any` back to its scheduling `Handle<S>`'s `S`; that is
/// infallible because a process keeps its state type for life (see [`Handle`]),
/// so the downcast `expect` there is a defensive assert, not a reachable failure.
type Action = Box<dyn FnOnce(&mut dyn Any, &mut Ctx<'_>)>;
type Observer = Box<dyn FnMut(&StepInfo)>;

/// How same-time steps are ordered.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum TieBreak {
    /// Deterministic FIFO: ties break by creation order (`seq`). The default,
    /// and the only mode that yields exact, stable ticks.
    #[default]
    Fifo,
    /// Seeded pseudo-random: ties break by a hash of `(seed, seq)`, decoupled
    /// from creation order. Still fully reproducible from `(config, seed)` — it
    /// is a deterministic reshuffle, not true non-determinism. Sweep the seed to
    /// explore different same-time orderings (fuzzing for order-dependence bugs).
    Randomized,
}

struct Entry {
    time: Time,
    /// Same-time ordering key (see [`TieBreak`]); always 0 under `Fifo`.
    tie_break: u64,
    /// Same-time priority class (from [`StepLabel::priority`]); lower runs first.
    priority: u32,
    seq: u64,
    process: ProcessId,
    /// Critical-path depth of the step that scheduled this one (`0` if scheduled
    /// outside any step). See [`SimMetrics::critical_path_len`].
    parent_depth: u64,
    label: StepLabel,
    cancel: CancelToken,
    action: Action,
}

// Ordering is by (time, tie_break, priority, seq). `seq` is unique and monotonic,
// so the order is always total. Note `tie_break` precedes `priority`:
// - under `TieBreak::Fifo` every `tie_break` is 0, so this reduces to
//   (time, priority, seq) — priority classes are honored at a tick, FIFO within
//   a class, with stable exact ticks;
// - under `TieBreak::Randomized` the per-seq `tie_break` is distinct, so it
//   dominates and `priority` is effectively ignored — same-time steps are fully
//   shuffled across classes (priority-respecting within-class fuzzing would put
//   `priority` first instead; deliberately not done here).
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
            && self.tie_break == other.tie_break
            && self.priority == other.priority
            && self.seq == other.seq
    }
}
impl Eq for Entry {}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time
            .cmp(&other.time)
            .then(self.tie_break.cmp(&other.tie_break))
            .then(self.priority.cmp(&other.priority))
            .then(self.seq.cmp(&other.seq))
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// SplitMix64 finalizer. Maps the unique, monotonic `seq` (combined with the
/// simulation seed) to a well-distributed, collision-free `tie_break`, so
/// same-time steps get a deterministic pseudo-random order under
/// [`TieBreak::Randomized`].
fn tie_break_key(seed: u64, seq: u64) -> u64 {
    let mut z = seq.wrapping_add(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A single-threaded discrete-event simulation: a time-ordered queue of steps
/// over a set of framework-owned processes.
///
/// Construct with [`Simulation::new`], register state with [`spawn`](Self::spawn),
/// schedule work with [`schedule`](Self::schedule) / [`schedule_global`](Self::schedule_global),
/// then drive it with one of the `run_*` methods.
pub struct Simulation {
    seed: u64,
    tie_break: TieBreak,
    rng: StdRng,
    now: Time,
    seq: u64,
    next_id: u64,
    processes: HashMap<ProcessId, Option<Box<dyn Any>>>,
    queue: BinaryHeap<Reverse<Entry>>,
    metrics: SimMetrics,
    observer: Option<Observer>,
    /// Critical-path depth of the last executed step on each process; maintains
    /// [`SimMetrics::critical_path_len`] incrementally.
    proc_depth: HashMap<ProcessId, u64>,
}

impl Simulation {
    /// A fresh, empty simulation seeded for deterministic randomness. The clock
    /// starts at [`Time(0)`](Time).
    pub fn new(seed: u64) -> Self {
        let mut processes = HashMap::new();
        processes.insert(ROOT, Some(Box::new(()) as Box<dyn Any>));
        Simulation {
            seed,
            tie_break: TieBreak::Fifo,
            rng: StdRng::seed_from_u64(seed),
            now: Time(0),
            seq: 0,
            next_id: 1,
            processes,
            queue: BinaryHeap::new(),
            metrics: SimMetrics::default(),
            observer: None,
            proc_depth: HashMap::new(),
        }
    }

    /// Choose how same-time steps are ordered (default [`TieBreak::Fifo`]).
    ///
    /// Set this before scheduling any steps — e.g. immediately after
    /// construction — so the chosen order applies to the whole run. Changing it
    /// mid-run only affects steps enqueued afterwards.
    pub fn set_tie_break(&mut self, tie_break: TieBreak) {
        self.tie_break = tie_break;
    }

    /// The current same-time ordering policy.
    pub fn tie_break(&self) -> TieBreak {
        self.tie_break
    }

    /// Return the simulation to its just-constructed state: all processes and
    /// scheduled steps are discarded, the clock and metrics reset, and the
    /// generator reseeded from the original seed. Handles obtained before the
    /// reset are invalid afterwards.
    pub fn reset(&mut self) {
        *self = Simulation::new(self.seed);
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The current global clock time. During a step this is the step's time;
    /// otherwise it is the time of the most recently executed step (or the
    /// target of the last [`run_until_time`](Self::run_until_time)).
    pub fn now(&self) -> Time {
        self.now
    }

    /// Number of steps executed so far. Cancelled (skipped) steps do not count.
    pub fn step_count(&self) -> u64 {
        self.metrics.steps_executed
    }

    pub fn metrics(&self) -> &SimMetrics {
        &self.metrics
    }

    /// Sample from `d` using the simulation's seeded generator.
    pub fn sample<T>(&mut self, d: impl Distribution<T>) -> T {
        d.sample(&mut self.rng)
    }

    /// Register process state and return a handle to it.
    pub fn spawn<S: 'static>(&mut self, state: S) -> Handle<S> {
        let id = ProcessId(self.next_id);
        self.next_id += 1;
        self.processes.insert(id, Some(Box::new(state)));
        Handle {
            id,
            _pd: PhantomData,
        }
    }

    /// Schedule `step` to run at absolute time `at` with exclusive `&mut S`
    /// access to the process behind `who`.
    ///
    /// Panics if `at` is before [`now`](Self::now). Panics when the step is
    /// executed if `who` does not refer to a live process or its state type does
    /// not match `S`.
    pub fn schedule<S: 'static>(
        &mut self,
        who: Handle<S>,
        at: Time,
        label: StepLabel,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + 'static,
    ) -> CancelToken {
        enqueue(
            &mut self.queue,
            &mut self.seq,
            self.seed,
            self.tie_break,
            &mut self.metrics,
            self.now,
            0,
            who.id,
            at,
            label,
            step,
        )
    }

    /// Like [`schedule`](Self::schedule) but at `now + delay`.
    pub fn schedule_after<S: 'static>(
        &mut self,
        who: Handle<S>,
        delay: Duration,
        label: StepLabel,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + 'static,
    ) -> CancelToken {
        self.schedule(who, self.now + delay, label, step)
    }

    /// Schedule a process-less step (setup, observation, fan-out scheduling)
    /// owned by the reserved root process. The step receives only a [`Ctx`].
    ///
    /// Panics if `at` is before [`now`](Self::now).
    pub fn schedule_global(
        &mut self,
        at: Time,
        label: StepLabel,
        step: impl FnOnce(&mut Ctx<'_>) + 'static,
    ) -> CancelToken {
        enqueue::<()>(
            &mut self.queue,
            &mut self.seq,
            self.seed,
            self.tie_break,
            &mut self.metrics,
            self.now,
            0,
            ROOT,
            at,
            label,
            move |_unit, ctx| step(ctx),
        )
    }

    /// Read the state behind `who`. Intended for use between steps (run-control
    /// predicates, assertions, the debugger).
    ///
    /// Panics if `who` is not a live process, its state is unavailable because a
    /// step on it is currently executing, or its state type does not match `S`.
    pub fn with<S: 'static, R>(&self, who: Handle<S>, f: impl FnOnce(&S) -> R) -> R {
        let state = self
            .processes
            .get(&who.id)
            .expect("unknown process handle")
            .as_ref()
            .expect("process state unavailable (called during its own step?)")
            .downcast_ref::<S>()
            .expect("handle/state type mismatch");
        f(state)
    }

    /// Mutable counterpart of [`with`](Self::with), for injecting input between
    /// steps. Same panic conditions.
    pub fn with_mut<S: 'static, R>(&mut self, who: Handle<S>, f: impl FnOnce(&mut S) -> R) -> R {
        let state = self
            .processes
            .get_mut(&who.id)
            .expect("unknown process handle")
            .as_mut()
            .expect("process state unavailable (called during its own step?)")
            .downcast_mut::<S>()
            .expect("handle/state type mismatch");
        f(state)
    }

    /// Remove a process and return its state. Steps already scheduled against it
    /// will be skipped when popped (their state is gone). The handle is invalid
    /// afterwards.
    ///
    /// Panics if `who` is not a live process, its state is unavailable because a
    /// step on it is currently executing, or its state type does not match `S`.
    pub fn despawn<S: 'static>(&mut self, who: Handle<S>) -> S {
        let boxed = self
            .processes
            .remove(&who.id)
            .expect("unknown process handle")
            .expect("process state unavailable (despawn during its own step?)");
        self.proc_depth.remove(&who.id);
        *boxed.downcast::<S>().expect("handle/state type mismatch")
    }

    /// Register a callback invoked after every executed step. Replaces any
    /// previously registered observer.
    pub fn on_step(&mut self, observer: impl FnMut(&StepInfo) + 'static) {
        self.observer = Some(Box::new(observer));
    }

    /// Time of the next queued step, ignoring whether it is cancelled. `None`
    /// when the queue is empty.
    pub fn peek_time(&self) -> Option<Time> {
        self.queue.peek().map(|Reverse(e)| e.time)
    }

    /// Execute the earliest queued step (skipping cancelled ones), returning its
    /// [`StepInfo`], or `None` if the queue is empty.
    pub fn next_step(&mut self) -> Option<StepInfo> {
        loop {
            let Reverse(entry) = self.queue.pop()?;
            if entry.cancel.is_cancelled() {
                self.metrics.steps_cancelled += 1;
                continue;
            }
            let time = entry.time;
            let seq = entry.seq;
            let process = entry.process;
            let label = entry.label.clone();
            let depth = self.dispatch(entry);
            self.metrics.steps_executed += 1;
            let info = StepInfo {
                time,
                seq,
                process,
                label,
                depth,
            };
            if let Some(observer) = self.observer.as_mut() {
                observer(&info);
            }
            return Some(info);
        }
    }

    /// Run until the queue is empty.
    pub fn run_to_completion(&mut self) -> RunOutcome {
        while self.next_step().is_some() {}
        RunOutcome::Drained
    }

    /// Run every step scheduled at time `<= t`, then advance the clock to `t`
    /// (never backwards). Returns [`RunOutcome::Drained`] if the queue emptied,
    /// otherwise [`RunOutcome::ReachedTime`].
    pub fn run_until_time(&mut self, t: Time) -> RunOutcome {
        loop {
            // Drop cancelled entries first so the time bound is applied to the
            // next step that will actually run, not to a skipped one.
            self.purge_cancelled_front();
            match self.peek_time() {
                Some(next) if next <= t => {
                    self.next_step();
                }
                Some(_) => {
                    self.advance_to(t);
                    return RunOutcome::ReachedTime(t);
                }
                None => {
                    self.advance_to(t);
                    return RunOutcome::Drained;
                }
            }
        }
    }

    /// Discard cancelled entries at the front of the queue so the next peek sees
    /// a runnable step.
    fn purge_cancelled_front(&mut self) {
        while let Some(Reverse(entry)) = self.queue.peek() {
            if entry.cancel.is_cancelled() {
                self.queue.pop();
                self.metrics.steps_cancelled += 1;
            } else {
                break;
            }
        }
    }

    /// Run up to `n` steps. Returns [`RunOutcome::Drained`] if the queue emptied
    /// first, otherwise [`RunOutcome::ReachedStepLimit`].
    pub fn run_steps(&mut self, n: u64) -> RunOutcome {
        for _ in 0..n {
            if self.next_step().is_none() {
                return RunOutcome::Drained;
            }
        }
        RunOutcome::ReachedStepLimit
    }

    /// Run while `pred` holds, evaluating it before each step. `pred` may inspect
    /// process state (via [`with`](Self::with)) and the clock. Returns
    /// [`RunOutcome::Stopped`] when `pred` returns `false`, or
    /// [`RunOutcome::Drained`] if the queue empties first.
    pub fn run_while(&mut self, mut pred: impl FnMut(&Simulation) -> bool) -> RunOutcome {
        while pred(&*self) {
            if self.next_step().is_none() {
                return RunOutcome::Drained;
            }
        }
        RunOutcome::Stopped
    }

    fn advance_to(&mut self, t: Time) {
        if t > self.now {
            self.now = t;
        }
    }

    fn dispatch(&mut self, entry: Entry) -> u64 {
        let pid = entry.process;
        self.now = entry.time;
        // A step may be left over for a process that was despawned after it was
        // scheduled; skip it (it does no work, so it is off the critical path).
        if !self.processes.contains_key(&pid) {
            return 0;
        }
        // Critical-path depth: one past the later of the step that scheduled this
        // one and the previous step on this process.
        let depth = entry
            .parent_depth
            .max(self.proc_depth.get(&pid).copied().unwrap_or(0))
            + 1;
        self.proc_depth.insert(pid, depth);
        self.metrics.critical_path_len = self.metrics.critical_path_len.max(depth);
        // Take the process's state out so `Ctx` can borrow the rest of `self`
        // (including `processes`, for spawns) without aliasing it.
        let mut state = self
            .processes
            .get_mut(&pid)
            .expect("scheduled step references an unknown process")
            .take()
            .expect("process state unavailable (reentrant step?)");
        {
            let mut ctx = Ctx {
                now: self.now,
                seed: self.seed,
                tie_break: self.tie_break,
                current_depth: depth,
                rng: &mut self.rng,
                queue: &mut self.queue,
                seq: &mut self.seq,
                next_id: &mut self.next_id,
                processes: &mut self.processes,
                metrics: &mut self.metrics,
            };
            (entry.action)(&mut *state, &mut ctx);
        }
        self.processes
            .get_mut(&pid)
            .expect("process slot vanished during step")
            .replace(state);
        depth
    }
}

/// Capabilities granted to a step while it runs: read the clock, sample
/// randomness, and schedule further work. It deliberately cannot reach any
/// process's state — cross-process interaction is always a scheduled step.
pub struct Ctx<'a> {
    now: Time,
    seed: u64,
    tie_break: TieBreak,
    /// Critical-path depth of the running step; stamped onto steps it schedules.
    current_depth: u64,
    rng: &'a mut StdRng,
    queue: &'a mut BinaryHeap<Reverse<Entry>>,
    seq: &'a mut u64,
    next_id: &'a mut u64,
    processes: &'a mut HashMap<ProcessId, Option<Box<dyn Any>>>,
    metrics: &'a mut SimMetrics,
}

impl<'a> Ctx<'a> {
    /// The time of the currently executing step (the owning process's clock).
    pub fn now(&self) -> Time {
        self.now
    }

    /// Sample from `d` using the simulation's seeded generator.
    pub fn sample<T>(&mut self, d: impl Distribution<T>) -> T {
        d.sample(&mut *self.rng)
    }

    /// Schedule a step on `who` at absolute time `at`. Panics if `at` is before
    /// [`now`](Self::now).
    pub fn schedule<S: 'static>(
        &mut self,
        who: Handle<S>,
        at: Time,
        label: StepLabel,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + 'static,
    ) -> CancelToken {
        enqueue(
            &mut *self.queue,
            &mut *self.seq,
            self.seed,
            self.tie_break,
            &mut *self.metrics,
            self.now,
            self.current_depth,
            who.id,
            at,
            label,
            step,
        )
    }

    /// Like [`schedule`](Self::schedule) but at `now + delay`.
    pub fn schedule_after<S: 'static>(
        &mut self,
        who: Handle<S>,
        delay: Duration,
        label: StepLabel,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + 'static,
    ) -> CancelToken {
        self.schedule(who, self.now + delay, label, step)
    }

    /// Schedule a process-less step on the reserved root process.
    pub fn schedule_global(
        &mut self,
        at: Time,
        label: StepLabel,
        step: impl FnOnce(&mut Ctx<'_>) + 'static,
    ) -> CancelToken {
        enqueue::<()>(
            &mut *self.queue,
            &mut *self.seq,
            self.seed,
            self.tie_break,
            &mut *self.metrics,
            self.now,
            self.current_depth,
            ROOT,
            at,
            label,
            move |_unit, ctx| step(ctx),
        )
    }

    /// Register a new process during a step. The handle is usable immediately
    /// (steps can be scheduled against it), and the process becomes runnable on
    /// the next popped step.
    pub fn spawn<S: 'static>(&mut self, state: S) -> Handle<S> {
        let id = ProcessId(*self.next_id);
        *self.next_id += 1;
        self.processes.insert(id, Some(Box::new(state)));
        Handle {
            id,
            _pd: PhantomData,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn enqueue<S: 'static>(
    queue: &mut BinaryHeap<Reverse<Entry>>,
    seq: &mut u64,
    seed: u64,
    mode: TieBreak,
    metrics: &mut SimMetrics,
    now: Time,
    parent_depth: u64,
    who: ProcessId,
    at: Time,
    label: StepLabel,
    step: impl FnOnce(&mut S, &mut Ctx<'_>) + 'static,
) -> CancelToken {
    assert!(
        at >= now,
        "cannot schedule a step in the past: now={now:?}, at={at:?}"
    );
    let s = *seq;
    *seq += 1;
    let tie_break = match mode {
        TieBreak::Fifo => 0,
        TieBreak::Randomized => tie_break_key(seed, s),
    };
    let priority = label.priority;
    let cancel = CancelToken::new();
    let action: Action = Box::new(move |any, ctx| {
        let state = any
            .downcast_mut::<S>()
            .expect("scheduled step state type does not match its handle");
        step(state, ctx);
    });
    queue.push(Reverse(Entry {
        time: at,
        tie_break,
        priority,
        seq: s,
        process: who,
        parent_depth,
        label,
        cancel: cancel.clone(),
        action,
    }));
    metrics.steps_scheduled += 1;
    metrics.max_queue_len = metrics.max_queue_len.max(queue.len());
    cancel
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use rand::distributions::Uniform;

    use super::*;
    use crate::time::millis;

    #[derive(Default)]
    struct Counter(u64);
    #[derive(Default)]
    struct Log(Vec<u64>);

    #[test]
    fn steps_run_in_time_order_and_advance_clock() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Log::default());
        sim.schedule(h, Time(30), StepLabel::default(), |s, _| s.0.push(30));
        sim.schedule(h, Time(10), StepLabel::default(), |s, _| s.0.push(10));
        sim.schedule(h, Time(20), StepLabel::default(), |s, _| s.0.push(20));

        assert_eq!(sim.run_to_completion(), RunOutcome::Drained);
        sim.with(h, |s| assert_eq!(s.0, vec![10, 20, 30]));
        assert_eq!(sim.now(), Time(30));
        assert_eq!(sim.step_count(), 3);
    }

    #[test]
    fn same_time_steps_run_fifo() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Log::default());
        for v in [1, 2, 3] {
            sim.schedule(h, Time(10), StepLabel::default(), move |s, _| s.0.push(v));
        }
        sim.run_to_completion();
        sim.with(h, |s| assert_eq!(s.0, vec![1, 2, 3]));
    }

    /// Run `n` steps all scheduled at the same time and return the order in
    /// which they executed, under the given tie-break policy and seed.
    fn same_time_order(seed: u64, mode: TieBreak, n: u64) -> Vec<u64> {
        let mut sim = Simulation::new(seed);
        sim.set_tie_break(mode);
        let h = sim.spawn(Log::default());
        for v in 0..n {
            sim.schedule(h, Time(10), StepLabel::default(), move |s, _| s.0.push(v));
        }
        sim.run_to_completion();
        sim.with(h, |s| s.0.clone())
    }

    #[test]
    fn fifo_tie_break_preserves_creation_order() {
        // The default policy: same-time steps run in creation order, regardless
        // of seed — this is what keeps exact-tick assertions stable.
        assert_eq!(
            same_time_order(7, TieBreak::Fifo, 8),
            (0..8).collect::<Vec<_>>()
        );
        assert_eq!(
            same_time_order(7, TieBreak::Fifo, 8),
            same_time_order(99, TieBreak::Fifo, 8)
        );
    }

    #[test]
    fn randomized_tie_break_reorders_but_stays_reproducible() {
        let a = same_time_order(7, TieBreak::Randomized, 8);

        // Reproducible: same (config, seed) yields the same order.
        assert_eq!(a, same_time_order(7, TieBreak::Randomized, 8));
        // It is a permutation of the creation order...
        let mut sorted = a.clone();
        sorted.sort();
        assert_eq!(sorted, (0..8).collect::<Vec<_>>());
        // ...but decoupled from it (not FIFO).
        assert_ne!(a, (0..8).collect::<Vec<_>>());
        // A different seed explores a different ordering (fuzzing).
        assert_ne!(a, same_time_order(8, TieBreak::Randomized, 8));
    }

    #[test]
    fn priority_orders_same_time_steps_under_fifo() {
        // Same-time steps run by ascending priority class (lower first), FIFO
        // within a class — regardless of creation order.
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Log::default());
        for (v, p) in [(0u64, 7u32), (1, 7), (2, 2), (3, 0), (4, 2)] {
            sim.schedule(
                h,
                Time(10),
                StepLabel::default().priority(p),
                move |s, _| s.0.push(v),
            );
        }
        sim.run_to_completion();
        // class 0: [3]; class 2: [2, 4] (FIFO); class 7: [0, 1] (FIFO).
        sim.with(h, |s| assert_eq!(s.0, vec![3, 2, 4, 0, 1]));
    }

    #[test]
    fn randomized_tie_break_ignores_priority() {
        // Under Randomized the per-seq tie-break dominates the priority class, so
        // assigning priorities does not constrain the order: it is identical to
        // the priority-free Randomized order at the same seed (full shuffle).
        let mut sim = Simulation::new(7);
        sim.set_tie_break(TieBreak::Randomized);
        let h = sim.spawn(Log::default());
        for v in 0..8u64 {
            // Later-created steps get higher priority — this WOULD reorder them
            // under Fifo, but must not under Randomized.
            let p = (8 - v) as u32;
            sim.schedule(
                h,
                Time(10),
                StepLabel::default().priority(p),
                move |s, _| s.0.push(v),
            );
        }
        sim.run_to_completion();
        let with_priority = sim.with(h, |s| s.0.clone());
        assert_eq!(with_priority, same_time_order(7, TieBreak::Randomized, 8));
    }

    #[test]
    fn despawn_returns_state_and_skips_pending_steps() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        sim.schedule(h, Time(10), StepLabel::default(), |s, _| s.0 += 5);
        // a step scheduled for after the despawn must be skipped, not panic
        sim.schedule(h, Time(30), StepLabel::default(), |s, _| s.0 += 100);
        sim.run_until_time(Time(20));

        let taken = sim.despawn(h);
        assert_eq!(taken.0, 5);
        assert_eq!(sim.run_to_completion(), RunOutcome::Drained);
    }

    #[test]
    fn cross_process_scheduling() {
        let mut sim = Simulation::new(0);
        let a = sim.spawn(());
        let b = sim.spawn(Counter::default());
        sim.schedule(a, Time(0), StepLabel::default(), move |_, ctx| {
            let now = ctx.now();
            ctx.schedule(b, now, StepLabel::default(), |s, _| s.0 += 1);
        });
        sim.run_to_completion();
        sim.with(b, |s| assert_eq!(s.0, 1));
    }

    #[test]
    fn self_rescheduling_step() {
        fn tick(s: &mut Counter, ctx: &mut Ctx<'_>, me: Handle<Counter>) {
            s.0 += 1;
            if s.0 < 3 {
                ctx.schedule_after(me, millis(1), StepLabel::default(), move |s, ctx| {
                    tick(s, ctx, me)
                });
            }
        }
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        sim.schedule(h, Time(0), StepLabel::default(), move |s, ctx| {
            tick(s, ctx, h)
        });
        sim.run_to_completion();
        sim.with(h, |s| assert_eq!(s.0, 3));
        assert_eq!(sim.now(), Time(0) + millis(2));
    }

    #[test]
    fn cancelled_step_does_not_run() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        let token = sim.schedule(h, Time(10), StepLabel::default(), |s, _| s.0 += 1);
        token.cancel();
        sim.run_to_completion();
        sim.with(h, |s| assert_eq!(s.0, 0));
        assert_eq!(sim.metrics().steps_executed, 0);
        assert_eq!(sim.metrics().steps_cancelled, 1);
    }

    #[test]
    fn rearmed_timer_only_fires_latest() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Log::default());
        let first = sim.schedule(h, Time(10), StepLabel::default(), |s, _| s.0.push(10));
        first.cancel();
        sim.schedule(h, Time(20), StepLabel::default(), |s, _| s.0.push(20));
        sim.run_to_completion();
        sim.with(h, |s| assert_eq!(s.0, vec![20]));
    }

    #[test]
    fn run_until_time_bounds_execution() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Log::default());
        for t in [10, 20, 30] {
            sim.schedule(h, Time(t), StepLabel::default(), move |s, _| {
                s.0.push(t as u64)
            });
        }

        assert_eq!(
            sim.run_until_time(Time(20)),
            RunOutcome::ReachedTime(Time(20))
        );
        assert_eq!(sim.now(), Time(20));
        sim.with(h, |s| assert_eq!(s.0, vec![10, 20]));

        // Advancing to a time before the next step runs nothing but moves the clock.
        assert_eq!(
            sim.run_until_time(Time(25)),
            RunOutcome::ReachedTime(Time(25))
        );
        assert_eq!(sim.now(), Time(25));
        sim.with(h, |s| assert_eq!(s.0, vec![10, 20]));

        assert_eq!(sim.run_until_time(Time(40)), RunOutcome::Drained);
        assert_eq!(sim.now(), Time(40));
        sim.with(h, |s| assert_eq!(s.0, vec![10, 20, 30]));
    }

    #[test]
    fn run_steps_counts_executed_steps() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        for t in 1..=5 {
            sim.schedule(h, Time(t), StepLabel::default(), |s, _| s.0 += 1);
        }
        assert_eq!(sim.run_steps(3), RunOutcome::ReachedStepLimit);
        assert_eq!(sim.step_count(), 3);
        assert_eq!(sim.run_steps(10), RunOutcome::Drained);
        assert_eq!(sim.step_count(), 5);
    }

    #[test]
    fn run_while_observes_process_state() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        for t in 1..=10 {
            sim.schedule(h, Time(t), StepLabel::default(), |s, _| s.0 += 1);
        }
        let outcome = sim.run_while(|sim| sim.with(h, |c| c.0) < 5);
        assert_eq!(outcome, RunOutcome::Stopped);
        assert_eq!(sim.with(h, |c| c.0), 5);

        assert_eq!(
            sim.run_while(|sim| sim.with(h, |c| c.0) < 100),
            RunOutcome::Drained
        );
        assert_eq!(sim.with(h, |c| c.0), 10);
    }

    #[test]
    fn global_step_schedules_onto_process() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        sim.schedule_global(Time(0), StepLabel::default(), move |ctx| {
            let now = ctx.now();
            ctx.schedule(h, now, StepLabel::default(), |s, _| s.0 += 1);
        });
        sim.run_to_completion();
        sim.with(h, |s| assert_eq!(s.0, 1));
    }

    #[test]
    fn spawn_during_step() {
        let mut sim = Simulation::new(0);
        let out = Rc::new(Cell::new(0u64));
        let out2 = out.clone();
        sim.schedule_global(Time(0), StepLabel::default(), move |ctx| {
            let h = ctx.spawn(out2);
            let now = ctx.now();
            ctx.schedule(h, now, StepLabel::default(), |s: &mut Rc<Cell<u64>>, _| {
                s.set(s.get() + 1)
            });
        });
        sim.run_to_completion();
        assert_eq!(out.get(), 1);
    }

    #[test]
    fn deterministic_for_a_given_seed() {
        let run = |seed| {
            let mut sim = Simulation::new(seed);
            let h = sim.spawn(Log::default());
            sim.schedule(h, Time(0), StepLabel::default(), |s, ctx| {
                for _ in 0..8 {
                    s.0.push(ctx.sample(Uniform::new(0u64, 1_000)));
                }
            });
            sim.run_to_completion();
            sim.with(h, |s| s.0.clone())
        };
        assert_eq!(run(7), run(7));
        assert_ne!(run(7), run(8));
    }

    #[test]
    fn observer_sees_every_step() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        let seen = Rc::new(RefCell::new(Vec::new()));
        let sink = seen.clone();
        sim.on_step(move |info| {
            sink.borrow_mut()
                .push((info.time, info.process, info.label.source))
        });
        sim.schedule(h, Time(5), StepLabel::source("a"), |s, _| s.0 += 1);
        sim.schedule(h, Time(7), StepLabel::source("b"), |s, _| s.0 += 1);
        sim.run_to_completion();

        let seen = seen.borrow();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (Time(5), h.id(), Some("a")));
        assert_eq!(seen[1], (Time(7), h.id(), Some("b")));
    }

    #[test]
    fn with_mut_injects_state() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        sim.with_mut(h, |c| c.0 = 42);
        sim.with(h, |c| assert_eq!(c.0, 42));
    }

    #[test]
    fn reset_clears_everything() {
        let mut sim = Simulation::new(1);
        let h = sim.spawn(Counter::default());
        sim.schedule(h, Time(5), StepLabel::default(), |s, _| s.0 += 1);
        sim.run_to_completion();
        assert_eq!(sim.now(), Time(5));

        sim.reset();
        assert_eq!(sim.now(), Time(0));
        assert_eq!(sim.step_count(), 0);
        assert!(sim.peek_time().is_none());
    }

    #[test]
    #[should_panic(expected = "in the past")]
    fn scheduling_in_the_past_panics() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(());
        sim.schedule(h, Time(10), StepLabel::default(), |_, _| {});
        sim.run_to_completion(); // now == Time(10)
        sim.schedule(h, Time(5), StepLabel::default(), |_, _| {});
    }

    #[test]
    fn critical_path_of_serial_chain_equals_its_length() {
        // A self-rescheduling chain on one process: every step depends on the
        // previous, so the critical path is the whole run and parallelism is 1.
        fn tick(s: &mut Counter, ctx: &mut Ctx<'_>, me: Handle<Counter>) {
            s.0 += 1;
            if s.0 < 5 {
                ctx.schedule_after(me, millis(1), StepLabel::default(), move |s, ctx| {
                    tick(s, ctx, me)
                });
            }
        }
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        sim.schedule(h, Time(0), StepLabel::default(), move |s, ctx| {
            tick(s, ctx, h)
        });
        sim.run_to_completion();

        assert_eq!(sim.metrics().steps_executed, 5);
        assert_eq!(sim.metrics().critical_path_len, 5);
        assert_eq!(sim.metrics().parallelism(), 1.0);
    }

    #[test]
    fn critical_path_of_fan_out_is_two() {
        // One step schedules N independent children, each on its own process.
        // Depth: root = 1, every child = 2 (no child depends on another), so the
        // critical path is 2 and parallelism is (N + 1) / 2.
        let n = 5u64;
        let mut sim = Simulation::new(0);
        let handles: Vec<_> = (0..n).map(|_| sim.spawn(Counter::default())).collect();
        sim.schedule_global(Time(0), StepLabel::default(), move |ctx| {
            let now = ctx.now();
            for h in handles {
                ctx.schedule(h, now, StepLabel::default(), |s, _| s.0 += 1);
            }
        });
        sim.run_to_completion();

        assert_eq!(sim.metrics().steps_executed, n + 1);
        assert_eq!(sim.metrics().critical_path_len, 2);
        assert_eq!(sim.metrics().parallelism(), (n + 1) as f64 / 2.0);
    }

    #[test]
    fn same_process_steps_serialize_in_critical_path() {
        // Independent steps scheduled onto one process still serialize (shared
        // state), so the critical path equals the step count.
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        for t in 1..=4 {
            sim.schedule(h, Time(t), StepLabel::default(), |s, _| s.0 += 1);
        }
        sim.run_to_completion();

        assert_eq!(sim.metrics().steps_executed, 4);
        assert_eq!(sim.metrics().critical_path_len, 4);
    }

    #[test]
    fn cancelled_steps_are_off_the_critical_path() {
        let mut sim = Simulation::new(0);
        let h = sim.spawn(Counter::default());
        sim.schedule(h, Time(10), StepLabel::default(), |s, _| s.0 += 1)
            .cancel();
        sim.schedule(h, Time(20), StepLabel::default(), |s, _| s.0 += 1);
        sim.run_to_completion();

        // only the one surviving step contributes
        assert_eq!(sim.metrics().steps_executed, 1);
        assert_eq!(sim.metrics().critical_path_len, 1);
    }

    #[test]
    fn critical_path_is_deterministic() {
        let run = || {
            let mut sim = Simulation::new(0);
            let h = sim.spawn(Counter::default());
            for t in 1..=6 {
                sim.schedule(h, Time(t), StepLabel::default(), |s, _| s.0 += 1);
            }
            sim.run_to_completion();
            sim.metrics().critical_path_len
        };
        assert_eq!(run(), run());
    }
}
