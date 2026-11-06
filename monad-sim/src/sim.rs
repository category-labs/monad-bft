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
//!
//! Processes live in *domains*. A [`Simulation::new`] simulation has a single
//! domain and runs every step on the calling thread. One built with
//! [`Simulation::builder`] and a [`Lookahead`] has several; their *groups* run
//! in parallel, in windows of simulated time no wider than the lookahead
//! between them. Results do not depend on the grouping or the thread count
//! (see `docs/parallel.md`).

use std::{
    any::Any,
    cell::Cell,
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc,
    },
    time::Duration,
};

use rand::{distributions::Distribution, rngs::StdRng, SeedableRng};
use rayon::prelude::*;

use crate::time::Time;

/// Identifies a process registered in a [`Simulation`]. Stable for the lifetime
/// of that process; ids are never reused.
///
/// The id encodes the process's domain (as a *lane*: `domain + 1`, with lane 0
/// for the root process) and its index within the domain. Indices are assigned
/// per domain, so they do not depend on how the execution of domains is
/// interleaved.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ProcessId(u64);

impl ProcessId {
    fn new(lane: u32, index: usize) -> Self {
        let index = u32::try_from(index).expect("too many processes in one domain");
        ProcessId(u64::from(lane) << 32 | u64::from(index))
    }

    fn lane(self) -> u32 {
        (self.0 >> 32) as u32
    }

    fn index(self) -> usize {
        (self.0 & 0xffff_ffff) as usize
    }

    /// The domain the process lives in; `None` for the root process, which owns
    /// global steps.
    pub fn domain(self) -> Option<Domain> {
        self.lane().checked_sub(1).map(Domain)
    }
}

/// The reserved root process that owns process-less ([`Simulation::schedule_global`])
/// steps. Its state is `()`.
const ROOT: ProcessId = ProcessId(0);
const ROOT_LANE: u32 = 0;
/// The lane of the step running on this thread, outside any step.
const NO_LANE: u32 = u32::MAX;
/// The group of the driver, which owns no queue.
const NO_GROUP: usize = usize::MAX;

/// Default target group count of [`Grouping::Auto`]: a constant, not the core
/// count, so the window schedule does not depend on the machine. Groups are
/// rayon tasks; many more groups than threads let work stealing even out
/// uneven domains (measured on the MCP scenarios at `n = 190`: 128 groups
/// were 13–19% faster than 32 on 10 threads, at about the same window width).
pub const DEFAULT_GROUPS: usize = 128;
/// [`Grouping::Auto`] accepts a wider window if it keeps at least this share of
/// the best achievable balance.
const GROUPING_BALANCE: f64 = 0.75;

thread_local! {
    /// The lane whose step runs on this thread, in a simulation with more than
    /// one domain; checked by [`CancelToken::cancel`].
    static CURRENT_LANE: Cell<u32> = const { Cell::new(NO_LANE) };
}

/// Marks this thread as running a step of `lane` while alive, restoring the
/// previous lane on drop (also when the step panics). Only used with several
/// domains.
struct LaneGuard(u32);

impl LaneGuard {
    fn enter(shared: &Shared, lane: u32) -> Option<Self> {
        shared
            .multi_domain()
            .then(|| LaneGuard(CURRENT_LANE.replace(lane)))
    }
}

impl Drop for LaneGuard {
    fn drop(&mut self) {
        CURRENT_LANE.set(self.0);
    }
}

/// A set of processes that always executes on one thread. Steps between
/// processes of different domains are delayed by at least the [`Lookahead`]
/// between the domains; within a domain they may have zero delay, and only
/// there may one process cancel another's steps.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Domain(pub u32);

impl Domain {
    fn lane(self) -> u32 {
        self.0.checked_add(1).expect("domain out of range")
    }
}

/// The declared lower bound on the delay of any step a process in one domain
/// schedules on a process in another. It is a bound on causality, not a latency
/// model; a network layer derives it from its latency model.
pub trait Lookahead {
    /// The number of domains, `0..domains()`.
    fn domains(&self) -> usize;

    /// The minimum delay from `from` to `to`. Must be positive for
    /// `from != to`; the value for `from == to` is ignored (it is 0).
    fn min_delay(&self, from: Domain, to: Domain) -> Duration;
}

/// The same lookahead between every pair of distinct domains.
#[derive(Clone, Copy, Debug)]
pub struct UniformLookahead {
    pub domains: usize,
    pub min_delay: Duration,
}

impl Lookahead for UniformLookahead {
    fn domains(&self) -> usize {
        self.domains
    }

    fn min_delay(&self, _from: Domain, _to: Domain) -> Duration {
        self.min_delay
    }
}

/// A full matrix, rows are senders.
impl Lookahead for Vec<Vec<Duration>> {
    fn domains(&self) -> usize {
        self.len()
    }

    fn min_delay(&self, from: Domain, to: Domain) -> Duration {
        self[from.0 as usize][to.0 as usize]
    }
}

/// How domains are grouped for execution. Each group has one queue and runs
/// as one task per window; the window width is the smallest lookahead between
/// domains in different groups. Results do not depend on the grouping; the
/// window schedule, and so where [`Simulation::run_while`] stops, does.
#[derive(Clone, Debug)]
pub enum Grouping {
    /// Choose groups from the lookahead and the number of processes per domain,
    /// aiming for `groups` balanced groups and the widest window that keeps the
    /// balance close to the best achievable.
    Auto { groups: usize },
    /// Group `i` holds the domains `d` with `assignment[d] == i`.
    Explicit(Vec<usize>),
    /// One group: sequential execution with per-step synchronization.
    Single,
}

impl Default for Grouping {
    fn default() -> Self {
        Grouping::Auto {
            groups: DEFAULT_GROUPS,
        }
    }
}

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
/// observability.
///
/// `priority` is a same-time ordering class (lower runs first); see [`TieBreak`]
/// and `Entry`'s ordering for how it interacts with the tie-break. The default
/// `0` leaves a step in the highest-priority class.
#[derive(Clone, Copy, Debug, Default)]
pub struct StepLabel {
    pub source: Option<&'static str>,
    pub kind: Option<&'static str>,
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

impl From<&'static str> for StepLabel {
    fn from(source: &'static str) -> Self {
        Self::source(source)
    }
}

/// Description of an executed step, passed to the observer and available for
/// run-control predicates.
#[derive(Clone, Debug)]
pub struct StepInfo {
    pub time: Time,
    /// The process that scheduled this step (the root process for steps
    /// scheduled from outside any step).
    pub creator: ProcessId,
    /// How many steps `creator` had scheduled before this one. `(creator,
    /// counter)` identifies the step uniquely.
    pub counter: u64,
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
///
/// A step may only be cancelled from a step of its own domain (where it cannot
/// race with execution), from a global step, or between runs; cancelling a step
/// of another domain panics. To cancel remotely, schedule a step in the target
/// domain that cancels locally.
#[derive(Clone)]
pub struct CancelToken(Arc<CancelState>);

struct CancelState {
    cancelled: AtomicBool,
    /// Lane of the process the step runs on.
    lane: u32,
}

impl CancelToken {
    fn new(lane: u32) -> Self {
        CancelToken(Arc::new(CancelState {
            cancelled: AtomicBool::new(false),
            lane,
        }))
    }

    /// Prevent the associated step from running.
    pub fn cancel(&self) {
        let target = self.0.lane;
        let current = CURRENT_LANE.with(Cell::get);
        assert!(
            current == NO_LANE || current == ROOT_LANE || target == ROOT_LANE || current == target,
            "cannot cancel a step of another domain (lane {target}) from lane {current}; \
             schedule a step there that cancels it"
        );
        self.0.cancelled.store(true, AtomicOrdering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(AtomicOrdering::Relaxed)
    }
}

/// Counters describing what a [`Simulation`] has done. Purely informational.
#[derive(Clone, Debug, Default)]
pub struct SimMetrics {
    pub steps_executed: u64,
    pub steps_scheduled: u64,
    /// Steps skipped because they were cancelled before being popped.
    pub steps_cancelled: u64,
    /// Largest queue length seen; per group, so the only metric that depends on
    /// the grouping.
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

    fn fold(&mut self, other: &SimMetrics) {
        self.steps_executed += other.steps_executed;
        self.steps_scheduled += other.steps_scheduled;
        self.steps_cancelled += other.steps_cancelled;
        self.max_queue_len = self.max_queue_len.max(other.max_queue_len);
        self.critical_path_len = self.critical_path_len.max(other.critical_path_len);
    }
}

/// A queued step with its state type erased, so one heap can hold steps for
/// heterogeneous processes. The closure (built by `new_entry::<S>`) downcasts the
/// erased `&mut dyn Any` back to its scheduling `Handle<S>`'s `S`; that is
/// infallible because a process keeps its state type for life (see [`Handle`]),
/// so the downcast `expect` there is a defensive assert, not a reachable failure.
type Action = Box<dyn FnOnce(&mut dyn Any, &mut Ctx<'_>) + Send>;
type Observer = Box<dyn FnMut(&StepInfo)>;
type Filter = Arc<dyn Fn(&StepInfo) -> bool + Send + Sync>;

/// How same-time steps are ordered.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum TieBreak {
    /// Deterministic FIFO: ties break by creation time, then by the scheduling
    /// process and its schedule counter, which keeps a process's own steps in
    /// creation order. The default, and the only mode that yields exact,
    /// stable ticks.
    #[default]
    Fifo,
    /// Seeded pseudo-random: ties break by a hash of `(seed, creator,
    /// counter)`, decoupled from creation order. Still fully reproducible from
    /// `(config, seed)` — it is a deterministic reshuffle, not true
    /// non-determinism. Sweep the seed to explore different same-time orderings
    /// (fuzzing for order-dependence bugs).
    Randomized,
}

struct Entry {
    time: Time,
    /// `lane << 96 | tie_break << 32 | priority`: the target's lane, the
    /// same-time ordering key (see [`TieBreak`]; always 0 under `Fifo`), and
    /// the same-time priority class (from [`StepLabel::priority`]; lower runs
    /// first), packed so that they compare at once.
    class: u128,
    /// Simulation time at which the step was scheduled.
    created_at: Time,
    /// `rank << 64 | counter`: the scheduling process's [rank](Slot::rank), and
    /// how many steps it had scheduled before.
    id: u128,
    /// The scheduling process. Breaks ties only if two ranks collide.
    creator: ProcessId,
    process: ProcessId,
    /// Critical-path depth of the step that scheduled this one (`0` if scheduled
    /// outside any step). See [`SimMetrics::critical_path_len`].
    parent_depth: u64,
    label: StepLabel,
    cancel: CancelToken,
    action: Action,
}

// Ordering is by (time, lane, tie_break, priority, created_at, creator,
// counter), where `lane` is the target process's domain (0 for global steps)
// and `creator` is the scheduling process's rank. `(creator, counter)` is
// unique (a rank collision falls back to the process id), so the order is
// always total. No component is a global sequence number: each depends only on
// the history of the scheduling process, so the order does not depend on how
// the execution of domains is interleaved.
//
// `creator` is the rank, not the `ProcessId`, because process ids encode the
// domain: comparing them would order the same two steps differently under
// different domain placements. Ranks do not depend on the placement, so
// neither does the order of the steps on any one process.
//
// `lane` right after `time` makes each instant run domain by domain (global
// steps first). Only a domain can add steps to its own current instant
// (zero-delay steps stay in a domain), so a domain's run of an instant is the
// same in whichever group it executes, and the groups' executed steps merge
// back by `(time, lane)` into exactly the one-group order.
//
// Note `tie_break` precedes `priority`:
// - under `TieBreak::Fifo` every `tie_break` is 0, so this reduces to
//   (time, lane, priority, created_at, creator, counter) — priority classes
//   are honored at a tick, earlier-scheduled first within a class, with stable
//   exact ticks;
// - under `TieBreak::Randomized` the per-step `tie_break` is distinct, so it
//   dominates and `priority` is effectively ignored — same-time steps are fully
//   shuffled across classes (priority-respecting within-class fuzzing would put
//   `priority` first instead; deliberately not done here).
//
// The components are packed into `class` and `id` and compared lazily, so most
// comparisons end at `time`.
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time
            .cmp(&other.time)
            .then_with(|| self.class.cmp(&other.class))
            .then_with(|| self.created_at.cmp(&other.created_at))
            .then_with(|| self.id.cmp(&other.id))
            .then_with(|| self.creator.cmp(&other.creator))
    }
}

impl Entry {
    fn creator(&self) -> ProcessId {
        self.creator
    }

    fn counter(&self) -> u64 {
        self.id as u64
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Entry {}

/// SplitMix64 finalizer over `a` and `b`: a well-distributed 64-bit mix.
fn mix(a: u64, b: u64) -> u64 {
    let mut z = b.wrapping_add(a.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Maps a step's identity `(creator rank, counter)`, combined with the
/// simulation seed, to a well-distributed `tie_break`, so same-time steps get a
/// deterministic pseudo-random order under [`TieBreak::Randomized`].
fn tie_break_key(seed: u64, creator_rank: u64, counter: u64) -> u64 {
    mix(mix(seed, creator_rank), counter)
}

/// A process's own generator, seeded from the simulation seed and its rank, so
/// its draws do not depend on what other processes sample, nor on the domain
/// it was placed in.
fn process_rng(seed: u64, rank: u64) -> Box<StdRng> {
    Box::new(StdRng::seed_from_u64(mix(
        seed ^ 0x6a09_e667_f3bc_c909,
        rank,
    )))
}

/// The rank of the root process.
const ROOT_RANK: u64 = 0;

/// The rank of the next process spawned by the process of rank `parent`, which
/// has spawned `spawned` processes so far (counted up here). The root's
/// children are ranked in spawn order, 1, 2, …; other children get a mix of
/// their parent's rank and spawn count, with the top bit set so that they
/// never meet a root child's rank. Either way a rank depends only on the
/// spawning process's history, not on any process's domain.
fn child_rank(parent: u64, spawned: &mut u64) -> u64 {
    *spawned += 1;
    if parent == ROOT_RANK {
        *spawned
    } else {
        1 << 63 | mix(parent, *spawned) >> 1
    }
}

/// The process that schedules a step: its id, and its rank for the step order.
#[derive(Clone, Copy)]
struct Creator {
    id: ProcessId,
    rank: u64,
}

const ROOT_CREATOR: Creator = Creator {
    id: ROOT,
    rank: ROOT_RANK,
};

/// A registered process: its state plus the per-process bookkeeping the
/// scheduler keeps for it.
struct Slot {
    /// `None` while one of its steps runs (the state is lent to the step).
    state: Option<Box<dyn Any + Send>>,
    /// A name for the process that does not depend on its domain (see
    /// [`child_rank`]). Steps it schedules are ordered by it, and its
    /// generator is seeded from it.
    rank: u64,
    /// Processes this process has spawned so far.
    spawned: u64,
    /// Steps this process has scheduled so far; the `counter` of the next one.
    counter: u64,
    /// The process's generator, created on first use.
    rng: Option<Box<StdRng>>,
    /// Critical-path depth of its last executed step; maintains
    /// [`SimMetrics::critical_path_len`] incrementally.
    depth: u64,
}

impl Slot {
    fn new(state: Box<dyn Any + Send>, rank: u64) -> Self {
        Slot {
            state: Some(state),
            rank,
            spawned: 0,
            counter: 0,
            rng: None,
            depth: 0,
        }
    }
}

/// The processes of one domain, indexed by [`ProcessId::index`]; `None` once
/// despawned.
struct DomainSlots {
    lane: u32,
    processes: Vec<Option<Slot>>,
}

/// A unit of execution: whole domains with one queue. Within a window a group
/// owns everything it touches; steps for other groups and global steps wait in
/// its outboxes until the barrier.
struct Group {
    queue: BinaryHeap<Reverse<Entry>>,
    domains: Vec<DomainSlots>,
    /// Steps for other groups, by target group.
    outbox: Vec<Vec<Entry>>,
    /// Global steps scheduled by this group's steps.
    globals: Vec<Entry>,
    metrics: SimMetrics,
    /// Executed steps kept for the observer, only while one is set.
    observed: Vec<StepInfo>,
    /// Time of the last step executed in the current window.
    last_time: Option<Time>,
}

impl Group {
    fn new(groups: usize) -> Self {
        Group {
            queue: BinaryHeap::new(),
            domains: Vec::new(),
            outbox: (0..groups).map(|_| Vec::new()).collect(),
            globals: Vec::new(),
            metrics: SimMetrics::default(),
            observed: Vec::new(),
            last_time: None,
        }
    }

    /// Run every step before `end`. Steps scheduled for other groups land at or
    /// after `end` (the window is no wider than the lookahead between groups),
    /// so the group needs nothing from them until the barrier.
    fn run_window(&mut self, group: usize, shared: &Shared, end: Time) {
        while self
            .queue
            .peek()
            .is_some_and(|Reverse(entry)| entry.time < end)
        {
            let Reverse(entry) = self.queue.pop().expect("peeked");
            if entry.cancel.is_cancelled() {
                self.metrics.steps_cancelled += 1;
                continue;
            }
            let info = run_step(shared, group, self, entry);
            self.last_time = Some(info.time);
            if shared.wants(&info) {
                self.observed.push(info);
            }
        }
    }

    fn purge_cancelled_front(&mut self) {
        while self
            .queue
            .peek()
            .is_some_and(|Reverse(entry)| entry.cancel.is_cancelled())
        {
            self.queue.pop();
            self.metrics.steps_cancelled += 1;
        }
    }
}

/// What every step may read while groups run in parallel. Changes only between
/// runs.
struct Shared {
    seed: u64,
    tie_break: TieBreak,
    /// Domains plus the root lane.
    lanes: usize,
    /// Lookahead in nanoseconds, `lanes × lanes`, rows are senders; empty with
    /// a single domain.
    lookahead: Vec<u64>,
    /// Per lane, the largest lookahead from its domain to any other: how far
    /// ahead a step of the domain must schedule a global step. A global step
    /// synchronizes every group, and any window is no wider than the lookahead
    /// from the domain to some domain in another group.
    global_lookahead: Vec<u64>,
    /// The group and the position within it of each lane's domain.
    group_of: Vec<usize>,
    slot_of: Vec<usize>,
    observe: bool,
    filter: Option<Filter>,
}

impl Shared {
    fn multi_domain(&self) -> bool {
        self.lanes > 2
    }

    fn lookahead(&self, from: u32, to: u32) -> u64 {
        self.lookahead[from as usize * self.lanes + to as usize]
    }

    fn wants(&self, info: &StepInfo) -> bool {
        self.observe && self.filter.as_ref().is_none_or(|filter| filter(info))
    }
}

/// Where the driver runs windows.
enum Threads {
    /// The calling thread runs every group in turn.
    One,
    /// rayon's global pool.
    Global,
    Pool(rayon::ThreadPool),
}

/// The driver's own outboxes, for steps scheduled from global steps and from
/// outside any step. Flushed right after each use.
struct DriverOut {
    outbox: Vec<Vec<Entry>>,
    globals: Vec<Entry>,
    metrics: SimMetrics,
}

/// Configures a [`Simulation`] with several domains; see
/// [`Simulation::builder`].
#[derive(Clone)]
pub struct SimulationBuilder {
    seed: u64,
    domains: usize,
    /// `domains × domains`, nanoseconds, validated.
    lookahead: Vec<u64>,
    grouping: Grouping,
    threads: Option<usize>,
}

impl SimulationBuilder {
    /// Declare the domains and the lookahead between them. Panics unless every
    /// pair of distinct domains has a positive lookahead.
    pub fn lookahead(mut self, lookahead: &impl Lookahead) -> Self {
        let domains = lookahead.domains();
        assert!(
            (1..u32::MAX as usize - 1).contains(&domains),
            "need at least one domain"
        );
        let mut matrix = vec![0; domains * domains];
        for from in 0..domains {
            for to in 0..domains {
                if from == to {
                    continue;
                }
                let delay = lookahead.min_delay(Domain(from as u32), Domain(to as u32));
                assert!(
                    !delay.is_zero(),
                    "the lookahead between distinct domains {from} and {to} must be positive; \
                     processes linked with zero delay belong in one domain"
                );
                matrix[from * domains + to] =
                    u64::try_from(delay.as_nanos()).expect("lookahead fits u64 nanoseconds");
            }
        }
        self.domains = domains;
        self.lookahead = matrix;
        self
    }

    /// How domains are grouped for execution (default [`Grouping::Auto`] with
    /// [`DEFAULT_GROUPS`]). Applied when the simulation first runs, so
    /// [`Grouping::Auto`] sees the processes spawned before then.
    pub fn grouping(mut self, grouping: Grouping) -> Self {
        self.grouping = grouping;
        self
    }

    /// Run windows on a dedicated pool of `threads` threads (`1`: on the calling
    /// thread). Default: rayon's global pool. Results do not depend on it.
    pub fn threads(mut self, threads: usize) -> Self {
        assert!(threads > 0, "need at least one thread");
        self.threads = Some(threads);
        self
    }

    pub fn build(self) -> Simulation {
        let lanes = self.domains + 1;
        let mut lookahead = Vec::new();
        let mut global_lookahead = vec![0; lanes];
        if self.domains > 1 {
            lookahead = vec![0; lanes * lanes];
            for from in 0..self.domains {
                for to in 0..self.domains {
                    let delay = self.lookahead[from * self.domains + to];
                    lookahead[(from + 1) * lanes + to + 1] = delay;
                    global_lookahead[from + 1] = global_lookahead[from + 1].max(delay);
                }
            }
        }
        // Until the first run every domain sits in one group; see `ensure_grouped`.
        let mut group = Group::new(1);
        group.domains = (1..lanes)
            .map(|lane| DomainSlots {
                lane: lane as u32,
                processes: Vec::new(),
            })
            .collect();
        let threads = match self.threads {
            None => Threads::Global,
            Some(1) => Threads::One,
            Some(threads) => Threads::Pool(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("thread pool"),
            ),
        };
        let pending_grouping = (self.domains > 1).then(|| self.grouping.clone());
        Simulation {
            shared: Shared {
                seed: self.seed,
                tie_break: TieBreak::Fifo,
                lanes,
                lookahead,
                global_lookahead,
                group_of: (0..lanes)
                    .map(|lane| if lane == 0 { NO_GROUP } else { 0 })
                    .collect(),
                slot_of: (0..lanes).map(|lane| lane.saturating_sub(1)).collect(),
                observe: false,
                filter: None,
            },
            groups: vec![group],
            globals: BinaryHeap::new(),
            root: Slot::new(Box::new(()), ROOT_RANK),
            driver: DriverOut {
                outbox: vec![Vec::new()],
                globals: Vec::new(),
                metrics: SimMetrics::default(),
            },
            now: Time(0),
            observer: None,
            pending_grouping,
            window: Duration::ZERO,
            threads,
            config: self,
        }
    }
}

/// A deterministic discrete-event simulation: time-ordered queues of steps over
/// a set of framework-owned processes, grouped into domains.
///
/// Construct with [`Simulation::new`] (one domain, sequential) or
/// [`Simulation::builder`] (several domains, parallel), register state with
/// [`spawn`](Self::spawn) / [`spawn_in`](Self::spawn_in), schedule work with
/// [`schedule`](Self::schedule) / [`schedule_global`](Self::schedule_global),
/// then drive it with one of the `run_*` methods.
pub struct Simulation {
    shared: Shared,
    groups: Vec<Group>,
    /// Global steps; they run on the driver, at a barrier, before all other
    /// steps of their instant.
    globals: BinaryHeap<Reverse<Entry>>,
    root: Slot,
    driver: DriverOut,
    now: Time,
    observer: Option<Observer>,
    /// Applied when the simulation first runs.
    pending_grouping: Option<Grouping>,
    /// Window width: the smallest lookahead between domains in different
    /// groups. Unused with one group.
    window: Duration,
    threads: Threads,
    /// For [`reset`](Self::reset).
    config: SimulationBuilder,
}

/// The next step to run: a global step or the head of a group's queue.
#[derive(Clone, Copy)]
enum Source {
    Global,
    Group(usize),
}

impl Simulation {
    /// A fresh, empty simulation with a single domain, seeded for deterministic
    /// randomness. Every step runs on the calling thread. The clock starts at
    /// [`Time(0)`](Time).
    pub fn new(seed: u64) -> Self {
        Simulation::builder(seed).build()
    }

    /// Configure a simulation with several domains (see [`Lookahead`],
    /// [`Grouping`]). Without [`lookahead`](SimulationBuilder::lookahead) it
    /// builds the same single-domain simulation as [`new`](Self::new).
    pub fn builder(seed: u64) -> SimulationBuilder {
        SimulationBuilder {
            seed,
            domains: 1,
            lookahead: vec![0],
            grouping: Grouping::default(),
            threads: None,
        }
    }

    /// Choose how same-time steps are ordered (default [`TieBreak::Fifo`]).
    ///
    /// Set this before scheduling any steps — e.g. immediately after
    /// construction — so the chosen order applies to the whole run. Changing it
    /// mid-run only affects steps enqueued afterwards.
    pub fn set_tie_break(&mut self, tie_break: TieBreak) {
        self.shared.tie_break = tie_break;
    }

    /// The current same-time ordering policy.
    pub fn tie_break(&self) -> TieBreak {
        self.shared.tie_break
    }

    /// Return the simulation to its just-constructed state: all processes and
    /// scheduled steps are discarded, the clock and metrics reset, and the
    /// generators reseeded from the original seed. Handles obtained before the
    /// reset are invalid afterwards.
    pub fn reset(&mut self) {
        *self = self.config.clone().build();
    }

    pub fn seed(&self) -> u64 {
        self.shared.seed
    }

    /// The current global clock time: the time of the most recently executed
    /// step (or the target of the last [`run_until_time`](Self::run_until_time)).
    /// After a window it is the latest time any group reached.
    pub fn now(&self) -> Time {
        self.now
    }

    /// Number of steps executed so far. Cancelled (skipped) steps do not count.
    pub fn step_count(&self) -> u64 {
        self.groups
            .iter()
            .map(|group| group.metrics.steps_executed)
            .sum::<u64>()
            + self.driver.metrics.steps_executed
    }

    /// The metrics, folded over all groups.
    pub fn metrics(&self) -> SimMetrics {
        let mut metrics = self.driver.metrics.clone();
        for group in &self.groups {
            metrics.fold(&group.metrics);
        }
        metrics
    }

    /// The number of execution groups (1 before the first run).
    pub fn groups(&self) -> usize {
        self.groups.len()
    }

    /// The window width with more than one group.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Sample from `d` using the root process's seeded generator (the one
    /// [`schedule_global`](Self::schedule_global) steps draw from).
    pub fn sample<T>(&mut self, d: impl Distribution<T>) -> T {
        let seed = self.shared.seed;
        d.sample(
            &mut **self
                .root
                .rng
                .get_or_insert_with(|| process_rng(seed, ROOT_RANK)),
        )
    }

    /// Register process state in domain 0 and return a handle to it.
    pub fn spawn<S: Send + 'static>(&mut self, state: S) -> Handle<S> {
        self.spawn_in(Domain(0), state)
    }

    /// Register process state in `domain` and return a handle to it.
    pub fn spawn_in<S: Send + 'static>(&mut self, domain: Domain, state: S) -> Handle<S> {
        let lane = domain.lane();
        assert!(
            (lane as usize) < self.shared.lanes,
            "{domain:?} out of range: the simulation has {} domains",
            self.shared.lanes - 1
        );
        let rank = child_rank(self.root.rank, &mut self.root.spawned);
        let slots = self.domain_slots(lane);
        let id = ProcessId::new(lane, slots.processes.len());
        slots.processes.push(Some(Slot::new(Box::new(state), rank)));
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
    pub fn schedule<S: Send + 'static>(
        &mut self,
        who: Handle<S>,
        at: Time,
        label: impl Into<StepLabel>,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + Send + 'static,
    ) -> CancelToken {
        let (entry, cancel) = new_entry(
            &self.shared,
            ROOT_CREATOR,
            &mut self.root.counter,
            self.now,
            0,
            who.id,
            at,
            label.into(),
            step,
        );
        self.push(entry);
        cancel
    }

    /// Like [`schedule`](Self::schedule) but at `now + delay`.
    pub fn schedule_after<S: Send + 'static>(
        &mut self,
        who: Handle<S>,
        delay: Duration,
        label: impl Into<StepLabel>,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + Send + 'static,
    ) -> CancelToken {
        self.schedule(who, self.now + delay, label, step)
    }

    /// Schedule a process-less step (setup, observation, fan-out scheduling)
    /// owned by the reserved root process. The step receives only a [`Ctx`].
    /// Global steps run on the driver, before all other steps of their instant;
    /// with several groups they synchronize all of them.
    ///
    /// Panics if `at` is before [`now`](Self::now).
    pub fn schedule_global(
        &mut self,
        at: Time,
        label: StepLabel,
        step: impl FnOnce(&mut Ctx<'_>) + Send + 'static,
    ) -> CancelToken {
        let (entry, cancel) = new_entry::<()>(
            &self.shared,
            ROOT_CREATOR,
            &mut self.root.counter,
            self.now,
            0,
            ROOT,
            at,
            label,
            move |_unit, ctx| step(ctx),
        );
        self.push(entry);
        cancel
    }

    /// Read the state behind `who`. Intended for use between steps (run-control
    /// predicates, assertions, the debugger).
    ///
    /// Panics if `who` is not a live process, its state is unavailable because a
    /// step on it is currently executing, or its state type does not match `S`.
    pub fn with<S: 'static, R>(&self, who: Handle<S>, f: impl FnOnce(&S) -> R) -> R {
        let lane = who.id.lane() as usize;
        let state = self.groups[self.shared.group_of[lane]].domains[self.shared.slot_of[lane]]
            .processes
            .get(who.id.index())
            .and_then(Option::as_ref)
            .expect("unknown process handle")
            .state
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
            .domain_slots(who.id.lane())
            .processes
            .get_mut(who.id.index())
            .and_then(Option::as_mut)
            .expect("unknown process handle")
            .state
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
            .domain_slots(who.id.lane())
            .processes
            .get_mut(who.id.index())
            .and_then(Option::take)
            .expect("unknown process handle")
            .state
            .expect("process state unavailable (despawn during its own step?)");
        *boxed
            .downcast::<S>()
            .unwrap_or_else(|_| panic!("handle/state type mismatch"))
    }

    /// Register a callback invoked after every executed step, in the canonical
    /// order: exactly the order of a single-group run. Replaces any previously
    /// registered observer. With several groups, executed steps are buffered
    /// and handed over at each barrier; the observer runs on the calling
    /// thread.
    pub fn on_step(&mut self, observer: impl FnMut(&StepInfo) + 'static) {
        self.observer = Some(Box::new(observer));
        self.shared.observe = true;
        self.shared.filter = None;
    }

    /// Like [`on_step`](Self::on_step), but only for steps `filter` accepts.
    /// The filter runs where the step ran, so with several groups only the
    /// matching steps are buffered.
    pub fn on_step_filtered(
        &mut self,
        filter: impl Fn(&StepInfo) -> bool + Send + Sync + 'static,
        observer: impl FnMut(&StepInfo) + 'static,
    ) {
        self.observer = Some(Box::new(observer));
        self.shared.observe = true;
        self.shared.filter = Some(Arc::new(filter));
    }

    /// Time of the next queued step, ignoring whether it is cancelled. `None`
    /// when the queue is empty.
    pub fn peek_time(&self) -> Option<Time> {
        let heads = self.groups.iter().filter_map(|group| group.queue.peek());
        self.globals
            .peek()
            .into_iter()
            .chain(heads)
            .map(|Reverse(entry)| entry.time)
            .min()
    }

    /// Execute the next step in the canonical order (skipping cancelled ones),
    /// returning its [`StepInfo`], or `None` if no step is queued. Exact with any
    /// number of groups: the step runs on the calling thread.
    pub fn next_step(&mut self) -> Option<StepInfo> {
        self.ensure_grouped();
        loop {
            let source = self.next_source()?;
            let entry = match source {
                Source::Global => self.globals.pop(),
                Source::Group(group) => self.groups[group].queue.pop(),
            }
            .expect("peeked")
            .0;
            if entry.cancel.is_cancelled() {
                match source {
                    Source::Global => self.driver.metrics.steps_cancelled += 1,
                    Source::Group(group) => self.groups[group].metrics.steps_cancelled += 1,
                }
                continue;
            }
            let info = match source {
                Source::Global => self.run_global(entry),
                Source::Group(group) => {
                    let info = run_step(&self.shared, group, &mut self.groups[group], entry);
                    self.flush_group(group);
                    info
                }
            };
            self.now = info.time;
            if let Some(observer) = self.observer.as_mut() {
                if self.shared.wants(&info) {
                    observer(&info);
                }
            }
            return Some(info);
        }
    }

    /// Run until the queue is empty.
    pub fn run_to_completion(&mut self) -> RunOutcome {
        if self.windowed() {
            return self.run_windows(None, &mut |_| false);
        }
        while self.next_step().is_some() {}
        RunOutcome::Drained
    }

    /// Run every step scheduled at time `<= t`, then advance the clock to `t`
    /// (never backwards). Returns [`RunOutcome::Drained`] if the queue emptied,
    /// otherwise [`RunOutcome::ReachedTime`].
    pub fn run_until_time(&mut self, t: Time) -> RunOutcome {
        let outcome = if self.windowed() {
            self.run_windows(Some(t), &mut |_| false)
        } else {
            loop {
                // Drop cancelled entries first so the time bound is applied to the
                // next step that will actually run, not to a skipped one.
                self.purge_cancelled_front();
                match self.peek_time() {
                    Some(next) if next <= t => {
                        self.next_step();
                    }
                    Some(_) => break RunOutcome::ReachedTime(t),
                    None => break RunOutcome::Drained,
                }
            }
        };
        self.advance_to(t);
        outcome
    }

    /// Run exactly `n` steps (fewer if the queue empties), on the calling thread.
    /// Returns [`RunOutcome::Drained`] if the queue emptied first, otherwise
    /// [`RunOutcome::ReachedStepLimit`]. See
    /// [`run_at_least_steps`](Self::run_at_least_steps) for large `n`.
    pub fn run_steps(&mut self, n: u64) -> RunOutcome {
        for _ in 0..n {
            if self.next_step().is_none() {
                return RunOutcome::Drained;
            }
        }
        RunOutcome::ReachedStepLimit
    }

    /// Run at least `n` steps, stopping at the first synchronization point after
    /// them: exactly `n` with one group, the end of a window with several.
    /// Returns [`RunOutcome::Drained`] if the queue emptied first, otherwise
    /// [`RunOutcome::ReachedStepLimit`].
    pub fn run_at_least_steps(&mut self, n: u64) -> RunOutcome {
        if !self.windowed() {
            return self.run_steps(n);
        }
        let target = self.step_count() + n;
        match self.run_windows(None, &mut |sim| sim.step_count() >= target) {
            RunOutcome::Stopped => RunOutcome::ReachedStepLimit,
            outcome => outcome,
        }
    }

    /// Run while `pred` holds, evaluating it at every synchronization point:
    /// before each step with one group, between windows with several. `pred` may
    /// inspect process state (via [`with`](Self::with)) and the clock. Returns
    /// [`RunOutcome::Stopped`] when `pred` returns `false`, or
    /// [`RunOutcome::Drained`] if the queue empties first.
    ///
    /// The synchronization points depend on the grouping, never on the thread
    /// count, so the state at which a run stops is the same on any number of
    /// threads.
    pub fn run_while(&mut self, mut pred: impl FnMut(&Simulation) -> bool) -> RunOutcome {
        if self.windowed() {
            return self.run_windows(None, &mut |sim| !pred(sim));
        }
        while pred(&*self) {
            if self.next_step().is_none() {
                return RunOutcome::Drained;
            }
        }
        RunOutcome::Stopped
    }

    /// Run to the next synchronization point: one step with one group, one
    /// window (or one global step) with several. Returns `false` if no step was
    /// queued. A building block for run loops whose stop condition needs more
    /// than `&Simulation`.
    pub fn run_window(&mut self) -> bool {
        if !self.windowed() {
            return self.next_step().is_some();
        }
        let mut first = true;
        let outcome = self.run_windows(None, &mut |_| !std::mem::take(&mut first));
        outcome != RunOutcome::Drained
    }

    fn advance_to(&mut self, t: Time) {
        if t > self.now {
            self.now = t;
        }
    }

    /// Whether runs execute in windows: more than one group, once grouped.
    fn windowed(&mut self) -> bool {
        self.ensure_grouped();
        self.groups.len() > 1
    }

    /// Run windows until `stop` holds at a barrier, the queue empties, or (with
    /// `until`) the next step is after `until`.
    fn run_windows(
        &mut self,
        until: Option<Time>,
        stop: &mut dyn FnMut(&Simulation) -> bool,
    ) -> RunOutcome {
        loop {
            if stop(self) {
                return RunOutcome::Stopped;
            }
            self.purge_cancelled_front();
            let Some(start) = self.peek_time() else {
                return RunOutcome::Drained;
            };
            if let Some(t) = until.filter(|t| start > *t) {
                return RunOutcome::ReachedTime(t);
            }
            // A global step runs first at its instant, on the driver.
            if self
                .globals
                .peek()
                .is_some_and(|Reverse(entry)| entry.time == start)
            {
                self.next_step();
                continue;
            }
            let mut end = start + self.window;
            if let Some(Reverse(global)) = self.globals.peek() {
                end = end.min(global.time);
            }
            if let Some(t) = until {
                end = end.min(Time(t.0 + 1));
            }
            self.run_groups(end);
            self.exchange();
            self.deliver_observed();
            for group in &mut self.groups {
                if let Some(last) = group.last_time.take() {
                    self.now = self.now.max(last);
                }
            }
        }
    }

    fn run_groups(&mut self, end: Time) {
        let shared = &self.shared;
        let groups = &mut self.groups;
        let run = |(index, group): (usize, &mut Group)| group.run_window(index, shared, end);
        match &self.threads {
            Threads::One => groups.iter_mut().enumerate().for_each(run),
            Threads::Global => groups.par_iter_mut().enumerate().for_each(run),
            Threads::Pool(pool) => pool.install(|| groups.par_iter_mut().enumerate().for_each(run)),
        }
    }

    /// Hand the steps executed in the last window to the observer, merged into
    /// the canonical order. A group's steps are ordered by `(time, lane)`, and
    /// a lane belongs to one group, so a stable sort by `(time, lane)`
    /// interleaves them exactly as a single group would have run them.
    fn deliver_observed(&mut self) {
        let Some(observer) = self.observer.as_mut() else {
            return;
        };
        let mut steps: Vec<StepInfo> = self
            .groups
            .iter_mut()
            .flat_map(|group| group.observed.drain(..))
            .collect();
        steps.sort_by_key(|info| (info.time, info.process.lane()));
        for info in &steps {
            observer(info);
        }
    }

    fn purge_cancelled_front(&mut self) {
        for group in &mut self.groups {
            group.purge_cancelled_front();
        }
        while self
            .globals
            .peek()
            .is_some_and(|Reverse(entry)| entry.cancel.is_cancelled())
        {
            self.globals.pop();
            self.driver.metrics.steps_cancelled += 1;
        }
    }

    /// The source of the next step in the canonical order: the smallest head.
    fn next_source(&self) -> Option<Source> {
        let mut best = self
            .globals
            .peek()
            .map(|Reverse(entry)| (entry, Source::Global));
        for (index, group) in self.groups.iter().enumerate() {
            if let Some(Reverse(head)) = group.queue.peek() {
                if best.as_ref().is_none_or(|(entry, _)| head < *entry) {
                    best = Some((head, Source::Group(index)));
                }
            }
        }
        best.map(|(_, source)| source)
    }

    fn domain_slots(&mut self, lane: u32) -> &mut DomainSlots {
        let lane = lane as usize;
        &mut self.groups[self.shared.group_of[lane]].domains[self.shared.slot_of[lane]]
    }

    /// Queue a step scheduled from outside any step.
    fn push(&mut self, entry: Entry) {
        self.driver.metrics.steps_scheduled += 1;
        let lane = entry.process.lane() as usize;
        if lane == ROOT_LANE as usize {
            self.globals.push(Reverse(entry));
            return;
        }
        let group = &mut self.groups[self.shared.group_of[lane]];
        group.queue.push(Reverse(entry));
        group.metrics.max_queue_len = group.metrics.max_queue_len.max(group.queue.len());
    }

    /// Run a global step on the driver.
    fn run_global(&mut self, entry: Entry) -> StepInfo {
        let metrics = &mut self.driver.metrics;
        metrics.steps_executed += 1;
        let depth = entry.parent_depth.max(self.root.depth) + 1;
        self.root.depth = depth;
        metrics.critical_path_len = metrics.critical_path_len.max(depth);
        let lane = Domain(0).lane() as usize;
        let home = &mut self.groups[self.shared.group_of[lane]].domains[self.shared.slot_of[lane]];
        let guard = LaneGuard::enter(&self.shared, ROOT_LANE);
        let mut ctx = Ctx {
            now: entry.time,
            me: ROOT,
            rank: ROOT_RANK,
            depth,
            counter: self.root.counter,
            spawned: self.root.spawned,
            rng: self.root.rng.take(),
            shared: &self.shared,
            group: NO_GROUP,
            queue: None,
            outbox: &mut self.driver.outbox,
            globals: &mut self.driver.globals,
            metrics,
            home,
        };
        let info = StepInfo {
            time: entry.time,
            creator: entry.creator(),
            counter: entry.counter(),
            process: ROOT,
            label: entry.label,
            depth,
        };
        (entry.action)(&mut (), &mut ctx);
        let Ctx {
            counter,
            spawned,
            rng,
            ..
        } = ctx;
        drop(guard);
        self.root.counter = counter;
        self.root.spawned = spawned;
        self.root.rng = rng;
        self.flush_driver();
        info
    }

    /// Move the steps a group scheduled for other groups and for the driver to
    /// their queues.
    fn flush_group(&mut self, source: usize) {
        for target in 0..self.groups.len() {
            if self.groups[source].outbox[target].is_empty() {
                continue;
            }
            let mut entries = std::mem::take(&mut self.groups[source].outbox[target]);
            let group = &mut self.groups[target];
            group.queue.extend(entries.drain(..).map(Reverse));
            group.metrics.max_queue_len = group.metrics.max_queue_len.max(group.queue.len());
            self.groups[source].outbox[target] = entries;
        }
        let globals = &mut self.groups[source].globals;
        self.globals.extend(globals.drain(..).map(Reverse));
    }

    fn flush_driver(&mut self) {
        for (target, entries) in self.driver.outbox.iter_mut().enumerate() {
            let group = &mut self.groups[target];
            group.queue.extend(entries.drain(..).map(Reverse));
            group.metrics.max_queue_len = group.metrics.max_queue_len.max(group.queue.len());
        }
        self.globals
            .extend(self.driver.globals.drain(..).map(Reverse));
    }

    /// The barrier's exchange: every outbox to its target queue.
    fn exchange(&mut self) {
        for source in 0..self.groups.len() {
            self.flush_group(source);
        }
    }

    /// Apply the configured grouping before the first run.
    fn ensure_grouped(&mut self) {
        if let Some(grouping) = self.pending_grouping.take() {
            self.regroup(grouping);
        }
    }

    fn regroup(&mut self, grouping: Grouping) {
        let domains = self.shared.lanes - 1;
        let assignment = match grouping {
            Grouping::Single => vec![0; domains],
            Grouping::Explicit(assignment) => {
                assert_eq!(
                    assignment.len(),
                    domains,
                    "an explicit grouping assigns every domain"
                );
                assignment
            }
            Grouping::Auto { groups } => {
                let weights: Vec<u64> = (1..=domains as u32)
                    .map(|lane| {
                        self.domain_slots(lane)
                            .processes
                            .iter()
                            .filter(|slot| slot.is_some())
                            .count() as u64
                    })
                    .collect();
                auto_grouping(&self.shared, &weights, groups)
            }
        };
        // Renumber groups densely, in order of their first domain.
        let mut renumber = std::collections::BTreeMap::new();
        let assignment: Vec<usize> = assignment
            .into_iter()
            .map(|group| {
                let next = renumber.len();
                *renumber.entry(group).or_insert(next)
            })
            .collect();
        let count = renumber.len();

        let mut slots: Vec<Option<DomainSlots>> = (0..domains).map(|_| None).collect();
        let mut entries = Vec::new();
        for mut group in std::mem::take(&mut self.groups) {
            self.driver.metrics.fold(&group.metrics);
            for domain in group.domains.drain(..) {
                let index = domain.lane as usize - 1;
                slots[index] = Some(domain);
            }
            entries.extend(group.queue.drain().map(|Reverse(entry)| entry));
        }
        self.groups = (0..count).map(|_| Group::new(count)).collect();
        for (index, slot) in slots.into_iter().enumerate() {
            let group = assignment[index];
            let lane = index + 1;
            self.shared.group_of[lane] = group;
            self.shared.slot_of[lane] = self.groups[group].domains.len();
            self.groups[group]
                .domains
                .push(slot.expect("every domain is in a group"));
        }
        for entry in entries {
            let group = self.shared.group_of[entry.process.lane() as usize];
            self.groups[group].queue.push(Reverse(entry));
        }
        for group in &mut self.groups {
            group.metrics.max_queue_len = group.queue.len();
        }
        self.driver.outbox = (0..count).map(|_| Vec::new()).collect();
        let mut window = u64::MAX;
        for from in 1..=domains {
            for to in 1..=domains {
                if self.shared.group_of[from] != self.shared.group_of[to] {
                    window = window.min(self.shared.lookahead(from as u32, to as u32));
                }
            }
        }
        self.window = if count > 1 {
            Duration::from_nanos(window)
        } else {
            Duration::ZERO
        };
    }
}

/// [`Grouping::Auto`]: the widest window whose grouping keeps the balance
/// within [`GROUPING_BALANCE`] of the best achievable.
///
/// Domains closer than a window width must share a group, so each candidate
/// window induces components (single-linkage clustering of the domains). The
/// components are packed into `groups` bins, largest first; the achievable
/// speedup is the total weight over the heaviest bin. Walking the domain pairs
/// by increasing distance merges components one pair at a time; before each
/// merge, the current components allow a window of that pair's distance.
fn auto_grouping(shared: &Shared, weights: &[u64], groups: usize) -> Vec<usize> {
    let domains = weights.len();
    let total: u64 = weights.iter().sum();
    let bins = groups.min(domains).max(1);
    if bins == 1 || total == 0 {
        return vec![0; domains];
    }
    let mut pairs = Vec::with_capacity(domains * (domains - 1) / 2);
    for a in 0..domains {
        for b in a + 1..domains {
            let (la, lb) = (a as u32 + 1, b as u32 + 1);
            let distance = shared.lookahead(la, lb).min(shared.lookahead(lb, la));
            pairs.push((distance, a, b));
        }
    }
    pairs.sort_unstable();

    let mut parent: Vec<usize> = (0..domains).collect();
    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }

    let mut best_speedup = None;
    let mut chosen = vec![0; domains];
    for (_, a, b) in pairs {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra == rb {
            continue;
        }
        let components: Vec<usize> = (0..domains).map(|d| find(&mut parent, d)).collect();
        let (assignment, heaviest) = pack(&components, weights, bins);
        let speedup = total as f64 / heaviest as f64;
        // The first evaluation, on singleton components, is the best balance.
        let best = *best_speedup.get_or_insert(speedup);
        if speedup >= GROUPING_BALANCE * best {
            chosen = assignment;
        }
        parent[ra.max(rb)] = ra.min(rb);
    }
    chosen
}

/// Pack components (given as each domain's component root) into `bins` bins,
/// heaviest first, each into the lightest bin. Returns each domain's bin and the
/// heaviest bin's weight.
fn pack(components: &[usize], weights: &[u64], bins: usize) -> (Vec<usize>, u64) {
    let mut weight_of = std::collections::BTreeMap::new();
    for (domain, &root) in components.iter().enumerate() {
        *weight_of.entry(root).or_insert(0) += weights[domain];
    }
    let mut order: Vec<(u64, usize)> = weight_of.iter().map(|(&root, &w)| (w, root)).collect();
    order.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut load = vec![0u64; bins];
    let mut bin_of = std::collections::BTreeMap::new();
    for (weight, root) in order {
        let bin = (0..bins).min_by_key(|&bin| (load[bin], bin)).expect("bins");
        load[bin] += weight;
        bin_of.insert(root, bin);
    }
    let assignment = components.iter().map(|root| bin_of[root]).collect();
    (assignment, load.into_iter().max().unwrap_or(0).max(1))
}

/// Run one step of `group`'s domain on the current thread.
fn run_step(shared: &Shared, index: usize, group: &mut Group, entry: Entry) -> StepInfo {
    let Group {
        queue,
        domains,
        outbox,
        globals,
        metrics,
        ..
    } = group;
    metrics.steps_executed += 1;
    let pid = entry.process;
    let lane = pid.lane();
    let mut info = StepInfo {
        time: entry.time,
        creator: entry.creator(),
        counter: entry.counter(),
        process: pid,
        label: entry.label,
        depth: 0,
    };
    let home = &mut domains[shared.slot_of[lane as usize]];
    // A step may be left over for a process that was despawned after it was
    // scheduled; skip it (it does no work, so it is off the critical path).
    let Some(Some(slot)) = home.processes.get_mut(pid.index()) else {
        return info;
    };
    // Critical-path depth: one past the later of the step that scheduled this
    // one and the previous step on this process.
    let depth = entry.parent_depth.max(slot.depth) + 1;
    slot.depth = depth;
    metrics.critical_path_len = metrics.critical_path_len.max(depth);
    // Take the process's state, counter and generator out so `Ctx` can borrow
    // the domain (for spawns) without aliasing them.
    let mut state = slot
        .state
        .take()
        .expect("process state unavailable (reentrant step?)");
    let (rank, counter, spawned) = (slot.rank, slot.counter, slot.spawned);
    let rng = slot.rng.take();
    let guard = LaneGuard::enter(shared, lane);
    let mut ctx = Ctx {
        now: entry.time,
        me: pid,
        rank,
        depth,
        counter,
        spawned,
        rng,
        shared,
        group: index,
        queue: Some(queue),
        outbox,
        globals,
        metrics,
        home,
    };
    (entry.action)(&mut *state, &mut ctx);
    let Ctx {
        counter,
        spawned,
        rng,
        home,
        ..
    } = ctx;
    drop(guard);
    let slot = home.processes[pid.index()]
        .as_mut()
        .expect("process slot vanished during step");
    slot.state = Some(state);
    slot.counter = counter;
    slot.spawned = spawned;
    slot.rng = rng;
    info.depth = depth;
    info
}

/// Capabilities granted to a step while it runs: read the clock, sample
/// randomness, and schedule further work. It deliberately cannot reach any
/// process's state — cross-process interaction is always a scheduled step.
pub struct Ctx<'a> {
    now: Time,
    /// The running process and its rank, the critical-path depth of the running
    /// step (stamped onto steps it schedules), and the process's schedule and
    /// spawn counters and generator (taken out of its slot for the duration of
    /// the step).
    me: ProcessId,
    rank: u64,
    depth: u64,
    counter: u64,
    spawned: u64,
    rng: Option<Box<StdRng>>,
    shared: &'a Shared,
    /// The running step's group and its queue; `NO_GROUP` and `None` on the
    /// driver, whose steps all go through the outboxes.
    group: usize,
    queue: Option<&'a mut BinaryHeap<Reverse<Entry>>>,
    outbox: &'a mut [Vec<Entry>],
    globals: &'a mut Vec<Entry>,
    metrics: &'a mut SimMetrics,
    /// The domain new processes are spawned in: the running process's (domain 0
    /// for global steps).
    home: &'a mut DomainSlots,
}

impl<'a> Ctx<'a> {
    /// The time of the currently executing step (the owning process's clock).
    pub fn now(&self) -> Time {
        self.now
    }

    /// Sample from `d` using the running process's own seeded generator.
    pub fn sample<T>(&mut self, d: impl Distribution<T>) -> T {
        let (seed, rank) = (self.shared.seed, self.rank);
        d.sample(&mut **self.rng.get_or_insert_with(|| process_rng(seed, rank)))
    }

    /// Schedule a step on `who` at absolute time `at`. Panics if `at` is before
    /// [`now`](Self::now), or if `who` is in another domain and `at` is closer
    /// than the lookahead to it.
    pub fn schedule<S: Send + 'static>(
        &mut self,
        who: Handle<S>,
        at: Time,
        label: StepLabel,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + Send + 'static,
    ) -> CancelToken {
        self.check_lookahead(who.id, at);
        let (entry, cancel) = new_entry(
            self.shared,
            self.creator(),
            &mut self.counter,
            self.now,
            self.depth,
            who.id,
            at,
            label,
            step,
        );
        self.push(entry);
        cancel
    }

    /// Like [`schedule`](Self::schedule) but at `now + delay`.
    pub fn schedule_after<S: Send + 'static>(
        &mut self,
        who: Handle<S>,
        delay: Duration,
        label: StepLabel,
        step: impl FnOnce(&mut S, &mut Ctx<'_>) + Send + 'static,
    ) -> CancelToken {
        self.schedule(who, self.now + delay, label, step)
    }

    /// Schedule a process-less step on the reserved root process. With several
    /// domains, a step of a domain must schedule it at least the largest
    /// lookahead from its domain ahead: a global step synchronizes every
    /// domain.
    pub fn schedule_global(
        &mut self,
        at: Time,
        label: StepLabel,
        step: impl FnOnce(&mut Ctx<'_>) + Send + 'static,
    ) -> CancelToken {
        self.check_lookahead(ROOT, at);
        let (entry, cancel) = new_entry::<()>(
            self.shared,
            self.creator(),
            &mut self.counter,
            self.now,
            self.depth,
            ROOT,
            at,
            label,
            move |_unit, ctx| step(ctx),
        );
        self.push(entry);
        cancel
    }

    /// Register a new process in the running process's domain (domain 0 from a
    /// global step). The handle is usable immediately (steps can be scheduled
    /// against it), and the process becomes runnable on the next popped step.
    pub fn spawn<S: Send + 'static>(&mut self, state: S) -> Handle<S> {
        let rank = child_rank(self.rank, &mut self.spawned);
        let id = ProcessId::new(self.home.lane, self.home.processes.len());
        self.home
            .processes
            .push(Some(Slot::new(Box::new(state), rank)));
        Handle {
            id,
            _pd: PhantomData,
        }
    }

    fn creator(&self) -> Creator {
        Creator {
            id: self.me,
            rank: self.rank,
        }
    }

    fn check_lookahead(&self, target: ProcessId, at: Time) {
        let (from, to) = (self.me.lane(), target.lane());
        if from == to || from == ROOT_LANE || !self.shared.multi_domain() {
            return;
        }
        let needed = if to == ROOT_LANE {
            self.shared.global_lookahead[from as usize]
        } else {
            self.shared.lookahead(from, to)
        };
        let delay = at.0 - self.now.0;
        assert!(
            delay >= i128::from(needed),
            "step from {:?} to {:?} scheduled {delay}ns ahead, below the lookahead of {needed}ns \
             between their domains",
            self.me,
            target,
        );
    }

    fn push(&mut self, entry: Entry) {
        self.metrics.steps_scheduled += 1;
        let lane = entry.process.lane() as usize;
        if lane == ROOT_LANE as usize {
            self.globals.push(entry);
            return;
        }
        let group = self.shared.group_of[lane];
        match self.queue.as_deref_mut() {
            Some(queue) if group == self.group => {
                queue.push(Reverse(entry));
                self.metrics.max_queue_len = self.metrics.max_queue_len.max(queue.len());
            }
            _ => self.outbox[group].push(entry),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn new_entry<S: Send + 'static>(
    shared: &Shared,
    creator: Creator,
    counter: &mut u64,
    now: Time,
    parent_depth: u64,
    who: ProcessId,
    at: Time,
    label: StepLabel,
    step: impl FnOnce(&mut S, &mut Ctx<'_>) + Send + 'static,
) -> (Entry, CancelToken) {
    assert!(
        at >= now,
        "cannot schedule a step in the past: now={now:?}, at={at:?}"
    );
    let c = *counter;
    *counter += 1;
    let tie_break = match shared.tie_break {
        TieBreak::Fifo => 0,
        TieBreak::Randomized => tie_break_key(shared.seed, creator.rank, c),
    };
    let cancel = CancelToken::new(who.lane());
    let action: Action = Box::new(move |any, ctx| {
        let state = any
            .downcast_mut::<S>()
            .expect("scheduled step state type does not match its handle");
        step(state, ctx);
    });
    let entry = Entry {
        time: at,
        class: u128::from(who.lane()) << 96
            | u128::from(tie_break) << 32
            | u128::from(label.priority),
        created_at: now,
        id: u128::from(creator.rank) << 64 | u128::from(c),
        creator: creator.id,
        process: who,
        parent_depth,
        label,
        cancel: cancel.clone(),
        action,
    };
    (entry, cancel)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::atomic::AtomicU64};

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
        // Under Randomized the per-step tie-break dominates the priority class, so
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
        let out = Arc::new(AtomicU64::new(0));
        let out2 = out.clone();
        sim.schedule_global(Time(0), StepLabel::default(), move |ctx| {
            let h = ctx.spawn(out2);
            let now = ctx.now();
            ctx.schedule(h, now, StepLabel::default(), |s: &mut Arc<AtomicU64>, _| {
                s.fetch_add(1, AtomicOrdering::Relaxed);
            });
        });
        sim.run_to_completion();
        assert_eq!(out.load(AtomicOrdering::Relaxed), 1);
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
    fn a_process_draws_independently_of_other_processes() {
        let run = |others_sample: bool| {
            let mut sim = Simulation::new(7);
            let other = sim.spawn(Log::default());
            let h = sim.spawn(Log::default());
            for t in 0..4 {
                if others_sample {
                    sim.schedule(other, Time(t), StepLabel::default(), |s, ctx| {
                        s.0.push(ctx.sample(Uniform::new(0u64, 1_000)))
                    });
                }
                sim.schedule(h, Time(t), StepLabel::default(), |s, ctx| {
                    s.0.push(ctx.sample(Uniform::new(0u64, 1_000)))
                });
            }
            sim.run_to_completion();
            sim.with(h, |s| s.0.clone())
        };
        assert_eq!(run(false), run(true));
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

#[cfg(test)]
mod domain_tests {
    use super::*;
    use crate::time::millis;

    #[derive(Default)]
    struct Counter(u64);

    // two domains 1 ms apart, a third 5 ms from both
    fn three_domains() -> Vec<Vec<Duration>> {
        vec![
            vec![millis(0), millis(1), millis(5)],
            vec![millis(1), millis(0), millis(5)],
            vec![millis(5), millis(5), millis(0)],
        ]
    }

    fn sim(grouping: Grouping) -> Simulation {
        Simulation::builder(0)
            .lookahead(&three_domains())
            .grouping(grouping)
            .threads(1)
            .build()
    }

    #[test]
    fn processes_know_their_domain() {
        let mut sim = sim(Grouping::Single);
        let a = sim.spawn_in(Domain(2), Counter::default());
        let b = sim.spawn(Counter::default());
        assert_eq!(a.id().domain(), Some(Domain(2)));
        assert_eq!(b.id().domain(), Some(Domain(0)));
        assert_ne!(a.id(), b.id());
    }

    #[test]
    #[should_panic(expected = "must be positive")]
    fn zero_lookahead_between_domains_is_rejected() {
        let mut lookahead = three_domains();
        lookahead[0][1] = Duration::ZERO;
        Simulation::builder(0).lookahead(&lookahead);
    }

    #[test]
    #[should_panic(expected = "below the lookahead")]
    fn cross_domain_steps_respect_the_lookahead() {
        let mut sim = sim(Grouping::Single);
        let a = sim.spawn_in(Domain(0), Counter::default());
        let b = sim.spawn_in(Domain(1), Counter::default());
        sim.schedule(a, Time(0), "kick", move |_, ctx| {
            let at = ctx.now() + Duration::from_micros(999);
            ctx.schedule(b, at, StepLabel::default(), |s, _| s.0 += 1);
        });
        sim.run_to_completion();
    }

    #[test]
    fn steps_at_the_lookahead_and_within_a_domain_are_fine() {
        let mut sim = sim(Grouping::Explicit(vec![0, 1, 1]));
        let a = sim.spawn_in(Domain(0), Counter::default());
        let a2 = sim.spawn_in(Domain(0), Counter::default());
        let b = sim.spawn_in(Domain(1), Counter::default());
        sim.schedule(a, Time(0), "kick", move |_, ctx| {
            let now = ctx.now();
            ctx.schedule(a2, now, StepLabel::default(), |s, _| s.0 += 1);
            ctx.schedule(b, now + millis(1), StepLabel::default(), |s, _| s.0 += 1);
        });
        sim.run_to_completion();
        sim.with(a2, |s| assert_eq!(s.0, 1));
        sim.with(b, |s| assert_eq!(s.0, 1));
        assert_eq!(sim.now(), Time(0) + millis(1));
    }

    #[test]
    #[should_panic(expected = "another domain")]
    fn cancelling_a_step_of_another_domain_panics() {
        let mut sim = sim(Grouping::Single);
        let a = sim.spawn_in(Domain(0), Counter::default());
        let b = sim.spawn_in(Domain(1), Counter::default());
        let token = sim.schedule(b, Time(0) + millis(10), "timer", |s, _| s.0 += 1);
        sim.schedule(a, Time(0), "cancel", move |_, _| token.cancel());
        sim.run_to_completion();
    }

    #[test]
    fn cancelling_within_a_domain_and_from_the_driver_works() {
        let mut sim = sim(Grouping::Single);
        let a = sim.spawn_in(Domain(1), Counter::default());
        let b = sim.spawn_in(Domain(1), Counter::default());
        let local = sim.schedule(b, Time(0) + millis(10), "timer", |s, _| s.0 += 1);
        let driver = sim.schedule(b, Time(0) + millis(20), "timer", |s, _| s.0 += 1);
        sim.schedule(a, Time(0), "cancel", move |_, _| local.cancel());
        driver.cancel();
        sim.run_to_completion();
        sim.with(b, |s| assert_eq!(s.0, 0));
        assert_eq!(sim.metrics().steps_cancelled, 2);
    }

    #[test]
    #[should_panic(expected = "below the lookahead")]
    fn global_steps_need_the_largest_lookahead_of_the_domain() {
        let mut sim = sim(Grouping::Single);
        let a = sim.spawn_in(Domain(0), Counter::default());
        sim.schedule(a, Time(0), "kick", |_, ctx| {
            // 1 ms reaches domain 1, but domain 2 is 5 ms away
            let at = ctx.now() + millis(1);
            ctx.schedule_global(at, StepLabel::default(), |_| {});
        });
        sim.run_to_completion();
    }

    #[test]
    fn auto_grouping_keeps_close_domains_together() {
        let mut sim = sim(Grouping::Auto { groups: 2 });
        for domain in 0..3 {
            sim.spawn_in(Domain(domain), Counter::default());
        }
        // the far domain holds a third of the work: keeping the two close
        // domains together balances 2:1 and widens the window to 5 ms
        sim.run_to_completion();
        assert_eq!(sim.groups(), 2);
        assert_eq!(sim.window(), millis(5));
    }

    #[test]
    fn auto_grouping_balances_uniform_lookahead() {
        let lookahead = UniformLookahead {
            domains: 8,
            min_delay: millis(2),
        };
        let build = || {
            let mut sim = Simulation::builder(0)
                .lookahead(&lookahead)
                .grouping(Grouping::Auto { groups: 4 })
                .build();
            for domain in 0..8 {
                sim.spawn_in(Domain(domain), Counter::default());
            }
            sim.run_to_completion();
            sim
        };
        let sim = build();
        assert_eq!(sim.groups(), 4);
        assert_eq!(sim.window(), millis(2));
        // the grouping is a function of the configuration
        let groups = |sim: &Simulation| -> Vec<usize> {
            (1..=8).map(|lane| sim.shared.group_of[lane]).collect()
        };
        assert_eq!(groups(&sim), groups(&build()));
    }

    #[test]
    fn run_at_least_steps_stops_at_a_window_end() {
        let mut sim = sim(Grouping::Explicit(vec![0, 1, 2]));
        let a = sim.spawn_in(Domain(0), Counter::default());
        for t in 0..10 {
            sim.schedule(a, Time(0) + millis(t), "tick", |s, _| s.0 += 1);
        }
        // windows are 1 ms wide and hold one tick each
        assert_eq!(sim.run_at_least_steps(3), RunOutcome::ReachedStepLimit);
        assert_eq!(sim.step_count(), 3);
        assert_eq!(sim.run_at_least_steps(100), RunOutcome::Drained);
        assert_eq!(sim.step_count(), 10);
    }

    #[test]
    fn a_single_domain_simulation_never_runs_windows() {
        let mut sim = Simulation::new(0);
        let a = sim.spawn(Counter::default());
        for t in 0..5 {
            sim.schedule(a, Time(t), "tick", |s, _| s.0 += 1);
        }
        assert_eq!(
            sim.run_while(|sim| sim.step_count() < 2),
            RunOutcome::Stopped
        );
        assert_eq!(sim.step_count(), 2);
        assert_eq!(sim.groups(), 1);
        assert_eq!(sim.window(), Duration::ZERO);
    }
}
