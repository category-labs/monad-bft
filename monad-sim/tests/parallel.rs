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

//! Results of a simulation with several domains do not depend on how the
//! domains are grouped, on the number of threads, or on switching between
//! exact stepping and windows: every process sees the same steps, the metrics
//! agree, and the observer sees the same canonical stream.
//!
//! Each case is a random process graph over a random lookahead matrix: steps
//! fan out to random processes (zero delay within a domain, at least the
//! lookahead across), re-arm and cancel self-timers, spawn processes, and
//! schedule global steps.

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use monad_sim::{
    CancelToken, Ctx, Domain, Grouping, Handle, ProcessId, Simulation, StepLabel, TieBreak, Time,
};
use rand::{distributions::Uniform, rngs::StdRng, Rng, SeedableRng};

const DOMAINS: usize = 6;
const PROCESSES: usize = 14;
const HORIZON: Time = Time(400_000_000);
/// Past this many steps a process stops fanning out, which keeps runs bounded.
const MAX_STEPS_PER_PROCESS: usize = 250;

/// A random case: the lookahead between domains and each process's domain.
#[derive(Clone)]
struct Case {
    seed: u64,
    tie_break: TieBreak,
    lookahead: Vec<Vec<Duration>>,
    domain_of: Vec<usize>,
}

impl Case {
    fn new(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let choices = [1, 2, 3, 5, 8];
        let lookahead = (0..DOMAINS)
            .map(|from| {
                (0..DOMAINS)
                    .map(|to| {
                        if from == to {
                            Duration::ZERO
                        } else {
                            Duration::from_millis(choices[rng.gen_range(0..choices.len())])
                        }
                    })
                    .collect()
            })
            .collect();
        let domain_of = (0..PROCESSES).map(|_| rng.gen_range(0..DOMAINS)).collect();
        let tie_break = if seed.is_multiple_of(2) {
            TieBreak::Fifo
        } else {
            TieBreak::Randomized
        };
        Case {
            seed,
            tie_break,
            lookahead,
            domain_of,
        }
    }
}

struct World {
    handles: Vec<Handle<Proc>>,
    /// Each process's domain in the case. Behaviour follows it, not the
    /// domain a run actually places the process in, so that one case can run
    /// under several placements.
    domain_of: Vec<usize>,
    lookahead: Vec<Vec<Duration>>,
    /// Per domain, the largest lookahead to another domain: how far ahead its
    /// steps schedule global steps.
    global_lookahead: Vec<Duration>,
}

struct Proc {
    domain: usize,
    me: Option<Handle<Proc>>,
    world: Option<Arc<World>>,
    /// Every step this process ran: when, and the tag its scheduler gave it.
    log: Vec<(Time, u64)>,
    timer: Option<CancelToken>,
}

impl Proc {
    fn new(domain: usize) -> Self {
        Proc {
            domain,
            me: None,
            world: None,
            log: Vec::new(),
            timer: None,
        }
    }
}

fn label(ctx: &mut Ctx) -> StepLabel {
    StepLabel::source("act").priority(ctx.sample(Uniform::new(0u32, 2)))
}

fn act(proc: &mut Proc, ctx: &mut Ctx, tag: u64) {
    let now = ctx.now();
    proc.log.push((now, tag));
    if proc.log.len() > MAX_STEPS_PER_PROCESS {
        return;
    }
    let world = proc.world.clone().expect("wired");
    let me = proc.me.expect("wired");

    for _ in 0..ctx.sample(Uniform::new(0usize, 3)) {
        let target = ctx.sample(Uniform::new(0, world.handles.len()));
        let to = world.domain_of(target);
        let base = if to == proc.domain {
            Duration::ZERO
        } else {
            world.lookahead[proc.domain][to]
        };
        let extra = Duration::from_micros(500 * ctx.sample(Uniform::new(0u64, 3)));
        let tag = ctx.sample(Uniform::new(0u64, u64::MAX));
        let label = label(ctx);
        ctx.schedule(
            world.handles[target],
            now + base + extra,
            label,
            move |p, ctx| act(p, ctx, tag),
        );
    }

    // re-arm a self-timer, cancelling the previous one
    if ctx.sample(Uniform::new(0u32, 3)) == 0 {
        if let Some(previous) = proc.timer.take() {
            previous.cancel();
        }
        let tag = ctx.sample(Uniform::new(0u64, u64::MAX));
        let label = label(ctx);
        let at = now + Duration::from_millis(2);
        proc.timer = Some(ctx.schedule(me, at, label, move |p, ctx| act(p, ctx, tag)));
    }

    // a helper process in this domain, kicked at zero delay
    if ctx.sample(Uniform::new(0u32, 40)) == 0 {
        let mut helper = Proc::new(proc.domain);
        helper.world = Some(world.clone());
        let handle = ctx.spawn(helper);
        let tag = ctx.sample(Uniform::new(0u64, u64::MAX));
        ctx.schedule(handle, now, StepLabel::source("helper"), move |p, ctx| {
            p.me = Some(handle);
            act(p, ctx, tag)
        });
    }

    // a global step that kicks a random process
    if ctx.sample(Uniform::new(0u32, 25)) == 0 {
        let target = world.handles[ctx.sample(Uniform::new(0, world.handles.len()))];
        let tag = ctx.sample(Uniform::new(0u64, u64::MAX));
        let at = now + world.global_lookahead[proc.domain];
        ctx.schedule_global(at, StepLabel::source("global"), move |ctx| {
            let now = ctx.now();
            ctx.schedule(target, now, StepLabel::source("kick"), move |p, ctx| {
                act(p, ctx, tag)
            });
        });
    }
}

impl World {
    fn domain_of(&self, process: usize) -> usize {
        self.domain_of[process]
    }
}

/// Where a run places the case's processes.
#[derive(Clone, Copy, Debug)]
enum Placement {
    /// In the case's domains.
    Case,
    /// In the case's domains, renumbered: the same partition with other
    /// lanes, and so other process ids.
    Relabelled,
    /// All in one domain, as a sequential simulation.
    Single,
}

impl Placement {
    fn domain(self, case_domain: usize) -> usize {
        match self {
            Placement::Case => case_domain,
            Placement::Relabelled => DOMAINS - 1 - case_domain,
            Placement::Single => 0,
        }
    }

    fn lookahead(self, case: &Case) -> Vec<Vec<Duration>> {
        match self {
            Placement::Case => case.lookahead.clone(),
            Placement::Relabelled => (0..DOMAINS)
                .map(|from| {
                    (0..DOMAINS)
                        .map(|to| case.lookahead[self.domain(from)][self.domain(to)])
                        .collect()
                })
                .collect(),
            Placement::Single => vec![vec![Duration::ZERO]],
        }
    }
}

/// Everything a run is compared on.
#[derive(Debug, PartialEq)]
struct Outcome {
    logs: Vec<Vec<(Time, u64)>>,
    steps_executed: u64,
    steps_scheduled: u64,
    steps_cancelled: u64,
    critical_path_len: u64,
    now: Time,
    stream: Vec<(Time, ProcessId, ProcessId, u64)>,
}

type Stream = Rc<RefCell<Vec<(Time, ProcessId, ProcessId, u64)>>>;

fn build(
    case: &Case,
    grouping: Grouping,
    threads: usize,
) -> (Simulation, Vec<Handle<Proc>>, Stream) {
    build_placed(case, Placement::Case, grouping, threads)
}

fn build_placed(
    case: &Case,
    placement: Placement,
    grouping: Grouping,
    threads: usize,
) -> (Simulation, Vec<Handle<Proc>>, Stream) {
    let mut sim = Simulation::builder(case.seed)
        .lookahead(&placement.lookahead(case))
        .grouping(grouping)
        .threads(threads)
        .build();
    sim.set_tie_break(case.tie_break);
    let handles: Vec<_> = case
        .domain_of
        .iter()
        .map(|&domain| {
            let placed = Domain(placement.domain(domain) as u32);
            sim.spawn_in(placed, Proc::new(domain))
        })
        .collect();
    let global_lookahead = case
        .lookahead
        .iter()
        .map(|row| row.iter().copied().max().expect("several domains"))
        .collect();
    let world = Arc::new(World {
        handles: handles.clone(),
        domain_of: case.domain_of.clone(),
        lookahead: case.lookahead.clone(),
        global_lookahead,
    });
    for (index, &handle) in handles.iter().enumerate() {
        sim.with_mut(handle, |p| {
            p.me = Some(handle);
            p.world = Some(world.clone());
        });
        for kick in 0..2u64 {
            let at = Time(i128::from(kick) * 1_000_000 + index as i128 % 3);
            sim.schedule(handle, at, StepLabel::source("start"), move |p, ctx| {
                act(p, ctx, kick)
            });
        }
    }
    let stream: Stream = Rc::default();
    let sink = stream.clone();
    sim.on_step(move |info| {
        sink.borrow_mut()
            .push((info.time, info.process, info.creator, info.counter))
    });
    (sim, handles, stream)
}

fn outcome(sim: &Simulation, handles: &[Handle<Proc>], stream: &Stream) -> Outcome {
    let metrics = sim.metrics();
    Outcome {
        logs: handles
            .iter()
            .map(|&h| sim.with(h, |p| p.log.clone()))
            .collect(),
        steps_executed: metrics.steps_executed,
        steps_scheduled: metrics.steps_scheduled,
        steps_cancelled: metrics.steps_cancelled,
        critical_path_len: metrics.critical_path_len,
        now: sim.now(),
        stream: stream.borrow().clone(),
    }
}

fn run(case: &Case, grouping: Grouping, threads: usize) -> Outcome {
    let (mut sim, handles, stream) = build(case, grouping, threads);
    sim.run_until_time(HORIZON);
    outcome(&sim, &handles, &stream)
}

fn random_grouping(seed: u64) -> Grouping {
    let mut rng = StdRng::seed_from_u64(seed ^ 0xabcd);
    let groups = rng.gen_range(2..=DOMAINS);
    Grouping::Explicit((0..DOMAINS).map(|_| rng.gen_range(0..groups)).collect())
}

/// Cases per test; `MONAD_SIM_PARALLEL_CASES` raises it for a longer sweep.
fn cases(default: u64) -> u64 {
    std::env::var("MONAD_SIM_PARALLEL_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(default)
}

#[test]
fn results_do_not_depend_on_grouping_or_threads() {
    for seed in 0..cases(24) {
        let case = Case::new(seed);
        let reference = run(&case, Grouping::Single, 1);
        assert!(
            reference.steps_executed > 1_000,
            "seed {seed}: the case should do real work ({} steps)",
            reference.steps_executed
        );
        let variants = [
            (random_grouping(seed), 1),
            (random_grouping(seed), 4),
            (random_grouping(seed + 100), 2),
            (Grouping::Auto { groups: 3 }, 2),
            (Grouping::default(), 4),
        ];
        for (grouping, threads) in variants {
            let label = format!("seed {seed}, {grouping:?}, {threads} threads");
            assert_eq!(run(&case, grouping, threads), reference, "{label}");
        }
    }
}

/// Process ids encode the domain, but nothing a process sees depends on them:
/// the same case placed in other domains, or all in one, runs every process
/// through the same steps. (The observer stream names processes by id, so it
/// is not compared here.)
#[test]
fn results_do_not_depend_on_domain_placement() {
    for seed in 0..cases(24) {
        let case = Case::new(seed);
        let placed = |placement, grouping, threads| {
            let (mut sim, handles, stream) = build_placed(&case, placement, grouping, threads);
            sim.run_until_time(HORIZON);
            Outcome {
                stream: Vec::new(),
                ..outcome(&sim, &handles, &stream)
            }
        };
        let reference = placed(Placement::Case, Grouping::Single, 1);
        let variants = [
            (Placement::Relabelled, Grouping::Single, 1),
            (Placement::Relabelled, Grouping::default(), 4),
            (Placement::Single, Grouping::Single, 1),
        ];
        for (placement, grouping, threads) in variants {
            let label = format!("seed {seed}, {placement:?}, {grouping:?}, {threads} threads");
            assert_eq!(placed(placement, grouping, threads), reference, "{label}");
        }
    }
}

#[test]
fn stop_points_do_not_depend_on_threads_and_stopping_does_not_change_the_run() {
    for seed in 0..cases(8) {
        let case = Case::new(seed);
        let reference = run(&case, Grouping::Single, 1);
        let grouping = random_grouping(seed);
        let mut stopped = Vec::new();
        for threads in [1, 2, 4] {
            let (mut sim, handles, stream) = build(&case, grouping.clone(), threads);
            assert!(sim.groups() == 1, "grouped on the first run");
            sim.run_while(|sim| sim.step_count() < 2_000);
            stopped.push(outcome(&sim, &handles, &stream));
            // exact steps, then windows again, then the rest
            sim.run_steps(37);
            sim.run_at_least_steps(500);
            sim.run_until_time(HORIZON);
            assert_eq!(
                outcome(&sim, &handles, &stream),
                reference,
                "seed {seed}, {threads} threads"
            );
        }
        assert!(stopped.iter().all(|o| *o == stopped[0]), "seed {seed}");
    }
}

#[test]
fn exact_stepping_follows_the_canonical_order() {
    for seed in 0..cases(6) {
        let case = Case::new(seed);
        let reference = run(&case, Grouping::Single, 1);
        let (mut sim, handles, stream) = build(&case, random_grouping(seed), 2);
        while sim.peek_time().is_some_and(|t| t <= HORIZON) {
            sim.next_step();
        }
        sim.run_until_time(HORIZON);
        assert_eq!(outcome(&sim, &handles, &stream), reference, "seed {seed}");
    }
}

#[test]
fn filtered_observer_sees_the_canonical_order_restricted() {
    let case = Case::new(3);
    let full = run(&case, Grouping::Single, 1);
    let (mut sim, _, _) = build(&case, random_grouping(3), 4);
    let seen: Stream = Rc::default();
    let sink = seen.clone();
    sim.on_step_filtered(
        |info| info.label.source == Some("global"),
        move |info| {
            sink.borrow_mut()
                .push((info.time, info.process, info.creator, info.counter))
        },
    );
    sim.run_until_time(HORIZON);
    let root = full
        .stream
        .iter()
        .filter(|(_, process, _, _)| process.domain().is_none())
        .cloned()
        .collect::<Vec<_>>();
    assert!(!root.is_empty());
    assert_eq!(*seen.borrow(), root);
}
