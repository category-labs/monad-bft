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

//! What one network hiccup costs a live set with no slack: eight
//! validators, two of them permanently Byzantine, so `2f+1` equals the six
//! live nodes and every quorum needs all of them.
//!
//! The reported failure, reproduced in [`fallback_transition`]. A slot that
//! cannot take the fast path needs the fallback path, and the fallback
//! transition is gated on `2f+1` *admitted* batch votes per index at
//! *every* node (`FastPath::on_commit_vote_deadline`). Six of six: one
//! dropped batch vote leaves its receiver short forever, that receiver
//! never casts its enter-fallback vote, the other five never reach the
//! `2f+1` enter-fallback votes an `EnterFallbackCert` needs, and nobody
//! enters the MVBA. The slot never decides, the finalization cap never
//! passes it, the conductor never runs the next window's deadline
//! agreement, and the chain stops for good.
//!
//! The precondition is a slot that has to fall back, which is routine here:
//! the strong QC threshold is six of six as well, so a proposal that
//! reaches one node late already costs the slot its fast path. Each premise
//! run (repeater off) is paired with its recovery run (repeater on).
//!
//! What the sweeps found, all of it measured here rather than argued:
//!
//! - [`sweep`] drops one message of each slot-message kind on each of the
//!   30 ordered live links of a fallback slot. Without the repeater the
//!   batch vote is the *only* fragile kind, and all 30 of its links stop
//!   the chain; the fallback vote, the enter-fallback certificate and the
//!   MVBA's own traffic all self-heal. With the repeater on, not even the
//!   batch vote stops it.
//! - [`lossy_network`] runs the everyday version -- announcements that
//!   sometimes miss their deadline, plus uniform loss on consensus
//!   messages. Every one of 32 seeds stops the chain without the repeater;
//!   none do with it.
//! - [`fallback_transition::the_repeater_bounds_the_stall_by_one_interval`]
//!   puts a number on the patch: the stall is not removed, it is bounded by
//!   one repeater interval, which the deployment sets to 5 s against a
//!   100 ms slot.
//! - [`da_layer`] and [`conductor`] are the two transports the repeater
//!   does not record, and a single packet lost on either still stops the
//!   chain with the patch in place. The deployed conductor hides its half
//!   behind `NopAcs`, which sends nothing at all; the exposure appears as
//!   soon as that placeholder is replaced by an ACS that communicates.
//! - [`fallback_transition::a_stuck_slot_gets_more_expensive_the_longer_it_waits`]
//!   is a side effect of the waiting itself: the re-arming fallback timers
//!   multiply, so a stopped chain costs more per `Delta` the longer it is
//!   stopped.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chorus::{
    CadenceDriverMsg, CadenceMessage, RepeaterConfig,
    conductor::{
        ConductorConfig, ConductorMessage, MonadConductor,
        acs::{median::MedianAcs, nop::NopAcs},
    },
    proposers::header_auth,
    proposing::{PlannerConfig, ProposalPlanner},
    slot::chorus::{
        Chorus, ChorusConfig, ChorusContext, ChorusDAEvent, ChorusMessage, FinalizationPath,
        SlotFinalization,
    },
    types::{
        NodeId, ProposerConfig, ProposerSchedule as _, RotatingProposerSchedule,
        RoundRobinLeaderSchedule, Slot, SlotDeadline, Stake, Timestamp, TimestampDelta,
        ValidatorData,
    },
};
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::{CadenceSwarm, CadenceSwarmBuilder, MockDa, SimMessage};
use monad_sim::Time;
use monad_sim_swarm::{ChaChaRng, Link, Network, NetworkModel};

// ---------------------------------------------------------------- topology

/// The validator set. Every threshold is computed over all eight.
const NODES: u64 = 8;
/// Nodes `0..LIVE` run; the rest are permanently Byzantine and never speak,
/// so `2f+1 = 6` equals the live set and no quorum has any slack.
const LIVE: u64 = 6;

const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const LAG_THRESHOLD: NonZeroU64 = SLOTS_PER_WINDOW;
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);
const DELTA: TimestampDelta = TimestampDelta::from_millis(100);
const LATENCY: TimestampDelta = TimestampDelta::from_millis(20);

/// How much a jittered announcement is delayed by: past the deadline it
/// should have been voted at, before the transition that follows it.
const JITTER_BY: Duration = Duration::from_millis(100);

const LEAD: TimestampDelta = TimestampDelta::from_millis(60);
const MIN_LEAD: TimestampDelta = TimestampDelta::from_millis(30);

/// Three windows: a chain that keeps running is unmistakable next to one
/// that stopped inside window 0.
const RUN_UNTIL: Timestamp = Timestamp::from_millis(3_000);
/// What every live node finalizes on a healthy run (two full windows).
const HEALTHY_THROUGH: u64 = 19;

/// Repeats land well inside the run, and retention spans a window so a
/// certificate is not dropped before a lagging node hears it.
const REPEATER: RepeaterConfig = RepeaterConfig {
    interval: TimestampDelta::from_millis(250),
    certificate_retention: 10,
};

type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
type MedianConductor = MonadConductor<MedianAcs<SlotDeadline>>;
type Msg = CadenceDriverMsg<Chorus, Conductor>;
type MedianMsg = CadenceDriverMsg<Chorus, MedianConductor>;
type Schedule = RotatingProposerSchedule<RoundRobinLeaderSchedule>;
type PathLog = Arc<Mutex<BTreeMap<(NodeId, Slot), (FinalizationPath, Timestamp)>>>;

fn nodes(count: u64) -> impl Iterator<Item = NodeId> {
    (0..count).map(NodeId::dummy)
}

fn live_nodes() -> impl Iterator<Item = NodeId> {
    nodes(LIVE)
}

fn conductor_config() -> ConductorConfig {
    ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        SLOT_INTERVAL,
        GENESIS_DEADLINE,
        LAG_THRESHOLD,
    )
    .unwrap()
}

fn validator_data() -> Arc<ValidatorData> {
    let validators = nodes(NODES).collect::<Vec<_>>();
    let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
    let mapping = validators
        .iter()
        .map(|id| (*id, id.keypair().pubkey()))
        .collect();
    Arc::new(ValidatorData::new(valset, mapping))
}

/// K = 1: one proposer per slot, rotating. One index keeps the failure
/// argument readable; the deployment runs five, which only multiplies the
/// number of transfers that all have to land.
fn proposer_config() -> ProposerConfig {
    ProposerConfig {
        concurrent_proposers: 1,
        observation_cutoff: 0,
        rotation_slack: 1,
        slots_per_epoch: 1_000,
    }
}

fn schedule(val_data: &Arc<ValidatorData>) -> Arc<Schedule> {
    let config = proposer_config();
    let algorithm = RoundRobinLeaderSchedule::new(&config);
    Arc::new(RotatingProposerSchedule::new(config, algorithm, val_data.clone()).unwrap())
}

fn deadline_of(slot: Slot) -> Timestamp {
    GENESIS_DEADLINE
        .checked_add_deltas(SLOT_INTERVAL, slot.get())
        .expect("slot deadline in range")
}

fn proposer_of(schedule: &Schedule, slot: Slot) -> Option<NodeId> {
    schedule.proposers_at(slot).ok()?.proposer(0)
}

/// The first slot at or after `from` whose proposer is live: only then is
/// there a proposal that can be late.
fn slot_with_live_proposer(schedule: &Schedule, from: u64) -> Slot {
    (from..from + NODES * 2)
        .map(Slot)
        .find(|slot| proposer_of(schedule, *slot).is_some_and(|node| u64::from(node) < LIVE))
        .expect("a live proposer within two rotations")
}

// ----------------------------------------------------------- network model

/// The message kinds a hiccup can hit, named so a sweep can enumerate them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Kind {
    /// the mock DA layer's proposal announcement
    Announcement,
    BatchVote,
    FastCommitVote,
    FastBlock,
    FallbackVote,
    FastCommitQc,
    EnterFallbackCert,
    /// anything inside the fallback MVBA
    Mvba,
    CapAdvance,
    DeadlineAgreement,
}

/// Everything the repeater records and re-sends.
const SLOT_KINDS: [Kind; 7] = [
    Kind::BatchVote,
    Kind::FastCommitVote,
    Kind::FastBlock,
    Kind::FallbackVote,
    Kind::FastCommitQc,
    Kind::EnterFallbackCert,
    Kind::Mvba,
];

/// Classify a wire message, with the slot it belongs to where it has one.
fn classify<A>(
    message: &SimMessage<CadenceMessage<ChorusMessage, ConductorMessage<A>>>,
) -> (Kind, Option<Slot>) {
    match message {
        SimMessage::Da(announcement) => (Kind::Announcement, Some(announcement.slot())),
        SimMessage::Cadence(CadenceMessage::Conductor(ConductorMessage::CapAdvance(_))) => {
            (Kind::CapAdvance, None)
        }
        SimMessage::Cadence(CadenceMessage::Conductor(ConductorMessage::DeadlineAgreement(_))) => {
            (Kind::DeadlineAgreement, None)
        }
        SimMessage::Cadence(CadenceMessage::Slot(slot, message)) => {
            let kind = match message {
                ChorusMessage::BatchVote(..) => Kind::BatchVote,
                ChorusMessage::FastCommitVote(..) => Kind::FastCommitVote,
                ChorusMessage::FastBlock(..) => Kind::FastBlock,
                ChorusMessage::FallbackVote(..) => Kind::FallbackVote,
                ChorusMessage::FastCommitQc(..) => Kind::FastCommitQc,
                ChorusMessage::EnterFallbackCert(..) => Kind::EnterFallbackCert,
                _ => Kind::Mvba,
            };
            (kind, Some(*slot))
        }
    }
}

/// Slow one slot's proposal announcement down at one node, so it arrives
/// after the deadline it should have been voted at.
#[derive(Clone, Copy)]
struct LateProposal {
    slot: Slot,
    to: NodeId,
    extra: Duration,
}

/// Drop the *first* message of `kind` on the link `from -> to` (and, where
/// given, of `slot`). Only the first: a repeat has to be able to get
/// through, or a repeater could not be told apart from a cut link.
#[derive(Clone, Copy, Debug)]
struct DropOnce {
    kind: Kind,
    slot: Option<Slot>,
    from: NodeId,
    to: NodeId,
}

/// A latency spike: everything arriving at `to` during `window` takes
/// `extra` longer.
#[derive(Clone)]
struct Spike {
    to: NodeId,
    window: Range<Time>,
    extra: Duration,
}

/// A constant-latency network with at most one late proposal, at most one
/// dropped packet, and at most one latency spike.
struct Hiccup {
    late: Option<LateProposal>,
    drop_once: Option<DropOnce>,
    spike: Option<Spike>,
    /// independent loss, applied to consensus messages only: the mock DA
    /// layer has no chunk recovery, so losing its announcements would
    /// measure the mock rather than the protocol (see [`da_layer`])
    loss: f64,
    /// probability that an announcement is delayed past its deadline,
    /// which is what puts a slot on the fallback path
    jitter: f64,
    /// set once the drop actually fired, so a sweep can tell a surviving
    /// configuration apart from one where no such message ever flew
    fired: Arc<AtomicBool>,
}

impl<A> NetworkModel<NodeId, SimMessage<CadenceMessage<ChorusMessage, ConductorMessage<A>>>>
    for Hiccup
{
    fn deliveries(
        &mut self,
        link: &Link<NodeId>,
        message: &SimMessage<CadenceMessage<ChorusMessage, ConductorMessage<A>>>,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        let (kind, slot) = classify(message);

        if self.loss > 0.0 && kind != Kind::Announcement && rand::Rng::r#gen::<f64>(rng) < self.loss
        {
            return Vec::new();
        }

        let jittered = self.jitter > 0.0
            && kind == Kind::Announcement
            && rand::Rng::r#gen::<f64>(rng) < self.jitter;

        if let Some(drop) = self.drop_once
            && !self.fired.load(Ordering::Relaxed)
            && drop.kind == kind
            && (drop.slot.is_none() || drop.slot == slot)
            && drop.from == link.from
            && drop.to == link.to
        {
            self.fired.store(true, Ordering::Relaxed);
            return Vec::new();
        }

        let mut latency = LATENCY.as_duration();
        if let Some(late) = self.late
            && kind == Kind::Announcement
            && Some(late.slot) == slot
            && late.to == link.to
        {
            latency += late.extra;
        }
        if let Some(spike) = &self.spike
            && spike.to == link.to
            && spike.window.contains(&link.now)
        {
            latency += spike.extra;
        }
        if jittered {
            latency += JITTER_BY;
        }
        vec![latency]
    }
}

// --------------------------------------------------------------- scenarios

#[derive(Clone, Default)]
struct Scenario {
    late: Option<LateProposal>,
    drop_once: Option<DropOnce>,
    spike: Option<Spike>,
    repeater: Option<RepeaterConfig>,
    /// how many of the eight validators actually run
    live: Option<u64>,
    loss: f64,
    jitter: f64,
    seed: u64,
    until: Option<Timestamp>,
}

impl Scenario {
    fn late(mut self, slot: Slot, to: NodeId, extra: Duration) -> Self {
        self.late = Some(LateProposal { slot, to, extra });
        self
    }

    fn drop_once(mut self, kind: Kind, slot: Option<Slot>, from: NodeId, to: NodeId) -> Self {
        self.drop_once = Some(DropOnce {
            kind,
            slot,
            from,
            to,
        });
        self
    }

    fn spike(mut self, to: NodeId, window: Range<Time>, extra: Duration) -> Self {
        self.spike = Some(Spike { to, window, extra });
        self
    }

    fn repeater(self) -> Self {
        self.repeater_every(REPEATER.interval)
    }

    fn repeater_every(mut self, interval: TimestampDelta) -> Self {
        self.repeater = Some(RepeaterConfig {
            interval,
            ..REPEATER
        });
        self
    }

    fn live(mut self, live: u64) -> Self {
        self.live = Some(live);
        self
    }

    /// A lossy, jittery network: consensus packets lost with probability
    /// `loss`, announcements delayed past their deadline with probability
    /// `jitter`.
    fn lossy(mut self, seed: u64, loss: f64, jitter: f64) -> Self {
        self.seed = seed;
        self.loss = loss;
        self.jitter = jitter;
        self
    }

    fn until(mut self, until: Timestamp) -> Self {
        self.until = Some(until);
        self
    }

    fn network<A: 'static>(
        &self,
        fired: Arc<AtomicBool>,
    ) -> Network<NodeId, SimMessage<CadenceMessage<ChorusMessage, ConductorMessage<A>>>> {
        Network::custom(Hiccup {
            late: self.late,
            drop_once: self.drop_once,
            spike: self.spike.clone(),
            loss: self.loss,
            jitter: self.jitter,
            fired,
        })
    }
}

/// One finished run.
struct Run<M: Clone + 'static> {
    swarm: CadenceSwarm<M, ChorusDAEvent>,
    paths: PathLog,
    /// how many validators were live in this run
    live: u64,
    /// whether the configured drop ever matched a message
    dropped: bool,
}

/// Build a swarm of proposer nodes over the eight-validator set and run it
/// to [`RUN_UNTIL`]. `conductor` supplies each node's conductor, which is
/// the only thing the two ACS variants differ in.
fn run_with<C, A, F>(scenario: &Scenario, mut conductor: F) -> Run<CadenceDriverMsg<Chorus, C>>
where
    C: chorus::Conductor<Message = ConductorMessage<A>> + 'static,
    A: Clone + 'static,
    F: FnMut(&Arc<ValidatorData>) -> C,
{
    let paths: PathLog = Arc::default();
    let fired = Arc::new(AtomicBool::new(false));
    let val_data = validator_data();
    let proposers = schedule(&val_data);
    let live = scenario.live.unwrap_or(LIVE);

    let mut builder = CadenceSwarmBuilder::new();
    builder.set_seed(scenario.seed);
    builder.set_network(scenario.network::<A>(fired.clone()));
    if let Some(repeater) = scenario.repeater {
        builder.set_repeater(repeater);
    }

    for id in nodes(live) {
        let header_auth = Arc::new(header_auth(proposers.clone()));
        let da = Arc::new(MockDa::new(id, header_auth.clone()));
        let context = ChorusContext {
            node_id: id,
            key: Arc::new(id.keypair()),
            validator_data: val_data.clone(),
            header_auth,
            proposers: proposers.clone(),
        };
        let planner = ProposalPlanner::new(
            id,
            proposers.clone(),
            PlannerConfig {
                lead: LEAD,
                min_lead: MIN_LEAD,
                observation_cutoff: proposer_config().observation_cutoff,
            },
        );
        let log = paths.clone();
        let observer = move |now: Timestamp, slot: Slot, data: &SlotFinalization| {
            log.lock()
                .expect("path log poisoned")
                .insert((id, slot), (data.path(), now));
        };

        builder.add_proposer_node::<Chorus, _, _>(
            id,
            conductor(&val_data),
            ChorusConfig { delta: DELTA },
            context,
            planner,
            da,
            observer,
        );
    }

    let mut swarm = builder.build();
    swarm.run_until(scenario.until.unwrap_or(RUN_UNTIL));
    Run {
        swarm,
        paths,
        live,
        dropped: fired.load(Ordering::Relaxed),
    }
}

/// The deployed configuration: the conductor's placeholder ACS, which
/// decides locally and sends nothing.
fn run(scenario: &Scenario) -> Run<Msg> {
    run_with(scenario, |_| -> Conductor {
        MonadConductor::genesis(conductor_config(), ()).unwrap()
    })
}

/// The conductor with a real, communicating ACS in place of the
/// placeholder.
fn run_median(scenario: &Scenario) -> Run<MedianMsg> {
    run_with(scenario, |val_data| -> MedianConductor {
        MonadConductor::genesis(conductor_config(), val_data.clone()).unwrap()
    })
}

// ---------------------------------------------------------------- verdicts

fn finalized<M: Clone + 'static>(
    swarm: &CadenceSwarm<M, ChorusDAEvent>,
    node: NodeId,
) -> BTreeSet<Slot> {
    swarm.log().get_finalized_slots(node).into_iter().collect()
}

impl<M: Clone + 'static> Run<M> {
    fn nodes(&self) -> impl Iterator<Item = NodeId> {
        nodes(self.live)
    }

    /// The highest slot any node finalized.
    fn high_water(&self) -> Option<Slot> {
        self.nodes()
            .filter_map(|node| finalized(&self.swarm, node).last().copied())
            .max()
    }

    /// The chain stopped: nobody got out of window 0, although the run
    /// lasted three windows.
    fn halted(&self) -> bool {
        self.high_water()
            .is_none_or(|slot| slot.get() < SLOTS_PER_WINDOW.get())
    }

    fn finalized_by(&self, node: NodeId, slot: Slot) -> bool {
        self.paths
            .lock()
            .expect("path log poisoned")
            .contains_key(&(node, slot))
    }

    fn path_of(&self, node: NodeId, slot: Slot) -> Option<FinalizationPath> {
        self.entry(node, slot).map(|(path, _)| path)
    }

    /// When `node` finalized `slot`.
    fn finalized_at(&self, node: NodeId, slot: Slot) -> Option<Timestamp> {
        self.entry(node, slot).map(|(_, at)| at)
    }

    fn entry(&self, node: NodeId, slot: Slot) -> Option<(FinalizationPath, Timestamp)> {
        self.paths
            .lock()
            .expect("path log poisoned")
            .get(&(node, slot))
            .copied()
    }

    /// Slots of `0..=HEALTHY_THROUGH` that `node` never finalized.
    fn missing(&self, node: NodeId) -> Vec<u64> {
        let slots = finalized(&self.swarm, node);
        (0..=HEALTHY_THROUGH)
            .filter(|slot| !slots.contains(&Slot(*slot)))
            .collect()
    }

    /// Run on to `until` and report how many simulation steps that took.
    fn extend(&mut self, until: Timestamp) -> u64 {
        let before = self.swarm.swarm().simulation().step_count();
        self.swarm.run_until(until);
        self.swarm.swarm().simulation().step_count() - before
    }

    fn assert_healthy(&self, context: &str) {
        for node in self.nodes() {
            let missing = self.missing(node);
            assert!(
                missing.is_empty(),
                "{context}: {node:?} never finalized slots {missing:?}"
            );
        }
    }
}

// ------------------------------------------------------------- the premise

/// Six of eight, nothing perturbed: the live set is exactly `2f+1`, and
/// that is enough for the fast path to carry every slot.
#[test]
fn six_live_of_eight_is_healthy() {
    let run = run(&Scenario::default());
    run.assert_healthy("unperturbed");
    for node in run.nodes() {
        assert_eq!(
            run.path_of(node, Slot(5)),
            Some(FinalizationPath::Fast),
            "{node:?} did not take the fast path"
        );
    }
}

// ----------------------------------------------- the reported failure mode

mod fallback_transition {
    use super::*;

    /// Lands the announcement between the deadline and `D+Delta`: too late
    /// to be voted on, early enough that the victim still admits its peers'
    /// positive votes before the transition, so the only thing wrong with
    /// the slot is that it has to fall back.
    const LATE_BY: Duration = Duration::from_millis(100);

    /// The target slot, the node its proposal reaches late, and the link a
    /// batch vote is dropped on. The three nodes are distinct, so the two
    /// perturbations do not overlap.
    fn setup() -> (Slot, NodeId, NodeId, NodeId) {
        let proposers = schedule(&validator_data());
        let slot = slot_with_live_proposer(&proposers, 3);
        let proposer = proposer_of(&proposers, slot).expect("live proposer");
        let others: Vec<NodeId> = live_nodes().filter(|node| *node != proposer).collect();
        (slot, others[0], others[1], others[2])
    }

    fn late(slot: Slot, to: NodeId) -> Scenario {
        Scenario::default().late(slot, to, LATE_BY)
    }

    /// The baseline the drop is measured against. A proposal that reaches
    /// one node late costs the slot its fast path outright -- with six live
    /// of eight the strong QC threshold is all six, so one divergent vote
    /// denies it -- and the fallback path carries the slot without help.
    #[test]
    fn a_late_proposal_alone_falls_back_and_the_chain_keeps_running() {
        let (slot, slow, _, _) = setup();
        let run = run(&late(slot, slow));

        for node in run.nodes() {
            assert_eq!(
                run.path_of(node, slot),
                Some(FinalizationPath::Fallback),
                "{node:?} did not take the fallback path on {slot:?}"
            );
        }
        run.assert_healthy("late proposal alone");
    }

    /// The reported failure. One batch vote of that same slot is dropped on
    /// one link. Its receiver never reaches `2f+1` admitted votes, so it
    /// never casts its enter-fallback vote; the other five never reach the
    /// `2f+1` enter-fallback votes the certificate needs; nobody enters the
    /// MVBA. The slot never decides and the chain stops inside window 0.
    #[test]
    fn a_single_dropped_batch_vote_stops_the_chain() {
        let (slot, slow, sender, victim) = setup();
        let run = run(&late(slot, slow).drop_once(Kind::BatchVote, Some(slot), sender, victim));

        assert!(run.dropped, "the batch vote was never on the wire");
        assert!(
            run.nodes().all(|node| !run.finalized_by(node, slot)),
            "{slot:?} finalized somewhere despite the dropped vote"
        );
        assert!(
            run.halted(),
            "the chain ran past window 0 (high water {:?})",
            run.high_water()
        );
        // the stuck slot holds the cap, so the rest of the open window
        // still finalizes -- it is the *next* window that never opens
        assert!(
            run.high_water().is_some_and(|high| high > slot),
            "later slots of the open window should still finalize"
        );
    }

    /// The patch. The repeater re-sends the undecided slot's own outbound
    /// messages, the dropped batch vote among them, so the receiver reaches
    /// `2f+1`, the enter-fallback certificate forms, and the slot decides.
    #[test]
    fn the_repeater_recovers_the_dropped_batch_vote() {
        let (slot, slow, sender, victim) = setup();
        let run = run(&late(slot, slow)
            .drop_once(Kind::BatchVote, Some(slot), sender, victim)
            .repeater());

        assert!(run.dropped, "the batch vote was never on the wire");
        for node in run.nodes() {
            assert!(
                run.finalized_by(node, slot),
                "{node:?} never finalized {slot:?} with the repeater on"
            );
        }
        run.assert_healthy("dropped batch vote, repeater on");
    }

    /// Why the late proposal is part of the premise: on a slot the fast
    /// path can carry, the same dropped batch vote is repaired for free.
    /// The five nodes that did get it form the fast block and broadcast it,
    /// and the short node adopts that and commit-votes off it. The drop
    /// only bites once the slot has to reach the fallback transition.
    #[test]
    fn the_same_drop_on_a_fast_path_slot_is_harmless() {
        let (slot, _, sender, victim) = setup();
        let run = run(&Scenario::default().drop_once(Kind::BatchVote, Some(slot), sender, victim));

        assert!(run.dropped, "the batch vote was never on the wire");
        assert_eq!(run.path_of(victim, slot), Some(FinalizationPath::Fast));
        run.assert_healthy("dropped batch vote on a fast-path slot");
    }

    /// What the patch actually buys, quantified: the stall is not removed,
    /// it is bounded by one repeater interval. Nothing else re-sends the
    /// lost vote, so the slot waits for the first tick at or after its
    /// interval and the whole chain waits with it -- the deployment runs a
    /// 5 s interval against a 100 ms slot, so one unrepaired drop costs
    /// roughly fifty slots of head-of-line blocking.
    #[test]
    fn the_repeater_bounds_the_stall_by_one_interval() {
        let (slot, slow, sender, victim) = setup();
        let delay_for = |interval: TimestampDelta| {
            let run = run(&late(slot, slow)
                .drop_once(Kind::BatchVote, Some(slot), sender, victim)
                .repeater_every(interval));
            let at = run
                .finalized_at(victim, slot)
                .expect("the repeater recovers the slot");
            at.duration_since(deadline_of(slot))
                .expect("finalized after its deadline")
        };

        let quick = delay_for(TimestampDelta::from_millis(200));
        let slow_repeater = delay_for(TimestampDelta::from_millis(600));

        // an unperturbed fallback slot decides a small multiple of Delta
        // after its deadline; with the vote lost, the wait is the interval
        assert!(
            quick >= TimestampDelta::from_millis(200),
            "recovered faster than the repeater interval: {quick:?}"
        );
        assert!(
            slow_repeater > quick,
            "a slower repeater should stall longer ({quick:?} vs {slow_repeater:?})"
        );
        assert!(
            slow_repeater >= TimestampDelta::from_millis(600),
            "recovered faster than the repeater interval: {slow_repeater:?}"
        );
    }

    /// A stuck slot is not only stuck, it also gets more expensive the
    /// longer it waits. `FallbackTransitionTimeout` arms a fresh
    /// `FallbackDecisionDelayElapsed` *every* time it fires, and each of
    /// those re-arms itself when the fallback entry is still blocked
    /// (`Chorus::handle_timer`), so on a node that never reaches `2f+1`
    /// admitted votes one new self-perpetuating timer is added per `Delta`
    /// and the work per `Delta` grows without bound. The chain has stopped
    /// producing traffic by then, so this is pure waste.
    #[test]
    fn a_stuck_slot_gets_more_expensive_the_longer_it_waits() {
        let (slot, slow, sender, victim) = setup();
        let mut stuck =
            run(&late(slot, slow).drop_once(Kind::BatchVote, Some(slot), sender, victim));
        assert!(stuck.halted(), "the premise is a chain that stopped");

        // three equal stretches, all of them well after the chain stopped
        let first = stuck.extend(Timestamp::from_millis(6_000));
        let second = stuck.extend(Timestamp::from_millis(9_000));
        let third = stuck.extend(Timestamp::from_millis(12_000));

        assert!(
            first < second && second < third,
            "the per-interval cost of a dead chain should not grow \
             ({first}, {second}, {third} steps)"
        );
    }
}

// ------------------------------------------------ what the repeater covers

/// The same question, asked of every slot-message kind on every ordered
/// pair of live nodes: does dropping one of them stop the chain?
mod sweep {
    use super::*;

    const LATE_BY: Duration = Duration::from_millis(100);

    fn target() -> (Slot, NodeId) {
        let proposers = schedule(&validator_data());
        let slot = slot_with_live_proposer(&proposers, 3);
        let proposer = proposer_of(&proposers, slot).expect("live proposer");
        let slow = live_nodes()
            .find(|node| *node != proposer)
            .expect("a node other than the proposer");
        (slot, slow)
    }

    /// For every (kind, link): whether such a packet flew at all, and
    /// whether dropping it stopped the chain.
    struct Outcome {
        halting: Vec<(Kind, NodeId, NodeId)>,
        /// per kind, how many of the 30 links carried one
        covered: BTreeMap<Kind, usize>,
    }

    fn outcome(repeater: bool) -> Outcome {
        let (slot, slow) = target();
        let mut halting = Vec::new();
        let mut covered = BTreeMap::new();

        for kind in SLOT_KINDS {
            covered.insert(kind, 0);
            for from in live_nodes() {
                for to in live_nodes() {
                    if from == to {
                        continue;
                    }
                    let mut scenario = Scenario::default().late(slot, slow, LATE_BY).drop_once(
                        kind,
                        Some(slot),
                        from,
                        to,
                    );
                    if repeater {
                        scenario = scenario.repeater();
                    }
                    let run = run(&scenario);
                    if !run.dropped {
                        continue;
                    }
                    *covered.get_mut(&kind).expect("kind seeded") += 1;
                    if run.halted() {
                        halting.push((kind, from, to));
                    }
                }
            }
        }
        Outcome { halting, covered }
    }

    fn report(label: &str, outcome: &Outcome) {
        eprintln!("{label}: links carrying the kind / links whose drop halted the chain");
        for (kind, links) in &outcome.covered {
            let halted = outcome
                .halting
                .iter()
                .filter(|(halting_kind, ..)| halting_kind == kind)
                .count();
            eprintln!("  {kind:?}: {links} carried, {halted} halted");
        }
    }

    /// The premise, swept: without the repeater the fallback slot is
    /// fragile in more than one place, and the batch vote is among them.
    #[test]
    fn without_the_repeater_single_drops_stop_the_chain() {
        let outcome = outcome(false);
        report("no repeater", &outcome);

        // every one of the 30 links carries a batch vote, and dropping any
        // one of them stops the chain
        assert_eq!(outcome.covered[&Kind::BatchVote], 30);
        let batch_votes = outcome
            .halting
            .iter()
            .filter(|(kind, ..)| *kind == Kind::BatchVote)
            .count();
        assert_eq!(
            batch_votes, 30,
            "expected every dropped batch vote to stop the chain: {:?}",
            outcome.halting
        );
    }

    /// ... and with the repeater on, no single slot message dropped
    /// anywhere on the live mesh stops the chain. This is the scope of the
    /// patch: everything `CadenceRuntime` routes through `Repeater::record`.
    #[test]
    fn with_the_repeater_no_single_slot_message_drop_stops_the_chain() {
        let outcome = outcome(true);
        report("repeater on", &outcome);
        assert!(
            outcome.halting.is_empty(),
            "the repeater did not cover these drops: {:?}",
            outcome.halting
        );
    }
}

// --------------------------------------------- what the repeater does not

/// The DA layer rides its own transport, and the repeater does not touch
/// it. Consensus makes that transport liveness-critical: a peer's positive
/// batch vote is only *admitted* once the DA layer has opened its root
/// locally (`GatedVotePool`), and it is admitted votes that
/// `on_commit_vote_deadline` counts against `2f+1`.
///
/// The mock DA layer here has no chunk recovery, so a dropped announcement
/// is a permanent hole -- production answers `ChorusDACommand::RecoverChunks`
/// (wired in `monad-mcp-node/src/cadence_task.rs`), and `recover_held` does
/// re-issue that request every `Delta` while a node is short. What these
/// tests pin down is the consensus-side consequence: for as long as the
/// hole is open the repeater cannot close it, because the missing packets
/// are not ones it ever recorded.
mod da_layer {
    use super::*;

    fn target() -> (Slot, NodeId, NodeId) {
        let proposers = schedule(&validator_data());
        let slot = slot_with_live_proposer(&proposers, 3);
        let proposer = proposer_of(&proposers, slot).expect("live proposer");
        let victim = live_nodes()
            .find(|node| *node != proposer)
            .expect("a node other than the proposer");
        (slot, proposer, victim)
    }

    /// One proposal announcement dropped at one node. That node votes
    /// negative and then holds every peer's positive vote behind the
    /// admission gate, so it counts one admitted vote where it needs six,
    /// and never transitions. The repeater re-sends batch votes the victim
    /// is already holding, which changes nothing.
    #[test]
    fn a_dropped_announcement_stops_the_chain_through_the_repeater() {
        let (slot, proposer, victim) = target();
        let run = run(&Scenario::default()
            .drop_once(Kind::Announcement, Some(slot), proposer, victim)
            .repeater());

        assert!(run.dropped, "the announcement was never on the wire");
        assert!(
            run.nodes().all(|node| !run.finalized_by(node, slot)),
            "{slot:?} finalized somewhere despite the DA hole"
        );
        assert!(
            run.halted(),
            "the chain ran past window 0 (high water {:?})",
            run.high_water()
        );
    }

    /// The same packet merely delayed, not lost, is survivable but not
    /// free: the victim votes negative, the slot loses its fast path, and
    /// the transition waits for a later `FallbackTransitionTimeout`.
    #[test]
    fn a_delayed_announcement_only_costs_the_slot_its_fast_path() {
        let (slot, _, victim) = target();
        // arrives after D+Delta, so the victim is still short at the first
        // transition timeout and only catches up at a later one
        let run = run(&Scenario::default().late(slot, victim, Duration::from_millis(250)));

        assert_eq!(run.path_of(victim, slot), Some(FinalizationPath::Fallback));
        run.assert_healthy("delayed announcement");
    }
}

/// The conductor's own messages are the other transport the repeater does
/// not record: `CadenceRuntime::step_once` hands them straight to
/// `Driver::broadcast_conductor`, and nothing re-sends them. The deployed
/// configuration hides this, because its ACS is `NopAcs` -- a placeholder
/// that decides on its own proposal and never sends anything. Any ACS that
/// actually communicates puts a second, unprotected quorum on the chain's
/// critical path, which is what these tests measure with `MedianAcs`.
mod conductor {
    use super::*;

    /// `MedianAcs` decides only once it has heard from *every* validator,
    /// so two permanently Byzantine nodes are enough on their own: window 1
    /// never opens and the chain stops at the end of window 0, repeater or
    /// not.
    #[test]
    fn a_communicating_acs_never_decides_with_two_nodes_down() {
        let run = run_median(&Scenario::default().repeater());

        assert!(
            run.halted(),
            "the chain opened a second window (high water {:?})",
            run.high_water()
        );
        // window 0 itself completes, so the stop really is the rotation
        assert_eq!(run.high_water(), Some(Slot(SLOTS_PER_WINDOW.get() - 1)));
    }

    /// With every validator live the same ACS does decide -- the premise
    /// for the drop below.
    #[test]
    fn a_communicating_acs_decides_when_every_validator_is_live() {
        let run = run_median(&Scenario::default().live(NODES).repeater());

        for node in nodes(NODES) {
            assert!(
                finalized(&run.swarm, node).contains(&Slot(SLOTS_PER_WINDOW.get())),
                "{node:?} never opened window 1"
            );
        }
    }

    /// One ACS packet dropped on one link, with the repeater on, and the
    /// whole chain stops one window later.
    ///
    /// Nothing re-sends the packet and the ACS has no retransmission of its
    /// own, so its receiver's round for window 1 never decides and that node
    /// never opens window 1. Its peers do, and run window 1 out without it
    /// -- but a node that never opened window 1 never proposes a deadline
    /// for window 2 either, and `MedianAcs` waits for every validator. So
    /// the loss of one packet in window 0 stops everybody at the end of
    /// window 1. The lag-threshold cap jump does eventually hand the lagging
    /// node window 2, but by then there is no quorum left to run it with.
    #[test]
    fn one_dropped_acs_packet_stops_the_chain_a_window_later() {
        let sender = NodeId::dummy(0);
        let victim = NodeId::dummy(1);
        let run = run_median(
            &Scenario::default()
                .live(NODES)
                .drop_once(Kind::DeadlineAgreement, None, sender, victim)
                .repeater(),
        );

        assert!(run.dropped, "no deadline agreement packet was dropped");

        // the victim never opens window 1
        let by_victim = finalized(&run.swarm, victim);
        let lost: Vec<u64> = (SLOTS_PER_WINDOW.get()..2 * SLOTS_PER_WINDOW.get())
            .filter(|slot| !by_victim.contains(&Slot(*slot)))
            .collect();
        assert_eq!(
            lost.len() as u64,
            SLOTS_PER_WINDOW.get(),
            "{victim:?} was expected to lose the whole of window 1, lost {lost:?}"
        );

        // its peers do, and then stop: window 2 opens nowhere
        for node in nodes(NODES) {
            let slots = finalized(&run.swarm, node);
            assert!(
                !slots.contains(&Slot(2 * SLOTS_PER_WINDOW.get())),
                "{node:?} opened window 2 although the ACS could not decide"
            );
        }
        let peer = NodeId::dummy(2);
        assert_eq!(
            finalized(&run.swarm, peer).last().copied(),
            Some(Slot(2 * SLOTS_PER_WINDOW.get() - 1)),
            "{peer:?} should have run window 1 out and then stopped"
        );
    }
}

/// A latency spike rather than a loss: one node's inbound traffic is
/// delayed across one slot's exchange.
mod latency_spike {
    use super::*;

    const SPIKE: Duration = Duration::from_millis(400);

    fn window_around(slot: u64) -> Range<Time> {
        let at = |ms: i128| Time(ms * 1_000_000);
        let deadline = GENESIS_DEADLINE
            .checked_add_deltas(SLOT_INTERVAL, slot)
            .unwrap();
        let deadline_ms = i128::try_from(deadline.as_nanos() / 1_000_000).unwrap();
        at(deadline_ms - 80)..at(deadline_ms + 20)
    }

    /// A spike is not a loss: every packet still arrives, just late. The
    /// slots it straddles lose their fast path, and the chain keeps running
    /// without any help from the repeater.
    #[test]
    fn a_spike_costs_slots_their_fast_path_but_not_the_chain() {
        let victim = NodeId::dummy(2);
        let run = run(&Scenario::default().spike(victim, window_around(4), SPIKE));

        run.assert_healthy("latency spike");
        let fell_back = (3..7)
            .map(Slot)
            .any(|slot| run.path_of(victim, slot) == Some(FinalizationPath::Fallback));
        assert!(
            fell_back,
            "the spike did not even cost a slot its fast path"
        );
    }

    /// A spike overlapping a drop is the dangerous combination: the spike
    /// puts the slot on the fallback path and the drop then denies the
    /// transition, exactly as a late proposal does.
    #[test]
    fn a_spike_plus_one_drop_stops_the_chain_and_the_repeater_fixes_it() {
        let victim = NodeId::dummy(2);
        let slot = Slot(4);
        let dropped = Scenario::default()
            .spike(victim, window_around(slot.get()), SPIKE)
            .drop_once(
                Kind::BatchVote,
                Some(slot),
                NodeId::dummy(0),
                NodeId::dummy(1),
            );

        let stuck = run(&dropped);
        assert!(stuck.dropped, "the batch vote was never on the wire");
        assert!(
            stuck.halted(),
            "the chain ran past window 0 (high water {:?})",
            stuck.high_water()
        );

        let recovered = run(&dropped.repeater());
        recovered.assert_healthy("spike plus drop, repeater on");
    }
}

/// The everyday version of the hiccup, over a seed sweep: announcements
/// that sometimes miss their deadline -- which is what puts a slot on the
/// fallback path, and what `mcp_test` sees on roughly a fifth of its slots
/// -- plus uniform loss on the consensus messages. Loss is kept off the
/// announcements because the mock DA layer cannot recover one, and the
/// resulting stall would measure the mock (see [`da_layer`]).
mod lossy_network {
    use super::*;

    const LOSS: f64 = 0.01;
    const JITTER: f64 = 0.06;
    const SEEDS: Range<u64> = 0..32;
    const UNTIL: Timestamp = Timestamp::from_millis(10_000);

    /// Slots near the high water may simply not have had their repeat
    /// yet when the run ended; only gaps below this margin count.
    const TAIL: u64 = 10;

    struct Health {
        /// the highest slot any node finalized
        high_water: u64,
        /// the lowest settled slot that some node is missing
        first_gap: Option<u64>,
    }

    fn health(seed: u64, repeater: bool) -> Health {
        let mut scenario = Scenario::default().lossy(seed, LOSS, JITTER).until(UNTIL);
        if repeater {
            scenario = scenario.repeater();
        }
        let run = run(&scenario);
        let high_water = run.high_water().map_or(0, |slot| slot.get());
        let first_gap = (0..high_water.saturating_sub(TAIL)).find(|slot| {
            run.nodes()
                .any(|node| !finalized(&run.swarm, node).contains(&Slot(*slot)))
        });
        Health {
            high_water,
            first_gap,
        }
    }

    /// What the run reaches when nothing is wrong, as the yardstick.
    fn clean_high_water() -> u64 {
        let run = run(&Scenario::default().until(UNTIL));
        run.high_water().expect("a clean run finalizes").get()
    }

    /// The premise: at these rates most seeds hit a slot that falls back
    /// and loses a vote, and the chain stops there for good.
    #[test]
    fn without_the_repeater_a_lossy_network_stops_the_chain() {
        let clean = clean_high_water();
        let stalled: Vec<(u64, u64)> = SEEDS
            .map(|seed| (seed, health(seed, false).high_water))
            .filter(|(_, high_water)| *high_water < clean)
            .collect();
        let worst = stalled.iter().map(|(_, high)| *high).min().unwrap_or(clean);
        let best = stalled.iter().map(|(_, high)| *high).max().unwrap_or(clean);
        eprintln!(
            "no repeater: {} of {} seeds fell short of slot {clean} \
             (stopped between slot {worst} and slot {best})",
            stalled.len(),
            SEEDS.count()
        );
        assert!(
            stalled.len() > SEEDS.count() / 2,
            "expected most seeds to stop short of slot {clean}, got {stalled:?}"
        );
    }

    /// ... and with the repeater every seed keeps going, with no node left
    /// behind a slot its peers finalized. It is not free: the repeats cost
    /// the chain time, so it ends up short of a clean run.
    #[test]
    fn the_repeater_carries_the_chain_through_a_lossy_network() {
        let clean = clean_high_water();
        let mut worst = u64::MAX;
        for seed in SEEDS {
            let health = health(seed, true);
            assert_eq!(
                health.first_gap, None,
                "seed {seed}: a node was left behind a slot its peers finalized"
            );
            worst = worst.min(health.high_water);
        }
        eprintln!("repeater on: worst seed reached slot {worst} against {clean} clean");
        assert!(
            worst > clean / 2,
            "the chain barely advanced: worst seed reached slot {worst} against {clean} clean"
        );
    }
}
