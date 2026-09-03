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

//! Cadence-facing proposer selection: who may propose at which proposal
//! index for a given slot.
//!
//! Every slot runs `K` concurrent proposals, one per *proposal index*
//! `j ∈ 0..K`. This module assigns each index a proposer by laying the
//! per-epoch leader sequence of a [`LeaderSchedule`] onto the slot timeline:
//!
//! * one leader per *rotation* of [`ProposerConfig::slots_per_rotation`]
//!   slots; the leader of (global) rotation `r` occupies proposal index
//!   `r mod K` for the rotation's whole lifetime;
//! * the leader sequence restarts from a fresh stake snapshot at every
//!   *epoch* of [`ProposerConfig::slots_per_epoch`] slots (epoch boundaries
//!   are where stakes update), with the no-repeat window carried across the
//!   seam.
//!
//! An index with no proposer is *vacant*: nothing may be proposed for it and
//! its proposals are empty. Vacancy has three causes:
//!
//! * **Rotation vacancy.** A handed-over index stays vacant for the `y`
//!   slots between the outgoing and the incoming proposer, where `y` =
//!   [`ProposerConfig::observation_cutoff`] is the duration of one *blind
//!   window* — the sliding window of the last `y` slots, whose proposals a
//!   party may not know yet (MCP spec, `main.tex`). The vacancy guarantees
//!   the incoming proposer knows every in-flight proposal at its index:
//!   the vacant slots are empty by definition, everything older is
//!   observable. At most one index at a time is vacant for this reason, so
//!   at least two concurrent proposers are needed to guarantee a proposer
//!   in every slot. The chain's first `y` slots have no handoff, which
//!   exempts exactly the first rotation. The spec mandates the full
//!   constant-`y` vacancy; in principle an incoming proposer whose
//!   chaining has caught up further could start earlier — a possible
//!   future optimization, deliberately not implemented.
//! * **Genesis ramp-up.** An index whose first rotation has not started yet
//!   is vacant.
//! * **Too few validators (per-epoch).** With fewer positively staked
//!   validators than `K` — not expected in production — the effective
//!   concurrency drops to the validator count: indices fill from 0 and the
//!   remaining ones stay vacant for the whole epoch.
//!
//! # Minimum validators (protocol contract)
//!
//! Scheduling requires **one positively staked validator** per epoch; zero
//! is unschedulable and surfaces as
//! [`ScheduleError::NotEnoughValidators`], never as a stall or panic. There
//! is no other minimum: the effective window `min(K, staked)` shrinks with
//! the validator set, so a shrinking set degrades concurrency — higher
//! indices go vacant — instead of stalling election. (Stakes only change at
//! epoch boundaries, so shrinkage happens at seams; today the window is
//! computed from the static snapshot and becomes per-epoch together with a
//! per-epoch stake source.) Mind the rotation-vacancy caveat above: a
//! single effective proposer under a nonzero observation cutoff leaves
//! rotation-start slots entirely vacant — they finalize empty, costing
//! throughput but not consensus progress.
//!
//! The only state chained across an epoch seam is the *boundary tail* — the
//! last `window − 1` leaders before it — captured as an [`EpochAnchor`]. An
//! anchor plus an epoch's stake snapshot fully determines that epoch, with
//! no dependency on anything older. A node can therefore persist (or the
//! protocol commit) the anchor at every boundary and re-anchor a fresh
//! schedule near the tip on restart or join
//! ([`RotatingProposerSchedule::anchored`]) — nothing replays from genesis.
//! Queries behind the anchor are refused; for finalized slots the quorum
//! certificates already witness the assignments.
//!
//! Two seams are abstracted for tests: the per-epoch scheduling algorithm
//! ([`LeaderSchedule`], stub: [`RoundRobinLeaderSchedule`]) and the
//! slot-level API itself ([`ProposerSchedule`], stub:
//! [`FixedProposerSchedule`]).
//!
//! Not to be confused with `monad-validator`'s `ElectedProposerSchedule`,
//! which gates broadcast authorization in raptorcast for the current
//! monad-bft protocol and is unrelated to MCP.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use super::{
    proposer_schedule::{EpochSchedule, ScheduleConfig, ScheduleError},
    types::{NodeId, Slot, ValidatorData},
};

/// Slot-timeline parameters of proposer selection. Protocol-wide constants;
/// changing any of them is a consensus-breaking change.
#[derive(Debug, Clone)]
pub struct ProposerConfig {
    /// `K`: number of concurrently active (staggered) proposer phases, and
    /// thereby the number of proposal indices per slot.
    pub concurrent_proposers: usize,
    /// `y`: the observation cutoff of the surrounding Cadence protocol —
    /// proposals of slot `r` build against the chained block of slot
    /// `r − y`, and the last `y` slots form the blind window whose
    /// proposals may not be known yet. Here it sets the rotation vacancy:
    /// a handed-over index stays vacant for `y` slots.
    ///
    /// A protocol constant of the surrounding Cadence deployment,
    /// independent of MCP. The conductor configuration does not model it
    /// yet, so the two configurations cannot be validated against each
    /// other: the node wiring is responsible for handing the same value to
    /// every consumer (this schedule, the proposal planner).
    /// TODO: derive this from the conductor configuration once it carries
    /// the parameter.
    pub observation_cutoff: u64,
    /// `z`: slots between the end of a rotation's vacancy and the next
    /// rotation — successive rotations are `y + z` slots apart. May be 0:
    /// then the newest index is always vacant and exactly `K − 1` indices
    /// carry a proposer in every slot.
    pub rotation_slack: u64,
    /// Slots per stake epoch; must be a multiple of the rotation length.
    pub slots_per_epoch: u64,
}

impl ProposerConfig {
    /// Saturating so that a nonsensical configuration cannot panic before
    /// [`Self::validate`] rejects it.
    pub const fn slots_per_rotation(&self) -> u64 {
        self.observation_cutoff.saturating_add(self.rotation_slack)
    }

    /// Zero for a zero rotation length — a configuration [`Self::validate`]
    /// rejects; guarded here so it cannot divide by zero first.
    pub const fn rotations_per_epoch(&self) -> u64 {
        match self.slots_per_rotation() {
            0 => 0,
            rotation => self.slots_per_epoch / rotation,
        }
    }

    /// Static checks only; the epoch-length requirement depends on the
    /// effective window and lives in [`RotatingProposerSchedule::new`].
    fn validate(&self) -> Result<(), ScheduleError> {
        if self.concurrent_proposers == 0 {
            return Err(ScheduleError::InvalidConfig(
                "concurrent_proposers must be > 0",
            ));
        }
        if self
            .observation_cutoff
            .checked_add(self.rotation_slack)
            .is_none()
        {
            return Err(ScheduleError::InvalidConfig(
                "observation_cutoff + rotation_slack overflows",
            ));
        }
        if self.slots_per_rotation() == 0 {
            return Err(ScheduleError::InvalidConfig(
                "observation_cutoff + rotation_slack must be > 0",
            ));
        }
        if !self
            .slots_per_epoch
            .is_multiple_of(self.slots_per_rotation())
        {
            return Err(ScheduleError::InvalidConfig(
                "slots_per_epoch must be a multiple of the rotation length",
            ));
        }
        Ok(())
    }
}

/// A per-epoch leader-sequence algorithm.
///
/// Contract: returns one leader per rotation of the epoch, with no id
/// appearing twice within any `window` consecutive rotations — also across
/// the epoch seam, given `prev_tail` (the last `window − 1` leaders before
/// this epoch; empty at genesis). `window` is the *effective* concurrency of
/// the epoch, `min(K, positively staked validators)`, supplied by the
/// caller.
///
/// Stakes are plain integer weights: quantizing protocol stake down to `u64`
/// is the caller's concern, not the algorithm's.
pub trait LeaderSchedule {
    fn epoch_leaders(
        &self,
        epoch: u64,
        window: usize,
        stakes: &[(NodeId, u64)],
        prev_tail: &[NodeId],
    ) -> Result<Vec<NodeId>, ScheduleError>;
}

/// The production algorithm: the guarded clipped-credit lottery of
/// [`super::proposer_schedule`].
#[derive(Debug)]
pub struct CreditLotterySchedule {
    cfg: ScheduleConfig,
}

impl CreditLotterySchedule {
    /// Lottery-specific knobs stay here; the shared slot-timeline parameters
    /// come from the [`ProposerConfig`].
    pub fn new(cfg: &ProposerConfig, lookahead: u64, guard_threshold: u64, seed: u64) -> Self {
        Self {
            cfg: ScheduleConfig {
                concurrent_proposers: cfg.concurrent_proposers,
                slots_per_rotation: cfg.slots_per_rotation(),
                slots_per_epoch: cfg.slots_per_epoch,
                lookahead,
                guard_threshold,
                seed,
            },
        }
    }

    pub fn recommended(cfg: &ProposerConfig, seed: u64) -> Self {
        Self::new(
            cfg,
            ScheduleConfig::RECOMMENDED_LOOKAHEAD,
            ScheduleConfig::RECOMMENDED_GUARD_THRESHOLD,
            seed,
        )
    }
}

impl LeaderSchedule for CreditLotterySchedule {
    fn epoch_leaders(
        &self,
        epoch: u64,
        window: usize,
        stakes: &[(NodeId, u64)],
        prev_tail: &[NodeId],
    ) -> Result<Vec<NodeId>, ScheduleError> {
        let cfg = ScheduleConfig {
            concurrent_proposers: window,
            ..self.cfg.clone()
        };
        let schedule = EpochSchedule::build(&cfg, epoch, stakes.iter().copied(), prev_tail)?;
        Ok(schedule.leaders().to_vec())
    }
}

/// Test stub: ignores stake weights and cycles through the validator set in
/// canonical order, continuing the cycle across epochs. Any id among the
/// last `window − 1` leaders (seeded from `prev_tail`) is skipped, so the
/// no-repeat window holds across epoch seams even when the validator set
/// changed in between; with a static set the skip never fires. Every
/// assignment stays predictable.
#[derive(Debug)]
pub struct RoundRobinLeaderSchedule {
    rotations_per_epoch: u64,
}

impl RoundRobinLeaderSchedule {
    pub fn new(cfg: &ProposerConfig) -> Self {
        Self {
            rotations_per_epoch: cfg.rotations_per_epoch(),
        }
    }
}

impl LeaderSchedule for RoundRobinLeaderSchedule {
    fn epoch_leaders(
        &self,
        epoch: u64,
        window: usize,
        stakes: &[(NodeId, u64)],
        prev_tail: &[NodeId],
    ) -> Result<Vec<NodeId>, ScheduleError> {
        let mut validators: Vec<NodeId> = stakes
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .map(|(id, _)| *id)
            .collect();
        validators.sort_unstable();
        // Duplicates would let `recent` cover every candidate and hang the
        // selection loop below; reject them like the lottery does. (Not
        // reachable via RotatingProposerSchedule: ValidatorData is
        // map-backed, so its ids are unique.)
        if validators.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ScheduleError::DuplicateValidator);
        }
        if validators.is_empty() || validators.len() < window {
            return Err(ScheduleError::NotEnoughValidators {
                staked: validators.len(),
                required: window.max(1),
            });
        }
        let len = validators.len() as u64;

        // The last `window − 1` selections, seeded with the previous
        // epoch's tail so the no-repeat window also holds across the seam.
        let tail_len = window.saturating_sub(1);
        let mut recent: VecDeque<NodeId> = prev_tail[prev_tail.len().saturating_sub(tail_len)..]
            .iter()
            .copied()
            .collect();

        // With a static validator set this continues the canonical cycle
        // across epochs and the skip below never fires.
        let mut cursor = epoch.wrapping_mul(self.rotations_per_epoch) % len;
        let mut leaders = Vec::with_capacity(self.rotations_per_epoch as usize);
        for _ in 0..self.rotations_per_epoch {
            // The next validator in cyclic order outside the window; found
            // within `window` steps because at most `window − 1` of the at
            // least `window` members are recent.
            let leader = loop {
                let candidate = validators[cursor as usize];
                cursor = (cursor + 1) % len;
                if !recent.contains(&candidate) {
                    break candidate;
                }
            };
            recent.push_back(leader);
            if recent.len() > tail_len {
                recent.pop_front();
            }
            leaders.push(leader);
        }
        Ok(leaders)
    }
}

/// The proposers of one slot: one `Option<NodeId>` per proposal index
/// `j ∈ 0..K`. A `None` index is vacant — no proposer may propose for it and
/// its proposals are empty (see the module docs for the vacancy causes).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProposerSet {
    slot: Slot,
    /// Global rotation containing `slot`.
    rotation: u64,
    proposers: Box<[Option<NodeId>]>,
}

impl ProposerSet {
    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn rotation(&self) -> u64 {
        self.rotation
    }

    /// `K`: the number of proposal indices per slot.
    pub fn num_indices(&self) -> usize {
        self.proposers.len()
    }

    /// The proposer at proposal index `index`; `None` when the index is
    /// vacant or out of range.
    pub fn proposer(&self, index: usize) -> Option<NodeId> {
        self.proposers.get(index).copied().flatten()
    }

    /// The proposal index `node` occupies, if it is a proposer of this slot.
    pub fn index_of(&self, node: &NodeId) -> Option<usize> {
        self.proposers
            .iter()
            .position(|proposer| *proposer == Some(*node))
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, Option<NodeId>)> + '_ {
        self.proposers
            .iter()
            .enumerate()
            .map(|(index, proposer)| (index, *proposer))
    }
}

/// Who may propose at which proposal index for a given slot.
///
/// Both directions are supported: slot to proposer set, and membership of a
/// node in a slot's proposer set. Dyn-compatible on purpose — this is the
/// mock seam for consensus tests.
pub trait ProposerSchedule {
    /// `K`: the number of proposal indices per slot.
    fn num_indices(&self) -> usize;

    /// The proposer occupying each proposal index at `slot`.
    fn proposers_at(&self, slot: Slot) -> Result<ProposerSet, ScheduleError>;

    /// The proposal index `node` holds at `slot`, if any.
    fn proposer_index_at(&self, slot: Slot, node: &NodeId) -> Result<Option<usize>, ScheduleError> {
        Ok(self.proposers_at(slot)?.index_of(node))
    }
}

/// The cross-seam state of a [`RotatingProposerSchedule`]: the last
/// `window − 1` leaders before `epoch` (fewer at genesis, or wherever the
/// protocol drains all proposer phases across a boundary). Together with the
/// epoch's stake snapshot it fully determines that epoch's leader sequence.
///
/// The anchor is tiny and deterministic, so it can be persisted locally — or
/// committed to consensus state — at every epoch boundary
/// ([`RotatingProposerSchedule::anchor_for_epoch`] produces it): a
/// restarting or newly joining node then anchors its schedule near the tip
/// instead of replaying every seam since genesis. Epoch management does not
/// exist yet, so that wiring is still to come; the API supports it already
/// so the design can settle early.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EpochAnchor {
    epoch: u64,
    tail: Vec<NodeId>,
}

impl EpochAnchor {
    /// The genesis anchor: epoch 0, seeded by an empty tail.
    pub const fn genesis() -> Self {
        Self {
            epoch: 0,
            tail: Vec::new(),
        }
    }

    /// Anchor at `epoch`, seeded by `tail`: the leaders of the last
    /// `window − 1` rotations before that epoch, oldest first.
    pub fn new(epoch: u64, tail: Vec<NodeId>) -> Self {
        Self { epoch, tail }
    }

    /// First epoch an anchored schedule can answer queries for.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The leaders of the last rotations before [`Self::epoch`], oldest
    /// first.
    pub fn tail(&self) -> &[NodeId] {
        &self.tail
    }
}

/// The production [`ProposerSchedule`]: rotates the leader sequence of a
/// [`LeaderSchedule`] through the proposal indices, one epoch at a time.
///
/// Epochs are fixed slot spans (`epoch = slot / slots_per_epoch`). Schedules
/// are built per epoch on demand and cached; building always proceeds in
/// epoch order so the no-repeat window chains correctly across seams.
/// Derivation starts at the [`EpochAnchor`] handed to [`Self::anchored`] —
/// earlier epochs return [`ScheduleError::BeforeAnchor`]. Only the newest
/// [`CACHED_EPOCHS`] epochs stay cached; a query behind the retained range
/// replays the chain from the anchor instead.
///
/// The effective concurrency is `min(K, positively staked validators)`:
/// rotations cycle through indices `0..effective` and any higher index stays
/// vacant. For now a single static stake snapshot feeds every epoch —
/// schedules still differ per epoch through the algorithm's epoch input —
/// so the effective concurrency is a constant; it becomes per-epoch once a
/// per-epoch stake source exists.
#[derive(Debug)]
pub struct RotatingProposerSchedule<A> {
    cfg: ProposerConfig,
    algorithm: A,
    stakes: Vec<(NodeId, u64)>,
    /// Effective concurrency: `min(K, positively staked validators)`.
    window: usize,
    /// Where derivation starts; epochs before it cannot be answered.
    anchor: EpochAnchor,
    /// The newest built epochs, contiguous, at most [`CACHED_EPOCHS`] of
    /// them. The newest entry is never evicted: extending forward chains
    /// from its boundary tail.
    cache: Mutex<BTreeMap<u64, Arc<Vec<NodeId>>>>,
}

/// Epochs retained by the schedule cache. Purely a performance knob: the
/// hot consensus working set is the epochs around the open slots (previous,
/// current, next); anything older is rebuilt by replaying from the anchor.
const CACHED_EPOCHS: usize = 4;

impl<A: LeaderSchedule> RotatingProposerSchedule<A> {
    /// Anchors the schedule at genesis: the whole chain is derivable, at
    /// the cost of replaying every seam since epoch 0 on a cache miss. For
    /// long-running production nodes prefer [`Self::anchored`] with a
    /// persisted anchor.
    pub fn new(
        cfg: ProposerConfig,
        algorithm: A,
        validator_data: Arc<ValidatorData>,
    ) -> Result<Self, ScheduleError> {
        Self::anchored(cfg, algorithm, validator_data, EpochAnchor::genesis())
    }

    /// Anchors the schedule at `anchor`: derivation starts there and
    /// queries about earlier epochs return
    /// [`ScheduleError::BeforeAnchor`]. Eagerly builds the anchor epoch, so
    /// configuration and validator-set errors surface at construction
    /// rather than mid-run.
    pub fn anchored(
        cfg: ProposerConfig,
        algorithm: A,
        validator_data: Arc<ValidatorData>,
        anchor: EpochAnchor,
    ) -> Result<Self, ScheduleError> {
        use crate::spec::{Stake as _, validator::ValidatorData as _};

        cfg.validate()?;
        let stakes: Vec<(NodeId, u64)> = validator_data
            .nodes()
            .map(|id| (*id, validator_data.get_stake(id).amount()))
            .collect();
        let staked = stakes.iter().filter(|(_, weight)| *weight > 0).count();
        if staked == 0 {
            return Err(ScheduleError::NotEnoughValidators {
                staked: 0,
                required: 1,
            });
        }
        // boundary_tail() takes the last window − 1 leaders; an epoch with
        // fewer rotations cannot seed the next epoch's window. Checked here
        // rather than in ProposerConfig::validate because the effective
        // window depends on the stake snapshot: with fewer staked validators
        // than K, an epoch needs only the shorter window's rotations.
        let window = cfg.concurrent_proposers.min(staked);
        if cfg.rotations_per_epoch() < window as u64 {
            return Err(ScheduleError::InvalidConfig(
                "epoch must cover at least one rotation per effective proposer phase",
            ));
        }
        let schedule = Self {
            window,
            cfg,
            algorithm,
            stakes,
            anchor,
            cache: Mutex::new(BTreeMap::new()),
        };
        schedule.leaders_of_epoch(schedule.anchor.epoch())?;
        Ok(schedule)
    }

    pub fn config(&self) -> &ProposerConfig {
        &self.cfg
    }

    /// The anchor this schedule derives from.
    pub fn anchor(&self) -> &EpochAnchor {
        &self.anchor
    }

    /// The anchor seeding `epoch`, for persisting at the epoch boundary:
    /// a fresh schedule [`Self::anchored`] on it yields the same
    /// assignments from `epoch` onward, without anything older.
    pub fn anchor_for_epoch(&self, epoch: u64) -> Result<EpochAnchor, ScheduleError> {
        if epoch < self.anchor.epoch() {
            return Err(ScheduleError::BeforeAnchor {
                queried: epoch,
                anchor: self.anchor.epoch(),
            });
        }
        if epoch == self.anchor.epoch() {
            return Ok(self.anchor.clone());
        }
        let leaders = self.leaders_of_epoch(epoch - 1)?;
        Ok(EpochAnchor::new(
            epoch,
            boundary_tail(&leaders, self.window - 1),
        ))
    }

    pub fn epoch_of_slot(&self, slot: Slot) -> u64 {
        slot.0 / self.cfg.slots_per_epoch
    }

    pub fn global_rotation_of_slot(&self, slot: Slot) -> u64 {
        let local = (slot.0 % self.cfg.slots_per_epoch) / self.cfg.slots_per_rotation();
        self.epoch_of_slot(slot) * self.cfg.rotations_per_epoch() + local
    }

    pub fn leader_of_global_rotation(&self, rotation: u64) -> Result<NodeId, ScheduleError> {
        let per_epoch = self.cfg.rotations_per_epoch();
        let leaders = self.leaders_of_epoch(rotation / per_epoch)?;
        Ok(leaders[(rotation % per_epoch) as usize])
    }

    fn leaders_of_epoch(&self, epoch: u64) -> Result<Arc<Vec<NodeId>>, ScheduleError> {
        if epoch < self.anchor.epoch() {
            return Err(ScheduleError::BeforeAnchor {
                queried: epoch,
                anchor: self.anchor.epoch(),
            });
        }

        let mut cache = self.cache.lock().expect("proposer schedule cache poisoned");
        if let Some(leaders) = cache.get(&epoch) {
            return Ok(leaders.clone());
        }

        // The retained epochs are contiguous, so a miss lies either behind
        // the oldest retained epoch or ahead of the newest.
        let tail_len = self.window - 1;
        if let Some((&newest, _)) = cache.iter().next_back()
            && epoch < newest
        {
            // Behind the retained range: replay the deterministic chain
            // from the anchor without touching the cache. Costs one
            // algorithm run per epoch since the anchor, but only queries
            // about slots older than the retained window get here.
            let mut leaders = self.build_epoch(self.anchor.epoch(), self.anchor.tail())?;
            for next in self.anchor.epoch() + 1..=epoch {
                let tail = boundary_tail(&leaders, tail_len);
                leaders = self.build_epoch(next, &tail)?;
            }
            return Ok(Arc::new(leaders));
        }

        // Ahead of the newest cached epoch (or the first build): extend
        // forward, chaining each boundary tail, then evict the oldest epochs
        // beyond the retention bound.
        let (mut next, mut tail) = match cache.iter().next_back() {
            Some((&newest, leaders)) => (newest + 1, boundary_tail(leaders, tail_len)),
            None => (self.anchor.epoch(), self.anchor.tail().to_vec()),
        };
        while next <= epoch {
            let leaders = self.build_epoch(next, &tail)?;
            tail = boundary_tail(&leaders, tail_len);
            cache.insert(next, Arc::new(leaders));
            if cache.len() > CACHED_EPOCHS {
                cache.pop_first();
            }
            next += 1;
        }

        Ok(cache
            .get(&epoch)
            .expect("epoch was built by the loop above")
            .clone())
    }

    fn build_epoch(&self, epoch: u64, prev_tail: &[NodeId]) -> Result<Vec<NodeId>, ScheduleError> {
        let leaders = self
            .algorithm
            .epoch_leaders(epoch, self.window, &self.stakes, prev_tail)?;
        if leaders.len() as u64 != self.cfg.rotations_per_epoch() {
            return Err(ScheduleError::InvalidConfig(
                "leader schedule returned the wrong number of rotations",
            ));
        }
        Ok(leaders)
    }

    #[cfg(test)]
    fn cached_epoch_count(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

fn boundary_tail(leaders: &[NodeId], tail_len: usize) -> Vec<NodeId> {
    leaders[leaders.len().saturating_sub(tail_len)..].to_vec()
}

impl<A: LeaderSchedule> ProposerSchedule for RotatingProposerSchedule<A> {
    fn num_indices(&self) -> usize {
        self.cfg.concurrent_proposers
    }

    fn proposers_at(&self, slot: Slot) -> Result<ProposerSet, ScheduleError> {
        let epoch = self.epoch_of_slot(slot);
        if epoch < self.anchor.epoch() {
            return Err(ScheduleError::BeforeAnchor {
                queried: epoch,
                anchor: self.anchor.epoch(),
            });
        }

        let window = self.window as u64;
        let rotation = self.global_rotation_of_slot(slot);
        // Exact because slots_per_epoch is a multiple of the rotation length.
        let slot_in_rotation = slot.0 % self.cfg.slots_per_rotation();
        // First rotation derived by this schedule; the up-to-`window − 1`
        // rotations before it resolve from the anchor tail.
        let anchor_rotation = self.anchor.epoch() * self.cfg.rotations_per_epoch();

        // Indices at or above the effective concurrency stay vacant.
        let mut proposers = vec![None; self.cfg.concurrent_proposers];
        for index in 0..window {
            // Newest rotation at or before `rotation` occupying `index`.
            let distance = (rotation % window + window - index) % window;
            let Some(serving) = rotation.checked_sub(distance) else {
                // The rotation would predate genesis: still vacant.
                continue;
            };
            // Rotation vacancy: a handed-over index stays vacant for one
            // blind window (`y` slots) so the incoming proposer knows all
            // in-flight proposals at its index. The chain's first `y`
            // slots have no handoff, which exempts exactly the first
            // rotation.
            if serving == rotation
                && slot_in_rotation < self.cfg.observation_cutoff
                && slot.0 >= self.cfg.observation_cutoff
            {
                continue;
            }
            proposers[index as usize] = if serving >= anchor_rotation {
                Some(self.leader_of_global_rotation(serving)?)
            } else {
                // Pre-anchor rotation: the anchor tail holds the leaders of
                // the last `window − 1` rotations before the anchor, oldest
                // first. A rotation beyond the tail is vacant — the
                // anchored counterpart of the genesis ramp-up.
                let back = (anchor_rotation - serving) as usize;
                let tail = self.anchor.tail();
                tail.len().checked_sub(back).map(|position| tail[position])
            };
        }

        Ok(ProposerSet {
            slot,
            rotation,
            proposers: proposers.into_boxed_slice(),
        })
    }
}

/// Test stub: a fixed assignment of proposers to proposal indices, identical
/// for every slot. No rotation, no vacancies.
#[derive(Debug)]
pub struct FixedProposerSchedule {
    proposers: Vec<NodeId>,
}

impl FixedProposerSchedule {
    /// The proposers must be distinct: one node holding two indices would
    /// make the reverse membership lookup ([`ProposerSet::index_of`])
    /// ambiguous.
    pub fn new(proposers: Vec<NodeId>) -> Self {
        assert!(!proposers.is_empty(), "at least one proposer required");
        let mut distinct = proposers.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert!(
            distinct.len() == proposers.len(),
            "proposers must be distinct"
        );
        Self { proposers }
    }
}

impl ProposerSchedule for FixedProposerSchedule {
    fn num_indices(&self) -> usize {
        self.proposers.len()
    }

    fn proposers_at(&self, slot: Slot) -> Result<ProposerSet, ScheduleError> {
        Ok(ProposerSet {
            slot,
            rotation: 0,
            proposers: self
                .proposers
                .iter()
                .map(|proposer| Some(*proposer))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{super::types::Stake, *};
    use crate::spec::vote::KeyPair as _;

    fn validator_data(stakes: &[(u64, u64)]) -> Arc<ValidatorData> {
        let valset = stakes
            .iter()
            .map(|&(id, weight)| (NodeId::dummy(id), Stake::from(weight)))
            .collect();
        let mapping = stakes
            .iter()
            .map(|&(id, _)| (NodeId::dummy(id), NodeId::dummy(id).keypair().pubkey()))
            .collect();
        Arc::new(ValidatorData::new(valset, mapping))
    }

    fn uniform(n: u64) -> Arc<ValidatorData> {
        let stakes: Vec<(u64, u64)> = (0..n).map(|id| (id, 1)).collect();
        validator_data(&stakes)
    }

    fn round_robin(
        cfg: ProposerConfig,
        validators: u64,
    ) -> RotatingProposerSchedule<RoundRobinLeaderSchedule> {
        let algorithm = RoundRobinLeaderSchedule::new(&cfg);
        RotatingProposerSchedule::new(cfg, algorithm, uniform(validators)).unwrap()
    }

    fn lottery(
        cfg: ProposerConfig,
        data: Arc<ValidatorData>,
    ) -> RotatingProposerSchedule<CreditLotterySchedule> {
        let algorithm = CreditLotterySchedule::recommended(&cfg, 0xC0FFEE);
        RotatingProposerSchedule::new(cfg, algorithm, data).unwrap()
    }

    fn occupied(set: &ProposerSet) -> usize {
        set.iter()
            .filter(|(_, proposer)| proposer.is_some())
            .count()
    }

    #[test]
    fn slot_and_rotation_math() {
        let cfg = ProposerConfig {
            concurrent_proposers: 2,
            observation_cutoff: 0,
            rotation_slack: 2,
            slots_per_epoch: 6, // 3 rotations of 2 slots
        };
        let schedule = round_robin(cfg, 4);

        assert_eq!(schedule.epoch_of_slot(Slot(5)), 0);
        assert_eq!(schedule.epoch_of_slot(Slot(6)), 1);

        let rotations: Vec<u64> = (0..14)
            .map(|s| schedule.global_rotation_of_slot(Slot(s)))
            .collect();
        // Continuous across the epoch seams at slots 6 and 12.
        assert_eq!(rotations, [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6]);
    }

    #[test]
    fn genesis_ramp_up() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 6,
        };
        let schedule = round_robin(cfg, 5);

        // One index per elapsed rotation until all K are live.
        assert_eq!(occupied(&schedule.proposers_at(Slot(0)).unwrap()), 1);
        assert_eq!(occupied(&schedule.proposers_at(Slot(1)).unwrap()), 2);
        for slot in 2..12 {
            assert_eq!(occupied(&schedule.proposers_at(Slot(slot)).unwrap()), 3);
        }
    }

    #[test]
    fn phase_is_stable_for_the_whole_rotation() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 3,
            slots_per_epoch: 9,
        };
        let schedule = round_robin(cfg, 5);

        for rotation in 0..6u64 {
            let index = (rotation % 3) as usize;
            let leader = schedule.leader_of_global_rotation(rotation).unwrap();
            for slot in rotation * 3..(rotation + 1) * 3 {
                let set = schedule.proposers_at(Slot(slot)).unwrap();
                assert_eq!(set.rotation(), rotation);
                assert_eq!(set.proposer(index), Some(leader));
            }
        }
    }

    #[test]
    fn rotation_handover_vacates_the_incoming_index() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 2,
            rotation_slack: 1,
            slots_per_epoch: 9,
        };
        let schedule = round_robin(cfg, 5);

        // The chain's first slots have no handoff: rotation 0 is exempt
        // from the rotation vacancy.
        for offset in 0..3 {
            assert!(
                schedule
                    .proposers_at(Slot(offset))
                    .unwrap()
                    .proposer(0)
                    .is_some()
            );
        }

        for rotation in 1..6u64 {
            let index = (rotation % 3) as usize;
            let leader = schedule.leader_of_global_rotation(rotation).unwrap();
            for offset in 0..3u64 {
                let set = schedule.proposers_at(Slot(rotation * 3 + offset)).unwrap();
                if offset < 2 {
                    assert_eq!(
                        set.proposer(index),
                        None,
                        "vacant during the rotation vacancy"
                    );
                } else {
                    assert_eq!(set.proposer(index), Some(leader));
                }
            }
        }
    }

    #[test]
    fn zero_slack_keeps_k_minus_one_indices_occupied() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 2,
            rotation_slack: 0,
            slots_per_epoch: 12,
        };
        let schedule = round_robin(cfg, 5);

        // Past the genesis ramp-up the incoming index is always inside its
        // blind window: exactly K - 1 indices carry a proposer.
        for slot in 4..24 {
            let set = schedule.proposers_at(Slot(slot)).unwrap();
            assert_eq!(occupied(&set), 2);
            assert_eq!(set.proposer((set.rotation() % 3) as usize), None);
        }
    }

    #[test]
    fn single_proposer_goes_vacant_during_the_rotation_vacancy() {
        // K = 1 is not special: with a blind window the only index is
        // vacant at every rotation start (except the chain's first), which
        // is why at least two concurrent proposers are needed to guarantee
        // a proposer in every slot.
        let cfg = ProposerConfig {
            concurrent_proposers: 1,
            observation_cutoff: 1,
            rotation_slack: 1,
            slots_per_epoch: 4,
        };
        let schedule = round_robin(cfg, 3);

        for slot in 0..12u64 {
            let set = schedule.proposers_at(Slot(slot)).unwrap();
            assert_eq!(set.num_indices(), 1);
            let rotation_start = slot.is_multiple_of(2);
            if rotation_start && slot > 0 {
                assert_eq!(set.proposer(0), None, "slot {slot}");
            } else {
                let leader = schedule.leader_of_global_rotation(slot / 2).unwrap();
                assert_eq!(set.proposer(0), Some(leader), "slot {slot}");
            }
        }
    }

    #[test]
    fn membership_inverts_the_proposer_set() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 6,
        };
        let schedule = round_robin(cfg, 5);

        for slot in 0..18 {
            let slot = Slot(slot);
            let set = schedule.proposers_at(slot).unwrap();
            let mut members = Vec::new();
            for (index, proposer) in set.iter() {
                let Some(proposer) = proposer else { continue };
                assert_eq!(
                    schedule.proposer_index_at(slot, &proposer).unwrap(),
                    Some(index)
                );
                members.push(proposer);
            }
            for id in 0..5 {
                let node = NodeId::dummy(id);
                if !members.contains(&node) {
                    assert_eq!(schedule.proposer_index_at(slot, &node).unwrap(), None);
                }
            }
        }
    }

    #[test]
    fn no_repeat_window_holds_across_epoch_seams() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 6,
        };
        let stakes: Vec<(u64, u64)> = (0..8).map(|id| (id, 10 + id)).collect();
        let schedule = lottery(cfg, validator_data(&stakes));

        // Five epochs of leaders, checked over every seam.
        let leaders: Vec<NodeId> = (0..30)
            .map(|rotation| schedule.leader_of_global_rotation(rotation).unwrap())
            .collect();
        for window in leaders.windows(3) {
            let mut seen = window.to_vec();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), 3, "repeat within the window: {window:?}");
        }
    }

    #[test]
    fn round_robin_honors_the_previous_tail() {
        // A seam where continuing the canonical cycle blindly would repeat:
        // the previous epoch ended ... 1, 2 and the cycle restarts at 1.
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 3,
        };
        let algorithm = RoundRobinLeaderSchedule::new(&cfg);
        let stakes: Vec<(NodeId, u64)> = (1..=3).map(|id| (NodeId::dummy(id), 1)).collect();
        let prev_tail = [NodeId::dummy(1), NodeId::dummy(2)];

        let leaders = algorithm.epoch_leaders(1, 3, &stakes, &prev_tail).unwrap();

        let mut seam: Vec<NodeId> = prev_tail.to_vec();
        seam.extend(&leaders);
        for window in seam.windows(3) {
            let mut seen = window.to_vec();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), 3, "repeat within the window: {window:?}");
        }
    }

    #[test]
    fn epoch_cache_is_bounded_and_evicted_epochs_rebuild_identically() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 6,
        };
        let stakes: Vec<(u64, u64)> = (0..8).map(|id| (id, 10 + id)).collect();
        let in_order = lottery(cfg.clone(), validator_data(&stakes));
        let jumped = lottery(cfg, validator_data(&stakes));

        // Jump far past the retention bound, evicting the early epochs.
        jumped.proposers_at(Slot(30 * 6)).unwrap();
        assert!(jumped.cached_epoch_count() <= CACHED_EPOCHS);

        // Evicted epochs are replayed from genesis, identically.
        for slot in 0..24 {
            assert_eq!(
                jumped.proposers_at(Slot(slot)).unwrap(),
                in_order.proposers_at(Slot(slot)).unwrap()
            );
        }
    }

    #[test]
    fn round_robin_rejects_duplicate_ids() {
        // Without the duplicate check this input hangs the selection loop:
        // three entries pass the length check, but the two distinct ids can
        // both be recent at once, leaving no eligible candidate.
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 3,
        };
        let algorithm = RoundRobinLeaderSchedule::new(&cfg);
        let stakes = vec![
            (NodeId::dummy(0), 1),
            (NodeId::dummy(0), 1),
            (NodeId::dummy(1), 1),
        ];
        assert!(matches!(
            algorithm.epoch_leaders(0, 3, &stakes, &[]).unwrap_err(),
            ScheduleError::DuplicateValidator
        ));
    }

    #[test]
    fn anchored_schedule_matches_the_genesis_chain() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 6,
        };
        let stakes: Vec<(u64, u64)> = (0..8).map(|id| (id, 10 + id)).collect();
        let genesis = lottery(cfg.clone(), validator_data(&stakes));

        // Persist the anchor at the epoch-2 boundary, as a node would, and
        // bring up a fresh schedule from it.
        let anchor = genesis.anchor_for_epoch(2).unwrap();
        let algorithm = CreditLotterySchedule::recommended(&cfg, 0xC0FFEE);
        let anchored =
            RotatingProposerSchedule::anchored(cfg, algorithm, validator_data(&stakes), anchor)
                .unwrap();

        // Identical assignments from the anchor onward, including the seam
        // lookback into pre-anchor rotations resolved from the tail.
        for slot in 12..36 {
            assert_eq!(
                anchored.proposers_at(Slot(slot)).unwrap(),
                genesis.proposers_at(Slot(slot)).unwrap(),
                "slot {slot}"
            );
        }

        // Anchors derived later agree as well.
        assert_eq!(
            anchored.anchor_for_epoch(4).unwrap(),
            genesis.anchor_for_epoch(4).unwrap()
        );

        // Epochs behind the anchor are not derivable.
        assert!(matches!(
            anchored.proposers_at(Slot(11)).unwrap_err(),
            ScheduleError::BeforeAnchor {
                queried: 1,
                anchor: 2
            }
        ));
        assert!(matches!(
            anchored.leader_of_global_rotation(11).unwrap_err(),
            ScheduleError::BeforeAnchor { .. }
        ));
        assert!(matches!(
            anchored.anchor_for_epoch(1).unwrap_err(),
            ScheduleError::BeforeAnchor { .. }
        ));
    }

    #[test]
    fn anchored_schedule_matches_under_a_rotation_vacancy() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 2,
            rotation_slack: 1,
            slots_per_epoch: 9,
        };
        let genesis = round_robin(cfg.clone(), 5);

        let anchor = genesis.anchor_for_epoch(1).unwrap();
        let algorithm = RoundRobinLeaderSchedule::new(&cfg);
        let anchored =
            RotatingProposerSchedule::anchored(cfg, algorithm, uniform(5), anchor).unwrap();

        // Blind-window vacancies and the seam lookback come out the same as
        // on the genesis-anchored chain; the first-rotation exemption does
        // not resurface at the anchor.
        for slot in 9..27 {
            assert_eq!(
                anchored.proposers_at(Slot(slot)).unwrap(),
                genesis.proposers_at(Slot(slot)).unwrap(),
                "slot {slot}"
            );
        }
    }

    #[test]
    fn capped_window_allows_short_epochs() {
        // Two staked validators under K = 4 need only two rotations per
        // epoch — one per effective phase — even though K itself would not
        // fit. Indices 2 and 3 stay vacant.
        let cfg = ProposerConfig {
            concurrent_proposers: 4,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 2,
        };
        let schedule = lottery(cfg, validator_data(&[(0, 10), (1, 20)]));

        for slot in 1..8u64 {
            let set = schedule.proposers_at(Slot(slot)).unwrap();
            assert_eq!(occupied(&set), 2);
            assert_eq!(set.proposer(2), None);
            assert_eq!(set.proposer(3), None);
        }
    }

    #[test]
    fn query_order_does_not_change_the_schedule() {
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 6,
        };
        let stakes: Vec<(u64, u64)> = (0..8).map(|id| (id, 10 + id)).collect();
        let in_order = lottery(cfg.clone(), validator_data(&stakes));
        let out_of_order = lottery(cfg, validator_data(&stakes));

        // Force the second scheduler to build epochs 0..=3 in one jump.
        out_of_order.proposers_at(Slot(20)).unwrap();

        for slot in 0..24 {
            assert_eq!(
                in_order.proposers_at(Slot(slot)).unwrap(),
                out_of_order.proposers_at(Slot(slot)).unwrap()
            );
        }
    }

    #[test]
    fn short_validator_set_caps_the_concurrency() {
        // Two staked validators under K = 4: indices 0 and 1 rotate between
        // them (effective window 2), indices 2 and 3 stay vacant for the
        // whole epoch.
        let cfg = ProposerConfig {
            concurrent_proposers: 4,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 4,
        };
        let schedule = lottery(cfg, validator_data(&[(0, 10), (1, 20)]));

        for slot in 0..12u64 {
            let set = schedule.proposers_at(Slot(slot)).unwrap();
            assert_eq!(set.num_indices(), 4);
            assert_eq!(set.proposer(2), None);
            assert_eq!(set.proposer(3), None);
            assert_eq!(occupied(&set), if slot == 0 { 1 } else { 2 });
        }

        let leaders: Vec<NodeId> = (0..12)
            .map(|rotation| schedule.leader_of_global_rotation(rotation).unwrap())
            .collect();
        for pair in leaders.windows(2) {
            assert_ne!(pair[0], pair[1], "effective window of 2 must alternate");
        }
    }

    #[test]
    fn invalid_configs_are_rejected() {
        let build = |cfg: ProposerConfig| {
            let algorithm = CreditLotterySchedule::recommended(&cfg, 0);
            RotatingProposerSchedule::new(cfg, algorithm, uniform(8)).unwrap_err()
        };

        let empty_rotation = build(ProposerConfig {
            concurrent_proposers: 2,
            observation_cutoff: 0,
            rotation_slack: 0,
            slots_per_epoch: 4,
        });
        assert!(matches!(empty_rotation, ScheduleError::InvalidConfig(_)));

        // The round-robin stub reads rotations_per_epoch before validation
        // can run: a zero rotation length must reject, not divide by zero.
        let cfg = ProposerConfig {
            concurrent_proposers: 2,
            observation_cutoff: 0,
            rotation_slack: 0,
            slots_per_epoch: 4,
        };
        let algorithm = RoundRobinLeaderSchedule::new(&cfg);
        let err = RotatingProposerSchedule::new(cfg, algorithm, uniform(8)).unwrap_err();
        assert!(matches!(err, ScheduleError::InvalidConfig(_)));

        let ragged_epoch = build(ProposerConfig {
            concurrent_proposers: 2,
            observation_cutoff: 0,
            rotation_slack: 2,
            slots_per_epoch: 7,
        });
        assert!(matches!(ragged_epoch, ScheduleError::InvalidConfig(_)));

        // Eight staked validators leave the effective window at K = 3: two
        // rotations per epoch cannot seed the next epoch's window.
        let short_epoch = build(ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 2,
        });
        assert!(matches!(short_epoch, ScheduleError::InvalidConfig(_)));

        let overflowing_rotation = build(ProposerConfig {
            concurrent_proposers: 2,
            observation_cutoff: u64::MAX,
            rotation_slack: 1,
            slots_per_epoch: 4,
        });
        assert!(matches!(
            overflowing_rotation,
            ScheduleError::InvalidConfig(_)
        ));
    }

    #[test]
    fn zero_staked_validators_are_rejected() {
        // ValidatorData cannot hold zero-stake entries, but a LeaderSchedule
        // can still be handed all-zero weights (e.g. by a stake quantization
        // that rounds everything down): both algorithms reject.
        let cfg = ProposerConfig {
            concurrent_proposers: 3,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 6,
        };
        let stakes: Vec<(NodeId, u64)> = vec![(NodeId::dummy(0), 0), (NodeId::dummy(1), 0)];

        let lottery = CreditLotterySchedule::recommended(&cfg, 0);
        assert!(matches!(
            lottery.epoch_leaders(0, 1, &stakes, &[]).unwrap_err(),
            ScheduleError::NotEnoughValidators { staked: 0, .. }
        ));

        let round_robin = RoundRobinLeaderSchedule::new(&cfg);
        assert!(matches!(
            round_robin.epoch_leaders(0, 1, &stakes, &[]).unwrap_err(),
            ScheduleError::NotEnoughValidators { staked: 0, .. }
        ));
    }

    #[test]
    fn wrong_rotation_count_is_rejected() {
        #[derive(Debug)]
        struct OneLeader;
        impl LeaderSchedule for OneLeader {
            fn epoch_leaders(
                &self,
                _epoch: u64,
                _window: usize,
                stakes: &[(NodeId, u64)],
                _prev_tail: &[NodeId],
            ) -> Result<Vec<NodeId>, ScheduleError> {
                Ok(vec![stakes[0].0])
            }
        }

        let cfg = ProposerConfig {
            concurrent_proposers: 1,
            observation_cutoff: 0,
            rotation_slack: 1,
            slots_per_epoch: 4,
        };
        let err = RotatingProposerSchedule::new(cfg, OneLeader, uniform(2)).unwrap_err();
        assert!(matches!(err, ScheduleError::InvalidConfig(_)));
    }

    #[test]
    #[should_panic(expected = "proposers must be distinct")]
    fn fixed_schedule_rejects_duplicates() {
        FixedProposerSchedule::new(vec![NodeId::dummy(0), NodeId::dummy(1), NodeId::dummy(0)]);
    }

    #[test]
    fn fixed_schedule_is_constant() {
        let proposers: Vec<NodeId> = (0..3).map(NodeId::dummy).collect();
        let schedule = FixedProposerSchedule::new(proposers.clone());

        assert_eq!(schedule.num_indices(), 3);
        for slot in [0, 7, 1_000] {
            let set = schedule.proposers_at(Slot(slot)).unwrap();
            for (index, expected) in proposers.iter().enumerate() {
                assert_eq!(set.proposer(index), Some(*expected));
            }
            assert_eq!(
                schedule
                    .proposer_index_at(Slot(slot), &proposers[1])
                    .unwrap(),
                Some(1)
            );
        }
    }
}
