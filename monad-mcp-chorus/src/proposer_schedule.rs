//! Stake-weighted proposer scheduling with a strict no-repeat window —
//! the *guarded clipped-credit lottery*.
//!
//! # What this module does
//!
//! For every stake epoch it derives — deterministically from the validator
//! set, the epoch number, and a fixed seed — the sequence of proposer
//! *rotations* (one leader per [`ScheduleConfig::slots_per_rotation`] slots)
//! such that
//!
//! 1. **Frequency**: each validator's share of rotations matches its share of
//!    total stake,
//! 2. **Diversity**: the sequence is pseudorandom — no small repeating set of
//!    proposer configurations, and a fresh schedule every epoch, and
//! 3. **Window (strict)**: no validator appears twice within any `K`
//!    consecutive rotations. The protocol runs `K` overlapping proposer
//!    phases; this guarantees a validator never holds two live phases.
//!
//! Requirement 3 is absolute; 1 and 2 are approximations, as tight as the
//! constraint allows. Feasibility needs a stake cap: a validator above `1/K`
//! of total stake cannot reach its share (and any valid schedule is forced to
//! run it almost periodically). The protocol should cap stake share below
//! `1/K`; a cap at or below `0.5/K` also removes the forced periodicity.
//!
//! # How it works
//!
//! One rotation at a time. Track each validator's *deficit* — the exact
//! number of rotations it is owed, scaled by total stake to stay in integers:
//! `d_x(t) = w_x·t − W·c_x(t)` (stake `w_x`, total stake `W`, rotations held
//! so far `c_x`). Then, per rotation:
//!
//! * **window** — validators drawn within the last `K−1` rotations are
//!   ineligible;
//! * **guard** — if some eligible validator is owed more than
//!   [`ScheduleConfig::guard_threshold`] whole rotations, the most-owed one is
//!   placed deterministically. Without this, validators near the `1/K` cap
//!   fall behind systematically (see *Constants* below);
//! * **lottery** — otherwise draw an eligible validator with probability
//!   proportional to its *credit* `max(0, d_x + w_x·L)`, its owed amount over
//!   the next `L` rotations, using one PRF value derived from
//!   `(seed, global rotation index)`.
//!
//! The deficit feedback is what makes frequencies track stake: window
//! exclusions (which hit heavy validators more often) grow a validator's
//! deficit until the lottery — or ultimately the guard — repays it.
//!
//! # Why this design
//!
//! * *Redraw on window collision* (rejection sampling) permanently skews
//!   frequencies — a 19%-stake validator loses ≈ 40% of its rotations at
//!   `K = 5` — because exclusions are never repaid.
//! * *Deterministic round-robin* (always pick the most-owed validator, no
//!   PRF) gives exact frequencies but visits only a tiny, repeating set of
//!   proposer configurations (on the order of the stake granularity) and
//!   replays identically in every epoch with an unchanged validator set.
//! * *Per-segment exact quotas with a constrained shuffle* offers provable
//!   ±1 fairness and O(1) random access, but requires urgency/restart/seam
//!   machinery; rejected for complexity. Revisit if provable per-epoch
//!   fairness bounds become a requirement.
//!
//! The chosen lottery is a short integer-only loop: O(V) memory, O(V) time
//! per rotation, no floating point (bit-exact across platforms), and no
//! state beyond the epoch's stake snapshot — any node reproduces a schedule
//! from `(seed, epoch, stakes)` alone.
//!
//! # Constants (measured; see `weighted-window-sampling.ipynb`)
//!
//! With ~200 validators, `K = 5`, and epochs of a few thousand rotations:
//!
//! * `lookahead L = 200`, `guard D = 2`: the heaviest validator's per-epoch
//!   shortfall is ≈ −0.2%, with ~22% of rotations placed by the guard
//!   (near-cap validators are forced almost periodic by the window
//!   constraint itself, not by this algorithm).
//! * The guarded residual is ≈ `−D/(π·n)` per epoch for a validator with
//!   stake share `π`: `D = 1` halves it but doubles guard placements (41%);
//!   `D ≥ 4` grows it linearly and buys nothing. `D = 2` is the sweet spot.
//! * `L` trades frequency tightness against schedule entropy; values in
//!   `50..=200` measure equivalently once the guard is active.
//! * Throughput: ≈ 0.3 µs per rotation single-threaded; a full 50k-rotation
//!   epoch precomputes in ~15 ms and stores one id per rotation.
//!
//! # Epoch boundaries
//!
//! Counts and deficits reset at every epoch (stakes change there anyway).
//! Only the last `K−1` leaders of the previous epoch are passed back in
//! ([`EpochSchedule::boundary_tail`]) to seed the window, so requirement 3
//! also holds across the boundary. Pass an empty tail at genesis, or wherever
//! the protocol drains all proposer phases across a boundary.

use super::types::{NodeId, Slot, ValidatorData};

/// Parameters of the proposer schedule. Protocol-wide constants; changing any
/// of them is a consensus-breaking change.
#[derive(Debug, Clone)]
pub struct ScheduleConfig {
    /// `K`: number of concurrently active (staggered) proposer phases, and
    /// thereby the size of the no-repeat window.
    pub concurrent_proposers: usize,
    /// Slots per proposer rotation, `r = BLIND_WINDOW + ROTATION_SLACK`.
    pub slots_per_rotation: u64,
    /// Slots per stake epoch. Stakes are constant within an epoch; the
    /// scheduler restarts at each epoch boundary.
    pub slots_per_epoch: u64,
    /// `L`: credit lookahead, in rotations. See module docs; default
    /// [`ScheduleConfig::RECOMMENDED_LOOKAHEAD`].
    pub lookahead: u64,
    /// `D`: guard threshold, in whole rotations of deficit. See module docs;
    /// default [`ScheduleConfig::RECOMMENDED_GUARD_THRESHOLD`].
    pub guard_threshold: u64,
    /// Fixed PRF seed. Needs no secrecy and no unpredictability — only that
    /// nobody can choose it; a protocol-wide constant is fine.
    pub seed: u64,
}

impl ScheduleConfig {
    pub const RECOMMENDED_LOOKAHEAD: u64 = 200;
    pub const RECOMMENDED_GUARD_THRESHOLD: u64 = 2;

    /// Rotations in one epoch. A final partial rotation (if the epoch length
    /// is not a multiple of the rotation length) still gets a leader.
    pub fn rotations_per_epoch(&self) -> u64 {
        self.slots_per_epoch.div_ceil(self.slots_per_rotation)
    }

    fn validate(&self) -> Result<(), ScheduleError> {
        if self.concurrent_proposers == 0 {
            return Err(ScheduleError::InvalidConfig(
                "concurrent_proposers must be > 0",
            ));
        }
        if self.slots_per_rotation == 0 {
            return Err(ScheduleError::InvalidConfig(
                "slots_per_rotation must be > 0",
            ));
        }
        if self.slots_per_epoch < self.slots_per_rotation {
            return Err(ScheduleError::InvalidConfig(
                "slots_per_epoch must cover at least one rotation",
            ));
        }
        if self.lookahead == 0 {
            return Err(ScheduleError::InvalidConfig("lookahead must be > 0"));
        }
        if self.guard_threshold == 0 {
            return Err(ScheduleError::InvalidConfig("guard_threshold must be > 0"));
        }
        // boundary_tail() takes the last K-1 leaders; an epoch with fewer
        // rotations cannot seed the next epoch's window.
        if self.rotations_per_epoch() < self.concurrent_proposers as u64 {
            return Err(ScheduleError::InvalidConfig(
                "epoch must cover at least concurrent_proposers rotations",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    InvalidConfig(&'static str),
    /// Fewer positively-staked validators than concurrent proposer phases:
    /// the no-repeat window could not be satisfied.
    NotEnoughValidators {
        staked: usize,
        required: usize,
    },
    /// The same validator id was supplied twice.
    DuplicateValidator,
}

impl std::fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduleError::InvalidConfig(msg) => write!(f, "invalid schedule config: {msg}"),
            ScheduleError::NotEnoughValidators { staked, required } => write!(
                f,
                "{staked} positively-staked validators, but {required} concurrent proposers required"
            ),
            ScheduleError::DuplicateValidator => write!(f, "duplicate validator id in stake set"),
        }
    }
}

impl std::error::Error for ScheduleError {}

/// The proposer schedule of one epoch: one leader per rotation.
///
/// Generic over the validator id so the algorithm can be tested and simulated
/// independently of the protocol types; use it as `EpochSchedule<NodeId>`.
/// `Id` must have a canonical total order (consensus depends on it).
#[derive(Debug, Clone)]
pub struct EpochSchedule<Id = NodeId> {
    epoch: u64,
    window: usize,
    slots_per_rotation: u64,
    slots_per_epoch: u64,
    leaders: Vec<Id>,
}

impl<Id: Ord + Clone> EpochSchedule<Id> {
    /// Build the schedule of `epoch` from that epoch's stake snapshot.
    ///
    /// `stakes` is the validator set with raw stake amounts; zero-stake
    /// entries are ignored. Iteration order does not matter — validators are
    /// put into canonical (sorted) order internally, so map-typed sources are
    /// safe. `prev_epoch_tail` must be [`EpochSchedule::boundary_tail`] of the
    /// previous epoch (empty at genesis): it seeds the no-repeat window so
    /// that the constraint also holds across the epoch boundary.
    pub fn build(
        cfg: &ScheduleConfig,
        epoch: u64,
        stakes: impl IntoIterator<Item = (Id, u64)>,
        prev_epoch_tail: &[Id],
    ) -> Result<Self, ScheduleError> {
        cfg.validate()?;

        // Canonical validator order. Consensus-critical: the lottery works on
        // indices, and every node must assign the same index to the same id.
        let mut validators: Vec<(Id, u64)> =
            stakes.into_iter().filter(|(_, stake)| *stake > 0).collect();
        validators.sort_by(|a, b| a.0.cmp(&b.0));
        if validators.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(ScheduleError::DuplicateValidator);
        }
        if validators.len() < cfg.concurrent_proposers {
            return Err(ScheduleError::NotEnoughValidators {
                staked: validators.len(),
                required: cfg.concurrent_proposers,
            });
        }

        let weights: Vec<u64> = validators.iter().map(|(_, stake)| *stake).collect();
        let rotations = cfg.rotations_per_epoch();
        let mut lottery = CreditLottery::new(
            weights,
            cfg.concurrent_proposers,
            cfg.lookahead,
            cfg.guard_threshold,
            cfg.seed,
            epoch.wrapping_mul(rotations), // global rotation index base
        );

        // Seed the window with the previous epoch's tail. Ids that left the
        // validator set are skipped: they cannot be scheduled anyway.
        let tail_len = cfg.concurrent_proposers - 1;
        let tail = &prev_epoch_tail[prev_epoch_tail.len().saturating_sub(tail_len)..];
        for (position, id) in tail.iter().enumerate() {
            if let Ok(index) = validators.binary_search_by(|v| v.0.cmp(id)) {
                lottery.preload_window(index, (tail.len() - position) as u64);
            }
        }

        let leaders = (0..rotations)
            .map(|_| validators[lottery.next_leader()].0.clone())
            .collect();

        Ok(EpochSchedule {
            epoch,
            window: cfg.concurrent_proposers,
            slots_per_rotation: cfg.slots_per_rotation,
            slots_per_epoch: cfg.slots_per_epoch,
            leaders,
        })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn first_slot(&self) -> Slot {
        Slot(self.epoch * self.slots_per_epoch)
    }

    /// Leaders of this epoch, one per rotation, in rotation order.
    pub fn leaders(&self) -> &[Id] {
        &self.leaders
    }

    pub fn leader_of_rotation(&self, rotation: u64) -> Option<&Id> {
        self.leaders.get(usize::try_from(rotation).ok()?)
    }

    /// Rotation index of `slot` within this epoch, or `None` if the slot
    /// belongs to a different epoch.
    pub fn rotation_of_slot(&self, slot: Slot) -> Option<u64> {
        let first = self.first_slot().0;
        if slot.0 < first || slot.0 - first >= self.slots_per_epoch {
            return None;
        }
        Some((slot.0 - first) / self.slots_per_rotation)
    }

    pub fn leader_at_slot(&self, slot: Slot) -> Option<&Id> {
        self.leader_of_rotation(self.rotation_of_slot(slot)?)
    }

    /// The last `K−1` leaders. Pass as `prev_epoch_tail` when building the
    /// next epoch, so the no-repeat window spans the boundary.
    pub fn boundary_tail(&self) -> &[Id] {
        &self.leaders[self.leaders.len() - (self.window - 1)..]
    }
}

impl EpochSchedule<NodeId> {
    /// Convenience wrapper over [`EpochSchedule::build`] for the shared
    /// protocol types.
    pub fn build_from_validator_data(
        cfg: &ScheduleConfig,
        epoch: u64,
        validators: &ValidatorData,
        prev_epoch_tail: &[NodeId],
    ) -> Result<Self, ScheduleError> {
        use crate::spec::{Stake as _, validator::ValidatorData as _};

        Self::build(
            cfg,
            epoch,
            validators
                .nodes()
                .map(|id| (*id, validators.get_stake(id).amount())),
            prev_epoch_tail,
        )
    }
}

// ---------------------------------------------------------------------------
// The core lottery, on validator indices `0..V` in canonical order.
// ---------------------------------------------------------------------------

/// All arithmetic is exact: deficits and credits are integers scaled by the
/// total stake `W`, kept in `i128` (raw `u64` stakes times an epoch's worth
/// of rotations fit comfortably; no quantization of stakes is needed).
struct CreditLottery {
    weights: Vec<u64>,
    total_weight: u128, // W
    counts: Vec<u64>,
    /// Rotation at which a validator was last drawn (`NEVER` if it was not).
    /// Window-seed entries from the previous epoch are negative.
    last_drawn: Vec<i64>,
    credits: Vec<u128>, // per-rotation scratch
    window: usize,      // K
    lookahead: i128,    // L
    guard_units: i128,  // D·W: deficits at or above force a placement
    t: i64,             // rotation within the epoch
    seed: u64,
    global_base: u64,
}

const NEVER: i64 = i64::MIN / 2;
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(GOLDEN);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl CreditLottery {
    fn new(
        weights: Vec<u64>,
        window: usize,
        lookahead: u64,
        guard_threshold: u64,
        seed: u64,
        global_base: u64,
    ) -> Self {
        let n = weights.len();
        let total_weight: u128 = weights.iter().map(|&w| u128::from(w)).sum();
        CreditLottery {
            total_weight,
            counts: vec![0; n],
            last_drawn: vec![NEVER; n],
            credits: vec![0; n],
            window,
            lookahead: i128::from(lookahead),
            guard_units: i128::from(guard_threshold) * total_weight as i128,
            t: 0,
            seed,
            global_base,
            weights,
        }
    }

    /// Mark `index` as drawn `distance` rotations before the epoch started
    /// (`distance = 1` is the immediately preceding rotation).
    fn preload_window(&mut self, index: usize, distance: u64) {
        self.last_drawn[index] = -(distance as i64);
    }

    /// The PRF value of the current rotation: a pure function of
    /// `(seed, global rotation index)`. Global indices never repeat, so equal
    /// stake snapshots in different epochs still yield different schedules.
    fn rotation_word(&self) -> u128 {
        let g = self.global_base.wrapping_add(self.t as u64);
        let hi = splitmix64(self.seed ^ g.wrapping_mul(GOLDEN));
        let lo = splitmix64(hi);
        (u128::from(hi) << 64) | u128::from(lo)
    }

    fn in_window(&self, index: usize) -> bool {
        self.t - self.last_drawn[index] < self.window as i64
    }

    fn next_leader(&mut self) -> usize {
        let t = i128::from(self.t);
        let total_weight = self.total_weight as i128;

        // Deficits and clipped credits of all eligible validators.
        let mut total_credit: u128 = 0;
        let mut max_deficit = i128::MIN;
        let mut most_owed = 0;
        for x in 0..self.weights.len() {
            if self.in_window(x) {
                self.credits[x] = 0;
                continue;
            }
            let weight = i128::from(self.weights[x]);
            let deficit = weight * t - total_weight * i128::from(self.counts[x]);
            if deficit > max_deficit {
                // Ties resolve to the lowest canonical index: deterministic.
                max_deficit = deficit;
                most_owed = x;
            }
            let credit = (deficit + weight * self.lookahead).max(0) as u128;
            self.credits[x] = credit;
            total_credit += credit;
        }

        let chosen = if max_deficit >= self.guard_units {
            // Guard: someone is owed more than D whole rotations.
            most_owed
        } else if total_credit > 0 {
            self.weighted_pick(&|x, lottery| lottery.credits[x], total_credit)
        } else {
            // No eligible validator holds credit (only reachable for very
            // small lookahead): fall back to stake-proportional selection.
            let eligible_stake: u128 = (0..self.weights.len())
                .filter(|&x| !self.in_window(x))
                .map(|x| u128::from(self.weights[x]))
                .sum();
            self.weighted_pick(&|x, lottery| u128::from(lottery.weights[x]), eligible_stake)
        };

        self.counts[chosen] += 1;
        self.last_drawn[chosen] = self.t;
        self.t += 1;
        chosen
    }

    /// Draw an eligible validator with probability proportional to
    /// `weight_of(x)`, which must sum to `total > 0` over eligible `x`.
    ///
    /// The 128-bit PRF value is reduced modulo `total`; the resulting
    /// selection bias is below 2⁻⁵⁰ relative and identical on every node.
    fn weighted_pick(&self, weight_of: &dyn Fn(usize, &Self) -> u128, total: u128) -> usize {
        debug_assert!(total > 0);
        let mut target = self.rotation_word() % total;
        for x in 0..self.weights.len() {
            if self.in_window(x) {
                continue;
            }
            let weight = weight_of(x, self);
            if target < weight {
                return x;
            }
            target -= weight;
        }
        unreachable!("target < total = sum of eligible weights")
    }
}

// ---------------------------------------------------------------------------
// Tests. The public API is generic over the id type, so the algorithm is
// exercised end to end with plain integer ids.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ScheduleConfig {
        ScheduleConfig {
            concurrent_proposers: 5,
            slots_per_rotation: 10,
            slots_per_epoch: 20_000, // 2_000 rotations
            lookahead: ScheduleConfig::RECOMMENDED_LOOKAHEAD,
            guard_threshold: ScheduleConfig::RECOMMENDED_GUARD_THRESHOLD,
            seed: 0xC0FFEE,
        }
    }

    /// 50 validators, skewed stakes, the heaviest at ~19% of the total —
    /// just under the 1/K = 20% feasibility cap.
    fn stakes() -> Vec<(u32, u64)> {
        let mut v: Vec<(u32, u64)> = (0..49u32).map(|i| (i, 60 + 4 * u64::from(i))).collect();
        v.push((49, 1_793));
        v
    }

    fn total_stake(stakes: &[(u32, u64)]) -> u64 {
        stakes.iter().map(|&(_, s)| s).sum()
    }

    fn assert_window_holds(leaders: &[u32], k: usize) {
        for window in leaders.windows(k) {
            let mut seen = window.to_vec();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), k, "validator repeated within {k} rotations");
        }
    }

    #[test]
    fn window_constraint_holds_within_and_across_epochs() {
        let cfg = cfg();
        let mut chained: Vec<u32> = Vec::new();
        let mut tail: Vec<u32> = Vec::new();
        for epoch in 0..3 {
            let schedule = EpochSchedule::build(&cfg, epoch, stakes(), &tail).unwrap();
            chained.extend_from_slice(schedule.leaders());
            tail = schedule.boundary_tail().to_vec();
        }
        // Checks every window, including the two epoch seams.
        assert_window_holds(&chained, cfg.concurrent_proposers);
    }

    #[test]
    fn frequencies_track_stake() {
        let cfg = cfg();
        let stakes = stakes();
        let total = total_stake(&stakes) as f64;
        let epochs = 40;
        let mut counts = vec![0u64; stakes.len()];
        let mut tail: Vec<u32> = Vec::new();
        for epoch in 0..epochs {
            let schedule = EpochSchedule::build(&cfg, epoch, stakes.clone(), &tail).unwrap();
            for &leader in schedule.leaders() {
                counts[leader as usize] += 1;
            }
            tail = schedule.boundary_tail().to_vec();
        }
        let rotations = (epochs * cfg.rotations_per_epoch()) as f64;
        for &(id, stake) in &stakes {
            let share = stake as f64 / total;
            let bias = counts[id as usize] as f64 / (rotations * share) - 1.0;
            if share >= 0.15 {
                // Near-cap validator: guard residual ≈ −D/(π·n) per epoch.
                assert!(bias.abs() < 0.015, "heavy validator bias {bias:+.4}");
            } else if share >= 0.01 {
                assert!(bias.abs() < 0.05, "validator {id} bias {bias:+.4}");
            }
        }
    }

    #[test]
    fn deterministic_and_epoch_dependent() {
        let cfg = cfg();
        let a = EpochSchedule::build(&cfg, 7, stakes(), &[]).unwrap();
        let b = EpochSchedule::build(&cfg, 7, stakes(), &[]).unwrap();
        assert_eq!(a.leaders(), b.leaders(), "same inputs must reproduce");

        let c = EpochSchedule::build(&cfg, 8, stakes(), &[]).unwrap();
        assert_ne!(a.leaders(), c.leaders(), "epochs must not replay");
    }

    #[test]
    fn stake_order_does_not_matter() {
        let cfg = cfg();
        let forward = EpochSchedule::build(&cfg, 1, stakes(), &[]).unwrap();
        let mut reversed = stakes();
        reversed.reverse();
        let backward = EpochSchedule::build(&cfg, 1, reversed, &[]).unwrap();
        assert_eq!(forward.leaders(), backward.leaders());
    }

    #[test]
    fn boundary_tail_seeds_the_window() {
        let cfg = cfg();
        let previous = EpochSchedule::build(&cfg, 0, stakes(), &[]).unwrap();
        let tail = previous.boundary_tail().to_vec();
        assert_eq!(tail.len(), cfg.concurrent_proposers - 1);
        let next = EpochSchedule::build(&cfg, 1, stakes(), &tail).unwrap();
        let mut seam = tail;
        seam.extend_from_slice(next.leaders());
        assert_window_holds(
            &seam[..2 * cfg.concurrent_proposers],
            cfg.concurrent_proposers,
        );
    }

    #[test]
    fn zero_stake_validators_are_never_scheduled() {
        let cfg = cfg();
        let mut stakes = stakes();
        stakes.push((99, 0));
        let schedule = EpochSchedule::build(&cfg, 0, stakes, &[]).unwrap();
        assert!(schedule.leaders().iter().all(|&id| id != 99));
    }

    #[test]
    fn rejects_too_few_validators() {
        let cfg = cfg();
        let few: Vec<(u32, u64)> = vec![(0, 10), (1, 10), (2, 10), (3, 0)];
        let err = EpochSchedule::build(&cfg, 0, few, &[]).unwrap_err();
        assert_eq!(
            err,
            ScheduleError::NotEnoughValidators {
                staked: 3,
                required: 5
            }
        );
    }

    #[test]
    fn rejects_duplicate_ids() {
        let cfg = cfg();
        let dup: Vec<(u32, u64)> = vec![(0, 10), (1, 10), (1, 20), (2, 10), (3, 10), (4, 10)];
        let err = EpochSchedule::build(&cfg, 0, dup, &[]).unwrap_err();
        assert_eq!(err, ScheduleError::DuplicateValidator);
    }

    #[test]
    fn slot_mapping() {
        let cfg = cfg();
        let schedule = EpochSchedule::build(&cfg, 2, stakes(), &[]).unwrap();
        let first = schedule.first_slot().0;
        assert_eq!(first, 2 * cfg.slots_per_epoch);
        assert!(schedule.leader_at_slot(Slot(first - 1)).is_none());
        assert_eq!(
            schedule.leader_at_slot(Slot(first)),
            schedule.leader_of_rotation(0)
        );
        assert_eq!(
            schedule.leader_at_slot(Slot(first + cfg.slots_per_rotation)),
            schedule.leader_of_rotation(1)
        );
        assert!(
            schedule
                .leader_at_slot(Slot(first + cfg.slots_per_epoch))
                .is_none()
        );
    }

    #[test]
    fn schedules_diverse_configurations() {
        // Distinct K-windows should keep accumulating (no small cycle).
        let cfg = cfg();
        let schedule = EpochSchedule::build(&cfg, 0, stakes(), &[]).unwrap();
        let k = cfg.concurrent_proposers;
        let mut seen = std::collections::HashSet::new();
        for window in schedule.leaders().windows(k) {
            seen.insert(window.to_vec());
        }
        let windows = schedule.leaders().len() - k + 1;
        assert!(
            seen.len() as f64 > 0.95 * windows as f64,
            "only {} distinct configurations in {} windows",
            seen.len(),
            windows
        );
    }
}
