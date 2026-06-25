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

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    num::NonZeroU64,
};

use thiserror::Error;
use tracing::{debug, warn};

pub mod acs;
pub mod deadline_agreement;
pub mod dummy;

use acs::Acs;
pub use deadline_agreement::{DeadlineAgreementManager, DeadlineAgreementMessage};

use super::types::{self, NodeId, Slot, Timestamp, TimestampDelta, WindowId};

pub trait Conductor
where
    Self: Sized,
{
    type Alarm;
    type Message;

    fn handle_message(&mut self, sender: NodeId, message: Self::Message);
    fn handle_alarm(&mut self, alarm: Self::Alarm);
    fn handle_slot_finalization(&mut self, at: Timestamp, slot: Slot);

    fn poll(&mut self) -> Option<ConductorOutput<Self>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConductorOutput<C>
where
    C: Conductor,
{
    Broadcast(C::Message),
    ScheduleAlarm(Timestamp, C::Alarm),

    // Open a batch of slots each with its deadline.
    // Invariant: the slots must be contiguous.
    OpenSlots(BTreeMap<Slot, Timestamp>),

    // Close all slots strictly earlier than the cap.
    CloseSlots { cap: Slot },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConductorConfig {
    slots_per_window: NonZeroU64,
    sync_boundary_slots: NonZeroU64,
    slot_interval: TimestampDelta,
    // Deadline of the first slot at genesis. Config field for now; can be
    // moved into a chain parameter in the future.
    genesis_deadline: Timestamp,
}

impl ConductorConfig {
    pub fn new(
        slots_per_window: NonZeroU64,
        sync_boundary_slots: NonZeroU64,
        slot_interval: TimestampDelta,
        genesis_deadline: Timestamp,
    ) -> Result<Self, ConductorError> {
        if sync_boundary_slots > slots_per_window {
            return Err(ConductorError::InvalidConfig(
                "sync_boundary_slots must not exceed slots_per_window",
            ));
        }

        let config = Self {
            slots_per_window,
            sync_boundary_slots,
            slot_interval,
            genesis_deadline,
        };

        Ok(config)
    }

    pub fn slots_per_window(&self) -> NonZeroU64 {
        self.slots_per_window
    }

    pub fn sync_boundary_slots(&self) -> NonZeroU64 {
        self.sync_boundary_slots
    }

    pub fn slot_interval(&self) -> TimestampDelta {
        self.slot_interval
    }

    pub fn genesis_deadline(&self) -> Timestamp {
        self.genesis_deadline
    }

    pub fn first_slot(&self, window: WindowId) -> Result<Slot, ConductorError> {
        window
            .get()
            .checked_mul(self.slots_per_window().get())
            .map(Slot)
            .ok_or(ConductorError::ArithmeticOverflow)
    }

    pub fn window_of(&self, slot: Slot) -> Result<WindowId, ConductorError> {
        Ok(WindowId(slot.get() / self.slots_per_window()))
    }

    pub fn sync_boundary_slot(&self, window: WindowId) -> Result<Slot, ConductorError> {
        let first = self.first_slot(window)?;
        first
            .checked_add(self.sync_boundary_slots().get() - 1)
            .ok_or(ConductorError::ArithmeticOverflow)
    }

    pub fn natural_first_slot_deadline(
        &self,
        window: WindowId,
    ) -> Result<Timestamp, ConductorError> {
        let first_slot = self.first_slot(window)?;
        self.genesis_deadline
            .checked_add_deltas(self.slot_interval(), first_slot.get())
            .ok_or(ConductorError::ArithmeticOverflow)
    }

    fn deadline_for_slot(
        &self,
        window: WindowId,
        first_deadline: Timestamp,
        slot: Slot,
    ) -> Result<Timestamp, ConductorError> {
        let next_window = window
            .checked_next()
            .ok_or(ConductorError::ArithmeticOverflow)?;
        let slot_start = self.first_slot(window)?;
        let slot_end = self.first_slot(next_window)?;

        if slot < slot_start || slot >= slot_end {
            return Err(ConductorError::InvalidSlot(slot));
        }

        let offset = slot
            .checked_sub(slot_start)
            .ok_or(ConductorError::ArithmeticOverflow)?;
        first_deadline
            .checked_add_deltas(self.slot_interval, offset)
            .ok_or(ConductorError::ArithmeticOverflow)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConductorError {
    #[error("invalid conductor config: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid conductor slot: {0:?}")]
    InvalidSlot(Slot),
    #[error("conductor arithmetic overflow")]
    ArithmeticOverflow,
    #[error("slot {slot:?} is not open; open slot cap is {open_slot_cap:?}")]
    SlotNotOpen { slot: Slot, open_slot_cap: Slot },
}

// Tracks completed slots and the finalization cap.
//
// Invariant: every slot strictly below `cap` is completed, and `completed`
// holds exactly the slots at or above `cap` completed out of order.
#[derive(Debug)]
struct CompletionTracker {
    cap: Slot,
    completed: BTreeSet<Slot>,
}

impl CompletionTracker {
    fn new() -> Self {
        Self {
            cap: Slot::FIRST,
            completed: BTreeSet::new(),
        }
    }

    fn cap(&self) -> Slot {
        self.cap
    }

    // Number of slots in `[cap, open_slot_cap)` not yet completed.
    fn open_slot_count(&self, open_slot_cap: Slot) -> Result<u64, ConductorError> {
        open_slot_cap
            .checked_sub(self.cap)
            .and_then(|tracked| tracked.checked_sub(self.completed.len() as u64))
            .ok_or(ConductorError::ArithmeticOverflow)
    }

    // Marks `slot` completed and advances the cap across the contiguous
    // completed prefix.
    fn mark_completed(&mut self, slot: Slot) -> Result<(), ConductorError> {
        // slots up to u64::MAX - 1 may be represented in the cap notation
        assert!(slot.get() <= u64::MAX - 1);
        if slot < self.cap {
            warn!(
                ?slot,
                cap = ?self.cap,
                "rejecting completed slot below completed cap"
            );
            return Err(ConductorError::InvalidSlot(slot));
        }

        if !self.completed.insert(slot) {
            warn!(?slot, "rejecting duplicate slot completion");
            return Err(ConductorError::InvalidSlot(slot));
        }

        while self.completed.remove(&self.cap) {
            self.cap = self
                .cap
                .checked_next()
                .ok_or(ConductorError::ArithmeticOverflow)?;
        }

        Ok(())
    }
}

pub struct MonadConductor<A>
where
    A: Acs<Timestamp>,
{
    config: ConductorConfig,
    deadline_agreement_manager: DeadlineAgreementManager<A>,
    // Exclusive: every window strictly below the cap is open.
    open_window_cap: WindowId,

    completion_tracker: CompletionTracker,

    outputs: VecDeque<ConductorOutput<Self>>,
}

impl<A> fmt::Debug for MonadConductor<A>
where
    A: Acs<Timestamp>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MonadConductor")
            .field("config", &self.config)
            .field("open_window_cap", &self.open_window_cap)
            .field("completion_tracker", &self.completion_tracker)
            .finish_non_exhaustive()
    }
}

impl<A> MonadConductor<A>
where
    A: Acs<Timestamp>,
{
    pub fn genesis(config: ConductorConfig, context: A::Context) -> Result<Self, ConductorError> {
        let genesis_deadline = config.genesis_deadline();
        let mut conductor = Self {
            config,
            deadline_agreement_manager: DeadlineAgreementManager::<A>::new(context),
            open_window_cap: WindowId::FIRST,
            completion_tracker: CompletionTracker::new(),
            outputs: VecDeque::new(),
        };

        conductor.open_window(genesis_deadline)?;

        Ok(conductor)
    }

    pub fn config(&self) -> ConductorConfig {
        self.config.clone()
    }

    pub fn open_window_cap(&self) -> WindowId {
        self.open_window_cap
    }

    pub fn completed_cap(&self) -> Slot {
        self.completion_tracker.cap()
    }

    pub fn open_slot_count(&self) -> Result<u64, ConductorError> {
        self.completion_tracker
            .open_slot_count(self.open_slot_cap()?)
    }

    fn handle_slot_completed(
        &mut self,
        slot: Slot,
        slot_completion: Timestamp,
    ) -> Result<(), ConductorError> {
        let open_slot_cap = self.open_slot_cap()?;
        if slot >= open_slot_cap {
            return Err(ConductorError::SlotNotOpen {
                slot,
                open_slot_cap,
            });
        }
        self.completion_tracker.mark_completed(slot)?;
        self.try_run_deadline_agreement(slot_completion)
    }

    fn try_run_deadline_agreement(
        &mut self,
        slot_completion: Timestamp,
    ) -> Result<(), ConductorError> {
        self.deadline_agreement_manager
            .handle_completion_cap_advance(
                &self.config,
                self.completion_tracker.cap(),
                slot_completion,
            )?;
        self.poll_deadline_agreement()
    }

    fn handle_deadline_agreement_msg(
        &mut self,
        sender: NodeId,
        msg: DeadlineAgreementMessage<A::Message>,
    ) -> Result<(), ConductorError> {
        self.deadline_agreement_manager.handle_message(sender, msg);
        self.poll_deadline_agreement()
    }

    fn poll_deadline_agreement(&mut self) -> Result<(), ConductorError> {
        while let Some(output) = self.deadline_agreement_manager.poll() {
            self.outputs.push_back(output);
        }

        // The decision always targets the window at the open-window cap: the
        // manager's active round is kept in lockstep by `open_window`.
        if let Some(decided_deadline) = self.deadline_agreement_manager.decision() {
            self.open_window(decided_deadline)?;
        }

        Ok(())
    }

    fn compute_open_slots(
        &self,
        window: WindowId,
        first_deadline: Timestamp,
    ) -> Result<BTreeMap<Slot, Timestamp>, ConductorError> {
        let next_window = window
            .checked_next()
            .ok_or(ConductorError::ArithmeticOverflow)?;
        let slot_start = self.config.first_slot(window)?;
        let slot_end = self.config.first_slot(next_window)?;
        let mut slots = BTreeMap::new();
        for slot in slot_start.get()..slot_end.get() {
            let slot = Slot(slot);
            let deadline = self
                .config
                .deadline_for_slot(window, first_deadline, slot)?;
            slots.insert(slot, deadline);
        }
        Ok(slots)
    }

    // Opens every slot of the window at the cap, advances the cap, and
    // rotates the deadline agreement round to target the new cap. Fallible
    // steps run first so a failure leaves the conductor untouched.
    fn open_window(&mut self, first_deadline: Timestamp) -> Result<(), ConductorError> {
        let window = self.open_window_cap;
        let next_window_cap = window
            .checked_next()
            .ok_or(ConductorError::ArithmeticOverflow)?;
        let slots = self.compute_open_slots(window, first_deadline)?;

        self.outputs.push_back(ConductorOutput::OpenSlots(slots));
        self.open_window_cap = next_window_cap;
        self.deadline_agreement_manager.start_round(next_window_cap);
        Ok(())
    }

    // Exclusive: every slot strictly below the cap is open.
    fn open_slot_cap(&self) -> Result<Slot, ConductorError> {
        self.config.first_slot(self.open_window_cap)
    }
}

impl<A> Conductor for MonadConductor<A>
where
    A: Acs<Timestamp>,
{
    type Alarm = std::convert::Infallible;
    type Message = DeadlineAgreementMessage<A::Message>;

    fn handle_message(&mut self, sender: NodeId, message: Self::Message) {
        if let Err(error) = self.handle_deadline_agreement_msg(sender, message) {
            debug!(%error, ?sender, "failed to handle conductor message");
        }
    }

    fn handle_alarm(&mut self, never: Self::Alarm) {
        match never {}
    }

    fn handle_slot_finalization(&mut self, at: Timestamp, slot: Slot) {
        if let Err(error) = self.handle_slot_completed(slot, at) {
            debug!(%error, ?slot, ?at, "failed to handle slot finalization");
        }
    }

    fn poll(&mut self) -> Option<ConductorOutput<Self>> {
        self.outputs.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};

    use super::{
        acs::{AcsOutput, nop::NopAcs},
        *,
    };

    const GENESIS_DEADLINE: Timestamp = Timestamp::from_millis(100);

    fn nz(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn config() -> ConductorConfig {
        ConductorConfig::new(
            nz(10),
            nz(8),
            TimestampDelta::from_nanos(10),
            GENESIS_DEADLINE,
        )
        .unwrap()
    }

    fn genesis_plus(nanos: u64) -> Timestamp {
        GENESIS_DEADLINE
            .checked_add_delta(TimestampDelta::from_nanos(nanos))
            .unwrap()
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct MessageAcs {
        decision: Option<Timestamp>,
    }

    impl Acs<Timestamp> for MessageAcs {
        type Message = Timestamp;
        type Context = ();

        fn new(_ctx: &Self::Context) -> Self {
            Self::default()
        }

        fn propose(&mut self, _deadline: Timestamp) {}

        fn handle_message(&mut self, _sender: NodeId, message: Self::Message) {
            self.decision = Some(message);
        }

        fn decision(&self) -> Option<&Timestamp> {
            self.decision.as_ref()
        }

        fn poll(&mut self) -> Option<AcsOutput<Self::Message>> {
            None
        }
    }

    fn conductor() -> MonadConductor<NopAcs<Timestamp>> {
        let mut conductor = MonadConductor::genesis(config(), ()).unwrap();
        assert!(matches!(
            conductor.poll(),
            Some(ConductorOutput::OpenSlots(_))
        ));
        assert!(conductor.poll().is_none());
        conductor
    }

    fn drain_outputs<A: Acs<Timestamp>>(
        conductor: &mut MonadConductor<A>,
    ) -> Vec<ConductorOutput<MonadConductor<A>>> {
        std::iter::from_fn(|| conductor.poll()).collect()
    }

    fn complete_slot<A: Acs<Timestamp>>(
        conductor: &mut MonadConductor<A>,
        slot: u64,
        time: u64,
    ) -> Result<Vec<ConductorOutput<MonadConductor<A>>>, ConductorError> {
        conductor.handle_slot_completed(Slot(slot), Timestamp::from_nanos(u128::from(time)))?;
        Ok(drain_outputs(conductor))
    }

    fn extract_open_slots<A: Acs<Timestamp>>(
        outputs: &[ConductorOutput<MonadConductor<A>>],
    ) -> BTreeMap<Slot, Timestamp> {
        let batches = outputs
            .iter()
            .filter_map(|output| match output {
                ConductorOutput::OpenSlots(slots) => Some(slots),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1, "expected exactly one open-slots output");
        batches[0].clone()
    }

    fn extract_runtime_open_slots<A: Acs<Timestamp>>(
        output: ConductorOutput<MonadConductor<A>>,
    ) -> BTreeMap<Slot, Timestamp> {
        match output {
            ConductorOutput::OpenSlots(slots) => slots,
            _ => panic!("expected runtime open-slots output"),
        }
    }

    #[test]
    fn config_validation_rejects_invalid_values() {
        assert!(
            ConductorConfig::new(
                nz(4),
                nz(5),
                TimestampDelta::from_nanos(10),
                GENESIS_DEADLINE
            )
            .is_err()
        );
    }

    #[test]
    fn window_arithmetic() {
        let config = config();

        assert_eq!(config.first_slot(WindowId(0)).unwrap(), Slot(0));
        assert_eq!(config.first_slot(WindowId(1)).unwrap(), Slot(10));
        assert_eq!(config.first_slot(WindowId(2)).unwrap(), Slot(20));
        assert_eq!(config.window_of(Slot(0)).unwrap(), WindowId(0));
        assert_eq!(config.window_of(Slot(9)).unwrap(), WindowId(0));
        assert_eq!(config.window_of(Slot(10)).unwrap(), WindowId(1));
        assert_eq!(config.sync_boundary_slot(WindowId(0)).unwrap(), Slot(7));
        assert_eq!(config.sync_boundary_slot(WindowId(1)).unwrap(), Slot(17));
        assert_eq!(
            config
                .deadline_for_slot(WindowId(0), Timestamp::from_nanos(100), Slot(9))
                .unwrap(),
            Timestamp::from_nanos(190)
        );
    }

    #[test]
    fn completion_tracker_advances_cap_through_contiguous_prefix() {
        let mut tracker = CompletionTracker::new();

        tracker.mark_completed(Slot(1)).unwrap();
        assert_eq!(tracker.cap(), Slot(0));

        tracker.mark_completed(Slot(0)).unwrap();
        assert_eq!(tracker.cap(), Slot(2));

        tracker.mark_completed(Slot(3)).unwrap();
        assert_eq!(tracker.cap(), Slot(2));

        tracker.mark_completed(Slot(2)).unwrap();
        assert_eq!(tracker.cap(), Slot(4));
    }

    #[test]
    fn completion_tracker_rejects_invalid_completions() {
        let mut tracker = CompletionTracker::new();

        tracker.mark_completed(Slot(0)).unwrap();
        // Below the cap.
        assert!(matches!(
            tracker.mark_completed(Slot(0)),
            Err(ConductorError::InvalidSlot(Slot(0)))
        ));
        // Duplicate above the cap.
        tracker.mark_completed(Slot(5)).unwrap();
        assert!(matches!(
            tracker.mark_completed(Slot(5)),
            Err(ConductorError::InvalidSlot(Slot(5)))
        ));
        assert_eq!(tracker.cap(), Slot(1));
    }

    #[test]
    fn completion_tracker_open_slot_count_tracks_completions() {
        let mut tracker = CompletionTracker::new();
        assert_eq!(tracker.open_slot_count(Slot(10)).unwrap(), 10);

        tracker.mark_completed(Slot(4)).unwrap();
        assert_eq!(tracker.open_slot_count(Slot(10)).unwrap(), 9);

        tracker.mark_completed(Slot(0)).unwrap();
        assert_eq!(tracker.open_slot_count(Slot(10)).unwrap(), 8);

        // Opening another window grows the count by its slots.
        assert_eq!(tracker.open_slot_count(Slot(20)).unwrap(), 18);
    }

    #[test]
    fn completion_beyond_open_slot_cap_is_rejected() {
        let mut conductor = conductor();

        assert!(matches!(
            complete_slot(&mut conductor, 10, 100),
            Err(ConductorError::SlotNotOpen {
                slot: Slot(10),
                open_slot_cap: Slot(10),
            })
        ));
    }

    #[test]
    fn genesis_opens_exactly_one_window() {
        let mut conductor = MonadConductor::<NopAcs<Timestamp>>::genesis(config(), ()).unwrap();
        let schedules = extract_runtime_open_slots(conductor.poll().unwrap());
        let slot_interval = config().slot_interval();
        let expected_schedules = (0..10)
            .map(|slot| {
                (
                    Slot(slot),
                    GENESIS_DEADLINE
                        .checked_add_delta(slot_interval.checked_mul(slot).unwrap())
                        .unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(schedules, expected_schedules);
        assert!(conductor.poll().is_none());
        assert_eq!(conductor.open_window_cap(), WindowId(1));
        assert_eq!(conductor.completed_cap(), Slot(0));
        assert_eq!(conductor.open_slot_count().unwrap(), 10);
    }

    #[test]
    fn genesis_effect_is_available_through_runtime_poll() {
        let mut conductor = MonadConductor::<NopAcs<Timestamp>>::genesis(config(), ()).unwrap();
        let slots = extract_runtime_open_slots(conductor.poll().unwrap());

        assert_eq!(
            slots,
            (0..10)
                .map(|slot| (Slot(slot), genesis_plus(slot * 10)))
                .collect()
        );
        assert!(conductor.poll().is_none());
    }

    #[test]
    fn runtime_finalization_opens_next_window_without_closing_slots() {
        let runtime_config = ConductorConfig::new(
            nz(4),
            nz(2),
            TimestampDelta::from_nanos(10),
            GENESIS_DEADLINE,
        )
        .unwrap();
        let mut conductor =
            MonadConductor::<NopAcs<Timestamp>>::genesis(runtime_config, ()).unwrap();
        let _ = conductor.poll().unwrap();

        conductor.handle_slot_finalization(Timestamp::from_nanos(11), Slot(0));
        assert!(conductor.poll().is_none());

        conductor.handle_slot_finalization(Timestamp::from_nanos(12), Slot(1));
        let slots = extract_runtime_open_slots(conductor.poll().unwrap());
        assert_eq!(
            slots.keys().copied().collect::<Vec<_>>(),
            (4..8).map(Slot).collect::<Vec<_>>()
        );
        assert_eq!(slots[&Slot(4)], genesis_plus(40));
        assert_eq!(slots[&Slot(7)], genesis_plus(70));
        assert!(conductor.poll().is_none());
    }

    #[test]
    fn duplicate_completion_is_rejected() {
        let mut conductor = conductor();

        assert!(complete_slot(&mut conductor, 0, 100).unwrap().is_empty());

        assert!(matches!(
            complete_slot(&mut conductor, 0, 100),
            Err(ConductorError::InvalidSlot(slot)) if slot == Slot(0)
        ));
        assert_eq!(conductor.completed_cap(), Slot(1));
        assert_eq!(conductor.open_slot_count().unwrap(), 9);
    }

    #[test]
    fn out_of_order_completions_only_advance_cap_through_contiguous_slots() {
        let mut conductor = conductor();

        assert!(complete_slot(&mut conductor, 1, 100).unwrap().is_empty());
        assert_eq!(conductor.completed_cap(), Slot(0));

        let _ = complete_slot(&mut conductor, 0, 100).unwrap();
        assert_eq!(conductor.completed_cap(), Slot(2));

        assert!(complete_slot(&mut conductor, 3, 100).unwrap().is_empty());
        assert_eq!(conductor.completed_cap(), Slot(2));

        let _ = complete_slot(&mut conductor, 2, 100).unwrap();
        assert_eq!(conductor.completed_cap(), Slot(4));
    }

    #[test]
    fn no_proposal_before_sync_boundary() {
        let mut conductor = conductor();

        for slot in 0..7 {
            assert!(complete_slot(&mut conductor, slot, 100).unwrap().is_empty());
        }
    }

    #[test]
    fn new_slot_only_opened_once() {
        let mut conductor = conductor();

        for slot in 0..7 {
            let _ = complete_slot(&mut conductor, slot, 100).unwrap();
        }
        assert!(complete_slot(&mut conductor, 8, 100).unwrap().is_empty());

        let output = complete_slot(&mut conductor, 7, 100).unwrap();
        let schedules = extract_open_slots(&output);
        assert_eq!(schedules.keys().next(), Some(&Slot(10)));

        assert!(matches!(
            complete_slot(&mut conductor, 7, 100),
            Err(ConductorError::InvalidSlot(slot)) if slot == Slot(7)
        ));
    }

    #[test]
    fn deadline_selection_ignores_event_time_and_uses_natural_deadline() {
        let mut conductor = conductor();

        for slot in 0..7 {
            let _ = complete_slot(&mut conductor, slot, 200).unwrap();
        }
        let output = complete_slot(&mut conductor, 7, 200).unwrap();
        let schedules = extract_open_slots(&output);

        // Window 1's first slot opens at its natural deadline: GENESIS_DEADLINE
        // + slot_interval * slots_per_window = GENESIS_DEADLINE + 10 * 10,
        // despite that window 0 slots finish much later.
        assert_eq!(schedules[&Slot(10)], genesis_plus(100));
    }

    #[test]
    fn deadline_decision_opens_next_window_and_derives_first_slot() {
        let mut conductor = MonadConductor::<MessageAcs>::genesis(config(), ()).unwrap();
        let _ = conductor.poll().unwrap();
        for slot in 0..=7 {
            assert!(complete_slot(&mut conductor, slot, 100).unwrap().is_empty());
        }
        conductor.handle_message(
            NodeId::dummy(1),
            DeadlineAgreementMessage {
                window: WindowId(1),
                acs_message: Timestamp::from_nanos(110),
            },
        );
        assert_eq!(conductor.open_window_cap(), WindowId(2));
        let schedules = extract_open_slots(&drain_outputs(&mut conductor));

        assert_eq!(schedules.len(), 10);
        assert_eq!(schedules[&Slot(10)], Timestamp::from_nanos(110));
        assert_eq!(schedules[&Slot(19)], Timestamp::from_nanos(200));
        assert_eq!(conductor.open_window_cap(), WindowId(2));
    }

    #[test]
    fn randomized_completion_orders_preserve_cap_invariant() {
        let mut rng = StdRng::seed_from_u64(0x5eed);
        for _case in 0..25 {
            let mut order: Vec<u64> = (0..10).collect();
            order.shuffle(&mut rng);

            let mut conductor = conductor();
            let mut completed = BTreeSet::new();

            for slot in order {
                let _ = complete_slot(&mut conductor, slot, 110).unwrap();
                completed.insert(slot);

                let expected_cap =
                    (0..10).take_while(|slot| completed.contains(slot)).count() as u64;
                assert_eq!(conductor.completed_cap().get(), expected_cap);
            }
        }
    }

    #[test]
    fn multi_window_run_never_opens_a_slot_twice() {
        let mut conductor = conductor();
        let mut opened: BTreeSet<Slot> = (0..10).map(Slot).collect();

        for window_id in 0..5 {
            let sync_boundary = config()
                .sync_boundary_slot(WindowId(window_id))
                .unwrap()
                .get();
            let mut output = Vec::new();
            for slot in conductor.completed_cap().get()..=sync_boundary {
                output = complete_slot(&mut conductor, slot, 1_000 + slot).unwrap();
            }

            for slot in extract_open_slots(&output).into_keys() {
                assert!(opened.insert(slot), "opened {:?} twice", slot);
            }
        }
    }

    #[test]
    fn bounded_open_invariant_holds_in_stalled_simulation() {
        let config = ConductorConfig::new(
            nz(10),
            nz(8),
            TimestampDelta::from_nanos(10),
            GENESIS_DEADLINE,
        )
        .unwrap();
        let bound = 2 * config.slots_per_window().get() - config.sync_boundary_slots().get();
        let mut conductor =
            MonadConductor::<NopAcs<Timestamp>>::genesis(config.clone(), ()).unwrap();
        let _ = conductor.poll().unwrap();

        for window_id in 0..4 {
            let sync_boundary = config
                .sync_boundary_slot(WindowId(window_id))
                .unwrap()
                .get();
            let mut output = Vec::new();
            for slot in conductor.completed_cap().get()..=sync_boundary {
                output = complete_slot(&mut conductor, slot, 10_000 + slot).unwrap();
                assert!(conductor.open_slot_count().unwrap() <= bound);
            }

            assert_eq!(
                extract_open_slots(&output).len(),
                config.slots_per_window().get() as usize
            );
            assert!(conductor.open_slot_count().unwrap() <= bound);
        }
    }
}
