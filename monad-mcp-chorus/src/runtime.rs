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

use std::collections::BTreeMap;

use super::{
    conductor::{Conductor, ConductorOutput},
    driver::{CadenceDriver, CadenceEvent, Driver, NodeEvent, WakeId},
    repeater::{Recipients, Repeater, RepeaterConfig},
    slot::{SlotConsensus, SlotOutput},
    slot_manager::SlotManager,
    types::{Slot, Timestamp, Validated},
};

// A Runtime describes the wiring logic of the three components of the
// consensus stack: slot manager, conductor, and driver.
pub trait Runtime<M> {
    // The local data-availability events this runtime accepts. Runtimes
    // driving a stack without a DA layer set this to ().
    type DAEvent;

    fn init(&mut self);
    fn wake(&mut self, now: Timestamp, wake: WakeId);
    fn receive(&mut self, now: Timestamp, message: Validated<M>);
    fn poll(&mut self) -> Option<NodeEvent<M>>;

    // Inject a data-availability event for `slot`. DA events are local
    // and trusted by construction, so they do not travel as messages.
    // The default drops them, for stacks without a DA layer.
    fn handle_da_event(&mut self, _now: Timestamp, _slot: Slot, _event: Self::DAEvent) {}
}

// A canonical runtime implementation for cadence.
pub struct CadenceRuntime<S, C, D = CadenceDriver<S, C>>
where
    S: SlotConsensus,
    C: Conductor,
    D: Driver<S, C>,
{
    clock: Timestamp,
    slot_manager: SlotManager<S>,
    conductor: C,
    driver: D,
    observer: Option<Box<ObserverOf<S>>>,
    da_sink: Option<Box<DASinkOf<S>>>,
    repeater: Option<Repeater<S>>,
}

type ObserverOf<S> = dyn FinalizationObserver<
        <S as SlotConsensus>::OptimisticCommitData,
        <S as SlotConsensus>::FinalizationData,
    > + Send;

type DASinkOf<S> = dyn DASink<<S as SlotConsensus>::DACommand> + Send;

impl<S, C> CadenceRuntime<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    pub fn new(slot_manager: SlotManager<S>, conductor: C) -> Self {
        Self {
            clock: Timestamp::GENESIS,
            slot_manager,
            conductor,
            driver: CadenceDriver::default(),
            observer: None,
            da_sink: None,
            repeater: None,
        }
    }

    pub fn with_driver<D>(self, driver: D) -> CadenceRuntime<S, C, D>
    where
        D: Driver<S, C>,
    {
        CadenceRuntime {
            clock: self.clock,
            slot_manager: self.slot_manager,
            conductor: self.conductor,
            observer: self.observer,
            da_sink: self.da_sink,
            repeater: self.repeater,
            driver,
        }
    }
}

impl<S, C, D> CadenceRuntime<S, C, D>
where
    S: SlotConsensus,
    C: Conductor,
    D: Driver<S, C>,
{
    pub fn on_finalization(
        &mut self,
        observer: impl FinalizationObserver<S::OptimisticCommitData, S::FinalizationData>
        + Send
        + 'static,
    ) {
        self.observer = Some(Box::new(observer));
    }

    pub fn with_repeater(mut self, config: RepeaterConfig) -> Self {
        self.repeater = Some(Repeater::new(config));
        self
    }

    pub fn on_da(&mut self, sink: impl DASink<S::DACommand> + Send + 'static) {
        self.da_sink = Some(Box::new(sink));
    }

    // in actual runtime this can be controlled by the system clock.
    pub fn advance_clock(&mut self, now: Timestamp) {
        assert!(now >= self.clock);
        self.clock = now;
    }

    fn step(&mut self) {
        while self.step_once() {}
    }

    // returns true if any progress was made
    fn step_once(&mut self) -> bool {
        // todo: use a fair poll order to avoid starvation
        if let Some((slot, out)) = self.slot_manager.poll_any() {
            let now = self.clock;

            match out {
                SlotOutput::ScheduleTimer(delta, timer) => {
                    self.driver.schedule_slot_timer(delta, slot, timer);
                }
                SlotOutput::Broadcast(message) => {
                    if let Some(repeater) = &mut self.repeater {
                        repeater.record(now, slot, Recipients::Everyone, &message);
                    }
                    self.driver.broadcast_slot(slot, message);
                }
                SlotOutput::Unicast { to, message } => {
                    if let Some(repeater) = &mut self.repeater {
                        repeater.record(now, slot, Recipients::Node(to), &message);
                    }
                    self.driver.unicast_slot(slot, to, message);
                }
                SlotOutput::DA(action) => {
                    if let Some(sink) = &mut self.da_sink {
                        sink.handle_command(slot, action);
                    }
                }
                SlotOutput::CommitOptimistic(data) => {
                    if let Some(observer) = &mut self.observer {
                        observer.handle_optimistic_commit(now, slot, &data);
                    }
                }
                SlotOutput::Finalize(data) => {
                    if let Some(repeater) = &mut self.repeater {
                        repeater.handle_finalization(slot, &data);
                    }
                    if let Some(observer) = &mut self.observer {
                        observer.handle_finalization(now, slot, &data);
                    }
                    if let Some(sink) = &mut self.da_sink {
                        sink.handle_lifecycle(slot, SlotLifecycle::Completed);
                    }
                    self.conductor.handle_slot_finalization(now, slot);
                    self.slot_manager.close(slot);
                }
                SlotOutput::Fault { reason } => {
                    tracing::warn!(?slot, reason = %reason, "slot faulted");
                    if let Some(repeater) = &mut self.repeater {
                        repeater.handle_completed(slot);
                    }
                    if let Some(sink) = &mut self.da_sink {
                        sink.handle_lifecycle(slot, SlotLifecycle::Completed);
                    }
                    self.slot_manager.close(slot);
                }
            }
            return true;
        }

        if let Some(out) = self.conductor.poll() {
            let now = self.clock;

            match out {
                ConductorOutput::Broadcast(msg) => {
                    self.driver.broadcast_conductor(msg);
                }
                ConductorOutput::ScheduleAlarm(at, timer) => {
                    self.driver.schedule_alarm(at, timer);
                }
                ConductorOutput::CloseSlots { cap } => {
                    self.slot_manager.advance_cap(cap);
                    if let Some(repeater) = &mut self.repeater {
                        repeater.handle_cap_advance(cap);
                    }
                    if let Some(observer) = &mut self.observer {
                        observer.handle_chain_advance(now, cap);
                    }
                }
                ConductorOutput::OpenSlots(slots) => {
                    if let Some(observer) = &mut self.observer {
                        observer.handle_slots_opened(now, &slots);
                    }
                    for (slot, deadline) in slots {
                        self.slot_manager.open(slot);
                        self.driver.schedule_slot_deadline(slot, deadline);
                        if let Some(sink) = &mut self.da_sink {
                            sink.handle_lifecycle(slot, SlotLifecycle::Opened { deadline });
                        }
                    }
                }
            }
            return true;
        }

        if let Some(event) = self.driver.poll_cadence_event() {
            let now = self.clock;
            match event {
                CadenceEvent::Alarm(alarm) => {
                    self.conductor.handle_alarm(alarm);
                }
                CadenceEvent::ConductorMessage(message) => {
                    let (message, author) = message.destructure();
                    self.conductor.handle_message(now, author, message);
                }
                CadenceEvent::SlotDeadline(slot) => {
                    if let Some(instance) = self.slot_manager.slot_instance(slot) {
                        instance.handle_deadline();
                    } else {
                        tracing::debug!(?slot, "deadline for a slot without instance");
                    }
                }
                CadenceEvent::SlotTimer(slot, timer) => {
                    if let Some(instance) = self.slot_manager.slot_instance(slot) {
                        instance.handle_timer(timer);
                    } else {
                        tracing::debug!(?slot, "timer for a slot without instance");
                    }
                }
                CadenceEvent::RepeaterTick => {
                    let repeats = self
                        .repeater
                        .as_mut()
                        .map(|repeater| (repeater.interval(), repeater.due(now)));
                    if let Some((interval, due)) = repeats {
                        for (to, slot, message) in due {
                            match to {
                                Recipients::Everyone => self.driver.broadcast_slot(slot, message),
                                Recipients::Node(to) => self.driver.unicast_slot(slot, to, message),
                            }
                        }
                        self.driver.schedule_repeater(interval);
                    }
                }
                CadenceEvent::SlotMessage(message) => {
                    let ((slot, message), author) = message.destructure();
                    if let Some(instance) = self.slot_manager.slot_instance(slot) {
                        instance.handle_message(author, message);
                    } else {
                        tracing::debug!(?slot, ?author, "message for a slot without instance");
                    }
                }
            }
            return true;
        }

        false
    }
}

impl<S, C, D> Runtime<D::WireMsg> for CadenceRuntime<S, C, D>
where
    S: SlotConsensus,
    C: Conductor,
    D: Driver<S, C>,
{
    type DAEvent = S::DAEvent;

    fn poll(&mut self) -> Option<NodeEvent<D::WireMsg>> {
        self.driver.poll_node_event()
    }

    fn init(&mut self) {
        if let Some(repeater) = &self.repeater {
            let interval = repeater.interval();
            self.driver.schedule_repeater(interval);
        }
        self.step();
    }

    fn wake(&mut self, now: Timestamp, wake: WakeId) {
        self.advance_clock(now);
        self.driver.handle_wake(wake);
        self.step();
    }

    fn receive(&mut self, now: Timestamp, message: Validated<D::WireMsg>) {
        self.advance_clock(now);
        self.driver.handle_message(message);
        self.step();
    }

    fn handle_da_event(&mut self, now: Timestamp, slot: Slot, event: S::DAEvent) {
        self.advance_clock(now);

        if let Some(instance) = self.slot_manager.slot_instance(slot) {
            // q: do we need to buffer the da events? da begins to
            // accept chunk ingestion at the same time as slot
            // consensus, which normally is already conservative.
            instance.handle_da_event(event);
        }
        self.step();
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SlotLifecycle {
    Opened { deadline: Timestamp },
    Completed,
}

pub trait DASink<A> {
    fn handle_lifecycle(&mut self, slot: Slot, event: SlotLifecycle);
    fn handle_command(&mut self, slot: Slot, action: A);
}

/// The runtime's outward event surface: consensus facts for components
/// outside the consensus core (ledger sequencing, the proposal planner,
/// test logs). Observers must not influence consensus — every method is a
/// notification, not a decision point.
pub trait FinalizationObserver<OD, FD> {
    fn handle_optimistic_commit(&mut self, _now: Timestamp, _slot: Slot, _data: &OD) {}
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, data: &FD);

    /// The conductor opened `slots`, each with its deadline.
    fn handle_slots_opened(&mut self, _now: Timestamp, _slots: &BTreeMap<Slot, Timestamp>) {}

    /// The contiguous finalized prefix advanced: every slot strictly below
    /// `cap` is finalized (consensus chaining).
    fn handle_chain_advance(&mut self, _now: Timestamp, _cap: Slot) {}
}

impl<OD, FD, F> FinalizationObserver<OD, FD> for F
where
    F: FnMut(Timestamp, Slot, &FD),
{
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, data: &FD) {
        self(now, slot, data)
    }
}

impl<OD, FD> FinalizationObserver<OD, FD> for Vec<(Timestamp, Slot)> {
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, _data: &FD) {
        self.push((now, slot));
    }
}

/// Observers compose as pairs (and, by nesting, as arbitrary trees).
impl<OD, FD, A, B> FinalizationObserver<OD, FD> for (A, B)
where
    A: FinalizationObserver<OD, FD>,
    B: FinalizationObserver<OD, FD>,
{
    fn handle_optimistic_commit(&mut self, now: Timestamp, slot: Slot, data: &OD) {
        self.0.handle_optimistic_commit(now, slot, data);
        self.1.handle_optimistic_commit(now, slot, data);
    }

    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, data: &FD) {
        self.0.handle_finalization(now, slot, data);
        self.1.handle_finalization(now, slot, data);
    }

    fn handle_slots_opened(&mut self, now: Timestamp, slots: &BTreeMap<Slot, Timestamp>) {
        self.0.handle_slots_opened(now, slots);
        self.1.handle_slots_opened(now, slots);
    }

    fn handle_chain_advance(&mut self, now: Timestamp, cap: Slot) {
        self.0.handle_chain_advance(now, cap);
        self.1.handle_chain_advance(now, cap);
    }
}

#[cfg(test)]
mod repeater_tests {
    use std::sync::Arc;

    use super::{
        super::{
            conductor::dummy::DummyConductor,
            repeater::RepeaterConfig,
            slot::dummy::{DummySlotConsensus, DummySlotConsensusConfig},
            types::{NodeId, TimestampDelta},
        },
        *,
    };

    const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
    const DEADLINE_OFFSET: TimestampDelta = TimestampDelta::from_millis(30);
    // no multiple of it lands on a slot deadline (30 + 100k) or on the
    // window alarm (500), so a tick never shares a wake time with them
    const INTERVAL: TimestampDelta = TimestampDelta::from_millis(220);

    type TestRuntime = CadenceRuntime<DummySlotConsensus, DummyConductor>;

    fn runtime() -> TestRuntime {
        let key = Arc::new(NodeId::dummy(0).keypair());
        // a quorum no vote pool ever reaches: the slot stays undecided
        let slot_manager = SlotManager::new(DummySlotConsensusConfig { quorum: 4 }, key);
        let conductor = DummyConductor::new(SLOT_INTERVAL, 5).set_deadline_offset(DEADLINE_OFFSET);
        CadenceRuntime::new(slot_manager, conductor)
    }

    fn repeater_config() -> RepeaterConfig {
        RepeaterConfig {
            interval: INTERVAL,
            certificate_retention: 2,
        }
    }

    // The node side of the runtime: pending wakes at their absolute due
    // time, and the broadcasts seen since the last check.
    struct Node {
        now: Timestamp,
        wakes: Vec<(Timestamp, WakeId)>,
        broadcasts: usize,
    }

    impl Node {
        fn start(runtime: &mut TestRuntime) -> Self {
            let mut node = Self {
                now: Timestamp::GENESIS,
                wakes: Vec::new(),
                broadcasts: 0,
            };
            runtime.init();
            node.drain(runtime);
            node
        }

        fn drain(&mut self, runtime: &mut TestRuntime) {
            while let Some(event) = runtime.poll() {
                match event {
                    NodeEvent::Wake(at, id) => self.wakes.push((at, id)),
                    NodeEvent::WakeAfter(delta, id) => self.wakes.push((self.now + delta, id)),
                    NodeEvent::Broadcast(_) => self.broadcasts += 1,
                    NodeEvent::Unicast { .. } => panic!("the dummy consensus does not unicast"),
                }
            }
        }

        // fire the single wake due at `at`
        fn fire(&mut self, runtime: &mut TestRuntime, at: Timestamp) {
            let due: Vec<_> = self
                .wakes
                .iter()
                .enumerate()
                .filter(|(_, (when, _))| *when == at)
                .map(|(index, _)| index)
                .collect();
            let [index] = due[..] else {
                panic!("expected exactly one wake at {at:?}, found {}", due.len());
            };
            let (_, id) = self.wakes.remove(index);
            self.now = at;
            runtime.wake(at, id);
            self.drain(runtime);
        }

        fn take_broadcasts(&mut self) -> usize {
            std::mem::replace(&mut self.broadcasts, 0)
        }
    }

    #[test]
    fn a_repeater_tick_re_sends_an_undecided_slots_broadcast() {
        let mut runtime = runtime().with_repeater(repeater_config());
        let mut node = Node::start(&mut runtime);

        // the conductor's genesis alarm opens the first window
        node.fire(&mut runtime, Timestamp::GENESIS);
        assert_eq!(node.take_broadcasts(), 0);

        // slot 0's deadline: the dummy consensus broadcasts its vote
        let vote_at = Timestamp::GENESIS + DEADLINE_OFFSET;
        node.fire(&mut runtime, vote_at);
        assert_eq!(node.take_broadcasts(), 1);

        // the first tick comes 190ms after the vote: too young to repeat
        node.fire(&mut runtime, Timestamp::GENESIS + INTERVAL);
        assert_eq!(node.take_broadcasts(), 0);

        // the second tick comes 410ms after it
        node.fire(&mut runtime, Timestamp::GENESIS + INTERVAL + INTERVAL);
        assert_eq!(node.take_broadcasts(), 1);
    }

    #[test]
    fn without_a_repeater_no_tick_is_ever_armed() {
        let mut runtime = runtime();
        let mut node = Node::start(&mut runtime);

        // only the conductor's genesis alarm
        assert_eq!(node.wakes, vec![(Timestamp::GENESIS, WakeId::FIRST)]);

        node.fire(&mut runtime, Timestamp::GENESIS);
        let vote_at = Timestamp::GENESIS + DEADLINE_OFFSET;
        node.fire(&mut runtime, vote_at);
        assert_eq!(node.take_broadcasts(), 1);

        // no wake is left that a tick could hide behind: deadlines and the
        // next window alarm only
        let deadlines = (1..5).map(|i| vote_at + SLOT_INTERVAL.checked_mul(i).unwrap());
        let alarm = Timestamp::GENESIS + SLOT_INTERVAL.checked_mul(5).unwrap();
        let expected: Vec<Timestamp> = deadlines.chain([alarm]).collect();
        let mut pending: Vec<Timestamp> = node.wakes.iter().map(|(at, _)| *at).collect();
        pending.sort();
        assert_eq!(pending, expected);
    }
}
