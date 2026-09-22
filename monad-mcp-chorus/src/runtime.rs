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

use std::collections::{BTreeMap, VecDeque};

use super::{
    conductor::{Conductor, ConductorOutput},
    message::{CadenceMessage, CadenceWireMsg, Outbound},
    slot::{SlotConsensus, SlotOutput},
    slot_manager::SlotManager,
    timers::Timers,
    types::{Slot, Timestamp, Validated},
};

// A Runtime describes the wiring logic of the consensus stack: slot
// manager, conductor and timers.
pub trait Runtime<M> {
    // The local data-availability events this runtime accepts. Runtimes
    // driving a stack without a DA layer set this to ().
    type DAEvent;

    fn init(&mut self);
    fn next_due(&self) -> Option<Timestamp>;
    fn handle_due(&mut self, now: Timestamp);
    fn receive(&mut self, now: Timestamp, message: Validated<M>);
    fn poll(&mut self) -> Option<Outbound<M>>;

    // Inject a data-availability event for `slot`. DA events are local
    // and trusted by construction, so they do not travel as messages.
    // The default drops them, for stacks without a DA layer.
    fn handle_da_event(&mut self, _now: Timestamp, _slot: Slot, _event: Self::DAEvent) {}
}

// A canonical runtime implementation for cadence.
pub struct CadenceRuntime<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    clock: Timestamp,
    slot_manager: SlotManager<S>,
    conductor: C,
    timers: Timers<PendingWake<S::Timer, C::Alarm>>,
    outbox: VecDeque<Outbound<CadenceWireMsg<S, C>>>,
    observer: Option<Box<ObserverOf<S>>>,
    da_sink: Option<Box<DASinkOf<S>>>,
}

enum PendingWake<Timer, Alarm> {
    Deadline(Slot),
    SlotTimer(Slot, Timer),
    Alarm(Alarm),
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
            timers: Timers::default(),
            outbox: VecDeque::new(),
            observer: None,
            da_sink: None,
        }
    }

    pub fn on_finalization(
        &mut self,
        observer: impl FinalizationObserver<S::OptimisticCommitData, S::FinalizationData>
        + Send
        + 'static,
    ) {
        self.observer = Some(Box::new(observer));
    }

    pub fn on_da(&mut self, sink: impl DASink<S::DACommand> + Send + 'static) {
        self.da_sink = Some(Box::new(sink));
    }

    // in actual runtime this can be controlled by the system clock.
    pub fn advance_clock(&mut self, now: Timestamp) {
        assert!(now >= self.clock);
        self.clock = now;
    }

    fn handle_wake(&mut self, wake: PendingWake<S::Timer, C::Alarm>) {
        match wake {
            PendingWake::Alarm(alarm) => {
                self.conductor.handle_alarm(alarm);
            }
            PendingWake::Deadline(slot)
                if let Some(instance) = self.slot_manager.slot_instance(slot) =>
            {
                instance.handle_deadline();
            }
            PendingWake::SlotTimer(slot, timer)
                if let Some(instance) = self.slot_manager.slot_instance(slot) =>
            {
                instance.handle_timer(timer);
            }
            PendingWake::Deadline(slot) | PendingWake::SlotTimer(slot, _) => {
                tracing::debug!(?slot, "wake for a slot without instance");
            }
        }
    }

    fn handle_message(&mut self, message: Validated<CadenceWireMsg<S, C>>) {
        let now = self.clock;
        let (message, author) = message.destructure();
        match message {
            CadenceMessage::Conductor(message) => {
                self.conductor.handle_message(now, author, message);
            }
            CadenceMessage::Slot(slot, message)
                if let Some(instance) = self.slot_manager.slot_instance(slot) =>
            {
                instance.handle_message(author, message);
            }
            CadenceMessage::Slot(slot, _) => {
                tracing::debug!(?slot, ?author, "message for a slot without instance");
            }
        }
    }

    fn step(&mut self) {
        while self.step_once() {}
    }

    // returns true if any progress was made
    fn step_once(&mut self) -> bool {
        // todo: use a fair poll order to avoid starvation
        if let Some((slot, out)) = self.slot_manager.poll_any() {
            self.handle_slot_output(slot, out);
            return true;
        }

        if let Some(out) = self.conductor.poll() {
            self.handle_conductor_output(out);
            return true;
        }

        false
    }

    fn handle_slot_output(&mut self, slot: Slot, out: SlotOutput<S>) {
        let now = self.clock;
        match out {
            SlotOutput::ScheduleTimer(delta, timer) => {
                self.timers
                    .schedule(now + delta, PendingWake::SlotTimer(slot, timer));
            }
            SlotOutput::Broadcast(message) => {
                let message = CadenceMessage::Slot(slot, message);
                self.outbox.push_back(Outbound::Broadcast(message));
            }
            SlotOutput::Unicast { to, message } => {
                let message = CadenceMessage::Slot(slot, message);
                self.outbox.push_back(Outbound::Unicast(to, message));
            }
            SlotOutput::DA(action) if let Some(sink) = &mut self.da_sink => {
                sink.handle_command(slot, action);
            }
            SlotOutput::CommitOptimistic(data) if let Some(observer) = &mut self.observer => {
                observer.handle_optimistic_commit(now, slot, &data);
            }
            // no sink or observer to tell
            SlotOutput::DA(_) | SlotOutput::CommitOptimistic(_) => {}
            SlotOutput::Finalize(data) => {
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
                if let Some(sink) = &mut self.da_sink {
                    sink.handle_lifecycle(slot, SlotLifecycle::Completed);
                }
                self.slot_manager.close(slot);
            }
        }
    }

    fn handle_conductor_output(&mut self, out: ConductorOutput<C>) {
        let now = self.clock;
        match out {
            ConductorOutput::Broadcast(message) => {
                let message = CadenceMessage::Conductor(message);
                self.outbox.push_back(Outbound::Broadcast(message));
            }
            ConductorOutput::ScheduleAlarm(at, alarm) => {
                self.timers.schedule(at, PendingWake::Alarm(alarm));
            }
            ConductorOutput::CloseSlots { cap } => {
                self.slot_manager.advance_cap(cap);
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
                    self.timers.schedule(deadline, PendingWake::Deadline(slot));
                    if let Some(sink) = &mut self.da_sink {
                        sink.handle_lifecycle(slot, SlotLifecycle::Opened { deadline });
                    }
                }
            }
        }
    }
}

impl<S, C> Runtime<CadenceWireMsg<S, C>> for CadenceRuntime<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    type DAEvent = S::DAEvent;

    fn poll(&mut self) -> Option<Outbound<CadenceWireMsg<S, C>>> {
        self.outbox.pop_front()
    }

    fn init(&mut self) {
        self.step();
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.timers.next_due()
    }

    // wakes are handled one at a time, each to quiescence
    fn handle_due(&mut self, now: Timestamp) {
        self.advance_clock(now);
        while let Some(wake) = self.timers.pop_due(now) {
            self.handle_wake(wake);
            self.step();
        }
    }

    fn receive(&mut self, now: Timestamp, message: Validated<CadenceWireMsg<S, C>>) {
        self.advance_clock(now);
        self.handle_message(message);
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
