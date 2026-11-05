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

use std::collections::VecDeque;

use tracing::{debug, trace};

use super::{
    conductor::{Conductor, ConductorOutput},
    message::{CadenceMessage, CadenceWireMsg, Outbound},
    slot::{SlotConsensus, SlotOutput},
    slot_manager::SlotManager,
    timers::Timers,
    types::{NodeId, Slot, SlotScoped, Timestamp},
};

// A Runtime is a process that exchanges messages with peers, takes local
// inputs and produces local outputs, and owns its timers.
pub trait Runtime {
    type Message;
    type LocalIn;
    type LocalOut;

    fn receive(&mut self, now: Timestamp, sender: NodeId, message: Self::Message);
    fn poll_msg(&mut self) -> Option<Outbound<Self::Message>>;

    fn handle_input(&mut self, now: Timestamp, input: Self::LocalIn);
    fn poll_output(&mut self) -> Option<Self::LocalOut>;

    fn handle_due(&mut self, now: Timestamp);
    fn next_due(&self) -> Option<Timestamp>;
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
    outputs: VecDeque<CadenceOutput<S>>,
}

// Invariant: a slot's Finalized precedes its Lifecycle(Completed).
pub enum CadenceOutput<S: SlotConsensus> {
    Lifecycle(SlotLifecycle),
    Local(Slot, S::LocalOut),
    // todo: attach FastCommitVote/FastBlock as data
    CommitOptimistic(Slot),
    Finalized(Slot, S::FinalizationData),
}

enum PendingWake<Timer, Alarm> {
    Deadline(Slot),
    SlotTimer(Slot, Timer),
    Alarm(Alarm),
}

impl<S, C> CadenceRuntime<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    pub fn new(slot_manager: SlotManager<S>, conductor: C) -> Self {
        let mut runtime = Self {
            clock: Timestamp::GENESIS,
            slot_manager,
            conductor,
            timers: Timers::default(),
            outbox: VecDeque::new(),
            outputs: VecDeque::new(),
        };
        runtime.step();
        runtime
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
                debug!(?slot, "wake for a slot without instance");
            }
        }
    }

    fn handle_message(&mut self, sender: NodeId, message: CadenceWireMsg<S, C>) {
        let now = self.clock;
        match message {
            CadenceMessage::Conductor(message) => {
                self.conductor.handle_message(now, sender, message);
            }
            CadenceMessage::Slot(message)
                if let Some(instance) = self.slot_manager.slot_instance(message.slot()) =>
            {
                instance.handle_message(sender, message);
            }
            CadenceMessage::Slot(message) => {
                let slot = message.slot();
                trace!(?slot, ?sender, "message for a slot without instance");
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
                debug_assert_eq!(message.slot(), slot);
                let message = CadenceMessage::Slot(message);
                self.outbox.push_back(Outbound::Broadcast(message));
            }
            SlotOutput::Unicast { to, message } => {
                debug_assert_eq!(message.slot(), slot);
                let message = CadenceMessage::Slot(message);
                self.outbox.push_back(Outbound::Unicast(to, message));
            }
            SlotOutput::Local(command) => {
                self.outputs.push_back(CadenceOutput::Local(slot, command));
            }
            SlotOutput::CommitOptimistic(_) => {
                self.outputs
                    .push_back(CadenceOutput::CommitOptimistic(slot));
            }
            SlotOutput::Finalize(data) => {
                self.outputs.push_back(CadenceOutput::Finalized(slot, data));
                self.conductor.handle_slot_finalization(now, slot);
                self.complete(slot);
            }
            SlotOutput::Fault { reason } => {
                tracing::warn!(?slot, reason = %reason, "slot faulted");
                self.complete(slot);
            }
        }
    }

    fn complete(&mut self, slot: Slot) {
        let completed = SlotLifecycle::Completed { slot };
        self.outputs.push_back(CadenceOutput::Lifecycle(completed));
        self.slot_manager.close(slot);
    }

    fn handle_conductor_output(&mut self, out: ConductorOutput<C>) {
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
                let advance = SlotLifecycle::CapAdvance { cap };
                self.outputs.push_back(CadenceOutput::Lifecycle(advance));
            }
            ConductorOutput::OpenSlots(slots) => {
                for (slot, deadline) in slots {
                    self.slot_manager.open(slot);
                    self.timers.schedule(deadline, PendingWake::Deadline(slot));
                    let opened = SlotLifecycle::Opened { slot, deadline };
                    self.outputs.push_back(CadenceOutput::Lifecycle(opened));
                }
            }
        }
    }
}

impl<S, C> Runtime for CadenceRuntime<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    type Message = CadenceWireMsg<S, C>;
    type LocalIn = (Slot, S::LocalIn);
    type LocalOut = CadenceOutput<S>;

    fn receive(&mut self, now: Timestamp, sender: NodeId, message: CadenceWireMsg<S, C>) {
        self.advance_clock(now);
        self.handle_message(sender, message);
        self.step();
    }

    fn poll_msg(&mut self) -> Option<Outbound<CadenceWireMsg<S, C>>> {
        self.outbox.pop_front()
    }

    fn handle_input(&mut self, now: Timestamp, (slot, event): (Slot, S::LocalIn)) {
        self.advance_clock(now);

        if let Some(instance) = self.slot_manager.slot_instance(slot) {
            // q: do we need to buffer the da events? da begins to
            // accept chunk ingestion at the same time as slot
            // consensus, which normally is already conservative.
            instance.handle_input(event);
        }
        self.step();
    }

    fn poll_output(&mut self) -> Option<CadenceOutput<S>> {
        self.outputs.pop_front()
    }

    // wakes are handled one at a time, each to quiescence
    fn handle_due(&mut self, now: Timestamp) {
        self.advance_clock(now);
        while let Some(wake) = self.timers.pop_due(now) {
            self.handle_wake(wake);
            self.step();
        }
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.timers.next_due()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SlotLifecycle {
    Opened { slot: Slot, deadline: Timestamp },
    Completed { slot: Slot },
    // every slot below cap (exclusive) is closed, finalized locally or not
    CapAdvance { cap: Slot },
}
