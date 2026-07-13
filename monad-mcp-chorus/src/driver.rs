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

use std::collections::{HashMap, VecDeque};

use crate::types::{Slot, Timestamp, TimestampDelta, Validated};

// A driver translates consensus effects into concrete node effects
// that are agnostic of the consensus protocol.
pub trait Driver<S, C>
where
    S: crate::slot::SlotConsensus,
    C: crate::conductor::Conductor,
{
    type WireMsg;

    // translate CadenceEvent into NodeEvent
    fn schedule_alarm(&mut self, at: Timestamp, alarm: C::Alarm);
    fn schedule_slot_deadline(&mut self, slot: Slot, deadline: Timestamp);
    fn schedule_slot_timer(&mut self, delta: TimestampDelta, slot: Slot, timer: S::Timer);
    fn broadcast_slot(&mut self, slot: Slot, message: S::Message);
    fn broadcast_conductor(&mut self, message: C::Message);
    fn poll_node_event(&mut self) -> Option<NodeEvent<Self::WireMsg>>;

    // translate NodeEvent into CadenceEvent
    fn handle_wake(&mut self, wake: WakeId);
    fn handle_message(&mut self, message: Validated<Self::WireMsg>);
    fn poll_cadence_event(
        &mut self,
    ) -> Option<CadenceEvent<S::Timer, C::Alarm, S::Message, C::Message>>;
}

pub enum CadenceEvent<Timer, Alarm, SMessage, CMessage> {
    SlotTimer(Slot, Timer),
    Alarm(Alarm),
    ConductorMessage(Validated<CMessage>),
    SlotMessage(Validated<(Slot, SMessage)>),
    SlotDeadline(Slot),
}

// An opaque token for a pending timer/alarm
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct WakeId(u64);

pub enum NodeEvent<M> {
    Wake(Timestamp, WakeId),
    WakeAfter(TimestampDelta, WakeId),
    Broadcast(M),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CadenceMessage<SM, CM> {
    Slot(Slot, SM),
    Conductor(CM),
}

// A type alias for quickly get the wire message type for a given
// SlotConsensus and Conductor pair.
pub type CadenceDriverMsg<S, C> = <CadenceDriver<S, C> as Driver<S, C>>::WireMsg;

// A canonical driver implementation for Cadence.
pub struct CadenceDriver<S, C>
where
    S: crate::slot::SlotConsensus,
    C: crate::conductor::Conductor,
{
    inbox: VecDeque<CadenceEvent<S::Timer, C::Alarm, S::Message, C::Message>>,
    outbox: VecDeque<NodeEvent<CadenceMessage<S::Message, C::Message>>>,

    wakes: HashMap<WakeId, PendingWake<S::Timer, C::Alarm>>,
    next_wake: WakeId,
}

enum PendingWake<Timer, Alarm> {
    Deadline(Slot),
    SlotTimer(Slot, Timer),
    Alarm(Alarm),
}

impl<S, C> Default for CadenceDriver<S, C>
where
    S: crate::slot::SlotConsensus,
    C: crate::conductor::Conductor,
{
    fn default() -> Self {
        Self {
            inbox: VecDeque::new(),
            outbox: VecDeque::new(),
            wakes: HashMap::new(),
            next_wake: WakeId(0),
        }
    }
}

impl<S, C> CadenceDriver<S, C>
where
    S: crate::slot::SlotConsensus,
    C: crate::conductor::Conductor,
{
    fn fresh_wake(&mut self) -> WakeId {
        let id = self.next_wake;
        self.next_wake.0 += 1;
        id
    }
}

impl<S, C> Driver<S, C> for CadenceDriver<S, C>
where
    S: crate::slot::SlotConsensus,
    C: crate::conductor::Conductor,
{
    type WireMsg = CadenceMessage<S::Message, C::Message>;

    fn schedule_alarm(&mut self, at: Timestamp, alarm: C::Alarm) {
        let id = self.fresh_wake();
        self.wakes.insert(id, PendingWake::Alarm(alarm));
        self.outbox.push_back(NodeEvent::Wake(at, id));
    }

    fn schedule_slot_timer(&mut self, delta: TimestampDelta, slot: Slot, timer: S::Timer) {
        let id = self.fresh_wake();
        self.wakes.insert(id, PendingWake::SlotTimer(slot, timer));
        self.outbox.push_back(NodeEvent::WakeAfter(delta, id));
    }

    fn schedule_slot_deadline(&mut self, slot: Slot, deadline: Timestamp) {
        let id = self.fresh_wake();
        self.wakes.insert(id, PendingWake::Deadline(slot));
        self.outbox.push_back(NodeEvent::Wake(deadline, id));
    }

    fn broadcast_slot(&mut self, slot: Slot, message: S::Message) {
        self.outbox
            .push_back(NodeEvent::Broadcast(CadenceMessage::Slot(slot, message)));
    }

    fn broadcast_conductor(&mut self, message: C::Message) {
        self.outbox
            .push_back(NodeEvent::Broadcast(CadenceMessage::Conductor(message)))
    }

    fn poll_node_event(&mut self) -> Option<NodeEvent<Self::WireMsg>> {
        self.outbox.pop_front()
    }

    fn handle_wake(&mut self, wake: WakeId) {
        if let Some(pending) = self.wakes.remove(&wake) {
            match pending {
                PendingWake::Deadline(slot) => {
                    self.inbox.push_back(CadenceEvent::SlotDeadline(slot));
                }
                PendingWake::Alarm(alarm) => {
                    self.inbox.push_back(CadenceEvent::Alarm(alarm));
                }
                PendingWake::SlotTimer(slot, timer) => {
                    self.inbox.push_back(CadenceEvent::SlotTimer(slot, timer));
                }
            }
        }
    }

    fn handle_message(&mut self, message: Validated<Self::WireMsg>) {
        let (message, author) = message.destructure();
        match message {
            CadenceMessage::Slot(slot, msg) => {
                // safety: message is already validated.
                let validated = Validated::new_unchecked((slot, msg), author);
                self.inbox.push_back(CadenceEvent::SlotMessage(validated));
            }
            CadenceMessage::Conductor(msg) => {
                // safety: message is already validated.
                let validated = Validated::new_unchecked(msg, author);
                self.inbox
                    .push_back(CadenceEvent::ConductorMessage(validated));
            }
        }
    }

    fn poll_cadence_event(
        &mut self,
    ) -> Option<CadenceEvent<S::Timer, C::Alarm, S::Message, C::Message>> {
        self.inbox.pop_front()
    }
}
