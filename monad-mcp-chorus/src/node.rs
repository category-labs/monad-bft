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

use super::{
    conductor::Conductor,
    driver::{CadenceDriverMsg, NodeEvent, WakeId},
    runtime::{CadenceRuntime, DAQueue, DASink, Runtime},
    slot::SlotConsensus,
    types::{NodeId, Slot, Timestamp, TimestampDelta, Validated},
};

/// The DA layer as a node runs it: consumes what consensus sinks plus its own
/// wire traffic, reports back slot-scoped events.
pub trait DataAvailability<S: SlotConsensus>: DASink<S::DACommand> {
    /// Chunk traffic, distinct from the consensus wire
    type WireMsg;

    fn handle_message(&mut self, now: Timestamp, message: Validated<Self::WireMsg>);
    fn wake(&mut self, now: Timestamp, wake: WakeId);
    fn poll(&mut self) -> Option<DAOutput<S::DAEvent, Self::WireMsg>>;
}

pub enum DAOutput<E, M> {
    Event(Slot, E),
    Broadcast(M),
    Unicast { to: NodeId, message: M },
    Schedule(TimestampDelta, WakeId),
}

/// The node's wire type, muxing consensus traffic and the DA layer's own
pub type CadenceNodeMsg<S, C, A> =
    NodeMessage<CadenceDriverMsg<S, C>, <A as DataAvailability<S>>::WireMsg>;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum NodeMessage<CM, DM> {
    Cadence(CM),
    DA(DM),
}

/// A cadence runtime composed with a data-availability layer. Each side
/// numbers its own wakes, so pending wakes are re-keyed at the node level.
pub struct NodeRuntime<S, C, A>
where
    S: SlotConsensus,
    C: Conductor,
    A: DataAvailability<S>,
{
    cadence: CadenceRuntime<S, C>,

    clock: Timestamp,
    wakes: HashMap<WakeId, Side>,
    next_wake: WakeId,

    da: A,
    da_inbox: DAQueue<S::DACommand>,
    da_outbox: VecDeque<NodeEvent<A::WireMsg>>,
}

enum Side {
    Cadence(WakeId),
    DA(WakeId),
}

impl<S, C, A> NodeRuntime<S, C, A>
where
    S: SlotConsensus + 'static,
    C: Conductor,
    A: DataAvailability<S>,
{
    pub fn new(mut cadence: CadenceRuntime<S, C>, da: A) -> Self {
        let da_inbox = DAQueue::default();
        cadence.on_da(da_inbox.clone());

        Self {
            clock: Timestamp::GENESIS,
            cadence,
            da_inbox,
            da,
            wakes: HashMap::new(),
            next_wake: WakeId::FIRST,
            da_outbox: VecDeque::new(),
        }
    }

    fn advance_clock(&mut self, now: Timestamp) {
        assert!(now >= self.clock);
        self.clock = now;
    }

    fn register_wake(&mut self, side: Side) -> WakeId {
        let id = self.next_wake.post_increment();
        self.wakes.insert(id, side);
        id
    }

    fn step(&mut self) {
        while self.step_once() {}
    }

    // returns true if any progress was made
    fn step_once(&mut self) -> bool {
        if self.da_inbox.pop_into(&mut self.da) {
            return true;
        }

        if let Some(output) = self.da.poll() {
            match output {
                DAOutput::Event(slot, event) => {
                    self.cadence.handle_da_event(self.clock, slot, event);
                }
                DAOutput::Broadcast(message) => {
                    self.da_outbox.push_back(NodeEvent::Broadcast(message));
                }
                DAOutput::Unicast { to, message } => {
                    self.da_outbox.push_back(NodeEvent::Unicast { to, message });
                }
                DAOutput::Schedule(delta, wake) => {
                    let id = self.register_wake(Side::DA(wake));
                    self.da_outbox.push_back(NodeEvent::WakeAfter(delta, id));
                }
            }
            return true;
        }

        false
    }
}

impl<S, C, A> Runtime<CadenceNodeMsg<S, C, A>> for NodeRuntime<S, C, A>
where
    S: SlotConsensus + 'static,
    C: Conductor,
    A: DataAvailability<S>,
{
    fn init(&mut self) {
        self.cadence.init();
        self.step();
    }

    fn wake(&mut self, now: Timestamp, wake: WakeId) {
        self.advance_clock(now);
        match self.wakes.remove(&wake) {
            Some(Side::Cadence(wake)) => self.cadence.wake(now, wake),
            Some(Side::DA(wake)) => self.da.wake(now, wake),
            None => {}
        }
        self.step();
    }

    fn receive(&mut self, now: Timestamp, message: Validated<CadenceNodeMsg<S, C, A>>) {
        self.advance_clock(now);

        // safety: message is already validated.
        let (message, author) = message.destructure();
        match message {
            NodeMessage::Cadence(message) => self
                .cadence
                .receive(now, Validated::new_unchecked(message, author)),
            NodeMessage::DA(message) => self
                .da
                .handle_message(now, Validated::new_unchecked(message, author)),
        }

        self.step();
    }

    fn poll(&mut self) -> Option<NodeEvent<CadenceNodeMsg<S, C, A>>> {
        if let Some(event) = self.cadence.poll() {
            let event = match event {
                NodeEvent::Wake(at, wake) => {
                    NodeEvent::Wake(at, self.register_wake(Side::Cadence(wake)))
                }
                NodeEvent::WakeAfter(delta, wake) => {
                    NodeEvent::WakeAfter(delta, self.register_wake(Side::Cadence(wake)))
                }
                event => event,
            };
            return Some(event.map_message(NodeMessage::Cadence));
        }

        self.da_outbox
            .pop_front()
            .map(|event| event.map_message(NodeMessage::DA))
    }
}
