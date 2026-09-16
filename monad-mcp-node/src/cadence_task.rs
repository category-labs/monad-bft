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

use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};
use tracing::{Instrument as _, Span};

use crate::{
    chorus::{
        CadenceDriverMsg, CadenceRuntime, DASink, FinalizationObserver, NodeEvent, Runtime as _,
        SlotLifecycle, WakeId,
        conductor::{MonadConductor, acs::median::MedianAcs},
        slot::chorus::{Chorus, ChorusDACommand, ChorusDAEvent, SlotFinalization},
        types::{Slot, SlotDeadline, Timestamp, Validated},
    },
    network::Link,
    node::Clock,
};

pub type Conductor = MonadConductor<MedianAcs<SlotDeadline>>;
pub type Cadence = CadenceRuntime<Chorus, Conductor>;
pub type CadenceWireMsg = CadenceDriverMsg<Chorus, Conductor>;

pub enum CadenceInput {
    Message(Validated<CadenceWireMsg>),
    Wake(WakeId),
    DAEvent(Slot, ChorusDAEvent),
}

pub enum CadenceOutput {
    NodeEvent(NodeEvent<CadenceWireMsg>),
    Lifecycle(Slot, SlotLifecycle),
    DACommand(Slot, ChorusDACommand),
    Finalized(Timestamp, Slot, SlotFinalization),
    // the contiguous finalized prefix reached cap (exclusive)
    CapAdvance(Timestamp, Slot),
}

pub struct CadenceTask {
    clock: Clock,
    cadence: Cadence,
    link: Link<CadenceOutput, CadenceInput>,
}

impl CadenceTask {
    pub fn spawn(
        mut cadence: Cadence,
        clock: Clock,
        link: Link<CadenceOutput, CadenceInput>,
    ) -> JoinHandle<()> {
        // da sink
        cadence.on_da(DAChannel(link.sender()));

        // finalization sink
        cadence.on_finalization(FinalizationChannel(link.sender()));

        let task = Self {
            clock,
            cadence,
            link,
        };
        tokio::spawn(task.run().instrument(Span::current()))
    }

    async fn run(mut self) {
        self.cadence.init();
        self.flush();
        while let Some(input) = self.link.recv().await {
            self.handle(input);
            self.flush();
        }
    }

    fn handle(&mut self, input: CadenceInput) {
        let now = self.clock.now();
        match input {
            CadenceInput::Message(message) => {
                self.cadence.receive(now, message);
            }
            CadenceInput::Wake(wake) => {
                self.cadence.wake(now, wake);
            }
            CadenceInput::DAEvent(slot, event) => {
                self.cadence.handle_da_event(now, slot, event);
            }
        }
    }

    fn flush(&mut self) {
        while let Some(event) = self.cadence.poll() {
            self.link.send(CadenceOutput::NodeEvent(event));
        }
    }
}

// the DASink handed to cadence: its calls become outputs
struct DAChannel(UnboundedSender<CadenceOutput>);

impl DASink<ChorusDACommand> for DAChannel {
    fn handle_lifecycle(&mut self, slot: Slot, event: SlotLifecycle) {
        self.0.send(CadenceOutput::Lifecycle(slot, event)).ok();
    }

    fn handle_command(&mut self, slot: Slot, action: ChorusDACommand) {
        self.0.send(CadenceOutput::DACommand(slot, action)).ok();
    }
}

// the FinalizationObserver handed to cadence: its calls become outputs
struct FinalizationChannel(UnboundedSender<CadenceOutput>);

impl<OD> FinalizationObserver<OD, SlotFinalization> for FinalizationChannel {
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, data: &SlotFinalization) {
        let event = CadenceOutput::Finalized(now, slot, data.clone());
        self.0.send(event).ok();
    }

    fn handle_chain_advance(&mut self, now: Timestamp, cap: Slot) {
        self.0.send(CadenceOutput::CapAdvance(now, cap)).ok();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    // the observer is generic over the optimistic-commit type; tests pick ()
    type Observer = dyn FinalizationObserver<(), SlotFinalization>;

    #[test]
    fn chain_advance_becomes_a_cap_advance_output() {
        let (tx, mut rx) = unbounded_channel();
        let channel: &mut Observer = &mut FinalizationChannel(tx);

        channel.handle_chain_advance(Timestamp::from_millis(1), Slot(7));

        let output = rx.try_recv().expect("cap advance reached the node");
        assert!(matches!(output, CadenceOutput::CapAdvance(_, Slot(7))));
    }

    // opens reach proposing via the DA sink's lifecycle, so the observer stays silent
    #[test]
    fn slots_opened_produces_no_output() {
        let (tx, mut rx) = unbounded_channel();
        let channel: &mut Observer = &mut FinalizationChannel(tx);

        let slots = BTreeMap::from([(Slot(1), Timestamp::from_millis(1_000))]);
        channel.handle_slots_opened(Timestamp::from_millis(1), &slots);

        assert!(rx.try_recv().is_err());
    }
}
