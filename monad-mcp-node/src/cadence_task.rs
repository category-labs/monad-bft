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
        CadenceDriverMsg, CadenceRuntime, DASink, NodeEvent, Runtime as _, SlotLifecycle, WakeId,
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
        let finalized = link.sender();
        cadence.on_finalization(move |now, slot, data: &SlotFinalization| {
            let event = CadenceOutput::Finalized(now, slot, data.clone());
            finalized.send(event).ok();
        });

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
