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
    collections::VecDeque,
    sync::mpsc::{Receiver, Sender, channel},
};

use super::Component;
use crate::chorus::{
    CadenceRuntime, DASink, FinalizationObserver, Outbound, Runtime as _, SlotLifecycle,
    SlotManager,
    conductor::{MonadConductor, acs::nop::NopAcs},
    slot::chorus::{Chorus, ChorusDACommand, ChorusDAEvent, SlotFinalization},
    types::{Slot, SlotDeadline, Timestamp, Validated},
};

pub type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
pub type CadenceWireMsg = crate::chorus::CadenceWireMsg<Chorus, Conductor>;

pub enum CadenceInput {
    Message(Validated<CadenceWireMsg>),
    DAEvent(Slot, ChorusDAEvent),
}

pub enum CadenceOutput {
    Outbound(Outbound<CadenceWireMsg>),
    Lifecycle(SlotLifecycle),
    DACommand(Slot, ChorusDACommand),
    Finalized(Timestamp, Slot, SlotFinalization),
    // the contiguous finalized prefix reached cap (exclusive)
    CapAdvance(Timestamp, Slot),
}

// the cadence runtime with its sink callbacks turned back into outputs.
// the channel goes away with the driver collapse in chorus.
pub struct Cadence {
    runtime: CadenceRuntime<Chorus, Conductor>,
    from_sinks: Receiver<CadenceOutput>,
    outbox: VecDeque<CadenceOutput>,
}

impl Cadence {
    pub fn new(slot_manager: SlotManager<Chorus>, conductor: Conductor) -> Self {
        let mut runtime = CadenceRuntime::new(slot_manager, conductor);
        let (to_outbox, from_sinks) = channel();
        runtime.on_da(DAChannel(to_outbox.clone()));
        runtime.on_finalization(FinalizationChannel(to_outbox));
        runtime.init();
        let mut cadence = Self {
            runtime,
            from_sinks,
            outbox: VecDeque::new(),
        };
        cadence.collect();
        cadence
    }

    fn collect(&mut self) {
        while let Ok(output) = self.from_sinks.try_recv() {
            self.outbox.push_back(output);
        }
        while let Some(event) = self.runtime.poll() {
            self.outbox.push_back(CadenceOutput::Outbound(event));
        }
    }
}

impl Component for Cadence {
    type Input = CadenceInput;
    type Output = CadenceOutput;

    fn handle(&mut self, now: Timestamp, input: CadenceInput) {
        match input {
            CadenceInput::Message(message) => self.runtime.receive(now, message),
            CadenceInput::DAEvent(slot, event) => self.runtime.handle_da_event(now, slot, event),
        }
        self.collect();
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.runtime.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.runtime.handle_due(now);
        self.collect();
    }

    fn poll(&mut self) -> Option<CadenceOutput> {
        self.outbox.pop_front()
    }
}

struct DAChannel(Sender<CadenceOutput>);

impl DASink<ChorusDACommand> for DAChannel {
    fn handle_lifecycle(&mut self, event: SlotLifecycle) {
        self.0.send(CadenceOutput::Lifecycle(event)).ok();
    }

    fn handle_command(&mut self, slot: Slot, action: ChorusDACommand) {
        self.0.send(CadenceOutput::DACommand(slot, action)).ok();
    }
}

struct FinalizationChannel(Sender<CadenceOutput>);

impl<OD> FinalizationObserver<OD, SlotFinalization> for FinalizationChannel {
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, data: &SlotFinalization) {
        let event = CadenceOutput::Finalized(now, slot, data.clone());
        self.0.send(event).ok();
    }

    fn handle_chain_advance(&mut self, now: Timestamp, cap: Slot) {
        self.0.send(CadenceOutput::CapAdvance(now, cap)).ok();
    }
}
