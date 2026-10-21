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

use monad_pq_chorus::{
    PqEvent, PqHalt, QuantumChorus,
    da::{ChorusDAEvent, PqDACommand},
};

use super::Component;
use crate::chorus::{
    CadenceRuntime, DASink, FinalizationObserver, Outbound, Runtime as _, SlotLifecycle,
    SlotManager,
    conductor::{MonadConductor, acs::nop::NopAcs},
    types::{Slot, SlotDeadline, Timestamp, Validated},
};

pub type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
pub type PqCadenceWireMsg = crate::chorus::CadenceWireMsg<QuantumChorus, Conductor>;

pub enum PqCadenceInput {
    Message(Validated<PqCadenceWireMsg>),
    DAEvent(Slot, ChorusDAEvent),
}

pub enum PqCadenceOutput {
    Outbound(Outbound<PqCadenceWireMsg>),
    Lifecycle(SlotLifecycle),
    DACommand(Slot, PqDACommand),
    // the slot's observer channel (§3.2): speculation, finalization, evidence
    Event(Timestamp, Slot, PqEvent),
    // the slot halted and closed (D4)
    Halted(Timestamp, Slot, PqHalt),
    // the conductor's completed prefix reached cap (exclusive)
    CapAdvance(Timestamp, Slot),
}

// the unchanged cadence runtime over QuantumChorus, its sink callbacks
// turned back into outputs
pub struct PqCadence {
    runtime: CadenceRuntime<QuantumChorus, Conductor>,
    from_sinks: Receiver<PqCadenceOutput>,
    outbox: VecDeque<PqCadenceOutput>,
}

impl PqCadence {
    pub fn new(slot_manager: SlotManager<QuantumChorus>, conductor: Conductor) -> Self {
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
            self.outbox.push_back(PqCadenceOutput::Outbound(event));
        }
    }
}

impl Component for PqCadence {
    type Input = PqCadenceInput;
    type Output = PqCadenceOutput;

    fn handle(&mut self, now: Timestamp, input: PqCadenceInput) {
        match input {
            PqCadenceInput::Message(message) => self.runtime.receive(now, message),
            PqCadenceInput::DAEvent(slot, event) => self.runtime.handle_da_event(now, slot, event),
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

    fn poll(&mut self) -> Option<PqCadenceOutput> {
        self.outbox.pop_front()
    }
}

struct DAChannel(Sender<PqCadenceOutput>);

impl DASink<PqDACommand> for DAChannel {
    fn handle_lifecycle(&mut self, event: SlotLifecycle) {
        self.0.send(PqCadenceOutput::Lifecycle(event)).ok();
    }

    fn handle_command(&mut self, slot: Slot, command: PqDACommand) {
        self.0.send(PqCadenceOutput::DACommand(slot, command)).ok();
    }
}

struct FinalizationChannel(Sender<PqCadenceOutput>);

impl FinalizationObserver<PqEvent, PqHalt> for FinalizationChannel {
    fn handle_optimistic_commit(&mut self, now: Timestamp, slot: Slot, event: &PqEvent) {
        let output = PqCadenceOutput::Event(now, slot, event.clone());
        self.0.send(output).ok();
    }

    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, halt: &PqHalt) {
        let output = PqCadenceOutput::Halted(now, slot, halt.clone());
        self.0.send(output).ok();
    }

    fn handle_chain_advance(&mut self, now: Timestamp, cap: Slot) {
        self.0.send(PqCadenceOutput::CapAdvance(now, cap)).ok();
    }
}
