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

use monad_mcp_common::{Slot, SlotDeadline, Timestamp};

use super::Component;
use crate::{
    chorus::{
        CadenceOutput as LocalOut, CadenceRuntime, Outbound, Runtime as _, SlotLifecycle,
        SlotManager,
        conductor::{CadenceConductor, acs::nop::NopAcs},
        slot::chorus::{Chorus, ChorusDACommand, ChorusDAEvent, SlotFinalization},
    },
    common::NodeId,
};

pub type Conductor = CadenceConductor<NopAcs<SlotDeadline>>;
pub type CadenceWireMsg = crate::chorus::CadenceWireMsg<Chorus, Conductor>;

pub enum CadenceInput {
    Message(NodeId, CadenceWireMsg),
    DAEvent(Slot, ChorusDAEvent),
}

pub enum CadenceOutput {
    Outbound(Outbound<CadenceWireMsg>),
    Lifecycle(SlotLifecycle),
    DACommand(Slot, ChorusDACommand),
    Finalized(Timestamp, Slot, SlotFinalization),
    FastBlock(Timestamp, Slot), // demo(tx-timeline)
}

pub struct Cadence {
    runtime: CadenceRuntime<Chorus, Conductor>,
    // the time of the latest input, which stamps the commits it causes
    now: Timestamp,
}

impl Cadence {
    pub fn new(slot_manager: SlotManager<Chorus>, conductor: Conductor) -> Self {
        Self {
            runtime: CadenceRuntime::new(slot_manager, conductor),
            now: Timestamp::GENESIS,
        }
    }
}

impl Component for Cadence {
    type Input = CadenceInput;
    type Output = CadenceOutput;

    fn handle(&mut self, now: Timestamp, input: CadenceInput) {
        self.now = now;
        match input {
            CadenceInput::Message(sender, message) => self.runtime.receive(now, sender, message),
            CadenceInput::DAEvent(slot, event) => self.runtime.handle_input(now, (slot, event)),
        }
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.runtime.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.now = now;
        self.runtime.handle_due(now);
    }

    fn poll(&mut self) -> Option<CadenceOutput> {
        if let Some(outbound) = self.runtime.poll_msg() {
            return Some(CadenceOutput::Outbound(outbound));
        }
        let output = match self.runtime.poll_output()? {
            LocalOut::Lifecycle(event) => CadenceOutput::Lifecycle(event),
            LocalOut::Local(slot, command) => CadenceOutput::DACommand(slot, command),
            LocalOut::CommitOptimistic(slot) => CadenceOutput::FastBlock(self.now, slot),
            LocalOut::Finalized(slot, finalization) => {
                CadenceOutput::Finalized(self.now, slot, finalization)
            }
        };
        Some(output)
    }
}
