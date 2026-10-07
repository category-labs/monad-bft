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

use super::Component;
use crate::{
    chorus::{
        SlotLifecycle,
        slot::chorus::ChorusDACommand,
        types::{NodeId, Slot, Timestamp},
    },
    da::{ChunkRecoveryRequest, DAOutput, DARuntime, ProposalEnvelope},
    epoch::NodeProposerSchedule,
};

pub type DA = DARuntime<NodeProposerSchedule>;

pub enum DAInput {
    Envelope(NodeId, ProposalEnvelope),
    ChunkRequest(NodeId, ChunkRecoveryRequest),
    Lifecycle(SlotLifecycle),
    Command(Slot, ChorusDACommand),
}

impl Component for DA {
    type Input = DAInput;
    type Output = DAOutput;

    fn handle(&mut self, now: Timestamp, input: DAInput) {
        match input {
            DAInput::Envelope(sender, envelope) => {
                if let Err(reason) = self.ingest(now, &sender, envelope) {
                    tracing::debug!(?reason, "rejected proposal envelope");
                }
            }
            DAInput::ChunkRequest(from, request) => self.handle_chunk_request(request, &from),
            DAInput::Lifecycle(event) => self.handle_slot_lifecycle(event),
            DAInput::Command(slot, command) => self.handle_command(command, slot, now),
        }
    }

    fn poll(&mut self) -> Option<DAOutput> {
        DARuntime::poll(self)
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.next_tick()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.handle_tick(now);
    }
}
