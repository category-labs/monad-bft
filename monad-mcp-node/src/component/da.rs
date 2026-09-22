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
    Envelope(ProposalEnvelope),
    ChunkRequest(NodeId, ChunkRecoveryRequest),
    Lifecycle(Slot, SlotLifecycle),
    Command(Slot, ChorusDACommand),
}

impl Component for DA {
    type Input = DAInput;
    type Output = DAOutput;

    fn handle(&mut self, _now: Timestamp, input: DAInput) {
        match input {
            DAInput::Envelope(envelope) => {
                if let Err(reason) = self.ingest(envelope) {
                    tracing::debug!(?reason, "rejected proposal envelope");
                }
            }
            DAInput::ChunkRequest(from, request) => self.handle_chunk_request(&from, request),
            DAInput::Lifecycle(slot, event) => self.handle_slot_event(slot, event),
            DAInput::Command(slot, command) => self.handle_command(slot, command),
        }
    }

    fn poll(&mut self) -> Option<DAOutput> {
        DARuntime::poll(self)
    }
}
