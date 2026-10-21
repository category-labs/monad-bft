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

use monad_mcp_node::NodeProposerSchedule;
use monad_pq_chorus::da::PqDACommand;

use super::Component;
use crate::{
    chorus::{
        SlotLifecycle,
        types::{NodeId, Slot, Timestamp},
    },
    da::{ChunkRecoveryRequest, DAOutput, DARuntime, ProposalEnvelope},
};

// the pq DA runtime as a component; a newtype since both the trait and
// the runtime live in other crates
pub struct PqDA(pub DARuntime<NodeProposerSchedule>);

pub enum PqDAInput {
    Envelope(NodeId, ProposalEnvelope),
    ChunkRequest(NodeId, ChunkRecoveryRequest),
    Lifecycle(SlotLifecycle),
    Command(Slot, PqDACommand),
}

impl Component for PqDA {
    type Input = PqDAInput;
    type Output = DAOutput;

    fn handle(&mut self, now: Timestamp, input: PqDAInput) {
        let da = &mut self.0;
        match input {
            PqDAInput::Envelope(sender, envelope) => {
                if let Err(reason) = da.ingest(&sender, envelope) {
                    tracing::debug!(?reason, "rejected proposal envelope");
                }
            }
            PqDAInput::ChunkRequest(from, request) => da.handle_chunk_request(request, &from, now),
            PqDAInput::Lifecycle(event) => da.handle_slot_lifecycle(event),
            PqDAInput::Command(slot, command) => da.handle_command(command, slot, now),
        }
    }

    fn poll(&mut self) -> Option<DAOutput> {
        self.0.poll()
    }

    fn next_due(&self) -> Option<Timestamp> {
        Some(self.0.next_tick())
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.0.handle_tick(now);
    }
}
