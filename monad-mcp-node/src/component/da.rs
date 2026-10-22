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

use bytes::Bytes;

use super::Component;
use crate::{
    chorus::{
        SlotLifecycle,
        slot::chorus::ChorusDACommand,
        types::{NodeId, Proposal, Slot, Timestamp},
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
    // our proposal, encoded at the time DA handles it
    Propose(Proposal<Bytes>),
}

impl Component for DA {
    type Input = DAInput;
    type Output = DAOutput;

    fn handle(&mut self, now: Timestamp, input: DAInput) {
        match input {
            DAInput::Envelope(sender, envelope) => {
                if let Err(reason) = self.ingest(&sender, envelope) {
                    tracing::debug!(?reason, "rejected proposal envelope");
                }
            }
            DAInput::ChunkRequest(from, request) => self.handle_chunk_request(request, &from, now),
            DAInput::Lifecycle(event) => self.handle_slot_lifecycle(event),
            DAInput::Command(slot, command) => self.handle_command(command, slot, now),
            DAInput::Propose(proposal) => self.propose(proposal, now),
        }
    }

    fn poll(&mut self) -> Option<DAOutput> {
        DARuntime::poll(self)
    }

    fn next_due(&self) -> Option<Timestamp> {
        Some(self.next_tick())
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.handle_tick(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chorus::types::Lane,
        config::{NodeConfig, ProposalConfig},
        node::Node,
    };

    #[test]
    fn the_largest_proposal_size_limit_still_encodes() {
        let config = NodeConfig::single_node(0, Timestamp::from_millis(10_000), "unused-ledger");
        let mut da = Node::new(&config).unwrap().da;
        let now = Timestamp::from_millis(1_000);
        let deadline = Timestamp::from_millis(10_000);
        let opened = SlotLifecycle::Opened {
            slot: Slot(0),
            deadline,
        };
        da.handle(now, DAInput::Lifecycle(opened));

        // a lone validator owns every chunk: an encoded proposal shows as its own decode
        let mut encodes = |len| {
            let proposal = Proposal {
                slot: Slot(0),
                lane: Lane::new_unchecked(0),
                payload: Bytes::from(vec![7; len]),
            };
            da.handle(now, DAInput::Propose(proposal));
            let mut outputs = 0;
            while Component::poll(&mut da).is_some() {
                outputs += 1;
            }
            outputs > 0
        };
        let max = ProposalConfig::MAX_PROPOSAL_SIZE_LIMIT;
        assert!(!encodes(max + 1));
        assert!(encodes(max));
    }
}
