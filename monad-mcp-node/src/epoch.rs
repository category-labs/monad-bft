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

use std::sync::Arc;

use monad_mcp_chorus::spec::validator::ValidatorData as _;

use crate::{
    chorus::{
        slot::chorus::ChorusContext,
        types::{HeaderAuth, KeyPair, NodeId, ProposalIndex, Slot, ValidatorData},
    },
    da::{self, ProposalKeyPair, ProposerElection},
};

// a union of all the fields of da's EpochHandle and chorus's
// ChorusContext.
#[derive(Clone)]
pub struct EpochHandle {
    pub self_id: NodeId,
    pub num_proposals: usize,
    // signs votes
    pub key: Arc<KeyPair>,
    // signs proposal headers
    pub proposal_key: Arc<ProposalKeyPair>,
    pub validator_data: Arc<ValidatorData>,
    pub election: Arc<StubElection>,
    pub header_auth: Arc<HeaderAuth>,
}

impl EpochHandle {
    pub fn da(&self) -> da::EpochHandle {
        da::EpochHandle {
            self_id: self.self_id,
            num_proposals: self.num_proposals,
            key_pair: self.proposal_key.clone(),
            header_auth: self.header_auth.clone(),
            validator_data: self.validator_data.clone(),
        }
    }

    pub fn chorus(&self) -> ChorusContext {
        ChorusContext {
            node_id: self.self_id,
            key: self.key.clone(),
            validator_data: self.validator_data.clone(),
            header_auth: self.header_auth.clone(),
        }
    }
}

// a window of num_proposals validators in canonical order, advancing
// one validator per slot: in slot s, proposal j is by validator
// (s + j) mod n.
pub struct StubElection {
    validators: Vec<NodeId>,
    num_proposals: usize,
}

impl StubElection {
    pub fn new(validator_data: &ValidatorData, num_proposals: usize) -> Self {
        Self {
            validators: validator_data.nodes().copied().collect(),
            num_proposals,
        }
    }

    fn rotation(&self, slot: Slot) -> usize {
        (slot.0 % self.validators.len() as u64) as usize
    }
}

impl ProposerElection for StubElection {
    fn get_proposer(&self, slot: Slot, index: ProposalIndex) -> Option<&NodeId> {
        if index >= self.num_proposals || index >= self.validators.len() {
            return None;
        }
        let position = (self.rotation(slot) + index) % self.validators.len();
        self.validators.get(position)
    }

    fn get_index(&self, slot: Slot, node: &NodeId) -> Option<ProposalIndex> {
        let n = self.validators.len();
        let position = self.validators.iter().position(|v| v == node)?;
        let index = (position + n - self.rotation(slot)) % n;
        (index < self.num_proposals).then_some(index)
    }
}
