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

use monad_mcp_common::{Slot, ValidatorIndex};

use crate::{
    cadence::{slot::chorus::ChorusContext, types::HeaderAuth},
    common::{KeyPair, NodeId, ValidatorData},
    da::{self, ProposalKeyPair},
    election::{RotatingProposerSchedule, RoundRobinLeaderSchedule},
};

/// The node's proposer schedule: the real rotating schedule over the
/// round-robin leader sequence, which keeps devnet assignments predictable.
pub type NodeProposerSchedule = RotatingProposerSchedule<RoundRobinLeaderSchedule>;

// a union of all the fields of da's EpochHandle and chorus's
// ChorusContext.
#[derive(Clone)]
pub struct EpochHandle {
    pub self_id: NodeId,
    // None if this node is not a validator of the epoch
    pub self_index: Option<ValidatorIndex>,
    // signs votes
    pub key: Arc<KeyPair>,
    // signs proposal headers
    pub proposal_key: Arc<ProposalKeyPair>,
    pub validator_data: Arc<ValidatorData>,
    pub proposers: Arc<NodeProposerSchedule>,
    pub header_auth: Arc<HeaderAuth>,
}

impl EpochHandle {
    pub fn da(&self) -> da::EpochHandle {
        da::EpochHandle {
            self_id: self.self_id,
            key_pair: self.proposal_key.clone(),
            header_auth: self.header_auth.clone(),
            validator_data: self.validator_data.clone(),
        }
    }

    // None if this node is not a validator of the epoch
    pub fn chorus(&self) -> Option<ChorusContext> {
        let context = ChorusContext {
            self_index: self.self_index?,
            key: self.key.clone(),
            validator_data: self.validator_data.clone(),
            header_auth: self.header_auth.clone(),
            occupancy: self.proposers.clone(),
        };
        Some(context)
    }
}

// The epoch of each slot. A validator index is valid in one epoch, so a
// layer that sees slots of several epochs finds the validator table here.
#[derive(Clone)]
pub struct Epochs {
    // one handle until epochs exist
    current: EpochHandle,
}

impl Epochs {
    pub fn new(current: EpochHandle) -> Self {
        Self { current }
    }

    // None for a slot whose epoch this node does not know
    pub fn at(&self, _slot: Slot) -> Option<&EpochHandle> {
        Some(&self.current)
    }

    // the id of this node, which no epoch scopes
    pub fn self_id(&self) -> NodeId {
        self.current.self_id
    }
}
