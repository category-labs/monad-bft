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

use crate::{
    chorus::{
        slot::chorus::ChorusContext,
        types::{
            HeaderAuth, KeyPair, NodeId, RotatingProposerSchedule, RoundRobinLeaderSchedule,
            ValidatorData,
        },
    },
    da::{self, ProposalKeyPair},
};

/// The node's proposer schedule: the real rotating schedule over the
/// round-robin leader sequence, which keeps devnet assignments predictable.
pub type NodeProposerSchedule = RotatingProposerSchedule<RoundRobinLeaderSchedule>;

// a union of all the fields of da's EpochHandle and chorus's
// ChorusContext.
#[derive(Clone)]
pub struct EpochHandle {
    pub self_id: NodeId,
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

    pub fn chorus(&self) -> ChorusContext {
        ChorusContext {
            node_id: self.self_id,
            key: self.key.clone(),
            validator_data: self.validator_data.clone(),
            header_auth: self.header_auth.clone(),
            proposers: self.proposers.clone(),
        }
    }
}
