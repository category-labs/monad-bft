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

use super::{
    fast::Entry,
    types::{
        EpochHandle, HeaderAuth, IsVote, KeyPair, Lanes, NodeCredential, ProposerSet,
        SignedProposalHeader, Slot, SlotOutOfBound, TimestampDelta, ValidatorData, ValidatorIndex,
        VoteMsg, VotePool,
    },
};
use crate::spec::ProposalHeader as _;

/// What each slot instance of this node shares.
#[derive(Clone)]
pub struct ChorusConfig {
    // capital Delta from the paper
    pub delta: TimestampDelta,
    pub credential: NodeCredential,
    pub header_auth: Arc<HeaderAuth>,
}

impl ChorusConfig {
    pub fn at_slot(
        &self,
        slot: Slot,
        epoch_handle: &EpochHandle,
    ) -> Result<ChorusContext, SlotOutOfBound> {
        use monad_mcp_common::spec::ValidatorData as _;

        let validator_data = epoch_handle.validators_at(slot)?;
        let proposer_set = epoch_handle.proposers_at(slot)?;
        // todo: a full node, which has no slot instance
        let self_index = validator_data
            .index_of(&self.credential.node_id)
            .expect("this node is not a validator of the slot");

        Ok(ChorusContext {
            slot,
            delta: self.delta,
            self_index,
            key: self.credential.vote_key.clone(),
            validator_data,
            proposer_set,
            header_auth: self.header_auth.clone(),
        })
    }
}

// What one Chorus instance knows: the config of this node joined with the
// validators and the proposers of its slot.
pub struct ChorusContext {
    pub(crate) slot: Slot,
    pub(crate) delta: TimestampDelta,

    // Read only by MonadMvba::leads.
    pub(crate) self_index: ValidatorIndex,
    // Read only by MonadMvba::own_proposal_ready: a pre-prepare signs its own byte form.
    pub(crate) key: Arc<KeyPair>,

    validator_data: Arc<ValidatorData>,
    proposer_set: ProposerSet,
    header_auth: Arc<HeaderAuth>,
}

impl ChorusContext {
    pub(crate) fn validators(&self) -> &ValidatorData {
        &self.validator_data
    }

    pub(crate) fn vote_pool<V: IsVote>(&self, scope: V::Scope) -> VotePool<V> {
        VotePool::new(scope, self.validator_data.clone())
    }

    pub(crate) fn sign<V: IsVote>(&self, scope: V::Scope, vote: V) -> VoteMsg<V> {
        VoteMsg::new_signed(scope, vote, &self.key)
    }

    pub(crate) fn num_lanes(&self) -> usize {
        self.proposer_set.num_lanes()
    }

    // one entry per lane, and a positive entry only on a lane with a proposer
    pub(crate) fn validate_lanes(&self, entries: Lanes<&Entry>) -> bool {
        if entries.size() != self.num_lanes() {
            return false;
        }
        let mut entries = entries.into_indexed_iter();
        entries.all(|(lane, entry)| match entry {
            Entry::Positive(_) => self.proposer_set.proposer(lane).is_some(),
            Entry::Negative => true,
        })
    }

    pub(crate) fn header_valid(&self, header: &SignedProposalHeader) -> bool {
        let Some(proposer) = self.proposer_set.proposer(header.lane()) else {
            return false;
        };
        (self.header_auth)(header, &self.validator_data, &proposer)
    }
}
