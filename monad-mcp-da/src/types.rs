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
pub(crate) use monad_mcp_common::{
    Lane, Lanes, MAX_PROPOSER_SET_SIZE, Slot, Timestamp, TimestampDelta,
};

pub(crate) use super::common::{NodeId, PubKey, Stake};
// re-export chorus types
pub use super::{
    chorus::{
        SlotLifecycle,
        env::{D25, EncodingScheme, MerkleHash, ProposalSignature, S11},
        slot::chorus::{ChorusDACommand, ChorusDAEvent, MissingHeaders, ProposalDAEvent},
        types::{
            EquivCert, HeaderAuth, MerkleRoot, ProposalHeader, ProposalId, ProposerSchedule,
            SignedProposalHeader, ValidatorData,
        },
    },
    env::ProposalKeyPair,
};

// a proposal's payload is opaque bytes to DA
pub type Proposal = monad_mcp_common::Proposal<Bytes>;

pub(crate) fn unix_seconds(now: Timestamp) -> u64 {
    let nanos_per_second = u128::from(TimestampDelta::NANOS_PER_MILLISECOND) * 1_000;
    (now.as_nanos() / nanos_per_second) as u64
}
