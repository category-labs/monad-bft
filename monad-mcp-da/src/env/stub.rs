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

use alloy_rlp::{Decodable, Encodable};
use bytes::BufMut;
pub(crate) use monad_mcp_cadence::stub as cadence;
pub(crate) use monad_mcp_common::stub as common;
pub(crate) use monad_mcp_epoch::stub as epoch;

use self::cadence::env::{
    EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, SignedProposalHeader,
};
pub(crate) use self::common::{NodeId, ProposalKeyPair, ProposalSignature};
use crate::spec::{DAMerkleRoot, DANodeId, DAProposalHeader, DAProposalSignature, SIGNATURE_LEN};

// the rlp form, zero padded to the field
impl DAProposalSignature for ProposalSignature {
    fn to_bytes(&self, out: &mut impl BufMut) {
        let padding = SIGNATURE_LEN - self.length();
        self.encode(out);
        out.put_bytes(0, padding);
    }

    fn from_bytes(field: &[u8]) -> Option<Self> {
        let mut padding = field;
        let signature = Self::decode(&mut padding).ok()?;
        if padding.iter().any(|byte| *byte != 0) {
            return None;
        }
        Some(signature)
    }
}

impl DAMerkleRoot for MerkleRoot {
    fn to_bytes(&self, out: &mut impl BufMut) {
        out.put_slice(&self.0.0);
    }

    fn from_bytes(field: &[u8]) -> Option<Self> {
        let hash = field.try_into().ok()?;
        Some(MerkleRoot(MerkleHash(hash)))
    }
}

impl DAProposalHeader for ProposalHeader {
    type Scheme = EncodingScheme;

    fn scheme(&self) -> &EncodingScheme {
        &self.scheme
    }
}

impl DAProposalHeader for SignedProposalHeader {
    type Scheme = EncodingScheme;

    fn scheme(&self) -> &EncodingScheme {
        &self.header.scheme
    }
}

// tag(1) key(8), zero padded to the field. the key sits where the
// seed derivation reads, after the tag byte.
impl DANodeId for NodeId {
    fn to_bytes(&self, field: &mut [u8]) {
        field.fill(0);
        field[1..9].copy_from_slice(&u64::from(*self).to_le_bytes());
    }
}

const _: () = crate::spec::assert_env::<
    NodeId,
    MerkleRoot,
    EncodingScheme,
    ProposalHeader,
    SignedProposalHeader,
    ProposalSignature,
    ProposalKeyPair,
>();
