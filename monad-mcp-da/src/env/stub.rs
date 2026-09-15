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

use bytes::BufMut;
pub(crate) use chorus::types::NodeId;
use monad_crypto::hasher::{Hasher as _, HasherType};
pub(crate) use monad_mcp_chorus::stub as chorus;

use self::chorus::env::{
    EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, ProposalSignature, PubKey,
    SignedProposalHeader,
};
use crate::spec::{
    DAMerkleRoot, DAProposalHeader, DAProposalKeyPair, DAProposalSignature, DAPubKey, SIGNATURE_LEN,
};

// The keypair used to sign/verify proposal. Not used for aggregation.
pub struct ProposalKeyPair(NodeId);

impl ProposalKeyPair {
    pub fn dummy(node_id: NodeId) -> Self {
        Self(node_id)
    }
}

impl DAProposalKeyPair for ProposalKeyPair {
    type Signature = ProposalSignature;

    fn sign(&self, signed_bytes: &[u8]) -> ProposalSignature {
        ProposalSignature {
            signer: self.0,
            checksum: checksum(signed_bytes),
        }
    }
}

// signer(8) checksum(8), zero padded to the field
impl DAProposalSignature for ProposalSignature {
    type NodeId = NodeId;

    fn to_bytes(&self, out: &mut impl BufMut) {
        out.put_u64_le(u64::from(self.signer));
        out.put_u64_le(self.checksum);
        out.put_bytes(0, SIGNATURE_LEN - 16);
    }

    fn from_bytes(field: &[u8]) -> Option<Self> {
        let (signer, rest) = field.split_first_chunk::<8>()?;
        let (checksum, padding) = rest.split_first_chunk::<8>()?;
        if padding.iter().any(|byte| *byte != 0) {
            return None;
        }

        Some(ProposalSignature {
            signer: NodeId::dummy(u64::from_le_bytes(*signer)),
            checksum: u64::from_le_bytes(*checksum),
        })
    }

    fn recover_author(&self, signed_bytes: &[u8]) -> Option<NodeId> {
        if checksum(signed_bytes) != self.checksum {
            return None;
        }
        Some(self.signer)
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
impl DAPubKey for PubKey {
    fn to_bytes(&self, field: &mut [u8]) {
        field.fill(0);
        field[1..9].copy_from_slice(&u64::from(*self).to_le_bytes());
    }
}

// a hash prefix of the signed bytes
fn checksum(signed_bytes: &[u8]) -> u64 {
    let mut hasher = HasherType::new();
    hasher.update(signed_bytes);
    let hash = hasher.hash();
    let prefix = hash.0[..8].try_into().expect("8 bytes");
    u64::from_le_bytes(prefix)
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
