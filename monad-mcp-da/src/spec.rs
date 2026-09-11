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
use monad_mcp_chorus::spec;

// the validator set an encoding scheme sizes its depth range for
pub const MAX_VALIDATOR_SET_SIZE: usize = 300;

// the proposals a slot carries, which a scheme's header must index
pub const MAX_PROPOSER_SET_SIZE: usize = 5;

// the signature field of a packet
pub const SIGNATURE_LEN: usize = 65;

// the bytes a merkle root serializes to
pub const ROOT_LEN: usize = 20;

pub trait DAMerkleRoot: spec::MerkleRoot {
    // writes exactly ROOT_LEN bytes
    fn to_bytes(&self, out: &mut impl BufMut);
    fn from_bytes(field: &[u8]) -> Option<Self>;
}

pub trait DAProposalHeader: spec::ProposalHeader {
    type Scheme;

    fn scheme(&self) -> &Self::Scheme;
}

pub trait DAProposalSignature: Clone + Eq + std::hash::Hash + std::fmt::Debug {
    type NodeId: spec::validator::NodeId;

    // writes exactly SIGNATURE_LEN bytes
    fn to_bytes(&self, out: &mut impl BufMut);
    fn from_bytes(field: &[u8]) -> Option<Self>;

    // None unless the signature authenticates signed_bytes
    fn recover_author(&self, signed_bytes: &[u8]) -> Option<Self::NodeId>;
}

pub trait DAProposalKeyPair {
    type Signature: DAProposalSignature;

    fn sign(&self, signed_bytes: &[u8]) -> Self::Signature;
}

// the compressed public key, as the assignment seed reads it
pub const PUBKEY_LEN: usize = 33;

pub trait DAPubKey: spec::vote::PubKey {
    // writes exactly PUBKEY_LEN bytes
    fn to_bytes(&self, field: &mut [u8]);
}

// Statically checks an env's proposal types against the spec
pub const fn assert_env<
    NodeId,
    MerkleRoot,
    EncodingScheme,
    ProposalHeader,
    SignedProposalHeader,
    ProposalSignature,
    ProposalKeyPair,
>()
where
    NodeId: spec::validator::NodeId,
    MerkleRoot: DAMerkleRoot,
    ProposalHeader: DAProposalHeader<Root = MerkleRoot, Scheme = EncodingScheme>,
    SignedProposalHeader: DAProposalHeader<Root = MerkleRoot, Scheme = EncodingScheme>
        + spec::SignedProposalHeader<Sig = ProposalSignature>,
    ProposalSignature: DAProposalSignature<NodeId = NodeId>,
    ProposalKeyPair: DAProposalKeyPair<Signature = ProposalSignature>,
{
}
