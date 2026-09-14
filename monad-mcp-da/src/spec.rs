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

use monad_mcp_chorus::spec;

pub trait DAMerkleRoot: spec::MerkleRoot {
    fn to_bytes(&self, field: &mut [u8]);
    fn from_bytes(field: &[u8]) -> Option<Self>;
}

pub trait DAProposalSignature: Clone + Eq + std::hash::Hash + std::fmt::Debug {
    type NodeId: spec::validator::NodeId;

    fn to_bytes(&self, field: &mut [u8]);
    fn from_bytes(field: &[u8]) -> Option<Self>;

    // None unless the signature authenticates signed_bytes
    fn recover_author(&self, signed_bytes: &[u8]) -> Option<Self::NodeId>;
}

pub trait DAProposalKeyPair {
    type Signature: DAProposalSignature;

    fn sign(&self, signed_bytes: &[u8]) -> Self::Signature;
}

// Statically checks an env's proposal types against the spec
pub const fn assert_env<NodeId, MerkleRoot, ProposalSignature, ProposalKeyPair>()
where
    NodeId: spec::validator::NodeId,
    MerkleRoot: DAMerkleRoot,
    ProposalSignature: DAProposalSignature<NodeId = NodeId>,
    ProposalKeyPair: DAProposalKeyPair<Signature = ProposalSignature>,
{
}
