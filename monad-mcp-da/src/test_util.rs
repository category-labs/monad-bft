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

// Shared test fixtures. By default: four equal-stake validators,
// validator 0 authoring proposals at index 0, the local node being
// validator 1.

use std::sync::Arc;

use bytes::Bytes;
use monad_mcp_chorus::spec::{validator::ValidatorData as _, vote::KeyPair as _};

use super::{
    assignment::{ChunkAssignment, ChunkId},
    chunk::{Chunk, ProposalEnvelope, WireChunkId},
    chunk_tree::ChunkTree,
    election::ProposerElection,
    encoding_scheme::{DAEncodingScheme as _, d25},
    header::header_auth,
    runtime::EpochHandle,
    types::{
        EncodingScheme, MerkleRoot, NodeId, ProposalHeader, ProposalIndex, ProposalKeyPair, Slot,
        Stake, ValidatorData,
    },
    wire,
};
use crate::spec::DAProposalKeyPair as _;

pub(crate) const SLOT: Slot = Slot(1);
pub(crate) const MESSAGE_LEN: usize = 1500;

pub(crate) fn author() -> NodeId {
    NodeId::dummy(0)
}

pub(crate) fn validator_data(n: u64) -> ValidatorData {
    let validators = (0..n).map(NodeId::dummy).collect::<Vec<_>>();
    let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
    let mapping = validators
        .iter()
        .map(|id| (*id, id.keypair().pubkey()))
        .collect();

    ValidatorData::new(valset, mapping)
}

// the node's view of `num_validators` equal-stake validators, where
// `proposers` propose at their position in every slot
pub(crate) fn epoch_handle_for(
    self_id: NodeId,
    num_validators: u64,
    proposers: Vec<NodeId>,
) -> EpochHandle {
    let num_proposals = proposers.len();
    let validator_data = Arc::new(validator_data(num_validators));
    let election = Arc::new(Proposers::new(proposers));
    EpochHandle {
        self_id,
        num_proposals,
        key_pair: Arc::new(ProposalKeyPair::dummy(self_id)),
        header_auth: Arc::new(header_auth(election, validator_data.clone())),
        validator_data,
    }
}

pub(crate) fn epoch_handle() -> EpochHandle {
    epoch_handle_for(NodeId::dummy(1), 4, vec![author()])
}

// the listed nodes propose at their position, in every slot
pub(crate) struct Proposers(Vec<NodeId>);

impl Proposers {
    pub(crate) fn new(proposers: Vec<NodeId>) -> Self {
        Self(proposers)
    }
}

impl ProposerElection for Proposers {
    fn get_proposer(&self, _slot: Slot, index: ProposalIndex) -> Option<&NodeId> {
        self.0.get(index)
    }

    fn get_index(&self, _slot: Slot, node: &NodeId) -> Option<ProposalIndex> {
        self.0.iter().position(|p| p == node)
    }
}

// the scheme a proposer picks for a MESSAGE_LEN message in the epoch
pub(crate) fn scheme(epoch_handle: &EpochHandle) -> EncodingScheme {
    let num_validators = epoch_handle.validator_data.len();
    let d25 = d25::for_message(MESSAGE_LEN, 0, num_validators).expect("fits a depth");
    EncodingScheme::D25(d25)
}

// the header as the author signs it
pub(crate) fn signed_header(
    slot: Slot,
    scheme: EncodingScheme,
    root: MerkleRoot,
    author_id: u64,
) -> ProposalHeader {
    let signed = wire::signed_bytes(slot, &scheme, &root);
    let sig = ProposalKeyPair::dummy(NodeId::dummy(author_id)).sign(&signed);
    ProposalHeader {
        slot,
        root,
        scheme,
        sig,
    }
}

// the header and every chunk of a complete tree, in wire id order
fn chunks_of(
    tree: &ChunkTree,
    assignment: &ChunkAssignment,
    header: ProposalHeader,
) -> (ProposalHeader, Vec<Chunk<'static>>) {
    let mut chunks = Vec::new();
    for chunk_id in assignment.chunk_ids() {
        let data = tree.chunk_data(chunk_id).expect("complete tree");
        chunks.push(Chunk::new(header.clone(), chunk_id.to_wire(), data));
    }
    (header, chunks)
}

// encode a distinct proposal per payload byte, authored by validator
// `author_id` for `slot`. Under the default fixture: 2 source chunks
// over 3 non-author validators at 2.5x redundancy = 6 chunks, 2 owned
// by each: ids 0 and 3 by validator 1, 1 and 4 by validator 2, 2 and 5
// by validator 3.
pub(crate) fn proposal_chunks_from(
    epoch_handle: &EpochHandle,
    author_id: u64,
    slot: Slot,
    payload: u8,
) -> (ProposalHeader, Vec<Chunk<'static>>) {
    let author = NodeId::dummy(author_id);
    let scheme = scheme(epoch_handle);
    let assignment = scheme.chunk_assignment(&author, &epoch_handle.validator_data);
    let message = vec![payload; MESSAGE_LEN];
    let tree = scheme
        .encode(&message, assignment.num_chunks())
        .expect("a message of the scheme's length");
    let header = signed_header(slot, scheme, tree.root(), author_id);
    chunks_of(&tree, &assignment, header)
}

pub(crate) fn proposal_chunks(
    epoch_handle: &EpochHandle,
    payload: u8,
) -> (ProposalHeader, Vec<Chunk<'static>>) {
    proposal_chunks_from(epoch_handle, 0, SLOT, payload)
}

// a proposal whose chunks all verify under its root but do not decode
// to a message that re-encodes to it: the first half of the symbols
// carry one payload, the second half another
pub(crate) fn inconsistent_proposal_chunks(
    epoch_handle: &EpochHandle,
) -> (ProposalHeader, Vec<Chunk<'static>>) {
    let scheme = scheme(epoch_handle);
    let assignment = scheme.chunk_assignment(&author(), &epoch_handle.validator_data);
    let num_chunks = assignment.num_chunks();
    let symbol_len = wire::symbol_len(scheme.depth());

    let mut symbols = Vec::with_capacity(num_chunks);
    for i in 0..num_chunks {
        let payload = if i < num_chunks / 2 { 1 } else { 2 };
        symbols.push(Bytes::from(vec![payload; symbol_len]));
    }
    let tree = ChunkTree::complete(scheme.depth(), symbols).expect("fits the depth");
    let header = signed_header(SLOT, scheme, tree.root(), 0);
    chunks_of(&tree, &assignment, header)
}

// the verified id of a wire chunk id under the header's assignment
pub(crate) fn chunk_id(
    epoch_handle: &EpochHandle,
    header: &ProposalHeader,
    wire: WireChunkId,
) -> ChunkId {
    let assignment = header
        .scheme
        .chunk_assignment(&author(), &epoch_handle.validator_data);
    assignment
        .resolve_chunk_id(wire)
        .expect("in range")
        .chunk_id()
}

pub(crate) fn group(chunks: &[Chunk<'_>]) -> ProposalEnvelope {
    let mut groups = ProposalEnvelope::group(chunks.iter().cloned());
    let envelope = groups.next().expect("nonempty chunks");
    assert!(groups.next().is_none(), "chunks share a header");
    envelope
}
