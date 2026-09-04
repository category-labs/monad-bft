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
    assignment::ChunkAssignment,
    chorus::env::{D25, EncodingScheme, ProposalSignature},
    chunk::{Chunk, ProposalEnvelope},
    chunk_tree::ChunkTree,
    election::ProposerElection,
    encoding_scheme::DAEncodingScheme as _,
    layout::PacketLayout,
    runtime::EpochHandle,
    types::{
        HeaderAuth, NodeId, ProposalHeader, ProposalIndex, ProposalKeyPair, Slot, Stake,
        ValidatorData,
    },
};

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
    let proposer_index =
        move |_slot: u64, signer: &NodeId| proposers.iter().position(|p| p == signer);
    EpochHandle {
        self_id,
        num_proposals,
        key_pair: Arc::new(ProposalKeyPair::dummy(self_id)),
        header_auth: Arc::new(HeaderAuth::new(proposer_index)),
        validator_data: Arc::new(validator_data(num_validators)),
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

fn scheme() -> EncodingScheme {
    EncodingScheme::D25(D25 {
        msg_len: MESSAGE_LEN,
        unix_ts: 0,
    })
}

fn layout_and_assignment(
    epoch_handle: &EpochHandle,
    author: &NodeId,
) -> (PacketLayout, ChunkAssignment) {
    let validator_data = &epoch_handle.validator_data;
    let layout = scheme()
        .packet_layout(validator_data.len())
        .expect("fits a layout");
    let assignment = scheme().chunk_assignment(&layout, author, validator_data);
    (layout, assignment)
}

// the header and every chunk of a complete tree, in wire id order
fn chunks_of(
    tree: &ChunkTree,
    assignment: &ChunkAssignment,
    slot: Slot,
    author_id: u64,
) -> (ProposalHeader, Vec<Chunk>) {
    let header = ProposalHeader {
        slot,
        root: tree.root(),
        sig: ProposalSignature(author_id),
        scheme: scheme(),
    };

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
) -> (ProposalHeader, Vec<Chunk>) {
    let author = NodeId::dummy(author_id);
    let (layout, assignment) = layout_and_assignment(epoch_handle, &author);
    let message = vec![payload; MESSAGE_LEN];
    let tree = scheme()
        .encode(&message, layout, assignment.num_chunks())
        .expect("valid encoding parameters");
    chunks_of(&tree, &assignment, slot, author_id)
}

pub(crate) fn proposal_chunks(
    epoch_handle: &EpochHandle,
    payload: u8,
) -> (ProposalHeader, Vec<Chunk>) {
    proposal_chunks_from(epoch_handle, 0, SLOT, payload)
}

// a proposal whose chunks all verify under its root but do not decode
// to a message that re-encodes to it: the first half of the symbols
// carry one payload, the second half another
pub(crate) fn inconsistent_proposal_chunks(
    epoch_handle: &EpochHandle,
) -> (ProposalHeader, Vec<Chunk>) {
    let (layout, assignment) = layout_and_assignment(epoch_handle, &author());
    let num_chunks = assignment.num_chunks();

    let mut symbols = Vec::with_capacity(num_chunks);
    for i in 0..num_chunks {
        let payload = if i < num_chunks / 2 { 1 } else { 2 };
        symbols.push(Bytes::from(vec![payload; MESSAGE_LEN]));
    }
    let tree = ChunkTree::complete(&layout, symbols).expect("fits the layout");
    chunks_of(&tree, &assignment, SLOT, 0)
}

pub(crate) fn group(chunks: &[Chunk]) -> ProposalEnvelope {
    let mut groups = ProposalEnvelope::group(chunks.iter().cloned());
    let (header, chunks) = groups.next().expect("nonempty chunks");
    assert!(groups.next().is_none(), "chunks share a header");
    ProposalEnvelope::new(header, chunks)
}
