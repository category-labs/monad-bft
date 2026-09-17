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

use bytes::{BufMut as _, Bytes, BytesMut};
use monad_mcp_chorus::spec::validator::ValidatorData as _;

use super::{
    super::{
        assignment::{ChunkAssignment, ChunkId, NodeIndex},
        chunk::{ChunkData, ProposalEnvelope},
        chunk_tree::ChunkTree,
        encoding_scheme::{self, DAEncodingScheme as _, d25, swiper},
        runtime::EpochHandle,
        types::{
            EncodingScheme, NodeId, ProposalHeader, ProposalIndex, ProposalKeyPair,
            SignedProposalHeader, Slot,
        },
    },
    PacketLayout as _, header_bytes, signed_bytes,
};
use crate::spec::{DAProposalHeader as _, DAProposalKeyPair as _};

// a proposal this node authored: the signed header over the complete
// tree of its chunks, and the assignment that routes them.
pub struct AssembledProposal {
    header: SignedProposalHeader,
    assignment: ChunkAssignment,
    chunk_tree: ChunkTree,
}

impl AssembledProposal {
    // None unless the message is the length the scheme encodes
    pub fn build_d25(
        epoch_handle: &EpochHandle,
        slot: Slot,
        message: &[u8],
        timestamp: u64,
    ) -> Option<Self> {
        let infer_scheme = || {
            let num_validators = epoch_handle.validator_data.len();
            let d25 = d25::for_message(slot, message.len(), timestamp, num_validators);
            d25.map(EncodingScheme::D25)
        };
        Self::build(epoch_handle, message, infer_scheme)
    }

    pub fn build_s11(
        epoch_handle: &EpochHandle,
        slot: Slot,
        proposer_index: ProposalIndex,
        message: &[u8],
        timestamp: u64,
    ) -> Option<Self> {
        let infer_scheme = || {
            let num_validators = epoch_handle.validator_data.len();
            let msg_len = message.len();
            let s11 = swiper::for_message(slot, msg_len, timestamp, proposer_index, num_validators);
            s11.map(EncodingScheme::S11)
        };
        Self::build(epoch_handle, message, infer_scheme)
    }

    fn build(
        epoch_handle: &EpochHandle,
        message: &[u8],
        infer_scheme: impl FnOnce() -> Option<EncodingScheme>,
    ) -> Option<Self> {
        let scheme = infer_scheme()?;
        let author = &epoch_handle.self_id;
        let valset = &epoch_handle.validator_data;

        let assignment = scheme.chunk_assignment(author, valset);
        let chunk_tree = encoding_scheme::chunk_tree(&scheme, message, assignment.num_chunks())?;
        assert_eq!(assignment.num_chunks(), chunk_tree.len());

        let header = ProposalHeader {
            root: chunk_tree.root(),
            scheme,
        };

        Self::assemble(&epoch_handle.key_pair, header, chunk_tree, assignment)
    }

    pub(crate) fn assemble(
        key_pair: &ProposalKeyPair,
        header: ProposalHeader,
        chunk_tree: ChunkTree,
        assignment: ChunkAssignment,
    ) -> Option<Self> {
        let preimage = signed_bytes(&header);
        let sig = key_pair.sign(&preimage);
        let header = SignedProposalHeader { header, sig };

        Some(Self {
            header,
            assignment,
            chunk_tree,
        })
    }

    pub fn header(&self) -> &SignedProposalHeader {
        &self.header
    }

    pub fn disseminate(&self) -> FirstHopDissemination<'_> {
        let mut chunks = Vec::with_capacity(self.assignment.num_chunks());
        for chunk_id in self.assignment.chunk_ids() {
            let data = self
                .chunk_tree
                .chunk_data(chunk_id)
                .expect("the chunk tree is complete");
            chunks.push((chunk_id, data));
        }

        let header_only = self.assignment.unassigned_nodes().iter().copied().collect();

        FirstHopDissemination {
            header: &self.header,
            header_bytes: header_bytes(&self.header),
            body_len: self.header.scheme().body_len(),
            chunks,
            header_only,
            assignment: &self.assignment,
        }
    }
}

pub struct FirstHopDissemination<'a> {
    header: &'a SignedProposalHeader,
    header_bytes: Bytes,
    body_len: usize,
    chunks: Vec<(ChunkId, ChunkData)>,
    header_only: Vec<NodeIndex>,
    assignment: &'a ChunkAssignment,
}

impl<'a> FirstHopDissemination<'a> {
    // take the chunks owned by a particular node
    pub fn split_off(&mut self, node: &NodeId) -> Option<ProposalEnvelope> {
        let index = self.assignment.index_of(node)?;
        let assignment = self.assignment;
        let owned_by_node = |(chunk_id, _): &mut (ChunkId, ChunkData)| {
            assignment.routing(*chunk_id).owner_index() == index
        };

        let mut envelope = ProposalEnvelope::from_header(self.header.clone());
        for (chunk_id, data) in self.chunks.extract_if(.., owned_by_node) {
            envelope.insert(chunk_id.to_wire(), data);
        }
        self.header_only.retain(|owed| *owed != index);
        Some(envelope)
    }

    pub fn into_packets(self) -> Vec<(&'a NodeId, Bytes)> {
        let num_chunks = self.chunks.len();
        let segment_len = self.header_bytes.len() + self.body_len;
        let mut buffer = BytesMut::with_capacity(segment_len * num_chunks);
        let mut packets = Vec::with_capacity(num_chunks + self.header_only.len());

        let layout = self.header.scheme();
        for (chunk_id, data) in self.chunks {
            let routing = self.assignment.routing(chunk_id);
            let recipient = self.assignment.node(routing.owner_index());

            buffer.put_slice(&self.header_bytes);
            layout.write_body(chunk_id.to_wire(), &data, &mut buffer);
            let packet = buffer.split().freeze();

            packets.push((recipient, packet));
        }

        for node_index in self.header_only {
            let node = self.assignment.node(node_index);
            packets.push((node, self.header_bytes.clone()));
        }

        packets
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        super::{
            super::{
                chunk::ProposalEnvelope,
                test_util::{MESSAGE_LEN, SLOT, author, epoch_handle_for, group, proposal_chunks},
            },
            read_envelope, write_chunk,
        },
        *,
    };

    // the author's view of the default fixture
    fn authoring_handle() -> EpochHandle {
        epoch_handle_for(author(), 4, vec![author()])
    }

    fn by_recipient(packets: Vec<(&NodeId, Bytes)>) -> HashMap<NodeId, Vec<Bytes>> {
        let mut by_recipient: HashMap<NodeId, Vec<Bytes>> = HashMap::new();
        for (node, packet) in packets {
            by_recipient.entry(*node).or_default().push(packet);
        }
        by_recipient
    }

    #[test]
    fn the_author_sends_each_chunk_to_its_owner_as_that_chunks_packet() {
        let epoch_handle = authoring_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let message = vec![1u8; MESSAGE_LEN];

        let proposal = AssembledProposal::build_d25(&epoch_handle, SLOT, &message, 0);
        let proposal = proposal.expect("the fixture's message fits");
        assert_eq!(proposal.header(), &header);
        assert!(AssembledProposal::build_d25(&epoch_handle, SLOT, &[], 0).is_none());

        let dissemination = proposal.disseminate();
        let packets = dissemination.into_packets();
        // one per chunk, then the header alone to the author itself
        assert_eq!(packets.len(), chunks.len() + 1);
        for ((recipient, packet), chunk) in packets.iter().zip(&chunks) {
            let owner = NodeId::dummy(1 + u64::from(chunk.chunk_id()) % 3);
            assert_eq!(**recipient, owner);
            assert_eq!(*packet, write_chunk(chunk));
        }
        let (recipient, packet) = &packets[chunks.len()];
        assert_eq!((**recipient, packet), (author(), &header_bytes(&header)));
    }

    #[test]
    fn splitting_off_a_node_takes_its_envelope_out_of_the_first_hop() {
        let epoch_handle = authoring_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let message = vec![1u8; MESSAGE_LEN];
        let proposal = AssembledProposal::build_d25(&epoch_handle, SLOT, &message, 0);
        let proposal = proposal.expect("the fixture's message fits");
        let mut dissemination = proposal.disseminate();

        // validator 1 owns chunks 0 and 3
        let own = dissemination
            .split_off(&NodeId::dummy(1))
            .expect("in the assignment");
        assert_eq!(own, group(&[chunks[0].clone(), chunks[3].clone()]));
        // the author owns nothing and is owed the header alone
        let authors = dissemination
            .split_off(&author())
            .expect("in the assignment");
        assert_eq!(authors, ProposalEnvelope::from_header(header));
        assert_eq!(dissemination.split_off(&NodeId::dummy(9)), None);

        let packets = by_recipient(dissemination.into_packets());
        assert!(!packets.contains_key(&NodeId::dummy(1)));
        assert!(!packets.contains_key(&author()));
        assert_eq!(packets[&NodeId::dummy(2)].len(), 2);
        assert_eq!(packets[&NodeId::dummy(3)].len(), 2);
    }

    #[test]
    fn a_node_owning_no_chunk_gets_the_header_alone() {
        let epoch_handle = authoring_handle();
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        let holders = [
            (NodeId::dummy(0), 0),
            (NodeId::dummy(1), 3),
            (NodeId::dummy(2), 0),
            (NodeId::dummy(3), 3),
        ];
        let assignment = ChunkAssignment::deal(&author(), holders);
        let message = vec![1u8; MESSAGE_LEN];
        let chunk_tree =
            encoding_scheme::chunk_tree(header.scheme(), &message, assignment.num_chunks())
                .expect("the fixture's message fits");
        let proposal = AssembledProposal::assemble(
            &epoch_handle.key_pair,
            header.header.clone(),
            chunk_tree,
            assignment,
        );
        let proposal = proposal.expect("assembled");
        assert_eq!(proposal.header(), &header);

        let packets = by_recipient(proposal.disseminate().into_packets());
        assert_eq!(packets[&NodeId::dummy(1)].len(), 3);
        assert_eq!(packets[&NodeId::dummy(3)].len(), 3);
        assert_eq!(packets[&author()], vec![header_bytes(&header)]);
        assert_eq!(packets[&NodeId::dummy(2)], vec![header_bytes(&header)]);

        let header_only = read_envelope(packets[&NodeId::dummy(2)][0].clone());
        assert_eq!(header_only, Ok(ProposalEnvelope::from_header(header)));
    }

    #[test]
    fn a_s11_proposal_is_built_under_its_own_scheme() {
        let epoch_handle = authoring_handle();
        let message = vec![1u8; MESSAGE_LEN];

        let proposal = AssembledProposal::build_s11(&epoch_handle, SLOT, 0, &message, 0);
        let proposal = proposal.expect("the message fits");
        assert!(matches!(proposal.header().scheme(), EncodingScheme::S11(_)));

        for (_, packet) in proposal.disseminate().into_packets() {
            let envelope = read_envelope(packet).expect("well-formed");
            assert_eq!(envelope.header(), proposal.header());
        }
    }
}
