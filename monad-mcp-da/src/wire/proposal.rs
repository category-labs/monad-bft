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

use monad_mcp_cadence::spec::validator::ValidatorData as _;

use super::{
    super::{
        assignment::{ChunkAssignment, ChunkId, NodeIndex},
        chunk::{ChunkData, ProposalEnvelope},
        egress::Dissemination,
        encoding_scheme::{ProposalChunks, d33},
        runtime::EpochHandle,
        types::{
            EncodingScheme, NodeId, Proposal, ProposalHeader, ProposalKeyPair, SignedProposalHeader,
        },
    },
    signed_bytes,
};

// a proposal this node authored: the signed header over its complete
// chunks, and the assignment that routes them.
pub struct AssembledProposal {
    header: SignedProposalHeader,
    assignment: ChunkAssignment,
    chunks: Box<dyn ProposalChunks>,
}

impl AssembledProposal {
    // None unless the message is the length the scheme encodes
    pub fn build_d33(
        epoch_handle: &EpochHandle,
        proposal: &Proposal,
        timestamp: u64,
    ) -> Option<Self> {
        let num_validators = epoch_handle.validator_data.len();
        let msg_len = proposal.payload.len();
        let lane = proposal.lane;
        let d33 = d33::for_message(proposal.slot, msg_len, timestamp, lane, num_validators)?;
        let scheme = EncodingScheme::D33(d33);
        Self::build(epoch_handle, &proposal.payload, scheme)
    }

    fn build(epoch_handle: &EpochHandle, message: &[u8], scheme: EncodingScheme) -> Option<Self> {
        use super::super::encoding_scheme::DAEncodingScheme as _;

        let author = &epoch_handle.self_id;
        let valset = &epoch_handle.validator_data;

        let assignment = scheme.chunk_assignment(author, valset);
        let chunks = scheme.encode(message, assignment.num_chunks())?;

        let header = ProposalHeader {
            root: chunks.root(),
            scheme,
        };

        let proposal = Self::assemble(&epoch_handle.key_pair, header, chunks, assignment);
        Some(proposal)
    }

    pub(crate) fn assemble(
        key_pair: &ProposalKeyPair,
        header: ProposalHeader,
        chunks: Box<dyn ProposalChunks>,
        assignment: ChunkAssignment,
    ) -> Self {
        use crate::spec::DAProposalKeyPair as _;

        let preimage = signed_bytes(&header);
        let sig = key_pair.sign(&preimage);
        let header = SignedProposalHeader { header, sig };

        Self {
            header,
            assignment,
            chunks,
        }
    }

    pub fn header(&self) -> &SignedProposalHeader {
        &self.header
    }

    pub fn disseminate(&self) -> FirstHopDissemination<'_> {
        let mut chunks = Vec::with_capacity(self.assignment.num_chunks());
        for chunk_id in self.assignment.chunk_ids() {
            let data = self
                .chunks
                .chunk_data(chunk_id)
                .expect("the encoded chunks are complete");
            chunks.push((chunk_id, data));
        }

        let header_only = self.assignment.unassigned_nodes().to_vec();

        FirstHopDissemination {
            header: &self.header,
            chunks,
            header_only,
            assignment: &self.assignment,
        }
    }
}

pub struct FirstHopDissemination<'a> {
    header: &'a SignedProposalHeader,
    chunks: Vec<(ChunkId, ChunkData)>,
    header_only: Vec<NodeIndex>,
    assignment: &'a ChunkAssignment,
}

impl<'a> FirstHopDissemination<'a> {
    pub fn envelope(&self) -> ProposalEnvelope {
        let mut envelope = ProposalEnvelope::from_header(self.header.clone());
        for (chunk_id, data) in &self.chunks {
            envelope.insert(chunk_id.to_wire(), data.clone());
        }
        envelope
    }

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

    pub fn into_disseminations(self) -> Vec<Dissemination> {
        let mut disseminations = Vec::with_capacity(self.chunks.len() + self.header_only.len());
        for (chunk_id, data) in self.chunks {
            let routing = self.assignment.routing(chunk_id);
            let recipient = self.assignment.node(routing.owner_index());

            let mut envelope = ProposalEnvelope::from_header(self.header.clone());
            envelope.insert(chunk_id.to_wire(), data);
            let to = [*recipient].into();
            disseminations.push(Dissemination { to, envelope });
        }

        for node_index in self.header_only {
            let node = self.assignment.node(node_index);
            let envelope = ProposalEnvelope::from_header(self.header.clone());
            let to = [*node].into();
            disseminations.push(Dissemination { to, envelope });
        }

        disseminations
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;

    use super::{
        super::{
            super::{
                assignment::{Obligation, Obligations},
                chunk::ProposalEnvelope,
                encoding_scheme::DAEncodingScheme as _,
                test_util::{
                    MESSAGE_LEN, PACKET_LOSS_RESISTANCE, SLOT, author, epoch_handle_for, group,
                    proposal_chunks,
                },
                types::{Lane, Stake},
            },
            header_bytes, read_envelope, write_chunk, write_envelope,
        },
        *,
    };
    use crate::spec::DAProposalHeader as _;

    // the author's view of the default fixture
    fn authoring_handle() -> EpochHandle {
        epoch_handle_for(author(), 4, vec![author()])
    }

    // the first hop as the runtime puts it on the wire
    fn packets(first_hop: FirstHopDissemination<'_>) -> Vec<(NodeId, Bytes)> {
        let mut packets = Vec::new();
        for Dissemination { to, envelope } in first_hop.into_disseminations() {
            for node in to {
                for packet in write_envelope(&envelope) {
                    packets.push((node, packet));
                }
            }
        }
        packets
    }

    fn by_recipient(packets: Vec<(NodeId, Bytes)>) -> HashMap<NodeId, Vec<Bytes>> {
        let mut by_recipient: HashMap<NodeId, Vec<Bytes>> = HashMap::new();
        for (node, packet) in packets {
            by_recipient.entry(node).or_default().push(packet);
        }
        by_recipient
    }

    fn proposal_of(payload: Vec<u8>) -> Proposal {
        Proposal {
            slot: SLOT,
            lane: Lane::new_unchecked(0),
            payload: payload.into(),
        }
    }

    #[test]
    fn the_author_sends_each_chunk_to_its_owner_as_that_chunks_packet() {
        let epoch_handle = authoring_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let message = vec![1u8; MESSAGE_LEN];

        let proposal = AssembledProposal::build_d33(&epoch_handle, &proposal_of(message), 0);
        let proposal = proposal.expect("the fixture's message fits");
        assert_eq!(proposal.header(), &header);
        let empty = proposal_of(vec![]);
        assert!(AssembledProposal::build_d33(&epoch_handle, &empty, 0).is_none());

        let dissemination = proposal.disseminate();
        assert_eq!(dissemination.envelope(), group(&chunks));
        let packets = packets(dissemination);
        // one per chunk: every validator, the author included, owns chunks
        assert_eq!(packets.len(), chunks.len());
        for ((recipient, packet), chunk) in packets.iter().zip(&chunks) {
            let routing = proposal.assignment.resolve_chunk_id(chunk.chunk_id());
            let owner = routing.expect("in the assignment").owner_index();
            assert_eq!(recipient, proposal.assignment.node(owner));
            assert_eq!(*packet, write_chunk(chunk));
        }
    }

    #[test]
    fn splitting_off_a_node_takes_its_envelope_out_of_the_first_hop() {
        let epoch_handle = authoring_handle();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        let message = vec![1u8; MESSAGE_LEN];
        let proposal = AssembledProposal::build_d33(&epoch_handle, &proposal_of(message), 0);
        let proposal = proposal.expect("the fixture's message fits");
        let mut dissemination = proposal.disseminate();

        // the fixture's chunks a node owns; the chunks are in wire id order
        let assignment = &proposal.assignment;
        let owned = |node: NodeId| {
            let index = assignment.index_of(&node).expect("in the assignment");
            let mut owned = Vec::new();
            for routing in assignment.owned_chunks(index) {
                let id = routing.chunk_id().to_wire();
                owned.push(chunks[usize::from(id)].clone());
            }
            owned
        };

        let own = dissemination
            .split_off(&NodeId::dummy(1))
            .expect("in the assignment");
        assert_eq!(own, group(&owned(NodeId::dummy(1))));
        // the author owns chunks too, and splits them off like any owner
        let authors = dissemination
            .split_off(&author())
            .expect("in the assignment");
        assert_eq!(authors, group(&owned(author())));
        assert_eq!(dissemination.split_off(&NodeId::dummy(9)), None);

        let packets = by_recipient(packets(dissemination));
        assert!(!packets.contains_key(&NodeId::dummy(1)));
        assert!(!packets.contains_key(&author()));
        for node in [NodeId::dummy(2), NodeId::dummy(3)] {
            assert_eq!(packets[&node].len(), owned(node).len());
        }
    }

    #[test]
    fn a_node_owning_no_chunk_gets_the_header_alone() {
        use monad_mcp_common::spec::Stake as _;

        let epoch_handle = authoring_handle();
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        let whole = |whole| Obligation {
            whole,
            partial: Stake::ZERO,
        };
        let holders = vec![
            (NodeId::dummy(0), whole(0)),
            (NodeId::dummy(1), whole(3)),
            (NodeId::dummy(2), whole(0)),
            (NodeId::dummy(3), whole(3)),
        ];
        let obligations = Obligations::new(Stake::from(4), holders);
        let assignment = ChunkAssignment::deal(&author(), obligations, PACKET_LOSS_RESISTANCE);
        let message = vec![1u8; MESSAGE_LEN];
        let chunks = header.scheme().encode(&message, assignment.num_chunks());
        let chunks = chunks.expect("the fixture's message fits");
        let proposal = AssembledProposal::assemble(
            &epoch_handle.key_pair,
            header.header.clone(),
            chunks,
            assignment,
        );
        assert_eq!(proposal.header(), &header);

        let packets = by_recipient(packets(proposal.disseminate()));
        assert_eq!(packets[&NodeId::dummy(1)].len(), 3);
        assert_eq!(packets[&NodeId::dummy(3)].len(), 3);
        assert_eq!(packets[&author()], vec![header_bytes(&header)]);
        assert_eq!(packets[&NodeId::dummy(2)], vec![header_bytes(&header)]);

        let header_only = read_envelope(packets[&NodeId::dummy(2)][0].clone());
        assert_eq!(header_only, Ok(ProposalEnvelope::from_header(header)));
    }
}
