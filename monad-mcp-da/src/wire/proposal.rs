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
use monad_mcp_cadence::spec::validator::ValidatorData as _;

#[cfg(feature = "enable_hollow")]
use super::super::encoding_scheme::Hollow;
use super::{
    super::{
        assignment::{ChunkAssignment, ChunkId, NodeIndex},
        chunk::ProposalEnvelope,
        egress::Dissemination,
        encoding_scheme::{DAEncodingScheme, Decoder, Encoded, d33},
        runtime::{EpochHandle, ProposalScheme},
        types::{
            EncodingScheme, NodeId, Proposal, ProposalHeader, ProposalKeyPair, SignedProposalHeader,
        },
    },
    signed_bytes,
};

// a proposal this node authored: the signed header over its encoding,
// and the chunks still on their way to the nodes that own them
pub struct AuthoredProposal {
    header: SignedProposalHeader,
    assignment: ChunkAssignment,
    encoded: Box<dyn Encoded>,
    // the chunks not split off yet
    remaining: Vec<ChunkId>,
    // the nodes that own no chunk, which get the header alone
    header_only: Vec<NodeIndex>,
}

impl AuthoredProposal {
    // None unless the message is of a length the scheme encodes
    pub fn build(
        kind: ProposalScheme,
        epoch_handle: &EpochHandle,
        proposal: &Proposal,
        timestamp: u64,
    ) -> Option<Self> {
        let num_validators = epoch_handle.validator_data.len();
        let msg_len = proposal.payload.len();
        let lane = proposal.lane;
        let d33 = d33::for_message(proposal.slot, msg_len, timestamp, lane, num_validators)?;
        match kind {
            ProposalScheme::D33 => Self::build_under(d33, epoch_handle, &proposal.payload),
            #[cfg(feature = "enable_hollow")]
            ProposalScheme::D33Hollow => {
                let hollow = Hollow::new(d33);
                Self::build_under(hollow, epoch_handle, &proposal.payload)
            }
            #[cfg(feature = "enable_hollow")]
            ProposalScheme::D33HollowInconsistent => {
                let hollow = Hollow::new(d33);
                let encoded = hollow.encode_inconsistent(&proposal.payload)?;
                Some(Self::build_with(hollow, epoch_handle, encoded))
            }
        }
    }

    fn build_under<S>(scheme: S, epoch_handle: &EpochHandle, message: &Bytes) -> Option<Self>
    where
        S: DAEncodingScheme + Into<EncodingScheme>,
        <S::Decoder as Decoder>::Encoded: 'static,
    {
        let author = &epoch_handle.self_id;
        let valset = &epoch_handle.validator_data;
        let num_chunks = scheme.chunk_assignment(author, valset).num_chunks();
        let encoded = scheme.encode(message, num_chunks)?;
        Some(Self::build_with(scheme, epoch_handle, encoded))
    }

    // the proposal of an encoding built under the scheme
    fn build_with<S>(scheme: S, epoch_handle: &EpochHandle, encoded: impl Encoded + 'static) -> Self
    where
        S: DAEncodingScheme + Into<EncodingScheme>,
    {
        let author = &epoch_handle.self_id;
        let valset = &epoch_handle.validator_data;

        let assignment = scheme.chunk_assignment(author, valset);
        let header = ProposalHeader {
            root: encoded.root(),
            scheme: scheme.into(),
        };

        let encoded = Box::new(encoded);
        Self::new(&epoch_handle.key_pair, header, encoded, assignment)
    }

    // signs the header over the encoding
    pub(crate) fn new(
        key_pair: &ProposalKeyPair,
        header: ProposalHeader,
        encoded: Box<dyn Encoded>,
        assignment: ChunkAssignment,
    ) -> Self {
        use crate::spec::DAProposalKeyPair as _;

        let preimage = signed_bytes(&header);
        let sig = key_pair.sign(&preimage);
        let header = SignedProposalHeader { header, sig };

        let remaining = assignment.chunk_ids().collect();
        let header_only = assignment.unassigned_nodes().to_vec();

        Self {
            header,
            assignment,
            encoded,
            remaining,
            header_only,
        }
    }

    pub fn header(&self) -> &SignedProposalHeader {
        &self.header
    }

    // every chunk not split off
    pub fn envelope(&self) -> ProposalEnvelope {
        let mut envelope = ProposalEnvelope::from_header(self.header.clone());
        for chunk_id in &self.remaining {
            let data = self.encoded.chunk_data(*chunk_id);
            envelope.insert(chunk_id.to_wire(), data);
        }
        envelope
    }

    // take the chunks owned by a particular node
    pub fn split_off(&mut self, node: &NodeId) -> Option<ProposalEnvelope> {
        let index = self.assignment.index_of(node)?;
        let assignment = &self.assignment;
        let owned_by_node =
            |chunk_id: &mut ChunkId| assignment.routing(*chunk_id).owner_index() == index;

        let mut envelope = ProposalEnvelope::from_header(self.header.clone());
        for chunk_id in self.remaining.extract_if(.., owned_by_node) {
            let data = self.encoded.chunk_data(chunk_id);
            envelope.insert(chunk_id.to_wire(), data);
        }
        self.header_only.retain(|owed| *owed != index);
        Some(envelope)
    }

    // the first hop: each chunk to its owner, and the header alone to
    // the nodes that own none
    pub fn into_disseminations(self) -> Vec<Dissemination> {
        let mut disseminations = Vec::with_capacity(self.remaining.len() + self.header_only.len());
        for chunk_id in self.remaining {
            let routing = self.assignment.routing(chunk_id);
            let recipient = self.assignment.node(routing.owner_index());

            let data = self.encoded.chunk_data(chunk_id);
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

    use super::{
        super::{
            super::{
                assignment::{Obligation, Obligations},
                chunk::ProposalEnvelope,
                encoding_scheme::DAEncodingScheme as _,
                test_util::{
                    MESSAGE_LEN, PACKET_LOSS_RESISTANCE, SLOT, author, epoch_handle_for, group,
                    proposal_chunks, scheme,
                },
                types::{Lane, Stake},
            },
            header_bytes, read_envelope, write_chunk, write_envelope,
        },
        *,
    };
    // the author's view of the default fixture
    fn authoring_handle() -> EpochHandle {
        epoch_handle_for(author(), 4, vec![author()])
    }

    // the first hop as the runtime puts it on the wire
    fn packets(proposal: AuthoredProposal) -> Vec<(NodeId, Bytes)> {
        let mut packets = Vec::new();
        for Dissemination { to, envelope } in proposal.into_disseminations() {
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

    fn build_d33(epoch_handle: &EpochHandle, proposal: &Proposal) -> Option<AuthoredProposal> {
        AuthoredProposal::build(ProposalScheme::D33, epoch_handle, proposal, 0)
    }

    #[test]
    fn the_author_sends_each_chunk_to_its_owner_as_that_chunks_packet() {
        let epoch_handle = authoring_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let message = vec![1u8; MESSAGE_LEN];

        let proposal = build_d33(&epoch_handle, &proposal_of(message));
        let proposal = proposal.expect("the fixture's message fits");
        assert_eq!(proposal.header(), &header);
        let empty = proposal_of(vec![]);
        assert!(build_d33(&epoch_handle, &empty).is_none());

        assert_eq!(proposal.envelope(), group(&chunks));
        let mut owners = Vec::new();
        for chunk in &chunks {
            let routing = proposal.assignment.resolve_chunk_id(chunk.chunk_id());
            let owner = routing.expect("in the assignment").owner_index();
            owners.push(*proposal.assignment.node(owner));
        }

        let packets = packets(proposal);
        // one per chunk: every validator, the author included, owns chunks
        assert_eq!(packets.len(), chunks.len());
        for (((recipient, packet), chunk), owner) in packets.iter().zip(&chunks).zip(&owners) {
            assert_eq!(recipient, owner);
            assert_eq!(*packet, write_chunk(chunk));
        }
    }

    #[test]
    fn splitting_off_a_node_takes_its_envelope_out_of_the_first_hop() {
        let epoch_handle = authoring_handle();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        let message = vec![1u8; MESSAGE_LEN];
        let proposal = build_d33(&epoch_handle, &proposal_of(message));
        let mut proposal = proposal.expect("the fixture's message fits");

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
        let nodes = [
            NodeId::dummy(1),
            author(),
            NodeId::dummy(2),
            NodeId::dummy(3),
        ];
        let [ones, authors, twos, threes] = nodes.map(owned);

        let split = proposal
            .split_off(&NodeId::dummy(1))
            .expect("in the assignment");
        assert_eq!(split, group(&ones));
        // the author owns chunks too, and splits them off like any owner
        let split = proposal.split_off(&author()).expect("in the assignment");
        assert_eq!(split, group(&authors));
        assert_eq!(proposal.split_off(&NodeId::dummy(9)), None);

        let packets = by_recipient(packets(proposal));
        assert!(!packets.contains_key(&NodeId::dummy(1)));
        assert!(!packets.contains_key(&author()));
        assert_eq!(packets[&NodeId::dummy(2)].len(), twos.len());
        assert_eq!(packets[&NodeId::dummy(3)].len(), threes.len());
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
        let message = Bytes::from(vec![1u8; MESSAGE_LEN]);
        let encoded = scheme(&epoch_handle).encode(&message, assignment.num_chunks());
        let encoded = encoded.expect("the fixture's message fits");
        let proposal = AuthoredProposal::new(
            &epoch_handle.key_pair,
            header.header.clone(),
            Box::new(encoded),
            assignment,
        );
        assert_eq!(proposal.header(), &header);

        let packets = by_recipient(packets(proposal));
        assert_eq!(packets[&NodeId::dummy(1)].len(), 3);
        assert_eq!(packets[&NodeId::dummy(3)].len(), 3);
        assert_eq!(packets[&author()], vec![header_bytes(&header)]);
        assert_eq!(packets[&NodeId::dummy(2)], vec![header_bytes(&header)]);

        let header_only = read_envelope(packets[&NodeId::dummy(2)][0].clone());
        assert_eq!(header_only, Ok(ProposalEnvelope::from_header(header)));
    }

    // the author is the first holder of a hollow message, until an
    // instance holds it
    #[cfg(feature = "enable_hollow")]
    #[test]
    fn an_authored_hollow_proposal_keeps_its_message_decodable() {
        use super::super::super::encoding_scheme::instantiate;

        let epoch_handle = authoring_handle();
        // a payload no other test publishes
        let message = vec![0x51u8; MESSAGE_LEN];
        let proposal = proposal_of(message.clone());
        let proposal =
            AuthoredProposal::build(ProposalScheme::D33Hollow, &epoch_handle, &proposal, 0);
        let proposal = proposal.expect("the fixture's message fits");

        let validator_data = &epoch_handle.validator_data;
        let header = &proposal.header().header;
        let (assignment, mut chunks) = instantiate(header, &author(), validator_data);
        let (_, envelope) = proposal.envelope().into_parts();
        for (wire_chunk_id, data) in envelope {
            let routing = assignment.resolve_chunk_id(wire_chunk_id);
            let chunk_id = routing.expect("in the assignment").chunk_id();
            assert_eq!(chunks.ingest(chunk_id, data), Ok(true));
        }
        assert_eq!(chunks.try_resolve(), Some(Ok(Bytes::from(message))));
    }

    // a Byzantine author's inconsistent hollow proposal: every chunk is
    // valid under the declared root, and the proposal resolves invalid
    #[cfg(feature = "enable_hollow")]
    #[test]
    fn an_inconsistent_hollow_proposal_resolves_invalid() {
        use super::super::super::encoding_scheme::{InvalidEncoding, instantiate};

        let epoch_handle = authoring_handle();
        // a payload no other test publishes
        let proposal = proposal_of(vec![0x52u8; MESSAGE_LEN]);
        let scheme = ProposalScheme::D33HollowInconsistent;
        let proposal = AuthoredProposal::build(scheme, &epoch_handle, &proposal, 0);
        let proposal = proposal.expect("the fixture's message fits");

        let validator_data = &epoch_handle.validator_data;
        let header = &proposal.header().header;
        let (assignment, mut chunks) = instantiate(header, &author(), validator_data);
        let (_, envelope) = proposal.envelope().into_parts();
        let mut resolved = Vec::new();
        for (wire_chunk_id, data) in envelope {
            let routing = assignment.resolve_chunk_id(wire_chunk_id);
            let chunk_id = routing.expect("in the assignment").chunk_id();
            // each chunk is new until the proposal resolves, then none is
            let was_resolved = chunks.is_resolved();
            assert_eq!(chunks.ingest(chunk_id, data), Ok(!was_resolved));
            resolved.extend(chunks.try_resolve());
        }
        assert_eq!(resolved, [Err(InvalidEncoding)]);
    }
}
