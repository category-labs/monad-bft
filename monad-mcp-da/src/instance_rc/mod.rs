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

mod obligation_tracker;
mod serving_tracker;

use std::collections::BTreeSet;

use bytes::Bytes;
use monad_mcp_chorus::spec::ProposalHeader as _;
use obligation_tracker::ObligationTracker;
use serving_tracker::ServingTracker;

use super::{
    assignment::{ChunkAssignment, ChunkId, NodeIndex},
    chunk::{ChunkData, ChunkRequest, ChunksSubset, WireChunkId},
    chunk_tree::ChunkTree,
    egress::ChunkEgress,
    encoding_scheme::{self, DAEncodingScheme as _, SymbolDecoder},
    runtime::EpochHandle,
    types::{
        EncodingScheme, MerkleRoot, NodeId, ProposalDAEvent, SignedProposalHeader, Timestamp,
        TimestampDelta,
    },
    util::{Tree, shuffle_sends},
};
use crate::spec::DAProposalHeader as _;

// by owner, each owner once, in assignment (or validator) order; the requests
// built from it are shuffled before they go out
pub(crate) type MissingChunks = Vec<(NodeId, ChunksSubset)>;

// decoded proposal does not reencode back to root
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidEncoding;

pub type Resolved = Result<Bytes, InvalidEncoding>;

#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(clippy::enum_variant_names)]
pub(crate) enum InvalidChunk {
    InvalidChunkId,
    UnexpectedSender,
    BadProof,
}

enum DecodingState {
    Decoding(Box<dyn SymbolDecoder>),
    Decoded,
    // the decoded message does not re-encode to the root
    Invalid,
}

// a tracked root: its chunks, their decoding, and the rebroadcast of our owned chunks
pub(crate) struct RaptorcastInstance {
    header: SignedProposalHeader,
    assignment: ChunkAssignment,
    // None when this node is outside the assignment (e.g. a full node)
    self_index: Option<NodeIndex>,
    // every node of the assignment, in the order we send to them
    send_order: Vec<NodeIndex>,
    decoding_threshold: usize,

    chunks: ChunkTree,
    obligation: ObligationTracker,
    serving: ServingTracker,

    pending_rebroadcast: BTreeSet<ChunkId>,
    state: DecodingState,
}

impl RaptorcastInstance {
    pub(crate) fn new(
        header: SignedProposalHeader,
        epoch_handle: &EpochHandle,
        author: &NodeId,
        serve_window: TimestampDelta,
    ) -> Self {
        let scheme = header.scheme();
        let assignment = scheme.chunk_assignment(author, &epoch_handle.validator_data);
        let self_index = assignment.index_of(&epoch_handle.self_id);
        let mut send_order: Vec<NodeIndex> = assignment.nodes().map(|(index, _)| index).collect();
        shuffle_sends(&mut send_order, &header.pid(), &epoch_handle.self_id);
        let num_chunks = assignment.num_chunks();
        let decoding_threshold = scheme.num_source_chunks();
        assert!(0 < decoding_threshold && decoding_threshold < num_chunks);
        let decoder = scheme.decoder(num_chunks);
        let obligation = ObligationTracker::new(&assignment, self_index);
        let serving = ServingTracker::new(&assignment, self_index, serve_window);

        Self {
            chunks: ChunkTree::new(*header.root()),
            header,
            assignment,
            self_index,
            send_order,
            decoding_threshold,
            obligation,
            serving,
            pending_rebroadcast: BTreeSet::new(),
            state: DecodingState::Decoding(decoder),
        }
    }

    fn accept(&self, chunk_id: WireChunkId, sender: &NodeId) -> Result<ChunkId, InvalidChunk> {
        let Some(routing) = self.assignment.resolve_chunk_id(chunk_id) else {
            return Err(InvalidChunk::InvalidChunkId);
        };
        let owner = routing.owner_index();
        let from_author = sender == self.assignment.node(self.assignment.author());
        let from_owner = sender == self.assignment.node(owner);
        let to_owner = self.self_index == Some(owner);
        if !(from_author || from_owner || to_owner) {
            return Err(InvalidChunk::UnexpectedSender);
        }
        Ok(routing.chunk_id())
    }

    pub(crate) fn ingest(
        &mut self,
        wire_chunk_id: WireChunkId,
        data: ChunkData,
        sender: &NodeId,
    ) -> Result<(), InvalidChunk> {
        let chunk_id = self.accept(wire_chunk_id, sender)?;
        let DecodingState::Decoding(decoder) = &mut self.state else {
            return Ok(());
        };
        if self.chunks.contains(chunk_id) {
            return Ok(());
        }

        // proof verification
        let root = self.chunks.root();
        if !verify_proof(chunk_id, &data, self.header.scheme(), &root) {
            return Err(InvalidChunk::BadProof);
        }

        // storing & decoding
        decoder.ingest(chunk_id, &data.symbol);
        self.chunks.insert(chunk_id, data);

        // obligation tracking
        let Some(self_index) = self.self_index else {
            return Ok(());
        };
        let routing = self.assignment.routing(chunk_id);
        self.obligation.mark(&routing, self_index);

        // rebroadcasting
        if routing.owner_index() == self_index {
            self.pending_rebroadcast.insert(chunk_id);
        }
        Ok(())
    }

    // once our owned chunks from the author are complete. Consensus waits only on that
    pub(crate) fn take_obligation_event(&mut self) -> Option<ProposalDAEvent> {
        if !self.obligation.take_fulfilled() {
            return None;
        }
        Some(ProposalDAEvent::ProposerObligationFulfilled(
            self.header.pid(),
        ))
    }

    pub(crate) fn try_decode(&mut self) -> Option<Resolved> {
        let DecodingState::Decoding(decoder) = &mut self.state else {
            return None;
        };
        if self.chunks.len() < self.decoding_threshold {
            return None;
        }
        // todo: None after enough chunks means the codec doesn't
        // guarantee decodability (e.g. r10).
        let message = decoder.try_decode()?;

        let Some(tree) = self.reencoded_tree(&message) else {
            self.state = DecodingState::Invalid;
            return Some(Err(InvalidEncoding));
        };
        self.queue_unreceived_chunks();
        self.chunks = tree;
        self.state = DecodingState::Decoded;
        Some(Ok(message))
    }

    // only our own chunks are queued, so the routing's owner is us
    pub(crate) fn rebroadcast_chunks(&mut self, egress: &mut ChunkEgress) {
        while let Some(chunk_id) = self.pending_rebroadcast.pop_first() {
            let routing = self.assignment.routing(chunk_id);
            let to = routing.rebroadcast_targets(&self.send_order);
            self.enqueue(chunk_id, to, egress);
        }
    }

    // None once resolved, or outside the assignment, whose owners serve only its nodes
    pub(crate) fn missing_chunks(&self) -> Option<MissingChunks> {
        let resolved = !matches!(self.state, DecodingState::Decoding(_));
        if resolved || self.self_index.is_none() {
            return None;
        }

        // by owner, indexed by NodeIndex
        let mut by_owner: Vec<Vec<ChunkId>> = vec![Vec::new(); self.assignment.num_nodes()];
        for chunk_id in self.assignment.chunk_ids() {
            if self.chunks.contains(chunk_id) {
                continue;
            }
            let owner = self.assignment.routing(chunk_id).owner_index();
            by_owner[usize::from(owner)].push(chunk_id);
        }

        let missing = self
            .assignment
            .nodes()
            .zip(by_owner)
            .filter(|(_, chunk_ids)| !chunk_ids.is_empty())
            .map(|((_, owner), chunk_ids)| (*owner, ChunksSubset::narrowed(chunk_ids)))
            .collect();
        Some(missing)
    }

    // serve the requester the requested chunks we hold
    pub(crate) fn handle_chunk_request(
        &mut self,
        request: ChunkRequest,
        requester: &NodeId,
        now: Timestamp,
        egress: &mut ChunkEgress,
    ) {
        let Some(requester_index) = self.assignment.index_of(requester) else {
            // not a validator: the assignment lists them all
            return;
        };
        // a header counts as one chunk
        if request.subset == ChunksSubset::HeaderOnly {
            if self.serving.grant(requester_index, 1, now) == 1 {
                egress.enqueue_header(vec![*requester], &self.header);
            }
            return;
        }

        let mut held = self.requested_chunks(request, requester_index);
        held.retain(|chunk_id| self.chunks.contains(*chunk_id));
        let granted = self.serving.grant(requester_index, held.len(), now);
        held.truncate(granted);
        for chunk_id in held {
            self.enqueue(chunk_id, vec![*requester], egress);
        }
    }

    // our chunks never received become derivable once decoded
    fn queue_unreceived_chunks(&mut self) {
        let Some(self_index) = self.self_index else {
            return;
        };
        for routing in self.assignment.owned_chunks(self_index) {
            let chunk_id = routing.chunk_id();
            if !self.chunks.contains(chunk_id) {
                self.pending_rebroadcast.insert(chunk_id);
            }
        }
    }

    // the caller must ensure the chunk is held
    fn enqueue(&self, chunk_id: ChunkId, to: Vec<NodeId>, egress: &mut ChunkEgress) {
        let data = self
            .chunks
            .chunk_data(chunk_id)
            .expect("caller ensures the chunk is held");
        egress.enqueue(to, &self.header, chunk_id.to_wire(), data);
    }

    // None unless the message re-encodes to our root
    fn reencoded_tree(&self, message: &[u8]) -> Option<ChunkTree> {
        let num_chunks = self.assignment.num_chunks();
        let tree = encoding_scheme::chunk_tree(self.header.scheme(), message, num_chunks)?;
        if tree.root() != *self.header.root() {
            return None;
        }
        Some(tree)
    }

    // the chunks a recovery request resolves to. Out-of-range ids are
    // dropped.
    fn requested_chunks(&self, request: ChunkRequest, requester: NodeIndex) -> Vec<ChunkId> {
        let ChunkRequest { kind, subset, .. } = request;
        // outside the assignment we own nothing
        let Some(owner) = kind.owner(Some(requester), self.self_index) else {
            return vec![];
        };

        let owned = self
            .assignment
            .owned_chunks(owner)
            .map(|routing| routing.chunk_id());
        subset.restrict(owned).collect()
    }
}

// whether the chunk's proof binds it to the root
fn verify_proof(
    chunk_id: ChunkId,
    data: &ChunkData,
    scheme: &EncodingScheme,
    root: &MerkleRoot,
) -> bool {
    use super::wire::PacketLayout as _;

    debug_assert_eq!(data.symbol.len(), scheme.symbol_len());
    let leaf_idx = chunk_id.to_wire();
    let leaf = scheme.leaf_hash(leaf_idx, &data.symbol);
    Tree::verify_proof(root, leaf_idx, &leaf, &data.proof)
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            chunk::{Chunk, ChunkRequestType},
            test_util::{
                MESSAGE_LEN, NOW, author, epoch_handle, epoch_handle_for,
                inconsistent_proposal_chunks, proposal_chunks, timing,
            },
        },
        *,
    };

    fn instance(header: &SignedProposalHeader, epoch_handle: &EpochHandle) -> RaptorcastInstance {
        RaptorcastInstance::new(
            header.clone(),
            epoch_handle,
            &author(),
            timing().serve_window,
        )
    }

    fn released() -> ChunkEgress {
        let mut egress = ChunkEgress::new();
        egress.release();
        egress
    }

    // one chunk from the author, handled as the proposer does
    fn ingest(
        instance: &mut RaptorcastInstance,
        chunk: &Chunk<'_>,
        egress: &mut ChunkEgress,
    ) -> Result<Option<Resolved>, InvalidChunk> {
        let (_, wire_chunk_id, data) = chunk.clone().into_parts();
        instance.ingest(wire_chunk_id, data, &author())?;
        let decoded = instance.try_decode();
        instance.rebroadcast_chunks(egress);
        Ok(decoded)
    }

    fn message() -> Bytes {
        Bytes::from(vec![1u8; MESSAGE_LEN])
    }

    // the wire ids in the drained messages, sorted
    fn sent(egress: &mut ChunkEgress) -> Vec<WireChunkId> {
        let mut ids = Vec::new();
        for message in egress.drain() {
            ids.extend(message.envelope.chunk_data().keys().copied());
        }
        ids.sort();
        ids
    }

    #[test]
    fn a_peer_may_send_only_its_own_chunks_or_ours() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let instance = instance(&header, &epoch_handle);
        let from = |sender: u64, chunk: &Chunk<'_>| {
            let (_, chunk_id, _) = chunk.clone().into_parts();
            instance.accept(chunk_id, &NodeId::dummy(sender))
        };

        // we are 1 and own 0 and 3; node 2 owns 1 and 4; node 3 owns 2 and 5
        assert_eq!(from(2, &chunks[2]), Err(InvalidChunk::UnexpectedSender));
        assert!(from(2, &chunks[1]).is_ok());
        assert!(from(3, &chunks[0]).is_ok());

        // the proposer may send any chunk
        assert!(from(0, &chunks[2]).is_ok());
    }

    #[test]
    fn malformed_chunks_are_rejected() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&header, &epoch_handle);
        let (_, _, data) = chunks[0].clone().into_parts();

        let out_of_range = instance.ingest(99, data.clone(), &author());
        assert_eq!(out_of_range, Err(InvalidChunk::InvalidChunkId));

        let mut symbol = data.symbol.to_vec();
        symbol[0] ^= 1;
        let tampered = ChunkData {
            symbol: Bytes::from(symbol),
            proof: data.proof.clone(),
        };
        let tampered = instance.ingest(0, tampered, &author());
        assert_eq!(tampered, Err(InvalidChunk::BadProof));

        // a good chunk under the wrong id fails its proof
        let misplaced = instance.ingest(1, data, &author());
        assert_eq!(misplaced, Err(InvalidChunk::BadProof));
    }

    #[test]
    fn a_repeated_chunk_counts_and_forwards_once() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&header, &epoch_handle);
        let mut egress = released();

        // chunk 0 is ours: forwarded on arrival, not on the repeat
        assert_eq!(ingest(&mut instance, &chunks[0], &mut egress), Ok(None));
        assert_eq!(sent(&mut egress), [0]);
        assert_eq!(ingest(&mut instance, &chunks[0], &mut egress), Ok(None));
        assert!(sent(&mut egress).is_empty());

        // the repeat did not count towards decoding: one more distinct
        // chunk is short, the next one decodes
        assert_eq!(ingest(&mut instance, &chunks[1], &mut egress), Ok(None));
        let decoded = ingest(&mut instance, &chunks[2], &mut egress);
        assert_eq!(decoded, Ok(Some(Ok(message()))));
    }

    #[test]
    fn inconsistent_chunks_resolve_invalid() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = inconsistent_proposal_chunks(&epoch_handle);
        let mut instance = instance(&header, &epoch_handle);
        let mut egress = released();

        assert_eq!(ingest(&mut instance, &chunks[0], &mut egress), Ok(None));
        assert_eq!(ingest(&mut instance, &chunks[1], &mut egress), Ok(None));
        let decoded = ingest(&mut instance, &chunks[2], &mut egress);
        assert_eq!(decoded, Ok(Some(Err(InvalidEncoding))));

        assert!(instance.missing_chunks().is_none());
        // later chunks are ignored
        assert_eq!(ingest(&mut instance, &chunks[4], &mut egress), Ok(None));

        // our chunk 0 went out on arrival; nothing is derivable from an
        // invalid proposal
        assert_eq!(sent(&mut egress), [0]);
    }

    #[test]
    fn our_obligation_from_the_author_completes_with_our_last_chunk() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&header, &epoch_handle);
        let mut egress = released();

        assert!(instance.take_obligation_event().is_none());

        // we own 0 and 3
        ingest(&mut instance, &chunks[0], &mut egress).expect("valid");
        assert!(instance.take_obligation_event().is_none());
        ingest(&mut instance, &chunks[3], &mut egress).expect("valid");
        assert_eq!(
            instance.take_obligation_event(),
            Some(ProposalDAEvent::ProposerObligationFulfilled(header.pid()))
        );

        // node 2 owns 1 and 4: one of them settles nothing
        ingest(&mut instance, &chunks[1], &mut egress).expect("valid");
        assert!(instance.take_obligation_event().is_none());
    }

    #[test]
    fn outside_the_assignment_decodes_without_owning_anything() {
        // validator 9 is not in the validator set
        let epoch_handle = epoch_handle_for(NodeId::dummy(9), 4, vec![author()]);
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&header, &epoch_handle);
        let mut egress = released();

        // owners serve only nodes in the assignment, so nothing is pulled
        assert!(instance.missing_chunks().is_none());

        let mut decoded = None;
        for chunk in &chunks[..3] {
            decoded = ingest(&mut instance, chunk, &mut egress).expect("valid");
        }
        assert_eq!(decoded, Some(Ok(message())));
        // nothing owned, nothing forwarded
        assert!(sent(&mut egress).is_empty());

        // nothing of ours to serve, but once decoded theirs is derivable
        let requester = NodeId::dummy(3);
        let root = header.pid();
        let your_chunks = ChunkRequest::all(root, ChunkRequestType::YourChunks);
        instance.handle_chunk_request(your_chunks, &requester, NOW, &mut egress);
        assert!(sent(&mut egress).is_empty());
        let my_chunks = ChunkRequest::all(root, ChunkRequestType::MyChunks);
        instance.handle_chunk_request(my_chunks, &requester, NOW, &mut egress);
        assert_eq!(sent(&mut egress), [2, 5]);
    }

    #[test]
    fn a_header_only_request_is_answered_for_validators_alone() {
        let epoch_handle = epoch_handle();
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&header, &epoch_handle);
        let mut egress = released();
        let request = ChunkRequest {
            pid: header.pid(),
            kind: ChunkRequestType::YourChunks,
            subset: ChunksSubset::HeaderOnly,
        };

        // validator 9 is not in the validator set
        instance.handle_chunk_request(request.clone(), &NodeId::dummy(9), NOW, &mut egress);
        assert!(egress.drain().is_empty());

        instance.handle_chunk_request(request, &NodeId::dummy(3), NOW, &mut egress);
        let replies = egress.drain();
        let [reply] = replies.as_slice() else {
            panic!("one reply");
        };
        assert!(reply.envelope.header_only());
        assert_eq!(reply.envelope.header(), &header);
        assert_eq!(reply.to, [NodeId::dummy(3)]);
    }

    #[test]
    fn decoding_needs_more_chunks_than_the_source_count() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&header, &epoch_handle);
        let mut egress = released();

        // two source chunks: two received is not enough
        assert_eq!(ingest(&mut instance, &chunks[1], &mut egress), Ok(None));
        assert_eq!(ingest(&mut instance, &chunks[2], &mut egress), Ok(None));
        let decoded = ingest(&mut instance, &chunks[4], &mut egress);
        assert_eq!(decoded, Ok(Some(Ok(message()))));
    }

    #[test]
    fn the_author_is_owed_nothing_by_itself() {
        let epoch_handle = epoch_handle_for(author(), 4, vec![author()]);
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&header, &epoch_handle);

        assert_eq!(
            instance.take_obligation_event(),
            Some(ProposalDAEvent::ProposerObligationFulfilled(header.pid()))
        );
    }
}
