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

mod decoding_tracker;

use std::collections::HashSet;

use bytes::Bytes;
use decoding_tracker::DecodingTracker;

use super::{
    assignment::{ChunkAssignment, ChunkId, ChunkRouting, NodeIndex},
    chunk::{ChunkData, WireChunkId},
    chunk_tree::ChunkTree,
    egress::ChunkEgress,
    encoding_scheme::{DAEncodingScheme as _, SymbolDecoder},
    header::DAProposalHeader as _,
    layout::PacketLayout,
    runtime::EpochHandle,
    types::{NodeId, ProposalDAEvent, ProposalHeader},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidChunk {
    InvalidChunkId,
    BadProof,
}

pub enum DecodingOutcome {
    Pending,
    Decoded(Bytes),
    // merkle root doesn't match the decoded-then-re-encoded message
    BadEncoding,
}

// per-(slot, proposal) raptorcast committed to single root
pub(crate) struct RaptorcastInstance {
    header: ProposalHeader,
    // None when this node is outside the assignment (e.g. a full node)
    self_index: Option<NodeIndex>,

    layout: PacketLayout,
    assignment: ChunkAssignment,
    chunk_tree: ChunkTree,

    decoding_outcome: DecodingOutcome,
    decoder: Box<dyn SymbolDecoder>,
    decoding_tracker: DecodingTracker,
}

impl RaptorcastInstance {
    pub(crate) fn new(
        epoch_handle: &EpochHandle,
        header: ProposalHeader,
        layout: PacketLayout,
        author: &NodeId,
    ) -> Self {
        let scheme = header.encoding_scheme();
        let assignment = scheme.chunk_assignment(&layout, author, &epoch_handle.validator_data);
        let self_index = assignment.index_of(&epoch_handle.self_id);

        let num_chunks = assignment.num_chunks();
        let decoder = scheme.decoder(layout, num_chunks);
        let decoding_outcome = DecodingOutcome::Pending;

        let decoding_threshold = layout.num_source_chunks();
        let decoding_tracker = DecodingTracker::new(num_chunks, decoding_threshold);

        Self {
            decoding_outcome,
            decoder,
            decoding_tracker,

            layout,
            assignment,
            chunk_tree: ChunkTree::partial(header.root),
            header,
            self_index,
        }
    }

    pub(crate) fn accepting_chunks(&self) -> bool {
        matches!(self.decoding_outcome, DecodingOutcome::Pending)
    }

    pub(crate) fn decoded_message(&self) -> Option<&Bytes> {
        let DecodingOutcome::Decoded(message) = &self.decoding_outcome else {
            return None;
        };
        Some(message)
    }

    pub(crate) fn ingest_chunk(
        &mut self,
        chunk_id: WireChunkId,
        data: ChunkData,
        egress: &mut ChunkEgress,
    ) -> Result<Option<ProposalDAEvent>, InvalidChunk> {
        if !self.accepting_chunks() {
            // root already decoded (or failed), refusing further chunks.
            return Ok(None);
        }

        let Some(routing) = self.assignment.resolve_chunk_id(chunk_id) else {
            // chunk id is out of range
            return Err(InvalidChunk::InvalidChunkId);
        };

        let chunk_id = routing.chunk_id();
        if self.decoding_tracker.already_received(chunk_id) {
            // chunk already ingested previously, skip ingestion.
            return Ok(None);
        }

        // todo: check the proof in chunk parsing, so a Chunk always
        // holds a valid merkle proof.
        if !self.chunk_tree.verify(&self.layout, chunk_id, &data) {
            return Err(InvalidChunk::BadProof);
        }

        // ingestion is infallible from here on.

        // store & update decoder state
        self.decoder.ingest(chunk_id, &data.symbol);
        self.chunk_tree.insert(chunk_id, data);

        // track decoding
        self.decoding_tracker.mark(chunk_id);

        // try rebroadcast & decode
        self.try_rebroadcast(routing, egress);
        let event = self.try_decode();
        if matches!(event, Some(ProposalDAEvent::Decoded(_))) {
            self.rebroadcast_remaining(egress);
            self.decoding_tracker.mark_all();
        }
        Ok(event)
    }

    fn try_decode(&mut self) -> Option<ProposalDAEvent> {
        if !self.decoding_tracker.ready() {
            return None;
        }
        let Some(message) = self.decoder.try_decode() else {
            // todo: decoder still not ready after ingesting enough
            // chunks. this means the codec doesn't guarantee
            // decodability (e.g. r10).
            return None;
        };

        let root = self.header.root;
        if !self.reencodes_to_root(&message) {
            self.decoding_outcome = DecodingOutcome::BadEncoding;
            return Some(ProposalDAEvent::DecodingFailed(root));
        }
        self.decoding_outcome = DecodingOutcome::Decoded(message);
        Some(ProposalDAEvent::Decoded(root))
    }

    fn reencodes_to_root(&mut self, message: &[u8]) -> bool {
        let scheme = self.header.encoding_scheme();
        let num_chunks = self.assignment.num_chunks();
        let Some(tree) = scheme.encode(message, self.layout, num_chunks) else {
            return false;
        };
        if tree.root() != self.header.root {
            return false;
        }
        self.chunk_tree = tree;

        true
    }

    fn try_rebroadcast(&self, routing: ChunkRouting<'_>, egress: &mut ChunkEgress) {
        if self.self_index != Some(routing.owner_index()) {
            // it's not our responsibility to rebroadcast this chunk
            return;
        }

        let to = match routing.partial_rebroadcast_targets() {
            Some(targets) => targets,
            None => self
                .assignment
                .full_rebroadcast_targets(routing.owner_index()),
        };
        let chunk_id = routing.chunk_id();
        self.enqueue(chunk_id, &to, egress);
    }

    // rebroadcast all remaining owned chunks (after decoding).
    fn rebroadcast_remaining(&self, egress: &mut ChunkEgress) {
        let Some(self_index) = self.self_index else {
            return;
        };
        for routing in self.assignment.owned_chunks(self_index) {
            let chunk_id = routing.chunk_id();
            if self.decoding_tracker.already_received(chunk_id) {
                continue;
            }
            self.try_rebroadcast(routing, egress);
        }
    }

    // the caller must ensure the chunk exists in chunk tree.
    fn enqueue(&self, chunk_id: ChunkId, to: &HashSet<NodeId>, egress: &mut ChunkEgress) {
        let data = self
            .chunk_tree
            .chunk_data(chunk_id)
            .expect("caller ensures the chunk is held");
        egress.enqueue(to, &self.header, chunk_id.to_wire(), data);
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::validator::ValidatorData as _;

    use super::{
        super::{
            chunk::Chunk,
            test_util::{
                MESSAGE_LEN, author, epoch_handle, inconsistent_proposal_chunks, proposal_chunks,
            },
        },
        *,
    };

    fn instance(epoch_handle: &EpochHandle, header: &ProposalHeader) -> RaptorcastInstance {
        let layout = header
            .encoding_scheme()
            .packet_layout(epoch_handle.validator_data.len())
            .expect("fits a layout");
        RaptorcastInstance::new(epoch_handle, header.clone(), layout, &author())
    }

    fn released() -> ChunkEgress {
        let mut egress = ChunkEgress::new();
        egress.release();
        egress
    }

    fn ingest(
        instance: &mut RaptorcastInstance,
        chunk: &Chunk,
        egress: &mut ChunkEgress,
    ) -> Result<Option<ProposalDAEvent>, InvalidChunk> {
        let (_, chunk_id, data) = chunk.clone().into_parts();
        instance.ingest_chunk(chunk_id, data, egress)
    }

    // the wire ids in the drained messages, sorted
    fn sent(egress: &mut ChunkEgress) -> Vec<WireChunkId> {
        let mut ids = Vec::new();
        for message in egress.drain() {
            ids.extend(message.envelope.chunks().keys().copied());
        }
        ids.sort();
        ids
    }

    #[test]
    fn malformed_chunks_are_rejected() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&epoch_handle, &header);
        let mut egress = released();
        let (_, _, data) = chunks[0].clone().into_parts();

        let out_of_range = instance.ingest_chunk(99, data.clone(), &mut egress);
        assert_eq!(out_of_range, Err(InvalidChunk::InvalidChunkId));

        let tampered = ChunkData {
            symbol: Bytes::from_static(b"tampered"),
            proof: data.proof.clone(),
        };
        let tampered = instance.ingest_chunk(0, tampered, &mut egress);
        assert_eq!(tampered, Err(InvalidChunk::BadProof));

        // a good chunk under the wrong id fails its proof
        let misplaced = instance.ingest_chunk(1, data, &mut egress);
        assert_eq!(misplaced, Err(InvalidChunk::BadProof));

        assert!(sent(&mut egress).is_empty());
    }

    #[test]
    fn a_repeated_chunk_counts_and_forwards_once() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&epoch_handle, &header);
        let mut egress = released();

        // chunk 0 is ours: forwarded on arrival, not on the repeat
        assert_eq!(ingest(&mut instance, &chunks[0], &mut egress), Ok(None));
        assert_eq!(sent(&mut egress), [0]);
        assert_eq!(ingest(&mut instance, &chunks[0], &mut egress), Ok(None));
        assert!(sent(&mut egress).is_empty());

        // the repeat did not count towards decoding: one more distinct
        // chunk is short, the next one decodes
        assert_eq!(ingest(&mut instance, &chunks[1], &mut egress), Ok(None));
        let event = ingest(&mut instance, &chunks[2], &mut egress);
        assert_eq!(event, Ok(Some(ProposalDAEvent::Decoded(header.root))));
        assert_eq!(
            instance.decoded_message(),
            Some(&Bytes::from(vec![1u8; MESSAGE_LEN]))
        );
    }

    #[test]
    fn inconsistent_chunks_resolve_invalid() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = inconsistent_proposal_chunks(&epoch_handle);
        let mut instance = instance(&epoch_handle, &header);
        let mut egress = released();

        assert_eq!(ingest(&mut instance, &chunks[0], &mut egress), Ok(None));
        assert_eq!(ingest(&mut instance, &chunks[1], &mut egress), Ok(None));
        let event = ingest(&mut instance, &chunks[2], &mut egress);
        assert_eq!(
            event,
            Ok(Some(ProposalDAEvent::DecodingFailed(header.root)))
        );

        assert!(!instance.accepting_chunks());
        assert!(instance.decoded_message().is_none());
        // later chunks are ignored
        assert_eq!(ingest(&mut instance, &chunks[4], &mut egress), Ok(None));

        // our chunk 0 went out on arrival; nothing is derivable from an
        // invalid proposal
        assert_eq!(sent(&mut egress), [0]);
    }

    #[test]
    fn decoding_needs_more_chunks_than_the_source_count() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = instance(&epoch_handle, &header);
        let mut egress = released();

        // two source chunks: two received is not enough
        assert_eq!(ingest(&mut instance, &chunks[1], &mut egress), Ok(None));
        assert_eq!(ingest(&mut instance, &chunks[2], &mut egress), Ok(None));
        assert!(instance.decoded_message().is_none());
        let event = ingest(&mut instance, &chunks[4], &mut egress);
        assert_eq!(event, Ok(Some(ProposalDAEvent::Decoded(header.root))));
    }
}
