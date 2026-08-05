use std::collections::HashSet;

use monad_mcp_chorus::spec::ProposalHeader as _;

use super::{
    assignment::{Assigner, ChunkAssignment, ChunkId, Upstream},
    chunk_store::ChunkStore,
    chunk::Chunk as _,
    codec::{Context, DecodingOutcome, InvalidChunk, RaptorcastCodec},
    layout::PacketLayout,
    types::{EquivCert, NodeId, ProposalDAEvent, ProposalHeader, Slot},
    util::{ChunkRecoveryTracker, DecodingTracker, ObligationTracker},
};

// per-(slot, proposal) raptorcast
pub struct RaptorcastInstance<R: RaptorcastCodec> {
    // populated when the first valid chunk commits this instance to a
    // proposal header
    inner: Option<RaptorcastInstanceInner<R>>,
}

impl<R: RaptorcastCodec> Default for RaptorcastInstance<R> {
    fn default() -> Self {
        Self { inner: None }
    }
}

pub enum RaptorcastMessage<C> {
    UnicastChunks { to: NodeId, chunks: Vec<C> },
    BroadcastChunks { to: Vec<NodeId>, chunks: Vec<C> },
}

// per-(slot, proposal) raptorcast committed to single root
struct RaptorcastInstanceInner<R: RaptorcastCodec> {
    // commitment
    committed_header: ProposalHeader,
    slot: Slot,
    author: NodeId,
    // whether HeaderAvailable was emitted for the committed header
    header_announced: bool,
    // a conflicting header observed after the commitment
    equivocation_header: Option<ProposalHeader>,

    assignment: ChunkAssignment,
    layout: PacketLayout,
    chunk_store: ChunkStore,
    obliged_chunks: HashSet<ChunkId>,

    decoding_outcome: DecodingOutcome<R::DecodingError>,
    decoder: R,
    decoding_tracker: DecodingTracker,
    obligation_tracker: ObligationTracker,
    recovery_tracker: ChunkRecoveryTracker,
}

// outcome of checking a chunk's header against the committed root
enum RootCheck {
    Committed,
    // rival root already registered as equivocation evidence
    KnownEquivocation,
    NewEquivocation(EquivCert),
}

impl<R> RaptorcastInstance<R>
where
    R: RaptorcastCodec,
{
    pub(crate) fn ingest_chunk(
        &mut self,
        chunk: R::Chunk,
        context: &Context,
    ) -> Result<Vec<ProposalDAEvent>, InvalidChunk> {
        let inner = self
            .inner
            .get_or_insert_with(|| RaptorcastInstanceInner::new(context, &chunk));

        if !inner.ingesting_chunks() {
            // already decoded ready/failed. all chunks are available.
            // q: do we still register equivocation after decoding?
            return Ok(vec![]);
        }

        let event = inner.ingest_chunk(chunk, context)?;

        // announce the committed header on the first successful ingest
        let mut events = Vec::new();
        if !inner.header_announced {
            inner.header_announced = true;
            events.push(ProposalDAEvent::HeaderSeen(inner.committed_header.clone()));
        }
        events.extend(event);
        Ok(events)
    }

    pub(crate) fn recover_chunks(
        &mut self,
        from: &NodeId,
        chunk_ids: HashSet<ChunkId>,
    ) -> Option<RaptorcastMessage<R::Chunk>> {
        let inner = self.inner.as_mut()?;

        let chunks = inner.recover_chunks(from, chunk_ids);
        if chunks.is_empty() {
            return None;
        }

        Some(RaptorcastMessage::UnicastChunks { to: *from, chunks })
    }

    pub(crate) fn rebroadcast(&self) -> Vec<RaptorcastMessage<R::Chunk>> {
        let Some(inner) = &self.inner else {
            return vec![];
        };

        inner.rebroadcast()
    }
}

impl<R> RaptorcastInstanceInner<R>
where
    R: RaptorcastCodec,
{
    // q: is it necessary to make this fallible?
    fn new(context: &Context, chunk: &R::Chunk) -> Self {
        let committed_header = chunk.proposal_header().clone();
        let decoder = R::new_decoder(context, chunk);
        let decoding_outcome = DecodingOutcome::Pending;

        let layout = chunk.layout();
        let assignment = <R::Assigner as Assigner>::from_layout(&layout, chunk.author(), context);

        let num_chunks = assignment.num_chunks();
        let decoding_threshold = assignment.num_source_chunks();
        let decoding_tracker = DecodingTracker::new(num_chunks, decoding_threshold);

        let obligation_tracker = ObligationTracker::new(&assignment, &context.self_id);
        let obliged_chunks = obligation_tracker.obliged_chunks().clone();
        let recovery_tracker = ChunkRecoveryTracker::new(num_chunks, &obliged_chunks);

        Self {
            header_announced: false,
            equivocation_header: None,
            decoding_outcome,
            decoder,
            decoding_tracker,
            obligation_tracker,
            recovery_tracker,
            obliged_chunks,

            layout,
            assignment,
            chunk_store: ChunkStore::new(num_chunks),
            committed_header,
            slot: chunk.slot(),
            author: *chunk.author(),
        }
    }

    fn ingesting_chunks(&self) -> bool {
        matches!(self.decoding_outcome, DecodingOutcome::Pending)
    }

    fn chunk_upstream<'a>(
        chunk_id: ChunkId,
        assignment: &'a ChunkAssignment,
        context: &Context,
    ) -> Result<Upstream<&'a NodeId>, InvalidChunk> {
        let Some(routing) = assignment.resolve_chunk_id(chunk_id) else {
            return Err(InvalidChunk::InvalidChunkId);
        };
        let Some(upstream) = routing.upstream(&context.self_id) else {
            return Err(InvalidChunk::UnexpectedChunk);
        };
        Ok(upstream)
    }

    fn check_root(&mut self, header: &ProposalHeader) -> RootCheck {
        // variant header fields under the same root are not
        // equivocation: the commitment is to the root (see the paper's
        // treatment; the header's DA fields may legitimately vary)
        if header.root == self.committed_header.root {
            return RootCheck::Committed;
        }

        let already_registered = self
            .equivocation_header
            .as_ref()
            .is_some_and(|eqv_header| eqv_header.root == header.root);
        if already_registered {
            return RootCheck::KnownEquivocation;
        }

        self.equivocation_header = Some(header.clone());
        let cert = EquivCert(self.committed_header.clone(), header.clone());
        RootCheck::NewEquivocation(cert)
    }

    pub(crate) fn ingest_chunk(
        &mut self,
        chunk: R::Chunk,
        context: &Context,
    ) -> Result<Option<ProposalDAEvent>, InvalidChunk> {
        let header = chunk.proposal_header();
        match self.check_root(header) {
            RootCheck::Committed => {}
            RootCheck::KnownEquivocation => return Ok(None),
            RootCheck::NewEquivocation(cert) => {
                return Ok(Some(ProposalDAEvent::Equivocation(cert)));
            }
        }

        let chunk_id = chunk.chunk_id();
        let upstream = Self::chunk_upstream(chunk_id, &self.assignment, context)?;

        if !self.ingesting_chunks() {
            // already decoded
            return Ok(None);
        }

        if self.decoding_tracker.already_received(chunk_id) {
            // chunk already ingested
            return Ok(None);
        }

        // chunk ingestion
        self.decoder.ingest_chunk(&chunk)?;
        self.chunk_store.insert(&chunk);

        let root = self.committed_header.root;

        // chunk decoding progress tracking
        self.decoding_tracker.mark_received(chunk_id);
        if self.decoding_tracker.ready() {
            // attempt decoding
            match self.decoder.try_decode(&mut self.chunk_store) {
                DecodingOutcome::Pending => {}
                outcome @ DecodingOutcome::Decoded(_) => {
                    self.decoding_outcome = outcome;
                    return Ok(Some(ProposalDAEvent::Decoded(root)));
                }
                outcome @ DecodingOutcome::BadEncoding => {
                    self.decoding_outcome = outcome;
                    return Ok(Some(ProposalDAEvent::DecodingFailed(root)));
                }
                outcome @ DecodingOutcome::InternalError(_) => {
                    self.decoding_outcome = outcome;
                    return Ok(Some(ProposalDAEvent::DecodingFailed(root)));
                }
            }
        }

        // upstream obligation tracking
        if self.obligation_tracker.record_received_chunk(upstream) {
            // obligation fulfilled
            let event = match upstream {
                Upstream::Author => ProposalDAEvent::ProposerObligationFulfilled(root),
                Upstream::Owner(owner) => ProposalDAEvent::OwnerObligationFulfilled {
                    owner: *owner,
                    root,
                },
            };
            return Ok(Some(event));
        }

        Ok(None)
    }

    pub(crate) fn rebroadcast(&self) -> Vec<RaptorcastMessage<R::Chunk>> {
        let mut full_rebroadcast_chunks = vec![];
        let mut full_rebroadcast_targets = None;
        let mut messages = vec![];

        for chunk_id in &self.obliged_chunks {
            assert!(self.chunk_store.contains(*chunk_id));

            let routing = self
                .assignment
                .resolve_chunk_id(*chunk_id)
                .expect("chunk_id in range");
            let chunk = self.construct_chunk(*chunk_id).expect("chunk_id in range");

            if !routing.is_full_rebroadcast() {
                let msg = RaptorcastMessage::BroadcastChunks {
                    to: routing.rebroadcast_targets(),
                    chunks: vec![chunk],
                };
                messages.push(msg);
                continue;
            }

            if full_rebroadcast_targets.is_none() {
                full_rebroadcast_targets = Some(routing.rebroadcast_targets());
            }
            full_rebroadcast_chunks.push(chunk);
        }

        if let Some(targets) = full_rebroadcast_targets {
            if full_rebroadcast_chunks.is_empty() {
                return messages;
            }
            if targets.is_empty() {
                return messages;
            }

            let msg = RaptorcastMessage::BroadcastChunks {
                to: targets,
                chunks: full_rebroadcast_chunks,
            };
            messages.push(msg);
        }

        messages
    }

    fn recover_chunks(&mut self, from: &NodeId, chunk_ids: HashSet<ChunkId>) -> Vec<R::Chunk> {
        let mut chunks = vec![];
        for chunk_id in chunk_ids {
            if usize::from(chunk_id) >= self.assignment.num_chunks() {
                // out-of-range id from a remote request
                continue;
            }

            if !self.chunk_store.contains(chunk_id) {
                // we don't have the chunk locally
                continue;
            }

            if !self.recovery_tracker.should_serve(from, chunk_id) {
                // we've already served this chunk to this node or it
                // has requested more chunks than the limit.
                continue;
            }

            let chunk = self
                .construct_chunk(chunk_id)
                .expect("chunk asserted existing in store");
            chunks.push(chunk);

            self.recovery_tracker.mark_served(from, chunk_id);
        }
        chunks
    }

    fn construct_chunk(&self, chunk_id: ChunkId) -> Option<R::Chunk> {
        let symbol = self.chunk_store.get_symbol(chunk_id)?;
        let proof = self.chunk_store.get_proof(chunk_id)?;

        R::construct_chunk(
            chunk_id,
            symbol.clone(),
            proof.into(),
            &self.committed_header,
            &self.layout,
            self.slot,
            &self.author,
        )
    }
}

enum ChunkIngestionOutcome {
    RaptorcastDAEventKind,
}
