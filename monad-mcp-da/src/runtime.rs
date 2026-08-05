use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    ops::Range,
};

use monad_mcp_chorus::spec::ProposalHeader as _;

use super::{
    assignment::ChunkId,
    chunk::Chunk as _,
    codec::{self, InvalidChunk, RaptorcastCodec},
    election::ProposerElection,
    instance::RaptorcastMessage,
    slot_rc::SlotRaptorcast,
    types::{ChorusDAEvent, NodeId, ProposalIndex, Slot},
    util::SlotCompletion,
};

pub struct DAConfig {
    pub num_proposals: usize,

    // keep completed slots for additional time (measured in slots)
    // for peer chunk recovery requests.
    pub completed_slot_retention: u64,

    // ingest chunks for unopened slots up to this lookahead (measured
    // in slots)
    pub ingestion_lookahead: u64,
}

pub struct DARuntime<R, E>
where
    R: RaptorcastCodec,
{
    config: DAConfig,
    // todo: make ctx slot dependent
    raptorcast_ctx: codec::Context,
    raptorcast_map: BTreeMap<Slot, SlotRaptorcast<R>>,
    election: E,

    // inclusive start, exclusive end
    ingestion_window: Range<Slot>,
    slot_completion: SlotCompletion,

    outbox: VecDeque<DAOutput<R::Chunk>>,
}

pub struct ChunkRecoveryRequest {
    slot: Slot,
    proposal_index: ProposalIndex,
    chunk_ids: HashSet<ChunkId>,
}

impl<R, E> DARuntime<R, E>
where
    R: RaptorcastCodec,
    E: ProposerElection,
{
    pub fn new() {
        todo!()
    }

    pub fn ingest_chunk(&mut self, chunk: R::Chunk) -> Result<(), InvalidChunk> {
        use std::collections::btree_map::Entry;
        let slot = chunk.slot();
        if !(self.ingestion_window.contains(&slot)) {
            return Err(InvalidChunk::SlotOutOfRange);
        }

        let slot_raptorcast = match self.raptorcast_map.entry(slot) {
            Entry::Vacant(e) => {
                let new_slot_rc = SlotRaptorcast::new(&self.raptorcast_ctx, slot, &self.election);
                e.insert(new_slot_rc)
            }
            Entry::Occupied(e) => e.into_mut(),
        };

        for event in slot_raptorcast.ingest_chunk(chunk)? {
            self.outbox.push_back(DAOutput::Consensus(slot, event));
        }
        Ok(())
    }

    pub fn handle_chunk_recovery(&mut self, from: &NodeId, req: ChunkRecoveryRequest) {
        let ChunkRecoveryRequest {
            slot,
            proposal_index,
            chunk_ids,
        } = req;

        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return;
        };

        let Some(message) = slot_raptorcast.handle_chunk_recovery(from, proposal_index, chunk_ids)
        else {
            return;
        };
        self.outbox.push_back(DAOutput::Message(message));
    }

    pub fn handle_slot_event(&mut self, slot: Slot, event: SlotEvent) {
        match event {
            SlotEvent::SlotOpened => {
                self.ingestion_window.end = slot
                    .checked_add(self.config.ingestion_lookahead)
                    .unwrap_or(Slot::MAX_CAP);
            }
            SlotEvent::SlotCompleted => {
                self.slot_completion.mark_completed(slot);
                self.ingestion_window.start = self.slot_completion.cap();
                self.close_slots();
            }
            SlotEvent::SlotVoted { positive_proposals } => {
                if let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) {
                    let messages = slot_raptorcast.rebroadcast(positive_proposals);
                    self.outbox
                        .extend(messages.into_iter().map(DAOutput::Message));
                }
            }
        }
    }

    pub fn poll(&mut self) -> Option<DAOutput<R::Chunk>> {
        self.outbox.pop_front()
    }

    fn close_slots(&mut self) {
        let kept_slot_cap = self
            .slot_completion
            .cap()
            .checked_sub(self.config.completed_slot_retention)
            .unwrap_or(Slot::MIN);

        self.raptorcast_map.retain(|&slot, _| slot > kept_slot_cap);
    }
}

pub enum DAOutput<C> {
    // notification for the consensus layer's slot instance
    Consensus(Slot, ChorusDAEvent),
    Message(RaptorcastMessage<C>),
}

pub enum SlotEvent {
    // advance cap for chunk ingestion
    SlotOpened,
    // close slot raptorcast instances
    SlotCompleted,
    // queue for broadcasting
    SlotVoted {
        positive_proposals: Vec<ProposalIndex>,
    },
}
