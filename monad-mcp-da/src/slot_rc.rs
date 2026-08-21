use std::collections::{HashMap, HashSet};

use monad_mcp_chorus::spec::{proposal::HeaderAuth as _, validator::ValidatorData as _};

use super::{
    assignment::ChunkId,
    chunk::Chunk as _,
    codec::{Context, InvalidChunk, RaptorcastCodec},
    election::ProposerElection,
    instance::{RaptorcastInstance, RaptorcastMessage},
    types::{ChorusDAEvent, NodeId, ProposalIndex, ProposalMap, Slot},
};

// per-slot raptorcast tracking. mainly handles proposer-related
// validation.
pub struct SlotRaptorcast<R: RaptorcastCodec> {
    context: Context,
    slot: Slot,
    instances: HashMap<NodeId, RaptorcastInstance<R>>,
    proposer_map: ProposalMap<Option<NodeId>>,
}

impl<R> SlotRaptorcast<R>
where
    R: RaptorcastCodec,
{
    pub fn new<E>(context: &Context, slot: Slot, election: &E) -> Self
    where
        E: ProposerElection,
    {
        let mut instances = HashMap::new();
        let mut proposer_map = Vec::new();
        for index in election.proposer_indices() {
            let proposer = election.get_proposer(slot, index);
            proposer_map.push(proposer.cloned());
            if let Some(proposer) = proposer {
                let instance = RaptorcastInstance::default();
                instances.insert(*proposer, instance);
            };
        }

        Self {
            context: context.clone(),
            slot,
            instances,
            proposer_map: ProposalMap::from(proposer_map.into_iter()),
        }
    }

    pub fn ingest_chunk(&mut self, chunk: R::Chunk) -> Result<Vec<ChorusDAEvent>, InvalidChunk> {
        let author = *chunk.author();
        let proposal_index = self
            .proposal_index(&author)
            .ok_or(InvalidChunk::InvalidProposer)?;

        let header_valid = self.context.header_auth.validate(
            chunk.proposal_header(),
            self.slot.get(),
            proposal_index,
        );
        if !header_valid {
            return Err(InvalidChunk::BadSignature);
        }

        let instance = self
            .instances
            .get_mut(&author)
            .expect("proposer implies instance");

        let events = instance
            .ingest_chunk(chunk, &self.context)?
            .into_iter()
            .map(|event| ChorusDAEvent {
                j: proposal_index,
                event,
            })
            .collect();

        Ok(events)
    }

    // the proposal index of an author, resolved against this slot's
    // frozen proposer map.
    fn proposal_index(&self, author: &NodeId) -> Option<ProposalIndex> {
        (0..self.proposer_map.size())
            .find(|index| self.proposer_map[*index].as_ref() == Some(author))
    }

    pub(crate) fn handle_chunk_recovery(
        &mut self,
        from: &NodeId,
        proposal_index: ProposalIndex,
        chunk_ids: HashSet<ChunkId>,
    ) -> Option<RaptorcastMessage<R::Chunk>> {
        if !self.context.validator_data.contains(from) {
            // not legit requester
            return None;
        }

        if proposal_index >= self.proposer_map.size() {
            // out-of-range index from a remote request
            return None;
        }

        // no proposer for the index
        let proposer = self.proposer_map[proposal_index].as_ref()?;

        let instance = self
            .instances
            .get_mut(proposer)
            .expect("instance must exist");

        instance.recover_chunks(from, chunk_ids)
    }

    pub(crate) fn rebroadcast(
        &mut self,
        proposals: Vec<ProposalIndex>,
    ) -> Vec<RaptorcastMessage<R::Chunk>> {
        let mut messages = Vec::new();
        for j in proposals {
            // todo: this invariant is too strongly coupled with
            // chorus implementation, maybe we should loosen it.
            let proposer = self.proposer_map[j]
                .as_ref()
                .expect("positively voted proposals must have a proposer");

            let instance = self
                .instances
                .get_mut(proposer)
                .expect("instance must exist");

            messages.extend(instance.rebroadcast());
        }
        messages
    }
}
