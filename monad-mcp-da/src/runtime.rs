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

use std::{
    collections::{BTreeMap, VecDeque},
    ops::Range,
    sync::Arc,
};

use bytes::Bytes;

use super::{
    chunk::{Chunk, ChunkRequest, ProposalEnvelope},
    egress::Dissemination,
    election::ProposerElection,
    header::InvalidProposalHeader,
    slot_rc::SlotRaptorcast,
    types::{
        ChorusDACommand, ChorusDAEvent, HeaderAuth, MerkleRoot, NodeId, ProposalDAEvent,
        ProposalIndex, ProposalKeyPair, Slot, SlotLifecycle, ValidatorData,
    },
    util::SlotCompletion,
};

// todo: give this type a more proper name. it encodes the concept of
// slot-scoped config, the identity and validator set. think about if
// we can further refine this concept to have a clearer boundary. note
// this is almost the same shape as ChorusContext, so maybe we can
// just share the type.
#[derive(Clone)]
pub struct EpochHandle {
    pub self_id: NodeId,
    pub num_proposals: usize,
    pub key_pair: Arc<ProposalKeyPair>,
    pub header_auth: Arc<HeaderAuth>,
    // todo: only this field is slot-scoped, should we isolate it out?
    pub validator_data: Arc<ValidatorData>,
}

pub struct DAConfig {
    // keep completed slots for additional time (measured in slots),
    // still ingesting chunks and serving peer chunk recovery requests.
    pub completed_slot_retention: u64,
}

pub struct DARuntime<E> {
    config: DAConfig,
    // todo: make epoch_handle slot dependent
    epoch_handle: EpochHandle,
    raptorcast_map: BTreeMap<Slot, SlotRaptorcast>,

    election: Arc<E>,

    // inclusive start, exclusive end
    ingestion_window: Range<Slot>,
    slot_completion: SlotCompletion,

    outbox: VecDeque<DAOutput>,
}

pub struct ChunkRecoveryRequest {
    pub slot: Slot,
    pub proposal_index: ProposalIndex,
    pub root: MerkleRoot,

    pub request: ChunkRequest,
}

impl<E> DARuntime<E>
where
    E: ProposerElection,
{
    pub fn new(config: DAConfig, epoch_handle: EpochHandle, election: Arc<E>) -> Self {
        Self {
            config,
            election,
            epoch_handle,
            raptorcast_map: Default::default(),
            // todo: set the lower bound with finalization certificate
            ingestion_window: Slot::MIN..Slot::MIN,
            slot_completion: SlotCompletion::new(),
            outbox: Default::default(),
        }
    }

    pub fn ingest_chunk(&mut self, chunk: Chunk) -> Result<(), InvalidProposalHeader> {
        let envelope = ProposalEnvelope::from_chunk(chunk);
        self.ingest(envelope)
    }

    pub fn ingest(&mut self, envelope: ProposalEnvelope) -> Result<(), InvalidProposalHeader> {
        let slot = envelope.header().slot;
        let Some(slot_raptorcast) = self.open_slot_raptorcast(slot) else {
            return Err(InvalidProposalHeader::SlotOutOfRange);
        };

        slot_raptorcast.ingest(envelope)?;
        self.collect(slot);
        Ok(())
    }

    // move the slot's pending events and messages to the outbox
    fn collect(&mut self, slot: Slot) {
        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return;
        };

        let mut outputs = Vec::new();
        for event in slot_raptorcast.drain_events() {
            if let ProposalDAEvent::Decoded(root) = &event.event {
                let message = slot_raptorcast
                    .decoded_message(event.j, root)
                    .expect("decoded event implies a decoded message")
                    .clone();
                outputs.push(DAOutput::Decoded {
                    slot,
                    proposal_index: event.j,
                    root: *root,
                    message,
                });
            }

            outputs.push(DAOutput::Consensus(slot, event));
        }
        for message in slot_raptorcast.drain_messages() {
            outputs.push(DAOutput::Disseminate(message));
        }
        self.outbox.extend(outputs);
    }

    // the slot's raptorcast, created on first use. None once the slot
    // is outside the ingestion window.
    fn open_slot_raptorcast(&mut self, slot: Slot) -> Option<&mut SlotRaptorcast> {
        if !self.ingestion_window.contains(&slot) {
            return None;
        }

        let slot_raptorcast = self
            .raptorcast_map
            .entry(slot)
            .or_insert_with(|| SlotRaptorcast::new(&self.epoch_handle, slot, &*self.election));
        Some(slot_raptorcast)
    }

    pub fn handle_chunk_request(&mut self, from: &NodeId, req: ChunkRecoveryRequest) {
        let ChunkRecoveryRequest {
            slot,
            proposal_index,
            root,
            request,
        } = req;

        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return;
        };

        slot_raptorcast.handle_chunk_request(from, proposal_index, root, request);
        self.collect(slot);
    }

    pub fn handle_slot_event(&mut self, slot: Slot, event: SlotLifecycle) {
        match event {
            SlotLifecycle::Opened => {
                let open_ingestion_slot = slot
                    .checked_next()
                    .unwrap_or(Slot::MAX_CAP)
                    .max(self.ingestion_window.end);
                self.ingestion_window.end = open_ingestion_slot;
            }
            SlotLifecycle::Completed => {
                self.slot_completion.mark_completed(slot);
                self.close_slots();
            }
        }
    }

    pub fn handle_command(&mut self, slot: Slot, command: ChorusDACommand) {
        let Some(slot_raptorcast) = self.open_slot_raptorcast(slot) else {
            return;
        };
        let outputs = slot_raptorcast.handle_command(command);
        self.outbox.extend(outputs);
        self.collect(slot);
    }

    pub fn poll(&mut self) -> Option<DAOutput> {
        self.outbox.pop_front()
    }

    // drop the completed slots past retention and stop ingesting for
    // them
    fn close_slots(&mut self) {
        let kept_slot_cap = self
            .slot_completion
            .cap()
            .checked_sub(self.config.completed_slot_retention)
            .unwrap_or(Slot::MIN);

        self.ingestion_window.start = kept_slot_cap;
        self.raptorcast_map.retain(|&slot, _| slot >= kept_slot_cap);
    }
}

pub enum DAOutput {
    // notification for the consensus layer's slot instance
    Consensus(Slot, ChorusDAEvent),

    // the message decoded under (slot, proposal_index, root), for the
    // layer that recovers proposals
    Decoded {
        slot: Slot,
        proposal_index: ProposalIndex,
        root: MerkleRoot,
        message: Bytes,
    },

    // deliver a proposal's chunks (possibly only its header) to peers
    Disseminate(Dissemination),

    // request chunks from a peer
    RecoveryRequest {
        to: NodeId,
        request: ChunkRecoveryRequest,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            test_util::{
                MESSAGE_LEN, Proposers, SLOT, author, epoch_handle, group, proposal_chunks,
                proposal_chunks_from,
            },
            types::ChunkRequestType,
        },
        *,
    };

    fn runtime(retention: u64) -> DARuntime<Proposers> {
        let config = DAConfig {
            completed_slot_retention: retention,
        };
        let election = Arc::new(Proposers::new(vec![author()]));
        DARuntime::new(config, epoch_handle(), election)
    }

    fn open(runtime: &mut DARuntime<Proposers>, slots: impl IntoIterator<Item = u64>) {
        for slot in slots {
            runtime.handle_slot_event(Slot(slot), SlotLifecycle::Opened);
        }
    }

    fn outputs(runtime: &mut DARuntime<Proposers>) -> Vec<DAOutput> {
        let mut outputs = Vec::new();
        while let Some(output) = runtime.poll() {
            outputs.push(output);
        }
        outputs
    }

    #[test]
    fn ingestion_is_bounded_by_the_opened_slots() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);

        let closed = runtime.ingest(group(&chunks[..1]));
        assert_eq!(closed, Err(InvalidProposalHeader::SlotOutOfRange));

        open(&mut runtime, [0, 1]);
        assert_eq!(runtime.ingest(group(&chunks[..1])), Ok(()));

        let (_, later) = proposal_chunks_from(&epoch_handle, 0, Slot(5), 1);
        let beyond = runtime.ingest(group(&later[..1]));
        assert_eq!(beyond, Err(InvalidProposalHeader::SlotOutOfRange));
    }

    #[test]
    fn completed_slots_are_kept_for_the_retention_then_dropped() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        open(&mut runtime, [0, 1, 2]);
        let (_, slot0) = proposal_chunks_from(&epoch_handle, 0, Slot(0), 1);
        let (_, slot1) = proposal_chunks_from(&epoch_handle, 0, Slot(1), 1);
        assert_eq!(runtime.ingest(group(&slot0[..1])), Ok(()));
        assert_eq!(runtime.ingest(group(&slot1[..1])), Ok(()));

        // completing 1 before 0 retires nothing
        runtime.handle_slot_event(Slot(1), SlotLifecycle::Completed);
        assert_eq!(runtime.ingest(group(&slot0[1..2])), Ok(()));

        // the cap reaches 2: slot 0 leaves the retention window, slot 1 stays
        runtime.handle_slot_event(Slot(0), SlotLifecycle::Completed);
        let retired = runtime.ingest(group(&slot0[2..3]));
        assert_eq!(retired, Err(InvalidProposalHeader::SlotOutOfRange));
        assert_eq!(runtime.ingest(group(&slot1[1..2])), Ok(()));
    }

    #[test]
    fn a_decode_reaches_execution_then_consensus_then_the_wire() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        open(&mut runtime, [0, 1]);
        runtime.handle_command(SLOT, ChorusDACommand::ReleaseChunks);
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        runtime.ingest(group(&chunks[..3])).expect("valid");

        let mut decoded_at = None;
        let mut consensus_decoded_at = None;
        let mut disseminated_at = Vec::new();
        for (i, output) in outputs(&mut runtime).iter().enumerate() {
            match output {
                DAOutput::Decoded {
                    slot,
                    proposal_index,
                    root,
                    message,
                } => {
                    assert_eq!((*slot, *proposal_index, *root), (SLOT, 0, header.root));
                    assert_eq!(message, &Bytes::from(vec![1u8; MESSAGE_LEN]));
                    decoded_at = Some(i);
                }
                DAOutput::Consensus(slot, event) => {
                    assert_eq!(*slot, SLOT);
                    if event.event == ProposalDAEvent::Decoded(header.root) {
                        consensus_decoded_at = Some(i);
                    }
                }
                DAOutput::Disseminate(dissemination) => {
                    // our chunk 0 on arrival and 3 once derivable, one message
                    let ids: Vec<_> = dissemination.envelope.chunks().keys().copied().collect();
                    assert_eq!(ids, [0, 3]);
                    disseminated_at.push(i);
                }
                DAOutput::RecoveryRequest { .. } => panic!("nothing to recover"),
            }
        }

        let decoded_at = decoded_at.expect("the message is delivered");
        assert_eq!(consensus_decoded_at, Some(decoded_at + 1));
        assert_eq!(disseminated_at.len(), 1);
        assert!(disseminated_at[0] > decoded_at);
    }

    #[test]
    fn recovery_requests_skip_ourselves_and_unknown_slots_are_ignored() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        open(&mut runtime, [0, 1]);
        let (header, _) = proposal_chunks(&epoch_handle, 1);

        let voters = vec![NodeId::dummy(1), NodeId::dummy(2), NodeId::dummy(3)];
        let command = ChorusDACommand::RecoverChunks {
            j: 0,
            root: header.root,
            request_type: ChunkRequestType::YourChunks,
            voters,
        };
        runtime.handle_command(SLOT, command);

        let mut peers = Vec::new();
        for output in outputs(&mut runtime) {
            let DAOutput::RecoveryRequest { to, request } = output else {
                panic!("only requests are produced");
            };
            assert_eq!((request.slot, request.proposal_index), (SLOT, 0));
            assert_eq!(request.root, header.root);
            assert_eq!(
                request.request,
                ChunkRequest::all(ChunkRequestType::YourChunks)
            );
            peers.push(to);
        }
        assert_eq!(peers, [NodeId::dummy(2), NodeId::dummy(3)]);

        let unknown_slot = ChunkRecoveryRequest {
            slot: Slot(7),
            proposal_index: 0,
            root: header.root,
            request: ChunkRequest::all(ChunkRequestType::YourChunks),
        };
        runtime.handle_chunk_request(&NodeId::dummy(2), unknown_slot);
        assert!(outputs(&mut runtime).is_empty());
    }
}
