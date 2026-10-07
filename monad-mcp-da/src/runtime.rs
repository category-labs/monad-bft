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
    collections::{BTreeMap, VecDeque, btree_map::Entry},
    sync::Arc,
};

use bytes::Bytes;
use monad_mcp_chorus::spec::ProposalHeader as _;

use super::{
    chunk::{ChunkRequest, ProposalEnvelope},
    egress::Dissemination,
    header::InvalidProposalHeader,
    slot_rc::SlotRaptorcast,
    types::{
        ChorusDACommand, ChorusDAEvent, HeaderAuth, MerkleRoot, NodeId, ProposalIndex,
        ProposalKeyPair, ProposerSchedule, Slot, SlotLifecycle, Timestamp, TimestampDelta,
        ValidatorData,
    },
};

// todo: give this type a more proper name. it encodes the concept of
// slot-scoped config, the identity and validator set. think about if
// we can further refine this concept to have a clearer boundary. note
// this is almost the same shape as ChorusContext, so maybe we can
// just share the type.
#[derive(Clone)]
pub struct EpochHandle {
    pub self_id: NodeId,
    pub key_pair: Arc<ProposalKeyPair>,
    pub header_auth: Arc<HeaderAuth>,
    // todo: only this field is slot-scoped, should we isolate it out?
    pub validator_data: Arc<ValidatorData>,
}

#[derive(Clone, Copy)]
pub struct DAConfig {
    // keep completed slots for additional time (measured in slots),
    // still ingesting chunks and serving peer chunk recovery requests.
    pub completed_slot_retention: u64,
    // capital Delta from the paper
    pub delta: TimestampDelta,
}

impl DAConfig {
    pub(crate) fn timing(&self) -> Timing {
        Timing {
            pull_delay: self.delta,
            pull_interval: self.delta * 2,
        }
    }
}

// when a proposer's pulls happen
#[derive(Clone, Copy)]
pub(crate) struct Timing {
    // from the slot deadline, when owners release their rebroadcasts
    pub(crate) pull_delay: TimestampDelta,
    pub(crate) pull_interval: TimestampDelta,
}

pub struct DARuntime<S> {
    config: DAConfig,
    // todo: make epoch_handle slot dependent
    epoch_handle: EpochHandle,
    raptorcast_map: BTreeMap<Slot, SlotRaptorcast>,

    // the proposer schedule consensus holds; DA reads the same one
    schedule: Arc<S>,

    // the next pass over every slot's pulls
    next_tick: Timestamp,

    outbox: VecDeque<DAOutput>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChunkRecoveryRequest {
    pub slot: Slot,
    pub proposal_index: ProposalIndex,
    pub request: ChunkRequest,
}

impl<S> DARuntime<S>
where
    S: ProposerSchedule,
{
    pub fn new(config: DAConfig, epoch_handle: EpochHandle, schedule: Arc<S>) -> Self {
        Self {
            config,
            schedule,
            epoch_handle,
            raptorcast_map: Default::default(),
            next_tick: Timestamp::GENESIS,
            outbox: Default::default(),
        }
    }

    pub fn ingest(
        &mut self,
        sender: &NodeId,
        envelope: ProposalEnvelope,
    ) -> Result<(), InvalidProposalHeader> {
        let slot = Slot(envelope.header().slot());
        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return Err(InvalidProposalHeader::SlotOutOfRange);
        };

        slot_raptorcast.ingest(sender, envelope)?;
        self.collect(slot);
        Ok(())
    }

    // move the slot's pending outputs to the outbox
    fn collect(&mut self, slot: Slot) {
        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return;
        };

        let mut outputs = Vec::new();
        for (to, request) in slot_raptorcast.drain_requests() {
            outputs.push(DAOutput::RecoveryRequest { to, request });
        }
        for (proposal_index, root, message) in slot_raptorcast.drain_decoded() {
            outputs.push(DAOutput::Decoded {
                slot,
                proposal_index,
                root,
                message,
            });
        }
        for event in slot_raptorcast.drain_events() {
            outputs.push(DAOutput::Consensus(slot, event));
        }
        for message in slot_raptorcast.drain_messages() {
            outputs.push(DAOutput::Disseminate(message));
        }
        self.outbox.extend(outputs);
    }

    // the first event of a slot. Consensus opens each slot once
    fn open_slot(&mut self, slot: Slot, deadline: Timestamp) {
        let Entry::Vacant(entry) = self.raptorcast_map.entry(slot) else {
            tracing::warn!(?slot, "ignoring a second opening of a slot");
            return;
        };
        entry.insert(SlotRaptorcast::new(
            &self.epoch_handle,
            slot,
            deadline,
            &*self.schedule,
            self.config.timing(),
        ));
    }

    pub fn handle_chunk_request(&mut self, req: ChunkRecoveryRequest, from: &NodeId) {
        let ChunkRecoveryRequest {
            slot,
            proposal_index,
            request,
        } = req;

        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            tracing::debug!(?slot, "dropping a chunk request for a slot not open");
            return;
        };

        slot_raptorcast.handle_chunk_request(request, proposal_index, from);
        self.collect(slot);
    }

    pub fn handle_slot_lifecycle(&mut self, event: SlotLifecycle) {
        match event {
            SlotLifecycle::Opened { slot, deadline } => self.open_slot(slot, deadline),
            SlotLifecycle::Completed { .. } => {}
            SlotLifecycle::CapAdvance { cap } => self.close_slots(cap),
        }
    }

    pub fn handle_command(&mut self, command: ChorusDACommand, slot: Slot, now: Timestamp) {
        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            tracing::warn!(?slot, ?command, "dropping a command for a slot not open");
            return;
        };
        slot_raptorcast.handle_command(command, now);
        self.collect(slot);
    }

    pub fn next_tick(&self) -> Timestamp {
        self.next_tick
    }

    // passes come Delta apart and act on whatever is due
    pub fn handle_tick(&mut self, now: Timestamp) {
        let slots: Vec<Slot> = self.raptorcast_map.keys().copied().collect();
        for slot in slots {
            let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
                continue;
            };
            slot_raptorcast.tick(now);
            self.collect(slot);
        }
        self.next_tick = now + self.config.delta;
    }

    pub fn poll(&mut self) -> Option<DAOutput> {
        self.outbox.pop_front()
    }

    // drop the slots closed by the cap, once past retention
    fn close_slots(&mut self, cap: Slot) {
        let kept_slot_cap = cap
            .checked_sub(self.config.completed_slot_retention)
            .unwrap_or(Slot::MIN);
        // todo: gc after execution certificate, or can affect fast path liveness
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
            chorus::types::Timestamp,
            chunk::ChunkRequestType,
            test_util::{
                FixedProposerSchedule, MESSAGE_LEN, NOW, SLOT, author, da_config, epoch_handle,
                group, proposal_chunks, proposal_chunks_from, proposer_schedule,
            },
            types::ProposalDAEvent,
        },
        *,
    };

    fn runtime(retention: u64) -> DARuntime<FixedProposerSchedule> {
        let config = DAConfig {
            completed_slot_retention: retention,
            ..da_config()
        };
        let schedule = proposer_schedule(vec![author()]);
        DARuntime::new(config, epoch_handle(), schedule)
    }

    fn open(runtime: &mut DARuntime<FixedProposerSchedule>, slots: impl IntoIterator<Item = u64>) {
        for slot in slots {
            let opened = SlotLifecycle::Opened {
                slot: Slot(slot),
                deadline: Timestamp::GENESIS,
            };
            runtime.handle_slot_lifecycle(opened);
        }
    }

    fn outputs(runtime: &mut DARuntime<FixedProposerSchedule>) -> Vec<DAOutput> {
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

        let closed = runtime.ingest(&author(), group(&chunks[..1]));
        assert_eq!(closed, Err(InvalidProposalHeader::SlotOutOfRange));

        open(&mut runtime, [0, 1]);
        assert_eq!(runtime.ingest(&author(), group(&chunks[..1])), Ok(()));

        let (_, later) = proposal_chunks_from(&epoch_handle, 0, Slot(5), 1);
        let beyond = runtime.ingest(&author(), group(&later[..1]));
        assert_eq!(beyond, Err(InvalidProposalHeader::SlotOutOfRange));
    }

    #[test]
    fn slots_below_the_cap_are_kept_for_the_retention_then_dropped() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        open(&mut runtime, [0, 1, 2]);
        let (_, slot0) = proposal_chunks_from(&epoch_handle, 0, Slot(0), 1);
        let (_, slot1) = proposal_chunks_from(&epoch_handle, 0, Slot(1), 1);
        assert_eq!(runtime.ingest(&author(), group(&slot0[..1])), Ok(()));
        assert_eq!(runtime.ingest(&author(), group(&slot1[..1])), Ok(()));

        // cap 1 closes slot 0, kept for one slot of retention
        runtime.handle_slot_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(1) });
        assert_eq!(runtime.ingest(&author(), group(&slot0[1..2])), Ok(()));

        // cap 2: slot 0 leaves the retention window, slot 1 stays
        runtime.handle_slot_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(2) });
        let retired = runtime.ingest(&author(), group(&slot0[2..3]));
        assert_eq!(retired, Err(InvalidProposalHeader::SlotOutOfRange));
        assert_eq!(runtime.ingest(&author(), group(&slot1[1..2])), Ok(()));
    }

    #[test]
    fn a_decode_reaches_execution_then_consensus_then_the_wire() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        open(&mut runtime, [0, 1]);
        runtime.handle_command(ChorusDACommand::ReleaseChunks, SLOT, NOW);
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        runtime
            .ingest(&author(), group(&chunks[..3]))
            .expect("valid");

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
                    assert_eq!((*slot, *proposal_index, *root), (SLOT, 0, *header.root()));
                    assert_eq!(message, &Bytes::from(vec![1u8; MESSAGE_LEN]));
                    decoded_at = Some(i);
                }
                DAOutput::Consensus(slot, event) => {
                    assert_eq!(*slot, SLOT);
                    if event.event == ProposalDAEvent::Decoded(*header.root()) {
                        consensus_decoded_at = Some(i);
                    }
                }
                DAOutput::Disseminate(dissemination) => {
                    // our chunk 0 on arrival and 3 once derivable, one message
                    let ids: Vec<_> = dissemination
                        .envelope
                        .chunk_data()
                        .keys()
                        .copied()
                        .collect();
                    assert_eq!(ids, [0, 3]);
                    disseminated_at.push(i);
                }
                DAOutput::RecoveryRequest { .. } => panic!("nothing to recover"),
            }
        }

        let decoded_at = decoded_at.expect("the message is delivered");
        let consensus_decoded_at = consensus_decoded_at.expect("consensus learns of it");
        assert!(consensus_decoded_at > decoded_at);
        assert_eq!(disseminated_at.len(), 1);
        assert!(disseminated_at[0] > decoded_at);
    }

    #[test]
    fn chunk_requests_for_unknown_slots_are_ignored() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        open(&mut runtime, [0, 1]);
        let (header, _) = proposal_chunks(&epoch_handle, 1);

        let unknown_slot = ChunkRecoveryRequest {
            slot: Slot(7),
            proposal_index: 0,
            request: ChunkRequest::all(*header.root(), ChunkRequestType::YourChunks),
        };
        runtime.handle_chunk_request(unknown_slot, &NodeId::dummy(2));
        assert!(outputs(&mut runtime).is_empty());
    }

    #[test]
    fn pulls_leave_as_recovery_requests_once_due() {
        let mut runtime = runtime(1);
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let at = |millis| NOW + TimestampDelta::from_millis(millis);
        for slot in [0, 1] {
            let opened = SlotLifecycle::Opened {
                slot: Slot(slot),
                deadline: at(100),
            };
            runtime.handle_slot_lifecycle(opened);
        }

        // the tick is due from the start, then every Delta
        assert_eq!(runtime.next_tick(), NOW);
        runtime
            .ingest(&author(), group(&chunks[..2]))
            .expect("valid");
        outputs(&mut runtime);

        // within the grace period nothing is asked
        runtime.handle_tick(at(100));
        assert_eq!(runtime.next_tick(), at(200));
        assert!(outputs(&mut runtime).is_empty());

        // then the author for ours and every owner for theirs
        runtime.handle_tick(at(200));
        let mut peers = Vec::new();
        for output in outputs(&mut runtime) {
            let DAOutput::RecoveryRequest { to, request } = output else {
                continue;
            };
            assert_eq!((request.slot, request.proposal_index), (SLOT, 0));
            assert_eq!(request.request.root, *header.root());
            peers.push(to);
        }
        peers.sort();
        assert_eq!(
            peers,
            [NodeId::dummy(0), NodeId::dummy(2), NodeId::dummy(3)]
        );
    }
}
