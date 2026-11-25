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
    sync::Arc,
};

use monad_mcp_cadence::spec::ProposalHeader as _;

use super::{
    chunk::{ChunkRequest, ProposalEnvelope},
    egress::Dissemination,
    encoding_scheme::Resolved,
    header::InvalidProposalHeader,
    slot_rc::SlotRaptorcast,
    types::{
        ChorusDACommand, ChorusDAEvent, EpochHandle, Lane, NodeCredential, NodeId, Outbound,
        Proposal, ProposalId, ProposalKeyPair, ProposalScope, Qos, Slot, SlotLifecycle,
        SlotOutOfBound, Timestamp, TimestampDelta, ValidatorData, WirePriority, unix_seconds,
    },
    wire::proposal::AuthoredProposal,
};

// What this node needs to take part in the DA of one slot: its id, its
// proposal key and the validators of the slot.
#[derive(Clone)]
pub struct DAContext {
    pub self_id: NodeId,
    pub key_pair: Arc<ProposalKeyPair>,
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

// the scheme family our proposals are encoded with
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProposalScheme {
    D33,
    // d33 without chunk data on the wire, for simulations
    #[cfg(feature = "enable_hollow")]
    D33Hollow,
    // a Byzantine proposer for simulations: d33 hollow proposals that
    // declare a root their message does not encode to, so every node
    // resolves them invalid. Set in code only, never from a config file.
    #[cfg(feature = "enable_hollow")]
    D33HollowInconsistent,
}

impl DAConfig {
    pub(crate) fn timing(&self) -> Timing {
        Timing {
            pull_delay: self.delta,
            pull_interval: self.delta * 2,
            serve_window: self.delta,
        }
    }
}

// when a proposer's pulls happen, and how often its requests are served
#[derive(Clone, Copy)]
pub(crate) struct Timing {
    // from the slot deadline, when owners release their relays
    pub(crate) pull_delay: TimestampDelta,
    pub(crate) pull_interval: TimestampDelta,
    // shorter than pull_interval, so jittered honest requests still fit
    pub(crate) serve_window: TimestampDelta,
}

pub struct DARuntime {
    config: DAConfig,
    credential: NodeCredential,
    epoch_handle: EpochHandle,
    raptorcast_map: BTreeMap<Slot, SlotRaptorcast>,

    // the next pass over every slot's pulls
    next_tick: Timestamp,

    outbox: VecDeque<DAOutput>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChunkPull {
    pub slot: Slot,
    pub lane: Lane,
    pub request: ChunkRequest,
}

impl DARuntime {
    pub fn new(config: DAConfig, credential: &NodeCredential, epoch_handle: EpochHandle) -> Self {
        Self {
            config,
            credential: credential.clone(),
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
        let slot = envelope.header().slot();
        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return Err(InvalidProposalHeader::SlotOutOfRange);
        };

        slot_raptorcast.ingest(sender, envelope)?;
        self.collect(slot);
        Ok(())
    }

    // our proposal. With a send order, the first hop goes out validator by
    // validator in that order (`AuthoredProposal::into_disseminations_in`);
    // without, in chunk-id order
    pub fn propose(
        &mut self,
        proposal_scheme: ProposalScheme,
        proposal: Proposal,
        send_order: Option<&[NodeId]>,
        now: Timestamp,
    ) {
        let Ok(context) = self.context_at(proposal.slot) else {
            tracing::warn!(slot = ?proposal.slot, "no validators for the slot of our proposal");
            return;
        };
        let unix_ts = unix_seconds(now);
        let authored = AuthoredProposal::build(proposal_scheme, &context, &proposal, unix_ts);
        let Some(mut authored) = authored else {
            tracing::warn!("proposal does not fit the scheme");
            return;
        };

        let self_id = self.credential.node_id;
        if let Some(own_envelope) = authored.split_off(&self_id) {
            self.ingest_own(own_envelope);
        }

        // optimization: ingest all chunks for early decode, so peers
        // don't have to relay back to author.
        self.ingest_own(authored.envelope());

        let disseminations = match send_order {
            Some(order) => authored.into_disseminations_in(order),
            None => authored.into_disseminations(),
        };
        for dissemination in disseminations {
            self.outbox.push_back(DAOutput::Send(dissemination));
        }
    }

    fn ingest_own(&mut self, envelope: ProposalEnvelope) {
        let self_id = self.credential.node_id;
        if let Err(reason) = self.ingest(&self_id, envelope) {
            tracing::error!(?reason, "rejected our own proposal envelope");
        }
    }

    // move the slot's pending outputs to the outbox
    fn collect(&mut self, slot: Slot) {
        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return;
        };

        let mut outputs = Vec::new();
        for (to, message) in slot_raptorcast.drain_requests() {
            let qos = Qos {
                priority: WirePriority::High,
                repeat_until: None,
            };
            outputs.push(DAOutput::Pull(Outbound { to, message, qos }));
        }
        for (lane, pid, message) in slot_raptorcast.drain_resolved() {
            let scope = ProposalScope::new(slot, lane);
            outputs.push(DAOutput::Resolved {
                scope,
                pid,
                message,
            });
        }
        for event in slot_raptorcast.drain_events() {
            outputs.push(DAOutput::Consensus(slot, event));
        }
        for message in slot_raptorcast.drain_messages() {
            outputs.push(DAOutput::Send(message));
        }
        self.outbox.extend(outputs);
    }

    // the first event of a slot. Consensus opens each slot once
    fn open_slot(&mut self, slot: Slot, deadline: Timestamp) {
        if self.raptorcast_map.contains_key(&slot) {
            tracing::warn!(?slot, "ignoring a second opening of a slot");
            return;
        }
        // Consensus opens a slot only where its epoch is known.
        let context = self
            .context_at(slot)
            .expect("no validators for an open slot");
        let proposer_set = self
            .epoch_handle
            .proposers_at(slot)
            .expect("no proposer set for an open slot");
        let timing = self.config.timing();
        let slot_raptorcast = SlotRaptorcast::new(context, slot, deadline, &proposer_set, timing);
        self.raptorcast_map.insert(slot, slot_raptorcast);
    }

    fn context_at(&self, slot: Slot) -> Result<DAContext, SlotOutOfBound> {
        let validator_data = self.epoch_handle.validators_at(slot)?;
        Ok(DAContext {
            self_id: self.credential.node_id,
            key_pair: self.credential.proposal_key.clone(),
            validator_data,
        })
    }

    pub fn handle_chunk_request(&mut self, req: ChunkPull, from: &NodeId, now: Timestamp) {
        let ChunkPull {
            slot,
            lane,
            request,
        } = req;

        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            tracing::debug!(?slot, "dropping a chunk request for a slot not open");
            return;
        };

        slot_raptorcast.handle_chunk_request(request, lane, from, now);
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

    // the message of the proposal (slot, lane, pid), or why it has none,
    // for the layer that recovers proposals
    Resolved {
        scope: ProposalScope,
        pid: ProposalId,
        message: Resolved,
    },

    // deliver a proposal's chunks (possibly only its header) to peers
    Send(Dissemination),

    // request chunks from a peer
    Pull(Outbound<ChunkPull, NodeId>),
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        super::{
            chunk::ChunkRequestType,
            test_util::{
                MESSAGE_LEN, NOW, SLOT, author, context, da_config, epoch_handle, group,
                proposal_chunks, proposal_chunks_from,
            },
            types::{NodeCredential, ProposalDAEvent, Timestamp},
        },
        *,
    };

    // the runtime of validator 1 among 4, where validator 0 proposes
    fn runtime(retention: u64) -> DARuntime {
        let config = DAConfig {
            completed_slot_retention: retention,
            ..da_config()
        };
        let epoch_handle = epoch_handle(4, vec![author()]);
        DARuntime::new(config, &NodeCredential::dummy(1), epoch_handle)
    }

    fn open(runtime: &mut DARuntime, slots: impl IntoIterator<Item = u64>) {
        for slot in slots {
            let opened = SlotLifecycle::Opened {
                slot: Slot(slot),
                deadline: Timestamp::GENESIS,
            };
            runtime.handle_slot_lifecycle(opened);
        }
    }

    fn outputs(runtime: &mut DARuntime) -> Vec<DAOutput> {
        let mut outputs = Vec::new();
        while let Some(output) = runtime.poll() {
            outputs.push(output);
        }
        outputs
    }

    #[test]
    fn ingestion_is_bounded_by_the_opened_slots() {
        let mut runtime = runtime(1);
        let context = context();
        let (_, chunks) = proposal_chunks(&context, 1);

        let closed = runtime.ingest(&author(), group(&chunks[..1]));
        assert_eq!(closed, Err(InvalidProposalHeader::SlotOutOfRange));

        open(&mut runtime, [0, 1]);
        assert_eq!(runtime.ingest(&author(), group(&chunks[..1])), Ok(()));

        let (_, later) = proposal_chunks_from(&context, 0, Slot(5), 1);
        let beyond = runtime.ingest(&author(), group(&later[..1]));
        assert_eq!(beyond, Err(InvalidProposalHeader::SlotOutOfRange));
    }

    #[test]
    fn slots_below_the_cap_are_kept_for_the_retention_then_dropped() {
        let mut runtime = runtime(1);
        let context = context();
        open(&mut runtime, [0, 1, 2]);
        let (_, slot0) = proposal_chunks_from(&context, 0, Slot(0), 1);
        let (_, slot1) = proposal_chunks_from(&context, 0, Slot(1), 1);
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
        let context = context();
        open(&mut runtime, [0, 1]);
        runtime.handle_command(ChorusDACommand::ReleaseChunks, SLOT, NOW);
        let (header, chunks) = proposal_chunks(&context, 1);
        runtime
            .ingest(&author(), group(&chunks[..3]))
            .expect("valid");

        let mut decoded_at = None;
        let mut consensus_decoded_at = None;
        let mut disseminated_at = Vec::new();
        for (i, output) in outputs(&mut runtime).iter().enumerate() {
            match output {
                DAOutput::Resolved {
                    scope,
                    pid,
                    message,
                } => {
                    assert_eq!(
                        (scope.slot, scope.lane, *pid),
                        (SLOT, Lane::new_unchecked(0), header.pid())
                    );
                    assert_eq!(message, &Ok(Bytes::from(vec![1u8; MESSAGE_LEN])));
                    decoded_at = Some(i);
                }
                DAOutput::Consensus(slot, event) => {
                    assert_eq!(*slot, SLOT);
                    if event.event == ProposalDAEvent::Decoded(header.pid()) {
                        consensus_decoded_at = Some(i);
                    }
                }
                DAOutput::Send(dissemination) => {
                    // we did not propose, so nothing is prioritized
                    assert_eq!(dissemination.qos, Qos::default());
                    // our chunk 2 on arrival and 6 once derivable, one message
                    let ids: Vec<_> = dissemination.message.chunk_data().keys().copied().collect();
                    assert_eq!(ids, [2, 6]);
                    disseminated_at.push(i);
                }
                DAOutput::Pull(_) => panic!("nothing to recover"),
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
        let context = context();
        open(&mut runtime, [0, 1]);
        let (header, _) = proposal_chunks(&context, 1);

        let unknown_slot = ChunkPull {
            slot: Slot(7),
            lane: Lane::new_unchecked(0),
            request: ChunkRequest::all(header.pid(), ChunkRequestType::YourChunks),
        };
        runtime.handle_chunk_request(unknown_slot, &NodeId::dummy(2), NOW);
        assert!(outputs(&mut runtime).is_empty());
    }

    #[test]
    fn pulls_leave_as_recovery_requests_once_due() {
        let mut runtime = runtime(1);
        let context = context();
        let (header, chunks) = proposal_chunks(&context, 1);
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
            .ingest(&author(), group(&chunks[..1]))
            .expect("valid");
        outputs(&mut runtime);

        // within the grace period nothing is asked
        runtime.handle_tick(at(100));
        assert_eq!(runtime.next_tick(), at(200));
        assert!(outputs(&mut runtime).is_empty());

        // then the author for ours and for its own, and every other owner
        // for theirs
        runtime.handle_tick(at(200));
        let mut peers = Vec::new();
        for output in outputs(&mut runtime) {
            let DAOutput::Pull(outbound) = output else {
                continue;
            };
            let request = outbound.message;
            assert_eq!((request.slot, request.lane), (SLOT, Lane::new_unchecked(0)));
            assert_eq!(request.request.pid, header.pid());
            peers.push(outbound.to);
        }
        peers.sort();
        assert_eq!(
            peers,
            [
                NodeId::dummy(0),
                NodeId::dummy(0),
                NodeId::dummy(2),
                NodeId::dummy(3)
            ]
        );
    }
}
