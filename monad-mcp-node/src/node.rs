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
    future::pending,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use monad_mcp_chorus::spec::{Deserializable as _, Serializable as _};
use tokio::{
    task::JoinHandle,
    time::{Interval, MissedTickBehavior, interval},
};

use crate::{
    RunError,
    cadence_task::{CadenceInput, CadenceOutput, CadenceTask, CadenceWireMsg},
    chorus::{
        CadenceMessage, CadenceRuntime, SlotLifecycle, SlotManager,
        conductor::MonadConductor,
        proposing::ProposalPlanner,
        slot::chorus::ChorusMessage,
        types::{ProposalIndex, Slot, Timestamp, TimestampDelta, Validated},
    },
    config::NodeConfig,
    da::{
        AssembledProposal, ChunkRecoveryRequest, DAOutput, DARuntime, Dissemination, read_envelope,
        write_envelope,
    },
    da_task::{DAInput, DATask},
    epoch::EpochHandle,
    finalization::{FinalizationCollector, FinalizedSlot},
    network::{Inbound, Link, NetworkHandle, Outbound, Packet},
    proposing_task::{ProposalCreation, ProposingInput, ProposingOutput, ProposingTask},
    repeater::{Recipients, Repeater},
};

// monotonic since start, anchored to unix time at start
#[derive(Clone, Copy)]
pub struct Clock {
    unix_at_start: Timestamp,
    start: Instant,
}

impl Clock {
    pub fn start() -> Self {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is past 1970");
        Self {
            unix_at_start: Timestamp::from_nanos(since_epoch.as_nanos()),
            start: Instant::now(),
        }
    }

    pub fn now(&self) -> Timestamp {
        let elapsed = self.start.elapsed().as_nanos();
        Timestamp::from_nanos(self.unix_at_start.as_nanos() + elapsed)
    }

    // the instant of a timestamp, now if already past
    pub fn instant_of(&self, at: Timestamp) -> Instant {
        let Some(since_start) = at.duration_since(self.unix_at_start) else {
            return Instant::now();
        };
        self.start + since_start.as_duration()
    }
}

// todo: the ledger / execution boundary
type FinalizationLogger = Box<dyn FnMut(FinalizedSlot) + Send>;

// wires the network, the cadence task, the DA task and the proposing
// task together
pub struct Node {
    epoch_handle: EpochHandle,
    clock: Clock,
    collector: FinalizationCollector,
    // re-sends slot messages over the lossy transport; off without a config
    repeater: Option<Repeater<ChorusMessage>>,
    repeater_tick: Option<Interval>,
    // outbound is dropped until set
    network: Option<NetworkHandle>,
    finalization: Option<FinalizationLogger>,

    cadence: Link<CadenceInput, CadenceOutput>,
    cadence_task: JoinHandle<()>,

    da: Link<DAInput, DAOutput>,
    da_task: JoinHandle<()>,

    proposing: Link<ProposingInput, ProposingOutput>,
    proposing_task: JoinHandle<()>,
}

impl Node {
    pub fn new(config: &NodeConfig) -> Result<Self, RunError> {
        let clock = Clock::start();
        let epoch_handle = config.epoch_handle()?;

        let slot_config = config.cadence.chorus();
        let slot_manager = SlotManager::new(slot_config, epoch_handle.chorus());
        let conductor_config = config.cadence.conductor(config.genesis_deadline)?;
        let conductor = MonadConductor::genesis(conductor_config, ())?;
        let cadence = CadenceRuntime::new(slot_manager, conductor);
        let (cadence_link, task_link) = Link::pair();
        let cadence_task = CadenceTask::spawn(cadence, clock, task_link);

        let da = DARuntime::new(
            config.da.runtime(),
            epoch_handle.da(),
            epoch_handle.proposers.clone(),
        );
        let (da_link, task_link) = Link::pair();
        let da_task = DATask::spawn(da, task_link);

        let creation = <ProposalPlanner as ProposalCreation>::new(&epoch_handle, &config.proposal);
        let (proposing_link, task_link) = Link::pair();
        let proposing_task = ProposingTask::spawn(creation, clock, task_link);

        let repeater_config = config.repeater();
        let repeater_tick = repeater_config.map(|config| {
            let mut tick = interval(config.interval.as_duration());
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tick
        });

        Ok(Self {
            epoch_handle,
            clock,
            collector: FinalizationCollector::default(),
            repeater: repeater_config.map(Repeater::new),
            repeater_tick,
            network: None,
            finalization: None,
            cadence: cadence_link,
            cadence_task,
            da: da_link,
            da_task,
            proposing: proposing_link,
            proposing_task,
        })
    }

    pub fn set_network(&mut self, network: NetworkHandle) {
        self.network = Some(network);
    }

    pub fn on_finalization(&mut self, logger: impl FnMut(FinalizedSlot) + Send + 'static) {
        self.finalization = Some(Box::new(logger));
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(inbound) = recv_inbound(&mut self.network) => self.handle_inbound(inbound),
                Some(output) = self.cadence.recv() => self.handle_cadence_output(output),
                Some(output) = self.da.recv() => self.handle_da_output(output),
                Some((slot, index, message)) = self.proposing.recv() => self.propose(slot, index, message),
                _ = next_tick(&mut self.repeater_tick) => self.repeat(),
                else => return,
            }
        }
    }

    fn send(&self, outbound: Outbound) {
        if let Some(network) = &self.network {
            network.send(outbound);
        }
    }

    fn handle_inbound(&mut self, inbound: Inbound) {
        let Inbound { from, packet } = inbound;
        match packet {
            Packet::Cadence(bytes) => {
                let Ok(message) = CadenceWireMsg::deserialize(&bytes) else {
                    tracing::debug!(?from, "malformed cadence message");
                    return;
                };
                // todo: signature checks for a transport that does
                // not authenticate the sender
                let message = Validated::new_unchecked(message, from);
                self.cadence.send(CadenceInput::Message(message));
            }
            Packet::Chunk(bytes) => {
                let Ok(envelope) = read_envelope(bytes) else {
                    tracing::debug!(?from, "malformed chunk packet");
                    return;
                };
                self.da.send(DAInput::Envelope(envelope));
            }
            Packet::ChunkRequest(bytes) => {
                let Ok(request) = ChunkRecoveryRequest::deserialize(&bytes) else {
                    tracing::debug!(?from, "malformed chunk request");
                    return;
                };
                self.da.send(DAInput::ChunkRequest(from, request));
            }
        }
    }

    fn handle_cadence_output(&mut self, output: CadenceOutput) {
        match output {
            CadenceOutput::Outbound(event) => self.handle_outbound(event),
            CadenceOutput::Lifecycle(slot, event) => {
                self.da.send(DAInput::Lifecycle(slot, event));
                if let SlotLifecycle::Opened { deadline } = event {
                    self.proposing
                        .send(ProposingInput::SlotOpen(slot, deadline));
                }
                self.collector.handle_lifecycle(slot, event);
                if let (SlotLifecycle::Completed, Some(repeater)) = (event, &mut self.repeater) {
                    repeater.handle_completed(slot);
                }
            }
            CadenceOutput::DACommand(slot, command) => {
                self.da.send(DAInput::Command(slot, command));
            }
            CadenceOutput::Finalized(now, slot, finalization) => {
                let committed = finalization.roots().into_iter().flatten().count();
                let path = finalization.path();
                tracing::debug!(slot = slot.0, ?path, committed, "cadence finalized");
                if let Some(repeater) = &mut self.repeater {
                    repeater.handle_finalization(slot, finalization.certificate_message());
                }
                self.collector.handle_finalization(now, slot, finalization);
                self.deliver_finalized();
            }
            CadenceOutput::CapAdvance(now, cap) => {
                tracing::debug!(cap = cap.0, at = now.as_nanos(), "chain advanced");
                if let Some(repeater) = &mut self.repeater {
                    repeater.handle_cap_advance(cap);
                }
                self.proposing.send(ProposingInput::CapAdvance(cap));
            }
        }
    }

    fn handle_outbound(&mut self, event: crate::chorus::Outbound<CadenceWireMsg>) {
        match event {
            crate::chorus::Outbound::Broadcast(message) => {
                self.record(Recipients::Everyone, &message);
                let packet = Packet::Cadence(message.serialize());
                self.send(Outbound::Broadcast(packet));
                self.loopback(message);
            }
            crate::chorus::Outbound::Unicast(to, message) => {
                if to == self.epoch_handle.self_id {
                    self.loopback(message);
                    return;
                }
                self.record(Recipients::Node(to), &message);
                let packet = Packet::Cadence(message.serialize());
                self.send(Outbound::Unicast(to, packet));
            }
        }
    }

    fn record(&mut self, to: Recipients, message: &CadenceWireMsg) {
        let Some(repeater) = &mut self.repeater else {
            return;
        };
        let CadenceMessage::Slot(slot, message) = message else {
            return;
        };
        repeater.record(self.clock.now(), *slot, to, message);
    }

    fn repeat(&mut self) {
        let Some(repeater) = &mut self.repeater else {
            return;
        };
        for (to, slot, message) in repeater.due(self.clock.now()) {
            let message: CadenceWireMsg = CadenceMessage::Slot(slot, message);
            let packet = Packet::Cadence(message.serialize());
            match to {
                Recipients::Everyone => self.send(Outbound::Broadcast(packet)),
                Recipients::Node(to) => self.send(Outbound::Unicast(to, packet)),
            }
        }
    }

    // a message we authored needs no validation
    fn loopback(&self, message: CadenceWireMsg) {
        let message = Validated::new_unchecked(message, self.epoch_handle.self_id);
        self.cadence.send(CadenceInput::Message(message));
    }

    // first hop of a proposal the proposing task released at (slot, index):
    // our own share goes to our DA, the rest to its owners
    fn propose(&mut self, slot: Slot, index: ProposalIndex, message: Bytes) {
        let self_id = self.epoch_handle.self_id;
        let epoch_handle = self.epoch_handle.da();
        let unix_ts = unix_seconds(self.clock.now());

        // encode with swiper-11 scheme.
        let proposal = AssembledProposal::build_s11(&epoch_handle, slot, index, &message, unix_ts);
        let Some(proposal) = proposal else {
            tracing::warn!(
                ?slot,
                len = message.len(),
                "message does not fit the scheme"
            );
            return;
        };

        tracing::info!(slot = slot.0, index, len = message.len(), "proposing");
        let mut first_hop = proposal.disseminate();
        if let Some(own_share) = first_hop.split_off(&self_id) {
            self.da.send(DAInput::Envelope(own_share));
        }
        for (to, packet) in first_hop.into_packets() {
            let packet = Packet::Chunk(packet);
            self.send(Outbound::Unicast(*to, packet));
        }
    }

    fn handle_da_output(&mut self, output: DAOutput) {
        match output {
            DAOutput::Consensus(slot, event) => {
                self.cadence.send(CadenceInput::DAEvent(slot, event));
            }
            DAOutput::Decoded {
                slot,
                proposal_index,
                root,
                message,
            } => {
                self.collector
                    .handle_decoded(slot, proposal_index, root, message);
                self.deliver_finalized();
            }
            DAOutput::Disseminate(Dissemination { to, envelope }) => {
                let packets = write_envelope(&envelope);
                for node in to {
                    for packet in &packets {
                        let packet = Packet::Chunk(packet.clone());
                        self.send(Outbound::Unicast(node, packet));
                    }
                }
            }
            DAOutput::RecoveryRequest { to, request } => {
                let packet = Packet::ChunkRequest(request.serialize());
                self.send(Outbound::Unicast(to, packet));
            }
        }
    }

    fn deliver_finalized(&mut self) {
        while let Some(finalized) = self.collector.poll() {
            if let Some(logger) = &mut self.finalization {
                logger(finalized);
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.cadence_task.abort();
        self.da_task.abort();
        self.proposing_task.abort();
    }
}

// pending forever without a network
async fn next_tick(tick: &mut Option<Interval>) {
    let Some(tick) = tick else {
        return pending().await;
    };
    tick.tick().await;
}

async fn recv_inbound(network: &mut Option<NetworkHandle>) -> Option<Inbound> {
    match network {
        Some(network) => network.recv().await,
        None => pending().await,
    }
}

// todo: confirm the header's unix_ts unit
fn unix_seconds(now: Timestamp) -> u64 {
    let nanos_per_second = u128::from(TimestampDelta::NANOS_PER_MILLISECOND) * 1_000;
    (now.as_nanos() / nanos_per_second) as u64
}
