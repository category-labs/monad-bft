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

use monad_mcp_chorus::spec::{Deserializable as _, Serializable as _};

use crate::{
    chorus::{
        CadenceMessage, SlotLifecycle,
        slot::chorus::ChorusMessage,
        types::{Timestamp, TimestampDelta, Validated},
    },
    component::{
        CadenceInput, CadenceOutput, CadenceWireMsg, DAInput, Dispatch, ProposingInput,
        ProposingOutput, Recipients, RepeaterInput, RepeaterOutput,
    },
    da::{
        AssembledProposal, ChunkRecoveryRequest, DAOutput, Dissemination, read_envelope,
        write_envelope,
    },
    epoch::EpochHandle,
    finalization::{FinalizationCollector, FinalizedSlot},
    network::{Inbound, Outbound, Packet},
};

// everything the wiring can emit, named by where it goes
pub enum Effect {
    Cadence(CadenceInput),
    DA(DAInput),
    Proposing(ProposingInput),
    Repeater(RepeaterInput<ChorusMessage>),
    Network(Outbound),
    // todo: the ledger / execution boundary
    Ledger(FinalizedSlot),
}

// the wiring: one component's output in, the effects that follow out.
// A scenario wraps the node runtime to tamper with the effects.
pub trait Runtime {
    fn handle_inbound(&mut self, inbound: Inbound, effects: &mut impl Dispatch<Effect>);
    fn handle_cadence(&mut self, output: CadenceOutput, effects: &mut impl Dispatch<Effect>);
    fn handle_da(&mut self, output: DAOutput, effects: &mut impl Dispatch<Effect>);
    fn handle_proposal(
        &mut self,
        now: Timestamp,
        proposal: ProposingOutput,
        effects: &mut impl Dispatch<Effect>,
    );
    fn handle_repeat(
        &mut self,
        repeat: RepeaterOutput<ChorusMessage>,
        effects: &mut impl Dispatch<Effect>,
    );
}

pub struct NodeRuntime {
    epoch_handle: EpochHandle,
    collector: FinalizationCollector,
}

impl NodeRuntime {
    pub fn new(epoch_handle: EpochHandle) -> Self {
        Self {
            epoch_handle,
            collector: FinalizationCollector::default(),
        }
    }

    fn handle_cadence_outbound(
        &mut self,
        event: crate::chorus::Outbound<CadenceWireMsg>,
        effects: &mut impl Dispatch<Effect>,
    ) {
        match event {
            crate::chorus::Outbound::Broadcast(message) => {
                record(Recipients::Everyone, &message, effects);
                let packet = Packet::Cadence(message.serialize());
                effects.dispatch(Effect::Network(Outbound::Broadcast(packet)));
                self.loopback(message, effects);
            }

            crate::chorus::Outbound::Unicast(to, message) => {
                if to == self.epoch_handle.self_id {
                    self.loopback(message, effects);
                    return;
                }
                record(Recipients::Node(to), &message, effects);
                let packet = Packet::Cadence(message.serialize());
                effects.dispatch(Effect::Network(Outbound::Unicast(to, packet)));
            }
        }
    }

    // a message we authored needs no validation
    fn loopback(&self, message: CadenceWireMsg, effects: &mut impl Dispatch<Effect>) {
        let message = Validated::new_unchecked(message, self.epoch_handle.self_id);
        effects.dispatch(Effect::Cadence(CadenceInput::Message(message)));
    }

    fn deliver_finalized(&mut self, effects: &mut impl Dispatch<Effect>) {
        while let Some(finalized) = self.collector.poll() {
            effects.dispatch(Effect::Ledger(finalized));
        }
    }
}

impl Runtime for NodeRuntime {
    fn handle_inbound(&mut self, inbound: Inbound, effects: &mut impl Dispatch<Effect>) {
        let Inbound { from, packet } = inbound;
        match packet {
            Packet::Cadence(bytes) => {
                let Ok(message) = CadenceWireMsg::deserialize(&bytes) else {
                    tracing::debug!(?from, "malformed cadence message");
                    return;
                };
                // todo: signature checks for a transport that does
                // not authenticate the sender.
                let message = Validated::new_unchecked(message, from);

                // todo: verify slot number against current state
                effects.dispatch(Effect::Cadence(CadenceInput::Message(message)));
            }

            Packet::Chunk(bytes) => {
                let Ok(envelope) = read_envelope(bytes) else {
                    tracing::debug!(?from, "malformed chunk packet");
                    return;
                };
                effects.dispatch(Effect::DA(DAInput::Envelope(envelope)));
            }

            Packet::ChunkRequest(bytes) => {
                let Ok(request) = ChunkRecoveryRequest::deserialize(&bytes) else {
                    tracing::debug!(?from, "malformed chunk request");
                    return;
                };
                effects.dispatch(Effect::DA(DAInput::ChunkRequest(from, request)));
            }
        }
    }

    fn handle_cadence(&mut self, output: CadenceOutput, effects: &mut impl Dispatch<Effect>) {
        match output {
            CadenceOutput::Outbound(event) => {
                self.handle_cadence_outbound(event, effects);
            }

            CadenceOutput::Lifecycle(event) => {
                effects.dispatch(Effect::DA(DAInput::Lifecycle(event)));
                match event {
                    SlotLifecycle::Opened { slot, deadline } => {
                        let input = ProposingInput::SlotOpen(slot, deadline);
                        effects.dispatch(Effect::Proposing(input));
                    }
                    SlotLifecycle::Completed { slot } => {
                        effects.dispatch(Effect::Repeater(RepeaterInput::Completed(slot)));
                    }
                    SlotLifecycle::CapAdvance { .. } => {}
                }
                self.collector.handle_lifecycle(event);
            }

            CadenceOutput::DACommand(slot, command) => {
                effects.dispatch(Effect::DA(DAInput::Command(slot, command)));
            }

            CadenceOutput::Finalized(now, slot, finalization) => {
                let committed = finalization.roots().into_iter().flatten().count();
                let path = finalization.path();
                tracing::debug!(slot = slot.0, ?path, committed, "cadence finalized");
                let certificate = finalization.certificate_message();
                effects.dispatch(Effect::Repeater(RepeaterInput::Finalization(
                    slot,
                    certificate,
                )));
                self.collector.handle_finalization(now, slot, finalization);
                self.deliver_finalized(effects);
            }

            CadenceOutput::CapAdvance(now, cap) => {
                tracing::debug!(cap = cap.0, at = now.as_nanos(), "chain advanced");
                effects.dispatch(Effect::Repeater(RepeaterInput::CapAdvance(cap)));
                effects.dispatch(Effect::Proposing(ProposingInput::CapAdvance(cap)));
            }
        }
    }

    fn handle_repeat(
        &mut self,
        (to, slot, message): RepeaterOutput<ChorusMessage>,
        effects: &mut impl Dispatch<Effect>,
    ) {
        let message: CadenceWireMsg = CadenceMessage::Slot(slot, message);
        let packet = Packet::Cadence(message.serialize());
        let outbound = match to {
            Recipients::Everyone => Outbound::Broadcast(packet),
            Recipients::Node(to) => Outbound::Unicast(to, packet),
        };
        effects.dispatch(Effect::Network(outbound));
    }

    // first hop of a proposal released at (slot, index): our own share
    // goes to our DA, the rest to its owners
    fn handle_proposal(
        &mut self,
        now: Timestamp,
        (slot, index, message): ProposingOutput,
        effects: &mut impl Dispatch<Effect>,
    ) {
        let self_id = self.epoch_handle.self_id;
        let epoch_handle = self.epoch_handle.da();
        let unix_ts = unix_seconds(now);

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
            effects.dispatch(Effect::DA(DAInput::Envelope(own_share)));
        }
        for (to, packet) in first_hop.into_packets() {
            let packet = Packet::Chunk(packet);
            effects.dispatch(Effect::Network(Outbound::Unicast(*to, packet)));
        }
    }

    fn handle_da(&mut self, output: DAOutput, effects: &mut impl Dispatch<Effect>) {
        match output {
            DAOutput::Consensus(slot, event) => {
                effects.dispatch(Effect::Cadence(CadenceInput::DAEvent(slot, event)));
            }
            DAOutput::Decoded {
                slot,
                proposal_index,
                root,
                message,
            } => {
                self.collector
                    .handle_decoded(slot, proposal_index, root, message);
                self.deliver_finalized(effects);
            }
            DAOutput::Disseminate(Dissemination { to, envelope }) => {
                let packets = write_envelope(&envelope);
                for node in to {
                    for packet in &packets {
                        let packet = Packet::Chunk(packet.clone());
                        effects.dispatch(Effect::Network(Outbound::Unicast(node, packet)));
                    }
                }
            }
            DAOutput::RecoveryRequest { to, request } => {
                let packet = Packet::ChunkRequest(request.serialize());
                effects.dispatch(Effect::Network(Outbound::Unicast(to, packet)));
            }
        }
    }
}

// only slot messages are repeated
fn record(to: Recipients, message: &CadenceWireMsg, effects: &mut impl Dispatch<Effect>) {
    let CadenceMessage::Slot(slot, message) = message else {
        return;
    };
    let input = RepeaterInput::Record(*slot, to, message.clone());
    effects.dispatch(Effect::Repeater(input));
}

// todo: confirm the header's unix_ts unit
fn unix_seconds(now: Timestamp) -> u64 {
    let nanos_per_second = u128::from(TimestampDelta::NANOS_PER_MILLISECOND) * 1_000;
    (now.as_nanos() / nanos_per_second) as u64
}
