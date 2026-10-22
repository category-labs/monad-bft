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

use monad_mcp_chorus::{
    ledger::Tx,
    spec::{Deserializable as _, Serializable as _},
};

use crate::{
    chorus::{CadenceMessage, SlotLifecycle, slot::chorus::ChorusMessage, types::Timestamp},
    component::{
        CadenceInput, CadenceOutput, CadenceWireMsg, DAInput, Dispatch, FinalizationInput,
        ProposingInput, ProposingOutput, Recipients, RepeaterInput, RepeaterOutput,
    },
    da::{ChunkRecoveryRequest, DAOutput, Dissemination, read_envelope, write_envelope},
    epoch::EpochHandle,
    finalization::FinalizedSlot,
    network::{Inbound, Outbound, Packet},
};

// everything the wiring can emit, named by where it goes
pub enum Effect {
    Cadence(CadenceInput),
    DA(DAInput),
    Finalization(FinalizationInput),
    Proposing(ProposingInput),
    Repeater(RepeaterInput<ChorusMessage>),
    Network(Outbound),
    // todo: the ledger / execution boundary
    Ledger(FinalizedSlot),
}

// the wiring: one component's output in, the effects that follow out.
// A scenario wraps the node runtime to tamper with the effects.
pub trait Runtime {
    // demo(tx-timeline): `now` stamps mempool admission
    fn handle_inbound(
        &mut self,
        now: Timestamp,
        inbound: Inbound,
        effects: &mut impl Dispatch<Effect>,
    );
    fn handle_cadence(&mut self, output: CadenceOutput, effects: &mut impl Dispatch<Effect>);
    // demo(tx-timeline): `now` stamps lane decodes
    fn handle_da(&mut self, now: Timestamp, output: DAOutput, effects: &mut impl Dispatch<Effect>);
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
    fn handle_finalized(&mut self, finalized: FinalizedSlot, effects: &mut impl Dispatch<Effect>);
}

pub struct NodeRuntime {
    epoch_handle: EpochHandle,
}

impl NodeRuntime {
    pub fn new(epoch_handle: EpochHandle) -> Self {
        Self { epoch_handle }
    }

    pub fn epoch_handle(&self) -> &EpochHandle {
        &self.epoch_handle
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
        let self_id = self.epoch_handle.self_id;
        effects.dispatch(Effect::Cadence(CadenceInput::Message(self_id, message)));
    }
}

impl Runtime for NodeRuntime {
    fn handle_inbound(
        &mut self,
        now: Timestamp,
        inbound: Inbound,
        effects: &mut impl Dispatch<Effect>,
    ) {
        let Inbound { from, packet } = inbound;
        match packet {
            Packet::Cadence(bytes) => {
                let Ok(message) = CadenceWireMsg::deserialize(&bytes) else {
                    tracing::debug!(?from, "malformed cadence message");
                    return;
                };
                // todo: signature checks for a transport that does
                // not authenticate the sender.

                // todo: verify slot number against current state
                effects.dispatch(Effect::Cadence(CadenceInput::Message(from, message)));
            }

            Packet::Chunk(bytes) => {
                let Ok(envelope) = read_envelope(bytes) else {
                    tracing::debug!(?from, "malformed chunk packet");
                    return;
                };
                effects.dispatch(Effect::DA(DAInput::Envelope(from, envelope)));
            }

            Packet::ChunkRequest(bytes) => {
                let Ok(request) = ChunkRecoveryRequest::deserialize(&bytes) else {
                    tracing::debug!(?from, "malformed chunk request");
                    return;
                };
                effects.dispatch(Effect::DA(DAInput::ChunkRequest(from, request)));
            }

            Packet::Tx(bytes) => {
                let tx = match Tx::decode_exact(&bytes) {
                    Ok(tx) => tx,
                    Err(error) => {
                        tracing::debug!(?from, %error, "malformed tx");
                        return;
                    }
                };
                let input = ProposingInput::Tx { at: now, from, tx }; // demo(tx-timeline)
                effects.dispatch(Effect::Proposing(input));
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
                let input = FinalizationInput::Lifecycle(event);
                effects.dispatch(Effect::Finalization(input));
            }

            CadenceOutput::DACommand(slot, command) => {
                effects.dispatch(Effect::DA(DAInput::Command(slot, command)));
            }

            CadenceOutput::Finalized(now, slot, finalization) => {
                let committed = finalization.pids().into_iter().flatten().count();
                let path = finalization.path();
                tracing::debug!(slot = slot.0, ?path, committed, "cadence finalized");
                let positive = finalization.pids().map(|pid| pid.is_some());
                let input = ProposingInput::Finalized(slot, positive);
                effects.dispatch(Effect::Proposing(input));
                let certificate = finalization.certificate_message();
                effects.dispatch(Effect::Repeater(RepeaterInput::Finalization(
                    slot,
                    certificate,
                )));
                let input = FinalizationInput::Finalized(now, slot, finalization);
                effects.dispatch(Effect::Finalization(input));
            }

            // demo(tx-timeline)
            CadenceOutput::FastBlock(now, slot) => {
                let input = FinalizationInput::FastBlock(now, slot);
                effects.dispatch(Effect::Finalization(input));
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

    fn handle_proposal(
        &mut self,
        _now: Timestamp,
        proposal: ProposingOutput,
        effects: &mut impl Dispatch<Effect>,
    ) {
        tracing::info!(
            ?proposal.slot,
            ?proposal.lane,
            len = proposal.payload.len(),
            "proposing"
        );
        let input = DAInput::Propose(proposal);
        effects.dispatch(Effect::DA(input));
    }

    fn handle_da(&mut self, now: Timestamp, output: DAOutput, effects: &mut impl Dispatch<Effect>) {
        match output {
            DAOutput::Consensus(slot, event) => {
                effects.dispatch(Effect::Cadence(CadenceInput::DAEvent(slot, event)));
            }
            DAOutput::Resolved {
                slot,
                lane,
                pid,
                message,
            } => {
                let input = FinalizationInput::Resolved {
                    at: now, // demo(tx-timeline)
                    slot,
                    lane,
                    pid,
                    message,
                };
                effects.dispatch(Effect::Finalization(input));
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

    fn handle_finalized(&mut self, finalized: FinalizedSlot, effects: &mut impl Dispatch<Effect>) {
        effects.dispatch(Effect::Ledger(finalized));
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        chorus::types::NodeId,
        config::{NodeConfig, SourceKind},
        node::Node,
    };

    fn tx(nonce: u64) -> Tx {
        Tx {
            sender: [5; 20],
            nonce,
            payload: Bytes::from_static(b"demux"),
            sent_at_ns: 0,             // demo(tx-timeline)
            rpc_received_at_ns: 0,     // demo(tx-timeline)
            mempool_admitted_at_ns: 0, // demo(tx-timeline)
        }
    }

    fn now() -> Timestamp {
        Timestamp::from_millis(1_000)
    }

    fn config(source: SourceKind) -> NodeConfig {
        let mut config =
            NodeConfig::single_node(0, Timestamp::from_millis(10_000), "unused-ledger");
        config.proposal.source = source;
        config
    }

    fn runtime_of(config: &NodeConfig) -> NodeRuntime {
        Node::new(config).unwrap().runtime
    }

    fn inbound(packet: Packet) -> Inbound {
        Inbound {
            from: NodeId::dummy(0),
            packet,
        }
    }

    #[test]
    fn a_tx_packet_goes_to_proposing_and_junk_goes_nowhere() {
        let mut runtime = runtime_of(&config(SourceKind::Mempool));
        let mut effects = Vec::new();
        let junk = Packet::Tx(Bytes::from_static(b"junk"));
        runtime.handle_inbound(now(), inbound(junk), &mut effects);
        assert!(effects.is_empty());

        runtime.handle_inbound(now(), inbound(Packet::Tx(tx(1).to_rlp())), &mut effects);
        // never forwarded: the rpc already chose this node
        let [Effect::Proposing(ProposingInput::Tx { at, tx, .. })] = &effects[..] else {
            panic!("one tx for proposing");
        };
        assert_eq!(*at, now()); // demo(tx-timeline)
        assert_eq!(tx.nonce, 1);
    }
}
