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
    chorus::{
        CadenceMessage, Outbound, SlotLifecycle, slot::chorus::ChorusMessage, types::Timestamp,
    },
    component::{
        CadenceInput, CadenceOutput, CadenceWireMsg, DAInput, FinalizationInput, ProposingInput,
        ProposingOutput, Recipients, RepeaterInput, RepeaterOutput,
    },
    da::{ChunkRecoveryRequest, DAOutput, Dissemination, read_envelope, write_envelope},
    epoch::EpochHandle,
    finalization::FinalizedSlot,
    network::{NodeInbound, NodeMessage, NodeOutbound},
};

// everything the wiring takes in, named by where it comes from
pub enum Effect {
    Network(NodeInbound),
    Cadence(CadenceOutput),
    DA(DAOutput),
    Proposing(ProposingOutput),
    Repeater(RepeaterOutput<ChorusMessage>),
    Finalization(FinalizedSlot),
}

// everything the wiring can emit, named by where it goes
pub enum Routed {
    Cadence(CadenceInput),
    DA(DAInput),
    Finalization(FinalizationInput),
    Proposing(ProposingInput),
    Repeater(RepeaterInput<ChorusMessage>),
    Network(NodeOutbound),
    // todo: the ledger / execution boundary
    Ledger(FinalizedSlot),
}

pub trait Sink<T> {
    fn send(&mut self, item: T);
}

impl<T> Sink<T> for Vec<T> {
    fn send(&mut self, item: T) {
        self.push(item);
    }
}

// The wiring of effects
pub trait Router {
    fn route(&mut self, now: Timestamp, effect: Effect, sink: &mut impl Sink<Routed>);
}

pub struct NodeRouter {
    epoch_handle: EpochHandle,
}

impl NodeRouter {
    pub fn new(epoch_handle: EpochHandle) -> Self {
        Self { epoch_handle }
    }

    pub fn epoch_handle(&self) -> &EpochHandle {
        &self.epoch_handle
    }

    fn route_cadence(&mut self, output: CadenceOutput, sink: &mut impl Sink<Routed>) {
        match output {
            CadenceOutput::Outbound(event) => {
                self.route_cadence_outbound(event, sink);
            }

            CadenceOutput::Lifecycle(event) => {
                sink.send(Routed::DA(DAInput::Lifecycle(event)));
                match event {
                    SlotLifecycle::Opened { slot, deadline } => {
                        let input = ProposingInput::SlotOpen(slot, deadline);
                        sink.send(Routed::Proposing(input));
                    }
                    SlotLifecycle::Completed { slot } => {
                        sink.send(Routed::Repeater(RepeaterInput::Completed(slot)));
                    }
                    SlotLifecycle::CapAdvance { .. } => {}
                }
                let input = FinalizationInput::Lifecycle(event);
                sink.send(Routed::Finalization(input));
            }

            CadenceOutput::DACommand(slot, command) => {
                sink.send(Routed::DA(DAInput::Command(slot, command)));
            }

            CadenceOutput::Finalized(now, slot, finalization) => {
                let committed = finalization.pids().into_iter().flatten().count();
                let path = finalization.path();
                tracing::debug!(slot = slot.0, ?path, committed, "cadence finalized");
                let positive = finalization.pids().map(|pid| pid.is_some());
                let input = ProposingInput::Finalized(slot, positive);
                sink.send(Routed::Proposing(input));
                let certificate = finalization.certificate_message();
                sink.send(Routed::Repeater(RepeaterInput::Finalization(
                    slot,
                    certificate,
                )));
                let input = FinalizationInput::Finalized(now, slot, finalization);
                sink.send(Routed::Finalization(input));
            }

            // demo(tx-timeline)
            CadenceOutput::FastBlock(now, slot) => {
                let input = FinalizationInput::FastBlock(now, slot);
                sink.send(Routed::Finalization(input));
            }

            CadenceOutput::CapAdvance(now, cap) => {
                tracing::debug!(cap = cap.0, at = now.as_nanos(), "chain advanced");
                sink.send(Routed::Repeater(RepeaterInput::CapAdvance(cap)));
                sink.send(Routed::Proposing(ProposingInput::CapAdvance(cap)));
            }
        }
    }

    fn route_cadence_outbound(
        &mut self,
        event: Outbound<CadenceWireMsg>,
        sink: &mut impl Sink<Routed>,
    ) {
        match event {
            Outbound::Broadcast(message) => {
                record(Recipients::Everyone, &message, sink);
                let node_message = NodeMessage::Cadence(message.serialize());
                sink.send(Routed::Network(Outbound::Broadcast(node_message)));
                self.loopback(message, sink);
            }

            Outbound::Unicast(to, message) => {
                if to == self.epoch_handle.self_id {
                    self.loopback(message, sink);
                    return;
                }
                record(Recipients::Node(to), &message, sink);
                let node_message = NodeMessage::Cadence(message.serialize());
                sink.send(Routed::Network(Outbound::Unicast(to, node_message)));
            }
        }
    }

    // a message we authored needs no validation
    fn loopback(&self, message: CadenceWireMsg, sink: &mut impl Sink<Routed>) {
        let self_id = self.epoch_handle.self_id;
        sink.send(Routed::Cadence(CadenceInput::Message(self_id, message)));
    }
}

impl Router for NodeRouter {
    fn route(&mut self, now: Timestamp, effect: Effect, sink: &mut impl Sink<Routed>) {
        match effect {
            Effect::Network(inbound) => route_inbound(now, inbound, sink),
            Effect::Cadence(output) => self.route_cadence(output, sink),
            Effect::DA(output) => route_da(now, output, sink),
            Effect::Proposing(proposal) => route_proposal(proposal, sink),
            Effect::Repeater(repeat) => route_repeat(repeat, sink),
            Effect::Finalization(finalized) => sink.send(Routed::Ledger(finalized)),
        }
    }
}

fn route_inbound(now: Timestamp, inbound: NodeInbound, sink: &mut impl Sink<Routed>) {
    let NodeInbound { from, message } = inbound;
    match message {
        NodeMessage::Cadence(bytes) => {
            let Ok(message) = CadenceWireMsg::deserialize(&bytes) else {
                tracing::debug!(?from, "malformed cadence message");
                return;
            };
            // todo: signature checks for a transport that does
            // not authenticate the sender.

            // todo: verify slot number against current state
            sink.send(Routed::Cadence(CadenceInput::Message(from, message)));
        }

        NodeMessage::Chunk(bytes) => {
            let Ok(envelope) = read_envelope(bytes) else {
                tracing::debug!(?from, "malformed chunk packet");
                return;
            };
            sink.send(Routed::DA(DAInput::Envelope(from, envelope)));
        }

        NodeMessage::ChunkRequest(bytes) => {
            let Ok(request) = ChunkRecoveryRequest::deserialize(&bytes) else {
                tracing::debug!(?from, "malformed chunk request");
                return;
            };
            sink.send(Routed::DA(DAInput::ChunkRequest(from, request)));
        }

        NodeMessage::Tx(bytes) => {
            let tx = match Tx::decode_exact(&bytes) {
                Ok(tx) => tx,
                Err(error) => {
                    tracing::debug!(?from, %error, "malformed tx");
                    return;
                }
            };
            let input = ProposingInput::Tx { at: now, from, tx }; // demo(tx-timeline)
            sink.send(Routed::Proposing(input));
        }
    }
}

fn route_repeat((to, slot, message): RepeaterOutput<ChorusMessage>, sink: &mut impl Sink<Routed>) {
    let message: CadenceWireMsg = CadenceMessage::Slot(slot, message);
    let node_message = NodeMessage::Cadence(message.serialize());
    let outbound = match to {
        Recipients::Everyone => Outbound::Broadcast(node_message),
        Recipients::Node(to) => Outbound::Unicast(to, node_message),
    };
    sink.send(Routed::Network(outbound));
}

fn route_proposal(proposal: ProposingOutput, sink: &mut impl Sink<Routed>) {
    tracing::info!(
        ?proposal.slot,
        ?proposal.lane,
        len = proposal.payload.len(),
        "proposing"
    );
    let input = DAInput::Propose(proposal);
    sink.send(Routed::DA(input));
}

fn route_da(now: Timestamp, output: DAOutput, sink: &mut impl Sink<Routed>) {
    match output {
        DAOutput::Consensus(slot, event) => {
            sink.send(Routed::Cadence(CadenceInput::DAEvent(slot, event)));
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
            sink.send(Routed::Finalization(input));
        }
        DAOutput::Disseminate(Dissemination { to, envelope }) => {
            let packets = write_envelope(&envelope);
            for node in to {
                for packet in &packets {
                    let message = NodeMessage::Chunk(packet.clone());
                    sink.send(Routed::Network(Outbound::Unicast(node, message)));
                }
            }
        }
        DAOutput::RecoveryRequest { to, request } => {
            let message = NodeMessage::ChunkRequest(request.serialize());
            sink.send(Routed::Network(Outbound::Unicast(to, message)));
        }
    }
}

// only slot messages are repeated
fn record(to: Recipients, message: &CadenceWireMsg, sink: &mut impl Sink<Routed>) {
    let CadenceMessage::Slot(slot, message) = message else {
        return;
    };
    let input = RepeaterInput::Record(*slot, to, message.clone());
    sink.send(Routed::Repeater(input));
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

    fn router_of(config: &NodeConfig) -> NodeRouter {
        Node::new(config).unwrap().router
    }

    fn inbound(message: NodeMessage) -> Effect {
        Effect::Network(NodeInbound {
            from: NodeId::dummy(0),
            message,
        })
    }

    #[test]
    fn a_tx_packet_goes_to_proposing_and_junk_goes_nowhere() {
        let mut router = router_of(&config(SourceKind::Mempool));
        let mut sink = Vec::new();
        let junk = NodeMessage::Tx(Bytes::from_static(b"junk"));
        router.route(now(), inbound(junk), &mut sink);
        assert!(sink.is_empty());

        router.route(now(), inbound(NodeMessage::Tx(tx(1).to_rlp())), &mut sink);
        // never forwarded: the rpc already chose this node
        let [Routed::Proposing(ProposingInput::Tx { at, tx, .. })] = &sink[..] else {
            panic!("one tx for proposing");
        };
        assert_eq!(*at, now()); // demo(tx-timeline)
        assert_eq!(tx.nonce, 1);
    }
}
