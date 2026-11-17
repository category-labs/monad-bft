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

use monad_mcp_cadence::{ledger::Tx, spec::Serializable as _};
use monad_mcp_common::{Outbound, ProposalScope, Timestamp, WirePriority};

use crate::{
    cadence::{self, CadenceMessage, Recipients, SlotLifecycle, slot::chorus::ChorusMessage},
    common::NodeId,
    component::{
        CadenceInput, CadenceOutput, CadenceWireMsg, DAInput, FinalizationInput, ProposingInput,
        ProposingOutput, RepeaterInput, RepeaterOutput,
    },
    config::NodeConfig,
    da::{ChunkPull, DAOutput, Dissemination, ProposalScheme, read_envelope, write_envelope},
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
    // the scheme our proposals go to DA under
    proposal_scheme: ProposalScheme,
    // copy every inbound batch vote to proposing (`ProposingInput::BatchVote`)
    observe_votes: bool,
}

impl NodeRouter {
    pub fn new(epoch_handle: EpochHandle, config: &NodeConfig) -> Self {
        Self {
            epoch_handle,
            proposal_scheme: config.proposal.encoding_scheme,
            observe_votes: false,
        }
    }

    // proposing sees every peer's batch vote with its arrival time, for a
    // proposing component that wants them (`wants_vote_facts`). Off, the
    // router emits exactly what it did before
    pub fn observe_votes(mut self, observe: bool) -> Self {
        self.observe_votes = observe;
        self
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
                    SlotLifecycle::CapAdvance { cap } => {
                        tracing::debug!(cap = cap.0, "chain advanced");
                        sink.send(Routed::Repeater(RepeaterInput::CapAdvance(cap)));
                        sink.send(Routed::Proposing(ProposingInput::CapAdvance(cap)));
                    }
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
                let input = ProposingInput::Finalized(slot, positive, path);
                sink.send(Routed::Proposing(input));
                let certificate = finalization.certificate_message();
                sink.send(Routed::Repeater(RepeaterInput::Finalization(certificate)));
                let input = FinalizationInput::Finalized(now, slot, finalization);
                sink.send(Routed::Finalization(input));
            }

            // demo(tx-timeline)
            CadenceOutput::FastBlock(now, slot) => {
                let input = FinalizationInput::FastBlock(now, slot);
                sink.send(Routed::Finalization(input));
            }
        }
    }

    fn route_cadence_outbound(
        &mut self,
        outbound: cadence::Outbound<CadenceWireMsg>,
        sink: &mut impl Sink<Routed>,
    ) {
        let Outbound { to, message, qos } = outbound;
        if to == Recipients::Node(self.epoch_handle.self_id) {
            self.loopback(message, sink);
            return;
        }

        record(to, &message, sink);
        let outbound = NodeOutbound {
            to,
            message: NodeMessage::Cadence(message.serialize()),
            qos: qos.priority,
        };
        sink.send(Routed::Network(outbound));
        if to == Recipients::Everyone {
            self.loopback(message, sink);
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
            Effect::Network(inbound) => {
                route_inbound(now, inbound, self.observe_votes, sink);
            }
            Effect::Cadence(output) => self.route_cadence(output, sink),
            Effect::DA(output) => route_da(now, output, sink),
            Effect::Proposing(proposal) => route_proposal(self.proposal_scheme, proposal, sink),
            Effect::Repeater(repeat) => route_repeat(repeat, sink),
            Effect::Finalization(finalized) => sink.send(Routed::Ledger(finalized)),
        }
    }
}

// the arrival time of a peer's batch vote, at the router: in the async node
// the time the network task handed it over, not the Cadence task's dequeue.
// Every copy goes out, the repeater's too; proposing keeps the first.
fn observe_vote(
    now: Timestamp,
    from: NodeId,
    message: &CadenceWireMsg,
    sink: &mut impl Sink<Routed>,
) {
    let CadenceMessage::Slot(ChorusMessage::BatchVote(vote)) = message else {
        return;
    };
    let input = ProposingInput::BatchVote {
        slot: vote.slot(),
        from,
        at: now,
        positive: vote.positive_lanes(),
    };
    sink.send(Routed::Proposing(input));
}

fn route_inbound(
    now: Timestamp,
    inbound: NodeInbound,
    observe_votes: bool,
    sink: &mut impl Sink<Routed>,
) {
    use monad_mcp_cadence::spec::Deserializable as _;

    let NodeInbound { sender, message } = inbound;
    match message {
        NodeMessage::Cadence(bytes) => {
            let Ok(message) = CadenceWireMsg::deserialize(&bytes) else {
                tracing::debug!(?sender, "malformed cadence message");
                return;
            };
            if observe_votes {
                observe_vote(now, sender, &message, sink);
            }
            // todo: signature checks for a transport that does
            // not authenticate the sender.

            // todo: verify slot number against current state
            sink.send(Routed::Cadence(CadenceInput::Message(sender, message)));
        }

        NodeMessage::Chunk(bytes) => {
            let Ok(envelope) = read_envelope(bytes) else {
                tracing::debug!(?sender, "malformed chunk packet");
                return;
            };
            sink.send(Routed::DA(DAInput::Envelope(sender, envelope)));
        }

        NodeMessage::ChunkRequest(bytes) => {
            let Ok(request) = ChunkPull::deserialize(&bytes) else {
                tracing::debug!(?sender, "malformed chunk request");
                return;
            };
            sink.send(Routed::DA(DAInput::ChunkRequest(sender, request)));
        }

        NodeMessage::Tx(bytes) => {
            let tx = match Tx::decode_exact(&bytes) {
                Ok(tx) => tx,
                Err(error) => {
                    tracing::debug!(?sender, %error, "malformed tx");
                    return;
                }
            };
            let from = sender;
            let proposal = ProposingInput::Tx { at: now, from, tx }; // demo(tx-timeline)
            sink.send(Routed::Proposing(proposal));
        }
    }
}

fn route_repeat((to, message): RepeaterOutput<ChorusMessage>, sink: &mut impl Sink<Routed>) {
    let message: CadenceWireMsg = CadenceMessage::Slot(message);
    let outbound = NodeOutbound {
        to,
        message: NodeMessage::Cadence(message.serialize()),
        qos: WirePriority::Regular,
    };
    sink.send(Routed::Network(outbound));
}

fn route_proposal(
    proposal_scheme: ProposalScheme,
    output: ProposingOutput,
    sink: &mut impl Sink<Routed>,
) {
    let ProposingOutput {
        proposal,
        send_order,
    } = output;
    tracing::info!(
        ?proposal.slot,
        ?proposal.lane,
        len = proposal.payload.len(),
        "proposing"
    );
    let input = DAInput::Propose {
        scheme: proposal_scheme,
        proposal,
        send_order,
    };
    sink.send(Routed::DA(input));
}

fn route_da(now: Timestamp, output: DAOutput, sink: &mut impl Sink<Routed>) {
    match output {
        DAOutput::Consensus(slot, event) => {
            sink.send(Routed::Cadence(CadenceInput::DAEvent(slot, event)));
        }
        DAOutput::Resolved {
            scope,
            pid,
            message,
        } => {
            let ProposalScope { slot, lane } = scope;
            let input = FinalizationInput::Resolved {
                at: now, // demo(tx-timeline)
                slot,
                lane,
                pid,
                message,
            };
            sink.send(Routed::Finalization(input));
        }
        DAOutput::Send(dissemination) => route_dissemination(dissemination, sink),
        DAOutput::Pull(Outbound { to, message, qos }) => {
            let message = NodeMessage::ChunkRequest(message.serialize());
            let outbound = NodeOutbound::unicast(to, message).with_qos(qos.priority);
            sink.send(Routed::Network(outbound));
        }
    }
}

fn route_dissemination(dissemination: Dissemination, sink: &mut impl Sink<Routed>) {
    let Dissemination { to, message, qos } = dissemination;
    let packets = write_envelope(&message);
    for node in to {
        for packet in &packets {
            let message = NodeMessage::Chunk(packet.clone());
            let outbound = NodeOutbound::unicast(node, message).with_qos(qos.priority);
            sink.send(Routed::Network(outbound));
        }
    }
}

// only slot messages are repeated
fn record(to: Recipients, message: &CadenceWireMsg, sink: &mut impl Sink<Routed>) {
    let CadenceMessage::Slot(message) = message else {
        return;
    };
    let input = RepeaterInput::Record(to, message.clone());
    sink.send(Routed::Repeater(input));
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        common::NodeId,
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
            sender: NodeId::dummy(0),
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
