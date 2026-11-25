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

use monad_mcp_common::{Inbound, Outbound, ProposalScope, Recipients, Timestamp};

use crate::{
    cadence::SlotLifecycle,
    component::{
        CadenceInput, CadenceOutput, DAInput, FinalizationInput, ProposingInput, ProposingOutput,
    },
    config::NodeConfig,
    da::{DAOutput, ProposalScheme},
    epoch::EpochHandle,
    finalization::FinalizedSlot,
    network::{
        AppMessage, NetworkInput, NetworkOutput, NodeAppMessage, NodeInbound, WireInbound,
        WireOutbound,
    },
};

// everything the wiring takes in, named by where it comes from
pub enum Effect<W> {
    // the next sealed epoch
    Epoch(EpochHandle),
    Transport(WireInbound<W>),
    Network(NetworkOutput<NodeAppMessage, W>),
    Cadence(CadenceOutput),
    DA(DAOutput),
    Proposing(ProposingOutput),
    Finalization(FinalizedSlot),
}

// everything the wiring can emit, named by where it goes
pub enum Routed<W> {
    Epoch(SlotLifecycle),
    Cadence(CadenceInput),
    DA(DAInput),
    Finalization(FinalizationInput),
    Proposing(ProposingInput),
    Network(NetworkInput<NodeAppMessage, W>),
    Transport(WireOutbound<W>),
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

// The wiring of effects. W is the form of a message on the wire.
pub trait Router<W> {
    fn route(&mut self, now: Timestamp, effect: Effect<W>, sink: &mut impl Sink<Routed<W>>);
}

pub struct NodeRouter {
    // the scheme our proposals go to DA under
    proposal_scheme: ProposalScheme,
}

impl NodeRouter {
    pub fn new(config: &NodeConfig) -> Self {
        Self {
            proposal_scheme: config.proposal.encoding_scheme,
        }
    }

    fn route_cadence<W>(&mut self, output: CadenceOutput, sink: &mut impl Sink<Routed<W>>) {
        match output {
            CadenceOutput::Outbound(outbound) => {
                let outbound = outbound.map(AppMessage::Cadence);
                sink.send(Routed::Network(NetworkInput::Send(outbound)));
            }

            CadenceOutput::Lifecycle(event) => {
                sink.send(Routed::Epoch(event));
                sink.send(Routed::DA(DAInput::Lifecycle(event)));
                // the network ends its repeats on these
                sink.send(Routed::Network(NetworkInput::Lifecycle(event)));
                if let SlotLifecycle::CapAdvance { cap } = event {
                    tracing::debug!(cap = cap.0, "chain advanced");
                }
                sink.send(Routed::Proposing(ProposingInput::Lifecycle(event)));
                let input = FinalizationInput::Lifecycle(event);
                sink.send(Routed::Finalization(input));
            }

            CadenceOutput::DACommand(slot, command) => {
                sink.send(Routed::DA(DAInput::Command(slot, command)));
            }

            CadenceOutput::VoteReceived(vote) => {
                sink.send(Routed::Proposing(ProposingInput::VoteReceived(vote)));
            }

            CadenceOutput::Finalized(now, slot, finalization) => {
                let committed = finalization.pids().into_iter().flatten().count();
                let path = finalization.path();
                tracing::debug!(slot = slot.0, ?path, committed, "cadence finalized");

                let positive = finalization.pids().map(|pid| pid.is_some());
                let input = ProposingInput::Finalized(slot, positive, path);
                sink.send(Routed::Proposing(input));

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

    fn route_inbound<W>(
        &self,
        now: Timestamp,
        inbound: NodeInbound,
        sink: &mut impl Sink<Routed<W>>,
    ) {
        let Inbound { sender, message } = inbound;
        match message {
            AppMessage::Cadence(message) => {
                // todo: signature checks for a transport that does
                // not authenticate the sender.

                // todo: verify slot number against current state
                let input = CadenceInput::Message(Inbound { sender, message });
                sink.send(Routed::Cadence(input));
            }
            AppMessage::Chunk(message) => {
                let input = DAInput::Envelope(Inbound { sender, message });
                sink.send(Routed::DA(input));
            }
            AppMessage::ChunkRequest(message) => {
                let input = DAInput::ChunkRequest(Inbound { sender, message });
                sink.send(Routed::DA(input));
            }
            AppMessage::Tx(tx) => {
                let from = sender;
                let proposal = ProposingInput::Tx { at: now, from, tx }; // demo(tx-timeline)
                sink.send(Routed::Proposing(proposal));
            }
        }
    }
}

impl<W> Router<W> for NodeRouter {
    fn route(&mut self, now: Timestamp, effect: Effect<W>, sink: &mut impl Sink<Routed<W>>) {
        match effect {
            Effect::Epoch(_epoch_handle) => {
                todo!("give the next epoch to the components that read epochs")
            }
            Effect::Transport(inbound) => {
                sink.send(Routed::Network(NetworkInput::FromWire(inbound)));
            }
            Effect::Network(NetworkOutput::ToWire(outbound)) => {
                sink.send(Routed::Transport(outbound));
            }
            Effect::Network(NetworkOutput::Received(inbound)) => {
                self.route_inbound(now, inbound, sink);
            }
            Effect::Cadence(output) => self.route_cadence(output, sink),
            Effect::DA(output) => route_da(now, output, sink),
            Effect::Proposing(proposal) => route_proposal(self.proposal_scheme, proposal, sink),
            Effect::Finalization(finalized) => sink.send(Routed::Ledger(finalized)),
        }
    }
}

fn route_proposal<W>(
    proposal_scheme: ProposalScheme,
    output: ProposingOutput,
    sink: &mut impl Sink<Routed<W>>,
) {
    let ProposingOutput {
        proposal,
        send_order: _,
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
    };
    sink.send(Routed::DA(input));
}

fn route_da<W>(now: Timestamp, output: DAOutput, sink: &mut impl Sink<Routed<W>>) {
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
        // todo: DA names its recipients by node id for now
        DAOutput::Send(dissemination) => {
            let Outbound { to, message, qos } = dissemination;
            let to = Recipients::Nodes(to);
            let message = AppMessage::Chunk(message);
            let outbound = Outbound { to, message, qos };
            sink.send(Routed::Network(NetworkInput::Send(outbound)));
        }
        DAOutput::Pull(request) => {
            let Outbound { to, message, qos } = request;
            let to = Recipients::Node(to);
            let message = AppMessage::ChunkRequest(message);
            let outbound = Outbound { to, message, qos };
            sink.send(Routed::Network(NetworkInput::Send(outbound)));
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use monad_mcp_ledger::Tx;

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
        let node: Node = Node::new(config).unwrap();
        node.router
    }

    fn inbound(message: NodeAppMessage) -> Effect<NodeAppMessage> {
        let sender = NodeId::dummy(0);
        let inbound = NodeInbound { sender, message };
        Effect::Network(NetworkOutput::Received(inbound))
    }

    #[test]
    fn a_tx_goes_to_proposing() {
        let mut router = router_of(&config(SourceKind::Mempool));
        let mut sink = Vec::new();
        router.route(now(), inbound(AppMessage::Tx(tx(1))), &mut sink);
        // never forwarded: the rpc already chose this node
        let [Routed::Proposing(ProposingInput::Tx { at, tx, .. })] = &sink[..] else {
            panic!("one tx for proposing");
        };
        assert_eq!(*at, now()); // demo(tx-timeline)
        assert_eq!(tx.nonce, 1);
    }
}
