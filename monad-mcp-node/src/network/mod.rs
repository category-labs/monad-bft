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

mod repeater;

use std::{collections::VecDeque, fmt::Debug};

use bytes::Bytes;
use monad_mcp_common::{
    Inbound, Outbound, Qos, RepeatUntil, Slot, Timestamp, ValidatorSet, WirePriority,
    spec::ValidatorData as _,
};
use monad_mcp_ledger::{Tx, TxError};

use self::repeater::Repeater;
pub use self::repeater::RepeaterConfig;
use crate::{
    cadence::{Recipients, SlotLifecycle},
    common::NodeId,
    component::{CadenceWireMsg, Component},
    da::{ChunkPull, MalformedPacket, ProposalEnvelope, read_envelope, write_envelope},
    epoch::EpochHandle,
};

// a message between nodes, by kind
#[derive(Clone, PartialEq, Debug)]
pub enum AppMessage<C, K, Q, T> {
    Cadence(C),
    // a header with some of its chunks
    Chunk(K),
    ChunkRequest(Q),
    // sent by the rpc to the proposer it chose
    Tx(T),
}

// the message at the node boundary
pub type NodeAppMessage = AppMessage<CadenceWireMsg, ProposalEnvelope, ChunkPull, Tx>;

// the app message of the transport: the kind is a field of its header
pub type WireAppMessage<W = Bytes> = AppMessage<W, W, W, W>;

#[derive(Debug)]
pub enum Malformed {
    Cadence(alloy_rlp::Error),
    Chunk(MalformedPacket),
    ChunkRequest(alloy_rlp::Error),
    Tx(TxError),
}

// rlp, except a chunk, which has the packet layout of DA
impl Wire<NodeAppMessage> for WireAppMessage {
    type Malformed = Malformed;

    // An envelope is one packet for each chunk, or its header alone.
    fn to_wire(message: &NodeAppMessage) -> impl Iterator<Item = Self> {
        let (one, packets) = match message {
            AppMessage::Cadence(message) => {
                let bytes = alloy_rlp::encode(message).into();
                (Some(Self::Cadence(bytes)), Vec::new())
            }
            AppMessage::Chunk(envelope) => (None, write_envelope(envelope)),
            AppMessage::ChunkRequest(request) => {
                let bytes = alloy_rlp::encode(request).into();
                (Some(Self::ChunkRequest(bytes)), Vec::new())
            }
            AppMessage::Tx(tx) => (Some(Self::Tx(tx.to_rlp())), Vec::new()),
        };
        one.into_iter().chain(packets.into_iter().map(Self::Chunk))
    }

    fn from_wire(wire: Self) -> Result<NodeAppMessage, Malformed> {
        let message = match wire {
            AppMessage::Cadence(bytes) => {
                let message = alloy_rlp::decode_exact(&bytes).map_err(Malformed::Cadence)?;
                NodeAppMessage::Cadence(message)
            }
            AppMessage::Chunk(bytes) => {
                let envelope = read_envelope(bytes).map_err(Malformed::Chunk)?;
                NodeAppMessage::Chunk(envelope)
            }
            AppMessage::ChunkRequest(bytes) => {
                let request = alloy_rlp::decode_exact(&bytes).map_err(Malformed::ChunkRequest)?;
                NodeAppMessage::ChunkRequest(request)
            }
            AppMessage::Tx(bytes) => {
                let tx = Tx::decode_exact(&bytes).map_err(Malformed::Tx)?;
                NodeAppMessage::Tx(tx)
            }
        };
        Ok(message)
    }
}

// A form of M on the wire. One message can need several wire messages.
pub trait Wire<M>: Clone + PartialEq + Sized {
    type Malformed: Debug;

    fn to_wire(message: &M) -> impl Iterator<Item = Self>;
    fn from_wire(wire: Self) -> Result<M, Self::Malformed>;
}

// a node whose wire form is the typed message does no ser/de
impl Wire<NodeAppMessage> for NodeAppMessage {
    type Malformed = std::convert::Infallible;

    fn to_wire(message: &NodeAppMessage) -> impl Iterator<Item = Self> {
        std::iter::once(message.clone())
    }

    fn from_wire(wire: Self) -> Result<NodeAppMessage, Self::Malformed> {
        Ok(wire)
    }
}

// what the node's components send and receive
pub type NodeOutbound = Outbound<NodeAppMessage>;
pub type NodeInbound = Inbound<NodeAppMessage, NodeId>;

// what the transport takes and gives: recipients without this node, and the
// sender it authenticated
pub type WireOutbound<W> = Outbound<W, Recipients, WirePriority>;
pub type WireInbound<W> = Inbound<W, NodeId>;

pub enum NetworkInput<M, W> {
    Send(Outbound<M>),
    FromWire(WireInbound<W>),
    Lifecycle(SlotLifecycle),
}

#[derive(PartialEq, Debug)]
pub enum NetworkOutput<M, W> {
    ToWire(WireOutbound<W>),
    Received(Inbound<M, NodeId>),
}

// The lowest layer of the node. It resolves the recipients of a message,
// loops a message to this node back, converts to and from the wire form W,
// and repeats a message as its qos asks.
pub struct Network<M, W> {
    // this node, which no epoch scopes
    node_id: NodeId,
    epoch_handle: EpochHandle,
    // absent: nothing is repeated
    repeater: Option<Repeater<W>>,
    outbox: VecDeque<NetworkOutput<M, W>>,
}

impl<M, W> Network<M, W> {
    pub fn new(node_id: NodeId, epoch_handle: EpochHandle) -> Self {
        Self {
            node_id,
            epoch_handle,
            repeater: None,
            outbox: VecDeque::new(),
        }
    }

    pub fn with_repeater(mut self, config: RepeaterConfig) -> Self {
        self.repeater = Some(Repeater::new(config));
        self
    }

    fn send(&mut self, now: Timestamp, outbound: Outbound<M>)
    where
        W: Wire<M>,
    {
        let Outbound { to, message, qos } = outbound;
        let sender = self.node_id;
        // A slot of an unknown epoch has no validator table, so no recipients.
        let Some((mut to, loopback)) = self.resolve(to) else {
            return;
        };

        if !to.is_empty() {
            let mut wires = W::to_wire(&message).peekable();
            while let Some(wire) = wires.next() {
                // the last wire message takes the recipients, the others a copy
                let to = match wires.peek() {
                    Some(_) => to.clone(),
                    None => std::mem::take(&mut to),
                };
                self.deliver(now, to, wire, qos);
            }
        }
        if loopback {
            let own = Inbound { sender, message };
            self.outbox.push_back(NetworkOutput::Received(own));
        }
    }

    fn deliver(&mut self, now: Timestamp, to: Recipients, message: W, qos: Qos)
    where
        W: Clone + PartialEq,
    {
        match qos.repeat_until {
            Some(RepeatUntil::Commit(slot)) => self.record(now, slot, &to, &message),
            Some(RepeatUntil::CapAdvanced(slot)) => self.certify(slot, &to, &message),
            None => {}
        }
        let qos = qos.priority;
        let to_wire = Outbound { to, message, qos };
        self.outbox.push_back(NetworkOutput::ToWire(to_wire));
    }

    // the recipients without this node, and whether the message loops back
    fn resolve(&self, to: Recipients) -> Option<(Recipients, bool)> {
        let me = self.node_id;
        let resolved = match to {
            Recipients::Validators(slot, mut validators) => {
                let table = self.epoch_handle.validators_at(slot).ok()?;
                let me = table.index_of(&me);
                let loopback = me.is_some_and(|me| validators.remove(me));
                (Recipients::Validators(slot, validators), loopback)
            }
            Recipients::Node(node) if node == me => (Recipients::default(), true),
            Recipients::Node(node) => (Recipients::Node(node), false),
            Recipients::Nodes(mut nodes) => {
                let loopback = nodes.contains(&me);
                nodes.retain(|node| *node != me);
                (Recipients::Nodes(nodes), loopback)
            }
        };
        Some(resolved)
    }

    fn record(&mut self, now: Timestamp, slot: Slot, to: &Recipients, message: &W)
    where
        W: Clone + PartialEq,
    {
        let Some(repeater) = &mut self.repeater else {
            return;
        };
        let repeat = Outbound {
            to: to.clone(),
            message: message.clone(),
            qos: WirePriority::Regular,
        };
        repeater.record(now, slot, repeat);
    }

    fn certify(&mut self, slot: Slot, to: &Recipients, message: &W)
    where
        W: Clone,
    {
        let Some(repeater) = &mut self.repeater else {
            return;
        };
        let repeat = Outbound {
            to: to.clone(),
            message: message.clone(),
            qos: WirePriority::Regular,
        };
        repeater.certify(slot, repeat);
    }

    fn receive(&mut self, inbound: WireInbound<W>)
    where
        W: Wire<M>,
    {
        let Inbound { sender, message } = inbound;
        match W::from_wire(message) {
            Ok(message) => {
                let received = Inbound { sender, message };
                self.outbox.push_back(NetworkOutput::Received(received));
            }
            Err(error) => tracing::debug!(?sender, ?error, "malformed message"),
        }
    }

    fn repeat(&mut self, now: Timestamp)
    where
        W: Clone,
    {
        let Some(repeater) = &mut self.repeater else {
            return;
        };
        for repeat in repeater.tick(now) {
            self.outbox.push_back(NetworkOutput::ToWire(repeat));
        }
    }
}

// the node id of each recipient, read from the table of the slot
pub fn node_ids<'a>(
    to: &'a Recipients,
    epoch_handle: &EpochHandle,
) -> impl Iterator<Item = NodeId> + 'a {
    let (validators, nodes) = match to {
        Recipients::Validators(slot, validators) => {
            (validator_ids(validators, *slot, epoch_handle), &[][..])
        }
        Recipients::Node(node) => (None, std::slice::from_ref(node)),
        Recipients::Nodes(nodes) => (None, nodes.as_slice()),
    };
    validators
        .into_iter()
        .flatten()
        .chain(nodes.iter().copied())
}

// None for a slot out of the bound of the handle
fn validator_ids<'a>(
    validators: &'a ValidatorSet,
    slot: Slot,
    epoch_handle: &EpochHandle,
) -> Option<impl Iterator<Item = NodeId> + 'a> {
    let table = epoch_handle.validators_at(slot).ok()?;
    Some(validators.iter().map(move |index| *table.node_id(index)))
}

impl<M, W: Wire<M>> Component for Network<M, W> {
    type Input = NetworkInput<M, W>;
    type Output = NetworkOutput<M, W>;

    fn handle(&mut self, now: Timestamp, input: NetworkInput<M, W>) {
        match input {
            NetworkInput::Send(outbound) => self.send(now, outbound),
            NetworkInput::FromWire(inbound) => self.receive(inbound),
            NetworkInput::Lifecycle(event) => {
                if let Some(repeater) = &mut self.repeater {
                    repeater.handle_lifecycle(event);
                }
            }
        }
        if let Some(repeater) = &mut self.repeater {
            repeater.arm(now);
        }
    }

    fn poll(&mut self) -> Option<NetworkOutput<M, W>> {
        self.outbox.pop_front()
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.repeater.as_ref().and_then(Repeater::next_due)
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.repeat(now);
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use monad_mcp_common::{TimestampDelta, ValidatorIndex, spec::KeyPair as _};

    use super::*;
    use crate::{
        common::KeyPair,
        config::{NodeConfig, ValidatorConfig},
        epoch::EpochHandle,
    };

    const INTERVAL: TimestampDelta = TimestampDelta::from_millis(5_000);

    type TestNetwork = Network<NodeAppMessage, WireAppMessage>;

    fn at(millis: u64) -> Timestamp {
        Timestamp::from_millis(millis)
    }

    fn node(id: u64) -> NodeId {
        NodeId::dummy(id)
    }

    const SLOT: Slot = Slot(3);

    // the epoch of node 0, one of three validators
    fn epoch() -> EpochHandle {
        let mut config = NodeConfig::single_node(0, at(10_000), "unused-ledger");
        for id in 1..3 {
            config.validators.push(ValidatorConfig {
                node_id: node(id),
                stake: 1,
                chorus_pubkey: KeyPair::dummy(id).pubkey(),
                address: SocketAddr::from(([127, 0, 0, 1], id as u16)),
            });
        }
        config.epoch_manager().expect("config builds").genesis()
    }

    // node 0, with a repeater
    fn network(epoch: &EpochHandle) -> TestNetwork {
        let repeater = RepeaterConfig {
            interval: INTERVAL,
            certificate_retention: 2,
        };
        Network::new(node(0), epoch.clone()).with_repeater(repeater)
    }

    fn index(epoch: &EpochHandle, id: u64) -> ValidatorIndex {
        let validators = epoch.validators_at(SLOT).expect("in bound");
        validators.index_of(&node(id)).expect("a validator")
    }

    // the ids of the other validators
    fn others(epoch: &EpochHandle) -> Vec<u64> {
        let validators = epoch.validators_at(SLOT).expect("in bound");
        let nodes = validators.indices().map(|index| *validators.node_id(index));
        nodes.map(u64::from).filter(|id| *id != 0).collect()
    }

    fn tx(nonce: u64) -> Tx {
        Tx {
            sender: [5; 20],
            nonce,
            payload: Bytes::from_static(b"network"),
            sent_at_ns: 0,             // demo(tx-timeline)
            rpc_received_at_ns: 0,     // demo(tx-timeline)
            mempool_admitted_at_ns: 0, // demo(tx-timeline)
        }
    }

    fn message(nonce: u64) -> NodeAppMessage {
        AppMessage::Tx(tx(nonce))
    }

    // message `nonce` for the validators with the given ids
    fn outbound(epoch: &EpochHandle, to: &[u64], nonce: u64) -> NodeOutbound {
        let validators = to.iter().map(|id| index(epoch, *id)).collect();
        Outbound {
            to: Recipients::Validators(SLOT, validators),
            message: message(nonce),
            qos: Qos::default(),
        }
    }

    fn on_the_wire(
        epoch: &EpochHandle,
        to: &[u64],
        nonce: u64,
        qos: WirePriority,
    ) -> NetworkOutput<NodeAppMessage, WireAppMessage> {
        let validators = to.iter().map(|id| index(epoch, *id)).collect();
        let to = Recipients::Validators(SLOT, validators);
        let typed = message(nonce);
        let mut wires = WireAppMessage::to_wire(&typed);
        let message = wires.next().expect("a tx is one wire message");
        NetworkOutput::ToWire(Outbound { to, message, qos })
    }

    fn outputs(network: &mut TestNetwork) -> Vec<NetworkOutput<NodeAppMessage, WireAppMessage>> {
        std::iter::from_fn(|| network.poll()).collect()
    }

    #[test]
    fn a_malformed_message_is_dropped_and_a_good_one_is_received() {
        let epoch = epoch();
        let mut network = network(&epoch);
        let junk = WireInbound {
            sender: node(1),
            message: AppMessage::Tx(Bytes::from_static(b"junk")),
        };
        network.handle(at(0), NetworkInput::FromWire(junk));
        assert!(outputs(&mut network).is_empty());

        let good = WireInbound {
            sender: node(1),
            message: AppMessage::Tx(tx(1).to_rlp()),
        };
        network.handle(at(0), NetworkInput::FromWire(good));
        let received = Inbound {
            sender: node(1),
            message: message(1),
        };
        assert_eq!(
            outputs(&mut network),
            vec![NetworkOutput::Received(received)]
        );
    }

    #[test]
    fn a_broadcast_goes_to_the_others_and_loops_back() {
        let epoch = epoch();
        let mut network = network(&epoch);
        let broadcast = outbound(&epoch, &[0, 1, 2], 1);
        network.handle(at(0), NetworkInput::Send(broadcast));

        let own = Inbound {
            sender: node(0),
            message: message(1),
        };
        assert_eq!(
            outputs(&mut network),
            vec![
                on_the_wire(&epoch, &others(&epoch), 1, WirePriority::Regular),
                NetworkOutput::Received(own),
            ]
        );
    }

    #[test]
    fn a_message_to_this_node_only_loops_back() {
        let epoch = epoch();
        let mut network = network(&epoch);
        let to_me = outbound(&epoch, &[0], 1);
        let to_me = to_me.with_priority(WirePriority::High);
        network.handle(at(0), NetworkInput::Send(to_me));

        let own = Inbound {
            sender: node(0),
            message: message(1),
        };
        assert_eq!(outputs(&mut network), vec![NetworkOutput::Received(own)]);
        assert_eq!(network.next_due(), None);
    }

    #[test]
    fn a_message_repeats_until_its_slot_completes() {
        let epoch = epoch();
        let mut network = network(&epoch);
        let vote = outbound(&epoch, &[1], 1);
        let vote = vote.repeat_until(RepeatUntil::Commit(SLOT));
        network.handle(at(0), NetworkInput::Send(vote));
        assert_eq!(
            outputs(&mut network),
            vec![on_the_wire(&epoch, &[1], 1, WirePriority::Regular)]
        );

        assert_eq!(network.next_due(), Some(at(5_000)));
        network.handle_due(at(5_000));
        assert_eq!(
            outputs(&mut network),
            vec![on_the_wire(&epoch, &[1], 1, WirePriority::Regular)]
        );

        let completed = SlotLifecycle::Completed { slot: SLOT };
        network.handle(at(6_000), NetworkInput::Lifecycle(completed));
        network.handle_due(at(10_000));
        assert!(outputs(&mut network).is_empty());
        assert_eq!(network.next_due(), None);
    }

    #[test]
    fn a_certificate_repeats_until_the_cap_is_past_its_slot() {
        let epoch = epoch();
        let mut network = network(&epoch);
        let certificate = outbound(&epoch, &[0, 1, 2], 1);
        let certificate = certificate.repeat_until(RepeatUntil::CapAdvanced(SLOT));
        network.handle(at(0), NetworkInput::Send(certificate));
        let own = Inbound {
            sender: node(0),
            message: message(1),
        };
        assert_eq!(
            outputs(&mut network),
            vec![
                on_the_wire(&epoch, &others(&epoch), 1, WirePriority::Regular),
                NetworkOutput::Received(own),
            ]
        );

        network.handle_due(at(5_000));
        assert_eq!(
            outputs(&mut network),
            vec![on_the_wire(
                &epoch,
                &others(&epoch),
                1,
                WirePriority::Regular
            )]
        );

        // past the retention of 2 slots
        let advanced = SlotLifecycle::CapAdvance { cap: Slot(6) };
        network.handle(at(6_000), NetworkInput::Lifecycle(advanced));
        network.handle_due(at(10_000));
        assert!(outputs(&mut network).is_empty());
    }

    #[test]
    fn a_prioritized_message_keeps_its_priority_on_the_wire() {
        let epoch = epoch();
        let mut network = network(&epoch);
        let request = outbound(&epoch, &[2], 1);
        let request = request.with_priority(WirePriority::High);
        network.handle(at(0), NetworkInput::Send(request));
        assert_eq!(
            outputs(&mut network),
            vec![on_the_wire(&epoch, &[2], 1, WirePriority::High)]
        );
    }
}
