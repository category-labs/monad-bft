use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    time::Duration,
};

use bytes::Buf;
use monad_crypto::{
    hasher::{Hasher, HasherType},
    secp256k1::KeyPair,
};
use monad_transformer::{BytesTransformerPipeline, LinkMessage, Pipeline, ID};
use monad_types::{NodeId, RouterTarget};
use rand::Rng;

use super::{Gossip, GossipEvent};
use crate::{AppMessage, ConnectionManager, ConnectionManagerEvent};

struct Node<G> {
    connection_manager: ConnectionManager<G>,
    pipeline: BytesTransformerPipeline,
}

pub struct Swarm<G> {
    current_tick: Duration,
    nodes: BTreeMap<NodeId, Node<G>>,
    pending_inbound_messages: BinaryHeap<Reverse<(Duration, usize, LinkMessage<AppMessage>)>>,
    seq_no: usize,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum SwarmEventType {
    GossipEvent,
    ScheduledMessage,
}

impl<G: Gossip> Swarm<G> {
    pub fn new(configs: impl Iterator<Item = (NodeId, G, BytesTransformerPipeline)>) -> Self {
        let mut nodes: BTreeMap<_, _> = configs
            .map(|(peer_id, gossip, pipeline)| {
                (
                    peer_id,
                    Node {
                        connection_manager: ConnectionManager::new(gossip),
                        pipeline,
                    },
                )
            })
            .collect();

        let peers: Vec<NodeId> = nodes.keys().copied().collect();

        for (id, node) in &mut nodes {
            for (idx, peer) in peers.iter().filter(|peer| &id != peer).enumerate() {
                node.connection_manager.connected(Duration::ZERO, *peer)
            }
        }

        Self {
            current_tick: Duration::ZERO,
            nodes,
            pending_inbound_messages: Default::default(),
            seq_no: 0,
        }
    }

    pub fn send(&mut self, from: &NodeId, to: RouterTarget, message: AppMessage) {
        self.nodes
            .get_mut(from)
            .expect("peer doesn't exist")
            .connection_manager
            .send(self.current_tick, to, message)
    }

    pub fn peek_event(&self) -> Option<(Duration, SwarmEventType, NodeId)> {
        self.nodes
            .iter()
            .filter_map(|(id, node)| {
                node.connection_manager
                    .peek_tick()
                    .map(|tick| (tick, SwarmEventType::GossipEvent, *id))
            })
            .chain(
                self.pending_inbound_messages
                    .peek()
                    .map(|Reverse((tick, _, message))| {
                        (
                            *tick,
                            SwarmEventType::ScheduledMessage,
                            *message.to.get_peer_id(),
                        )
                    }),
            )
            .min()
    }

    pub fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<(Duration, NodeId, (NodeId, AppMessage))> {
        while let Some((tick, event_type, id)) = self.peek_event() {
            if tick > until {
                break;
            }

            let event = match event_type {
                SwarmEventType::GossipEvent => {
                    let node = self.nodes.get_mut(&id).expect("invariant broken");
                    let connection_manager_event = node.connection_manager.poll(tick);
                    match connection_manager_event {
                        None => continue,
                        Some(ConnectionManagerEvent::RequestConnect(to)) => {
                            todo!("multiple connections not supported")
                        }
                        Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Emit(
                            from,
                            message,
                        ))) => (from, message),
                        Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Send(
                            to,
                            mut bytes,
                        ))) => {
                            let scheduled_messages = node.pipeline.process(LinkMessage {
                                from: ID::new(id),
                                to: ID::new(to),
                                message: bytes.copy_to_bytes(bytes.remaining()),

                                from_tick: tick,
                            });

                            for (scheduled_tick, message) in scheduled_messages {
                                self.pending_inbound_messages.push(Reverse((
                                    tick + scheduled_tick,
                                    self.seq_no,
                                    message,
                                )));
                                self.seq_no += 1;
                            }

                            continue;
                        }
                    }
                }
                SwarmEventType::ScheduledMessage => {
                    let Reverse((scheduled_tick, _, gossip_message)) = self
                        .pending_inbound_messages
                        .pop()
                        .expect("invariant broken");
                    assert_eq!(tick, scheduled_tick);

                    let to_node = self
                        .nodes
                        .get_mut(gossip_message.to.get_peer_id())
                        .expect("invariant broken");

                    to_node.connection_manager.handle_unframed_gossip_message(
                        scheduled_tick,
                        *gossip_message.from.get_peer_id(),
                        gossip_message.message,
                    );

                    continue;
                }
            };
            self.current_tick = tick;
            return Some((tick, id, event));
        }
        self.current_tick = until;
        None
    }
}

pub fn make_swarm<G: Gossip>(
    num_nodes: u16,
    make_gossip: impl Fn(&[NodeId], &NodeId) -> G,
    make_transformer: impl Fn(&[NodeId], &NodeId) -> BytesTransformerPipeline,
) -> Swarm<G> {
    let peers: Vec<_> = (1_u32..)
        .take(num_nodes.into())
        .map(|idx| {
            let mut secret = {
                let mut hasher = HasherType::new();
                hasher.update(idx.to_le_bytes());
                hasher.hash().0
            };
            let keypair = KeyPair::from_bytes(&mut secret).unwrap();
            NodeId(keypair.pubkey())
        })
        .collect();
    Swarm::new(peers.iter().map(|node_id| {
        (
            *node_id,
            make_gossip(&peers, node_id),
            make_transformer(&peers, node_id),
        )
    }))
}

pub fn test_broadcast<G: Gossip>(
    rng: &mut impl Rng,
    swarm: &mut Swarm<G>,
    max_tick: Duration,

    payload_size_bytes: usize,
    max_payload_broadcasts: usize,
    // expected_delivery_rate of 1.0 == everything delivered
    expected_delivery_rate: f64,
) -> Duration {
    let start = swarm.current_tick;
    assert!((0.0..=1.0).contains(&expected_delivery_rate));
    let peer_ids: Vec<_> = swarm.nodes.keys().copied().collect();
    let mut pending_messages = HashSet::new();
    for tx_peer in peer_ids.iter().take(max_payload_broadcasts) {
        let message: AppMessage = (0..payload_size_bytes).map(|_| rng.gen()).collect();
        let target = RouterTarget::Broadcast;

        for rx_peer in &peer_ids {
            pending_messages.insert((*rx_peer, (*tx_peer, message.clone())));
        }
        swarm.send(tx_peer, target, message);
    }

    // some random extra messages to flush pipeline transformers
    for _ in 0..10 {
        for tx_peer in &peer_ids {
            let message: AppMessage = (0..10).map(|_| rng.gen()).collect();
            let target = RouterTarget::Broadcast;
            swarm.send(tx_peer, target, message);
        }
    }

    let num_expected = pending_messages.len();
    while let Some((tick, rx_peer, (tx_peer, message))) = swarm.step_until(max_tick) {
        pending_messages.remove(&(rx_peer, (tx_peer, message)));
        let num_delivered = num_expected - pending_messages.len();
        if num_delivered as f64 / num_expected as f64 >= expected_delivery_rate {
            return tick - start;
        }
    }
    let num_delivered = num_expected - pending_messages.len();
    let expected_percentage = expected_delivery_rate * 100.0;
    let received_percentage = (num_delivered as f64 / num_expected as f64) * 100.0;
    unreachable!("stepped until max_tick without {expected_percentage}% percentage of messages being received. received percentage: {received_percentage}%");
}

pub fn test_direct<G: Gossip>(
    rng: &mut impl Rng,
    swarm: &mut Swarm<G>,
    max_tick: Duration,

    payload_size_bytes: usize,
) -> Duration {
    let start = swarm.current_tick;
    let peer_ids: Vec<_> = swarm.nodes.keys().copied().collect();
    let mut pending_messages = HashSet::new();
    for tx_peer in &peer_ids {
        for rx_peer in &peer_ids {
            let message: AppMessage = (0..payload_size_bytes).map(|_| rng.gen()).collect();
            let target = RouterTarget::PointToPoint(*rx_peer);
            swarm.send(tx_peer, target, message.clone());

            pending_messages.insert((*rx_peer, (*tx_peer, message)));
        }
    }

    // some random extra messages to flush pipeline transformers
    for _ in 0..10 {
        for tx_peer in &peer_ids {
            let message: AppMessage = (0..10).map(|_| rng.gen()).collect();
            for rx_peer in &peer_ids {
                let target = RouterTarget::PointToPoint(*rx_peer);
                swarm.send(tx_peer, target, message.clone());
            }
        }
    }

    while let Some((tick, rx_peer, (tx_peer, message))) = swarm.step_until(max_tick) {
        pending_messages.remove(&(rx_peer, (tx_peer, message)));
        if pending_messages.is_empty() {
            return tick - start;
        }
    }
    unreachable!("stepped until max_tick without all pending_messages being received");
}
