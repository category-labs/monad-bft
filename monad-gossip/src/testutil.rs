use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    time::Duration,
};

use bytes::{Buf, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hasher, HasherType},
};
use monad_transformer::{BytesTransformerPipeline, LinkMessage, Pipeline, ID};
use monad_types::{Epoch, NodeId, RouterTarget};
use rand::Rng;

use super::Gossip;
use crate::{AppMessage, ConnectionManager, ConnectionManagerEvent};

struct Node<G: Gossip> {
    connection_manager: ConnectionManager<G>,
    pipeline: BytesTransformerPipeline<G::NodeIdPubKey>,
}

pub struct Swarm<G: Gossip> {
    current_tick: Duration,
    nodes: BTreeMap<NodeId<G::NodeIdPubKey>, Node<G>>,
    pending_inbound_messages: BinaryHeap<Reverse<PendingInboundMessage<G::NodeIdPubKey>>>,
    seq_no: usize,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct PendingInboundMessage<PT: PubKey> {
    tick: Duration,
    seq_no: usize,
    message: LinkMessage<PT, AppMessage>,
    is_datagram: bool,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum SwarmEventType {
    GossipEvent,
    ScheduledMessage,
}

impl<G: Gossip> Swarm<G> {
    pub fn new(
        configs: impl Iterator<
            Item = (
                NodeId<G::NodeIdPubKey>,
                G,
                BytesTransformerPipeline<G::NodeIdPubKey>,
            ),
        >,
    ) -> Self {
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

        let peers: Vec<NodeId<_>> = nodes.keys().copied().collect();

        for (id, node) in &mut nodes {
            for peer in peers.iter().filter(|peer| &id != peer) {
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

    pub fn send(
        &mut self,
        from: &NodeId<G::NodeIdPubKey>,
        to: RouterTarget<G::NodeIdPubKey>,
        message: AppMessage,
    ) {
        self.nodes
            .get_mut(from)
            .expect("peer doesn't exist")
            .connection_manager
            .send(self.current_tick, to, message)
    }

    pub fn peek_event(&self) -> Option<(Duration, SwarmEventType, NodeId<G::NodeIdPubKey>)> {
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
                    .map(|Reverse(pending_message)| {
                        (
                            pending_message.tick,
                            SwarmEventType::ScheduledMessage,
                            *pending_message.message.to.get_peer_id(),
                        )
                    }),
            )
            .min()
    }

    pub fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<(
        Duration,
        NodeId<G::NodeIdPubKey>,
        (NodeId<G::NodeIdPubKey>, AppMessage),
    )> {
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
                            todo!(
                                "multiple connections not supported; {:?} tried to send to {:?}",
                                id,
                                to
                            )
                        }
                        Some(ConnectionManagerEvent::Emit(from, message)) => (from, message),
                        Some(ConnectionManagerEvent::Send(to, mut bytes)) => {
                            let scheduled_messages = node.pipeline.process(LinkMessage {
                                from: ID::new(id),
                                to: ID::new(to),
                                message: bytes.copy_to_bytes(bytes.remaining()),

                                from_tick: tick,
                            });

                            for (scheduled_tick, message) in scheduled_messages {
                                self.pending_inbound_messages.push(Reverse(
                                    PendingInboundMessage {
                                        tick: tick + scheduled_tick,
                                        seq_no: self.seq_no,
                                        message,
                                        is_datagram: false,
                                    },
                                ));
                                self.seq_no += 1;
                            }

                            continue;
                        }
                        Some(ConnectionManagerEvent::SendDatagram(to, bytes)) => {
                            let scheduled_messages = node.pipeline.process(LinkMessage {
                                from: ID::new(id),
                                to: ID::new(to),
                                message: bytes,

                                from_tick: tick,
                            });

                            for (scheduled_tick, message) in scheduled_messages {
                                self.pending_inbound_messages.push(Reverse(
                                    PendingInboundMessage {
                                        tick: tick + scheduled_tick,
                                        seq_no: self.seq_no,
                                        message,
                                        is_datagram: true,
                                    },
                                ));
                                self.seq_no += 1;
                            }

                            continue;
                        }
                    }
                }
                SwarmEventType::ScheduledMessage => {
                    let Reverse(pending_message) = self
                        .pending_inbound_messages
                        .pop()
                        .expect("invariant broken");
                    assert_eq!(tick, pending_message.tick);

                    let to_node = self
                        .nodes
                        .get_mut(pending_message.message.to.get_peer_id())
                        .expect("invariant broken");

                    if pending_message.is_datagram {
                        to_node.connection_manager.handle_datagram(
                            tick,
                            *pending_message.message.from.get_peer_id(),
                            pending_message.message.message,
                        );
                    } else {
                        to_node.connection_manager.handle_unframed_gossip_message(
                            tick,
                            *pending_message.message.from.get_peer_id(),
                            pending_message.message.message,
                        );
                    }

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

pub fn make_swarm<
    ST: CertificateSignatureRecoverable,
    G: Gossip<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
>(
    num_nodes: u16,
    make_gossip: impl Fn(&[NodeId<G::NodeIdPubKey>], &NodeId<G::NodeIdPubKey>) -> G,
    make_transformer: impl Fn(
        &[NodeId<G::NodeIdPubKey>],
        &NodeId<G::NodeIdPubKey>,
    ) -> BytesTransformerPipeline<G::NodeIdPubKey>,
) -> Swarm<G> {
    let peers: Vec<_> = (1_u32..)
        .take(num_nodes.into())
        .map(|idx| {
            let mut secret = {
                let mut hasher = HasherType::new();
                hasher.update(idx.to_le_bytes());
                hasher.hash().0
            };
            let keypair = <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut secret).unwrap();
            NodeId::new(keypair.pubkey())
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
        let message = {
            let mut message = BytesMut::zeroed(payload_size_bytes);
            rng.fill_bytes(&mut message);
            message.freeze()
        };
        let target = RouterTarget::Raptorcast(
            // FIXME
            Epoch(0),
        );

        for rx_peer in &peer_ids {
            pending_messages.insert((*rx_peer, (*tx_peer, message.clone())));
        }
        swarm.send(tx_peer, target, message);
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
            let message = {
                let mut message = BytesMut::zeroed(payload_size_bytes);
                rng.fill_bytes(&mut message);
                message.freeze()
            };
            let target = RouterTarget::PointToPoint(*rx_peer);
            swarm.send(tx_peer, target, message.clone());

            pending_messages.insert((*rx_peer, (*tx_peer, message)));
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
