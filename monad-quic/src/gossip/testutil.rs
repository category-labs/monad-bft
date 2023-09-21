use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    time::Duration,
};

use monad_executor_glue::{PeerId, RouterTarget};
use monad_mock_swarm::transformer::{BytesTransformerPipeline, LinkMessage, Pipeline};
use rand::Rng;

use super::{Gossip, GossipEvent};

type BytesType = Vec<u8>;

pub(crate) struct Swarm<G> {
    current_tick: Duration,
    nodes: BTreeMap<PeerId, (G, BytesTransformerPipeline)>,
    pending_inbound_messages: BinaryHeap<Reverse<(Duration, usize, LinkMessage<BytesType>)>>,
    seq_no: usize,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum SwarmEventType {
    GossipEvent,
    ScheduledMessage,
}

impl<G: Gossip> Swarm<G> {
    pub fn new(
        configs: impl Iterator<Item = (PeerId, G::Config, BytesTransformerPipeline)>,
    ) -> Self {
        let nodes = configs
            .map(|(peer_id, config, pipeline)| (peer_id, (G::new(config), pipeline)))
            .collect();

        Self {
            current_tick: Duration::ZERO,
            nodes,
            pending_inbound_messages: Default::default(),
            seq_no: 0,
        }
    }

    pub fn send(&mut self, from: &PeerId, to: RouterTarget, message: &[u8]) -> G::MessageId {
        self.nodes
            .get_mut(from)
            .expect("peer doesn't exist")
            .0
            .send(self.current_tick, to, message)
    }

    pub fn forget(&mut self, from: &PeerId, to: RouterTarget, message_id: G::MessageId) {
        self.nodes
            .get_mut(from)
            .expect("peer doesn't exist")
            .0
            .forget(self.current_tick, to, message_id)
    }

    pub fn peek_event(&self) -> Option<(Duration, SwarmEventType, PeerId)> {
        self.nodes
            .iter()
            .filter_map(|(id, (node, _))| {
                node.peek_tick()
                    .map(|tick| (tick, SwarmEventType::GossipEvent, *id))
            })
            .chain(
                self.pending_inbound_messages
                    .peek()
                    .map(|Reverse((tick, _, message))| {
                        (*tick, SwarmEventType::ScheduledMessage, message.to)
                    }),
            )
            .min()
    }

    pub fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<(Duration, PeerId, (PeerId, BytesType))> {
        while let Some((tick, event_type, peer_id)) = self.peek_event() {
            if tick > until {
                break;
            }

            let event = match event_type {
                SwarmEventType::GossipEvent => {
                    let (gossip, pipeline) =
                        self.nodes.get_mut(&peer_id).expect("invariant broken");
                    let gossip_event = gossip.poll(tick);
                    match gossip_event {
                        None => continue,
                        Some(GossipEvent::Emit(from, message)) => (from, message),
                        Some(GossipEvent::Send(to, bytes)) => {
                            let scheduled_messages = pipeline.process(LinkMessage {
                                from: peer_id,
                                to,
                                message: bytes,

                                from_tick: tick,
                            });

                            for (scheduled_tick, message) in scheduled_messages {
                                self.pending_inbound_messages.push(Reverse((
                                    scheduled_tick,
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

                    self.nodes
                        .get_mut(&gossip_message.to)
                        .expect("invariant broken")
                        .0
                        .handle_gossip_message(
                            scheduled_tick,
                            gossip_message.from,
                            &gossip_message.message,
                        );

                    continue;
                }
            };
            self.current_tick = tick;
            return Some((tick, peer_id, event));
        }
        self.current_tick = until;
        None
    }
}

pub(crate) fn test_broadcast<G: Gossip>(
    rng: &mut impl Rng,
    swarm: &mut Swarm<G>,
    max_tick: Duration,
) {
    let peer_ids: Vec<_> = swarm.nodes.keys().copied().collect();
    let mut pending_messages = HashSet::new();
    let mut forget = Vec::new();
    for tx_peer in peer_ids.iter().copied() {
        let message: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
        let target = RouterTarget::Broadcast;
        let message_id = swarm.send(&tx_peer, target, &message);
        forget.push((tx_peer, target, message_id));

        for rx_peer in &peer_ids {
            pending_messages.insert((*rx_peer, (tx_peer, message.clone())));
        }
    }

    // some random extra messages to flush pipeline transformers
    for _ in 0..10 {
        for tx_peer in peer_ids.iter().copied() {
            let message: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
            let target = RouterTarget::Broadcast;
            let message_id = swarm.send(&tx_peer, target, &message);
            forget.push((tx_peer, target, message_id));
        }
    }

    while let Some((tick, rx_peer, (tx_peer, message))) = swarm.step_until(max_tick) {
        pending_messages.remove(&(rx_peer, (tx_peer, message)));
        if pending_messages.is_empty() {
            for (peer, target, message_id) in forget {
                swarm.forget(&peer, target, message_id)
            }
            return;
        }
    }
    unreachable!("stepped until max_tick without all pending_messages being received");
}

pub(crate) fn test_direct<G: Gossip>(rng: &mut impl Rng, swarm: &mut Swarm<G>, max_tick: Duration) {
    let peer_ids: Vec<_> = swarm.nodes.keys().copied().collect();
    let mut pending_messages = HashSet::new();
    let mut forget = Vec::new();
    for tx_peer in peer_ids.iter().copied() {
        for rx_peer in &peer_ids {
            let message: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
            let target = RouterTarget::PointToPoint(*rx_peer);
            let message_id = swarm.send(&tx_peer, target, &message);
            forget.push((tx_peer, target, message_id));

            pending_messages.insert((*rx_peer, (tx_peer, message)));
        }
    }

    // some random extra messages to flush pipeline transformers
    for _ in 0..10 {
        for tx_peer in peer_ids.iter().copied() {
            let message: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
            let target = RouterTarget::Broadcast;
            let message_id = swarm.send(&tx_peer, target, &message);
            forget.push((tx_peer, target, message_id));
        }
    }

    while let Some((tick, rx_peer, (tx_peer, message))) = swarm.step_until(max_tick) {
        pending_messages.remove(&(rx_peer, (tx_peer, message)));
        if pending_messages.is_empty() {
            for (peer, target, message_id) in forget {
                swarm.forget(&peer, target, message_id)
            }
            return;
        }
    }
    unreachable!("stepped until max_tick without all pending_messages being received");
}
