use std::{collections::VecDeque, time::Duration};

use monad_types::{NodeId, RouterTarget};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, GossipMessage};

pub struct MockGossipConfig {
    pub all_peers: Vec<NodeId>,
    pub me: NodeId,
}

impl MockGossipConfig {
    pub fn build(self) -> MockGossip {
        MockGossip {
            config: self,

            events: VecDeque::default(),
            current_tick: Duration::ZERO,
        }
    }
}

pub struct MockGossip {
    config: MockGossipConfig,

    events: VecDeque<GossipEvent>,
    current_tick: Duration,
}

impl Gossip for MockGossip {
    fn send(&mut self, time: Duration, to: RouterTarget, message: AppMessage) {
        self.current_tick = time;
        match to {
            RouterTarget::Broadcast => {
                for to in &self.config.all_peers {
                    if to == &self.config.me {
                        self.events
                            .push_back(GossipEvent::Emit(self.config.me, message.clone()))
                    } else {
                        self.events.push_back(GossipEvent::Send(
                            *to,
                            std::iter::once(message.clone()).collect(),
                        ))
                    }
                }
            }
            RouterTarget::PointToPoint(to) => {
                if to == self.config.me {
                    self.events
                        .push_back(GossipEvent::Emit(self.config.me, message))
                } else {
                    self.events
                        .push_back(GossipEvent::Send(to, std::iter::once(message).collect()))
                }
            }
        }
    }

    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: NodeId,
        gossip_message: GossipMessage,
    ) {
        self.current_tick = time;
        self.events
            .push_back(GossipEvent::Emit(from, gossip_message));
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.current_tick)
        } else {
            None
        }
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent> {
        assert!(time >= self.current_tick);
        self.events.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_transformer::{BytesSplitterTransformer, BytesTransformer, LatencyTransformer};
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;

    use super::MockGossipConfig;
    use crate::testutil::{make_swarm, test_broadcast, test_direct};

    const NUM_NODES: u16 = 10;
    const PAYLOAD_SIZE_BYTES: usize = 1024;

    #[test]
    fn test_framed_messages() {
        let mut swarm = make_swarm(
            NUM_NODES,
            |all_peers, me| {
                MockGossipConfig {
                    all_peers: all_peers.to_vec(),
                    me: *me,
                }
                .build()
            },
            |_all_peers, _me| {
                vec![BytesTransformer::Latency(LatencyTransformer(
                    Duration::from_millis(5),
                ))]
            },
        );

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(
            &mut rng,
            &mut swarm,
            Duration::from_secs(1),
            PAYLOAD_SIZE_BYTES,
            usize::MAX,
            1.0,
        );
        test_direct(
            &mut rng,
            &mut swarm,
            Duration::from_secs(1),
            PAYLOAD_SIZE_BYTES,
        );
    }

    #[test]
    fn test_split_messages() {
        let mut swarm = make_swarm(
            NUM_NODES,
            |all_peers, me| {
                MockGossipConfig {
                    all_peers: all_peers.to_vec(),
                    me: *me,
                }
                .build()
            },
            |_all_peers, _me| {
                vec![
                    BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(5))),
                    BytesTransformer::BytesSplitter(BytesSplitterTransformer::new()),
                ]
            },
        );

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(
            &mut rng,
            &mut swarm,
            Duration::from_secs(1),
            PAYLOAD_SIZE_BYTES,
            usize::MAX,
            1.0,
        );
        test_direct(
            &mut rng,
            &mut swarm,
            Duration::from_secs(1),
            PAYLOAD_SIZE_BYTES,
        );
    }
}
