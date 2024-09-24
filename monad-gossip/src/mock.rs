use std::{collections::BinaryHeap, time::Duration};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, RouterTarget};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, GossipMessage};

pub struct MockGossipConfig<PT: PubKey> {
    pub all_peers: Vec<NodeId<PT>>,
    pub me: NodeId<PT>,
    pub message_delay: Duration,
}

impl<PT: PubKey> MockGossipConfig<PT> {
    pub fn build(self) -> MockGossip<PT> {
        MockGossip {
            config: self,

            events: BinaryHeap::default(),
            current_tick: Duration::ZERO,
        }
    }
}

struct TimedGossipEvent<PT: PubKey> {
    deliver: Duration,
    event: GossipEvent<PT>,
}

impl<PT: PubKey> PartialOrd for TimedGossipEvent<PT> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.deliver.cmp(&self.deliver))
    }
}

impl<PT: PubKey> Ord for TimedGossipEvent<PT> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deliver.cmp(&self.deliver)
    }
}

impl<PT: PubKey> PartialEq for TimedGossipEvent<PT> {
    fn eq(&self, other: &Self) -> bool {
        self.deliver.eq(&other.deliver)
    }
}

impl<PT: PubKey> Eq for TimedGossipEvent<PT> {}

impl<PT: PubKey> GossipEvent<PT> {
    fn into_timed(self, time: Duration) -> TimedGossipEvent<PT> {
        TimedGossipEvent {
            deliver: time,
            event: self,
        }
    }
}

pub struct MockGossip<PT: PubKey> {
    config: MockGossipConfig<PT>,

    events: BinaryHeap<TimedGossipEvent<PT>>,
    current_tick: Duration,
}

impl<PT: PubKey> Gossip for MockGossip<PT> {
    type NodeIdPubKey = PT;

    fn send(&mut self, time: Duration, to: RouterTarget<Self::NodeIdPubKey>, message: AppMessage) {
        self.current_tick = time;
        match to {
            RouterTarget::Broadcast(_) | RouterTarget::Raptorcast(_) => {
                for to in &self.config.all_peers {
                    if to == &self.config.me {
                        self.events.push(
                            GossipEvent::Emit(self.config.me, message.clone())
                                .into_timed(time + self.config.message_delay),
                        )
                    } else {
                        self.events.push(
                            GossipEvent::Send(*to, std::iter::once(message.clone()).collect())
                                .into_timed(time),
                        )
                    }
                }
            }
            RouterTarget::PointToPoint(to) | RouterTarget::TcpPointToPoint(to) => {
                if to == self.config.me {
                    self.events.push(
                        GossipEvent::Emit(self.config.me, message)
                            .into_timed(time + self.config.message_delay),
                    )
                } else {
                    self.events.push(
                        GossipEvent::Send(to, std::iter::once(message).collect()).into_timed(time),
                    )
                }
            }
        }
    }

    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: NodeId<Self::NodeIdPubKey>,
        gossip_message: GossipMessage,
    ) {
        self.current_tick = time;
        self.events
            .push(GossipEvent::Emit(from, gossip_message).into_timed(time));
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.events.peek().unwrap().deliver)
        } else {
            None
        }
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::NodeIdPubKey>> {
        assert!(time >= self.current_tick);
        if let Some(event) = self.events.peek() {
            if event.deliver <= time {
                return self.events.pop().map(|te| te.event);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_crypto::NopSignature;
    use monad_transformer::{BytesTransformer, LatencyTransformer};
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;

    use super::MockGossipConfig;
    use crate::testutil::{make_swarm, test_broadcast, test_direct};

    const NUM_NODES: u16 = 10;
    const PAYLOAD_SIZE_BYTES: usize = 1024;

    #[test]
    fn test_framed_messages() {
        let mut swarm = make_swarm::<NopSignature, _>(
            NUM_NODES,
            |all_peers, me| {
                MockGossipConfig {
                    all_peers: all_peers.to_vec(),
                    me: *me,
                    message_delay: Duration::ZERO,
                }
                .build()
            },
            |_all_peers, _me| {
                vec![BytesTransformer::Latency(LatencyTransformer::new(
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
}
