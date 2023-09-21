use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use monad_executor_glue::{PeerId, RouterTarget};

use super::{Gossip, GossipEvent};

pub struct MockGossipConfig {
    pub all_peers: Vec<PeerId>,
}

/// MockGossip is an implementation of Gossip that's not fully spec-compliant - it does not
/// actually support retry
pub struct MockGossip {
    config: MockGossipConfig,

    read_buffers: HashMap<PeerId, VecDeque<u8>>,
    events: VecDeque<GossipEvent>,
    current_tick: Duration,
}

type MessageLenType = u32;
const MESSAGE_HEADER_LEN: usize = std::mem::size_of::<MessageLenType>();

impl Gossip for MockGossip {
    type Config = MockGossipConfig;
    type MessageId = ();

    fn new(config: Self::Config) -> Self {
        Self {
            config,

            read_buffers: Default::default(),
            events: VecDeque::default(),
            current_tick: Duration::ZERO,
        }
    }

    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]) {
        self.current_tick = time;
        let mut gossip_message = Vec::from((message.len() as MessageLenType).to_le_bytes());
        gossip_message.extend_from_slice(message);
        match to {
            RouterTarget::Broadcast => {
                for peer in &self.config.all_peers {
                    self.events
                        .push_back(GossipEvent::Send(*peer, gossip_message.clone()))
                }
            }
            RouterTarget::PointToPoint(to) => {
                self.events.push_back(GossipEvent::Send(to, gossip_message))
            }
        }
    }
    fn forget(&mut self, _time: Duration, _to: RouterTarget, _message: ()) {
        // empty because MockGossip doesn't support retry anyways
    }

    fn handle_gossip_message(&mut self, time: Duration, from: PeerId, gossip_message: &[u8]) {
        self.current_tick = time;
        let read_buffer = self.read_buffers.entry(from).or_default();
        read_buffer.extend(gossip_message.iter());
        while {
            let buffer_len = read_buffer.len();
            if buffer_len < MESSAGE_HEADER_LEN {
                false
            } else {
                let message_len = MessageLenType::from_le_bytes(
                    read_buffer
                        .iter()
                        .copied()
                        .take(MESSAGE_HEADER_LEN)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                );
                buffer_len >= MESSAGE_HEADER_LEN + message_len as usize
            }
        } {
            let message_len = MessageLenType::from_le_bytes(
                read_buffer
                    .drain(..MESSAGE_HEADER_LEN)
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            ) as usize;
            let emit: Vec<u8> = read_buffer.drain(..message_len).collect();
            self.events.push_back(GossipEvent::Emit(from, emit))
        }
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

    use monad_crypto::secp256k1::KeyPair;
    use monad_executor_glue::PeerId;
    use monad_mock_swarm::transformer::{
        BytesSplitterTransformer, BytesTransformer, LatencyTransformer,
    };
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;

    use super::{super::testutil::Swarm, MockGossip, MockGossipConfig};
    use crate::gossip::testutil::{test_broadcast, test_direct};

    #[test]
    fn test_framed_messages() {
        let peers: Vec<_> = (1..=10_u8)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = KeyPair::from_bytes(&mut key).unwrap();
                PeerId(keypair.pubkey())
            })
            .collect();
        let mut swarm: Swarm<MockGossip> = Swarm::new(peers.iter().map(|peer_id| {
            (
                *peer_id,
                MockGossipConfig {
                    all_peers: peers.clone(),
                },
                vec![BytesTransformer::Latency(LatencyTransformer(
                    Duration::from_millis(5),
                ))],
            )
        }));

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(&mut rng, &mut swarm, Duration::from_secs(1));
        test_direct(&mut rng, &mut swarm, Duration::from_secs(1));
    }

    #[test]
    fn test_split_messages() {
        let peers: Vec<_> = (1..=10_u8)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = KeyPair::from_bytes(&mut key).unwrap();
                PeerId(keypair.pubkey())
            })
            .collect();
        let mut swarm: Swarm<MockGossip> = Swarm::new(peers.iter().map(|peer_id| {
            (
                *peer_id,
                MockGossipConfig {
                    all_peers: peers.clone(),
                },
                vec![
                    BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(5))),
                    BytesTransformer::BytesSplitter(BytesSplitterTransformer::new()),
                ],
            )
        }));

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(&mut rng, &mut swarm, Duration::from_secs(1));
        test_direct(&mut rng, &mut swarm, Duration::from_secs(1));
    }
}
