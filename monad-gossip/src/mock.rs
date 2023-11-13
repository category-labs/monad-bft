use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use monad_types::{NodeId, RouterTarget};

use super::{Gossip, GossipEvent};

pub struct MockGossipConfig {
    pub all_peers: Vec<NodeId>,
}

impl MockGossipConfig {
    pub fn build(self) -> MockGossip {
        MockGossip {
            config: self,

            read_buffers: Default::default(),
            events: VecDeque::default(),
            current_tick: Duration::ZERO,
        }
    }
}

pub struct MockGossip {
    config: MockGossipConfig,

    read_buffers: HashMap<NodeId, (Option<MessageLenType>, Vec<u8>)>,
    events: VecDeque<GossipEvent<Vec<u8>>>,
    current_tick: Duration,
}

type MessageLenType = u32;
const MESSAGE_HEADER_LEN: usize = std::mem::size_of::<MessageLenType>();

impl Gossip for MockGossip {
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

    fn handle_gossip_message(&mut self, time: Duration, from: NodeId, gossip_message: &[u8]) {
        self.current_tick = time;
        let (maybe_message_len, read_buffer) = self.read_buffers.entry(from).or_default();
        read_buffer.extend(gossip_message.iter());
        loop {
            if maybe_message_len.is_none() && read_buffer.len() >= MESSAGE_HEADER_LEN {
                *maybe_message_len = Some(MessageLenType::from_le_bytes(
                    read_buffer
                        .iter()
                        .copied()
                        .take(MESSAGE_HEADER_LEN)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                ));
                read_buffer.drain(0..MESSAGE_HEADER_LEN);
            }
            if let Some(message_len) = *maybe_message_len {
                if read_buffer.len() >= message_len as usize {
                    let remaining = read_buffer.split_off(message_len as usize);
                    let message = std::mem::replace(read_buffer, remaining);
                    self.events.push_back(GossipEvent::Emit(from, message));
                    *maybe_message_len = None;
                    continue;
                }
            }
            break;
        }
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.current_tick)
        } else {
            None
        }
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Vec<u8>>> {
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
            |all_peers, _me| {
                MockGossipConfig {
                    all_peers: all_peers.to_vec(),
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
            |all_peers, _me| {
                MockGossipConfig {
                    all_peers: all_peers.to_vec(),
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
