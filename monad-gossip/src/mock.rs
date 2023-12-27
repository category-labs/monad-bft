use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use bytes::Buf;
use bytes_utils::SegmentedBuf;
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

            read_buffers: Default::default(),
            events: VecDeque::default(),
            current_tick: Duration::ZERO,
        }
    }
}

pub struct MockGossip {
    config: MockGossipConfig,

    read_buffers: HashMap<NodeId, (Option<MessageLenType>, SegmentedBuf<GossipMessage>)>,
    events: VecDeque<GossipEvent>,
    current_tick: Duration,
}

type MessageLenType = u32;
const MESSAGE_HEADER_LEN: usize = std::mem::size_of::<MessageLenType>();

impl Gossip for MockGossip {
    fn send(&mut self, time: Duration, to: RouterTarget, message: AppMessage) {
        self.current_tick = time;
        let gossip_message_header: GossipMessage =
            Vec::from((message.len() as MessageLenType).to_le_bytes()).into();
        let gossip_message =
            std::iter::once(gossip_message_header).chain(std::iter::once(message.clone()));
        match to {
            RouterTarget::Broadcast => {
                for peer in self
                    .config
                    .all_peers
                    .iter()
                    .filter(|to| to != &&self.config.me)
                {
                    self.events.extend(
                        gossip_message
                            .clone()
                            .map(|gossip_message| GossipEvent::Send(*peer, gossip_message)),
                    );
                }
                self.events
                    .push_back(GossipEvent::Emit(self.config.me, message))
            }
            RouterTarget::PointToPoint(to) => {
                if to == self.config.me {
                    self.events
                        .push_back(GossipEvent::Emit(self.config.me, message))
                } else {
                    self.events.extend(
                        gossip_message.map(|gossip_message| GossipEvent::Send(to, gossip_message)),
                    )
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
        let (maybe_message_len, read_buffer) = self.read_buffers.entry(from).or_default();
        read_buffer.push(gossip_message);
        loop {
            if maybe_message_len.is_none() && read_buffer.remaining() >= MESSAGE_HEADER_LEN {
                *maybe_message_len = Some(MessageLenType::from_le_bytes(
                    read_buffer
                        .copy_to_bytes(MESSAGE_HEADER_LEN)
                        .to_vec()
                        .try_into()
                        .unwrap(),
                ));
            }
            if let Some(message_len) = *maybe_message_len {
                if read_buffer.remaining() >= message_len as usize {
                    let message = read_buffer.copy_to_bytes(message_len as usize);
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
