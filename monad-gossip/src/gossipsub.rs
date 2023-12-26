use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use bytes::Buf;
use bytes_utils::SegmentedBuf;
use monad_crypto::{
    hasher::{Hash, Hasher, HasherType},
    secp256k1::PubKey,
};
use monad_types::{NodeId, RouterTarget};
use rand::{seq::IteratorRandom, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, GossipMessage};

pub struct UnsafeGossipsubConfig {
    pub seed: [u8; 32],
    pub me: NodeId,
    pub all_peers: Vec<NodeId>,
    pub fanout: usize,
}

impl UnsafeGossipsubConfig {
    pub fn build(self) -> UnsafeGossipsub {
        UnsafeGossipsub {
            rng: ChaCha20Rng::from_seed(self.seed),
            config: self,

            connections: Default::default(),
            events: VecDeque::default(),
            current_tick: Duration::ZERO,

            message_cache: Default::default(),
        }
    }
}

struct Connection {
    buffer_status: BufferStatus,
    buffer: SegmentedBuf<GossipMessage>,
}

impl Connection {
    fn new() -> Self {
        Self {
            buffer_status: BufferStatus::None,
            buffer: SegmentedBuf::new(),
        }
    }
}

pub struct UnsafeGossipsub {
    config: UnsafeGossipsubConfig,
    rng: ChaCha20Rng,

    connections: HashMap<NodeId, Connection>,
    events: VecDeque<GossipEvent>,
    current_tick: Duration,

    /// Cache of received message_ids
    /// We don't GC this, as UnsafeGossipsub is only used for tests/benchmarks
    message_cache: HashSet<Hash>,
}

/// The validity of anything in MessageHeader is unverified - hence UnsafeGossipsub
/// For example, `creator` could be spoofed
#[derive(Clone, Serialize, Deserialize)]
struct MessageHeader {
    id: [u8; 32],
    creator: Vec<u8>, // serialized NodeId
    broadcast: bool,
    message_len: u32,
}

type MessageHeaderLenType = u32;
const MESSAGE_HEADER_LEN_LEN: usize = std::mem::size_of::<MessageHeaderLenType>();

enum BufferStatus {
    None,
    HeaderLen(MessageHeaderLenType),
    Header(MessageHeader),
}
impl Default for BufferStatus {
    fn default() -> Self {
        Self::None
    }
}

impl UnsafeGossipsub {
    /// to must not be self
    fn send_message(&mut self, to: NodeId, header: MessageHeader, message: AppMessage) {
        if self.connections.contains_key(&to) {
            assert_eq!(header.message_len, message.len() as u32);
            let header = bincode::serialize(&header).unwrap();
            let gossip_message = std::iter::once(
                Vec::from((header.len() as MessageHeaderLenType).to_le_bytes()).into(),
            )
            .chain(std::iter::once(header.into()))
            .chain(std::iter::once(message));
            self.events
                .extend(gossip_message.map(|gossip_message| GossipEvent::Send(to, gossip_message)));
        } else {
            self.events.push_back(GossipEvent::RequestConnect(to));
        }
    }

    fn handle_message(&mut self, from: NodeId, header: MessageHeader, message: AppMessage) {
        if self.message_cache.contains(&Hash(header.id)) {
            return;
        }
        self.message_cache.insert(Hash(header.id));
        let creator = NodeId(
            PubKey::from_slice(&header.creator).expect("invalid pubkey in GossipSub message"),
        );

        if header.broadcast {
            for to in self
                .config
                .all_peers
                .iter()
                .copied()
                .filter(|peer| peer != &self.config.me && peer != &from && peer != &creator)
                .choose_multiple(&mut self.rng, self.config.fanout)
            {
                self.send_message(to, header.clone(), message.clone());
            }
        }
        self.events.push_back(GossipEvent::Emit(creator, message));
    }
}

impl Gossip for UnsafeGossipsub {
    fn connected(&mut self, time: Duration, peer: NodeId) {
        self.current_tick = time;
        let removed = self.connections.insert(peer, Connection::new());
        assert!(removed.is_none());
    }

    fn disconnected(&mut self, time: Duration, peer: NodeId) {
        self.current_tick = time;
        let removed = self.connections.remove(&peer);
        assert!(removed.is_some());

        // remove all pending sends to the disconnected peer
        self.events
            .retain(|event| !matches!(event, GossipEvent::Send(to, _) if to == &peer))
    }

    fn send(&mut self, time: Duration, to: RouterTarget, message: AppMessage) {
        self.current_tick = time;
        match to {
            // when self.handle_message is called, the broadcast actually happens
            RouterTarget::Broadcast => self.handle_message(
                self.config.me,
                MessageHeader {
                    id: {
                        let mut hasher = HasherType::new();
                        hasher.update(&message);
                        hasher.hash().0
                    },
                    creator: self.config.me.0.bytes(),
                    broadcast: true,
                    message_len: message.len().try_into().unwrap(),
                },
                message,
            ),
            RouterTarget::PointToPoint(to) => {
                if to == self.config.me {
                    self.events.push_back(GossipEvent::Emit(to, message));
                } else {
                    self.send_message(
                        to,
                        MessageHeader {
                            id: {
                                let mut hasher = HasherType::new();
                                hasher.update(&message);
                                hasher.hash().0
                            },
                            creator: self.config.me.0.bytes(),
                            broadcast: false,
                            message_len: message.len().try_into().unwrap(),
                        },
                        message,
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
        let connection = self
            .connections
            .get_mut(&from)
            .expect("invariant: Gossip::connected must have been called before");
        connection.buffer.push(gossip_message);
        loop {
            let connection = self
                .connections
                .get_mut(&from)
                .expect("invariant: Gossip::connected must have been called before");
            match &connection.buffer_status {
                BufferStatus::None => {
                    if connection.buffer.remaining() >= MESSAGE_HEADER_LEN_LEN {
                        connection.buffer_status =
                            BufferStatus::HeaderLen(MessageHeaderLenType::from_le_bytes(
                                connection
                                    .buffer
                                    .copy_to_bytes(MESSAGE_HEADER_LEN_LEN)
                                    .to_vec()
                                    .try_into()
                                    .unwrap(),
                            ));
                        continue;
                    }
                }
                BufferStatus::HeaderLen(header_len) => {
                    let header_len = *header_len as usize;
                    if connection.buffer.remaining() >= header_len {
                        connection.buffer_status = BufferStatus::Header(
                            bincode::deserialize(&connection.buffer.copy_to_bytes(header_len))
                                .unwrap(),
                        );
                        continue;
                    }
                }
                BufferStatus::Header(header) => {
                    if connection.buffer.remaining() >= header.message_len as usize {
                        let message = connection.buffer.copy_to_bytes(header.message_len as usize);
                        let header = header.clone();
                        connection.buffer_status = BufferStatus::None;
                        self.handle_message(from, header, message);
                        continue;
                    }
                }
            };
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

    use monad_crypto::hasher::{Hasher, HasherType};
    use monad_transformer::{BytesSplitterTransformer, BytesTransformer, LatencyTransformer};
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;

    use super::UnsafeGossipsubConfig;
    use crate::testutil::{make_swarm, test_broadcast, test_direct};

    const NUM_NODES: u16 = 100;
    const PAYLOAD_SIZE_BYTES: usize = 1024;
    const FANOUT: usize = 7;

    #[test]
    fn test_framed_messages() {
        let mut swarm = make_swarm(
            NUM_NODES,
            |all_peers, me| {
                UnsafeGossipsubConfig {
                    seed: {
                        let mut hasher = HasherType::new();
                        hasher.update(&me.0.bytes());
                        hasher.hash().0
                    },
                    me: *me,
                    all_peers: all_peers.to_vec(),
                    fanout: FANOUT,
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
            0.95,
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
                UnsafeGossipsubConfig {
                    seed: {
                        let mut hasher = HasherType::new();
                        hasher.update(&me.0.bytes());
                        hasher.hash().0
                    },
                    me: *me,
                    all_peers: all_peers.to_vec(),
                    fanout: FANOUT,
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
            0.95,
        );
        test_direct(
            &mut rng,
            &mut swarm,
            Duration::from_secs(1),
            PAYLOAD_SIZE_BYTES,
        );
    }
}
