use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use monad_crypto::{
    hasher::{Hash, Hasher, HasherType},
    secp256k1::PubKey,
};
use monad_executor_glue::{PeerId, RouterTarget};
use rand::{seq::IteratorRandom, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};

use super::{Gossip, GossipEvent};

pub struct UnsafeGossipsubConfig {
    pub seed: [u8; 32],
    pub me: PeerId,
    pub all_peers: Vec<PeerId>,
    pub fanout: usize,
}

pub struct UnsafeGossipsub {
    config: UnsafeGossipsubConfig,
    rng: ChaCha20Rng,

    read_buffers: HashMap<PeerId, (BufferStatus, Vec<u8>)>,
    events: VecDeque<GossipEvent<Vec<u8>>>,
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
    creator: Vec<u8>, // serialized PeerId
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
    fn send_message(&mut self, to: PeerId, header: MessageHeader, message: &[u8]) {
        assert_eq!(header.message_len, message.len() as u32);
        let header = bincode::serialize(&header).unwrap();
        let mut gossip_message = Vec::from((header.len() as MessageHeaderLenType).to_le_bytes());
        gossip_message.extend_from_slice(&header);
        gossip_message.extend_from_slice(message);
        self.events.push_back(GossipEvent::Send(to, gossip_message));
    }

    fn handle_message(&mut self, from: PeerId, header: MessageHeader, message: Vec<u8>) {
        if self.message_cache.contains(&Hash(header.id)) {
            return;
        }
        self.message_cache.insert(Hash(header.id));
        let creator = PeerId(
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
                self.send_message(to, header.clone(), &message);
            }
        }
        self.events.push_back(GossipEvent::Emit(creator, message));
    }
}

impl Gossip for UnsafeGossipsub {
    type Config = UnsafeGossipsubConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            rng: ChaCha20Rng::from_seed(config.seed),
            config,

            read_buffers: Default::default(),
            events: VecDeque::default(),
            current_tick: Duration::ZERO,

            message_cache: Default::default(),
        }
    }

    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]) {
        self.current_tick = time;
        match to {
            // when self.handle_message is called, the broadcast actually happens
            RouterTarget::Broadcast => self.send_message(
                self.config.me,
                MessageHeader {
                    id: {
                        let mut hasher = HasherType::new();
                        hasher.update(message);
                        hasher.hash().0
                    },
                    creator: self.config.me.0.bytes(),
                    broadcast: true,
                    message_len: message.len().try_into().unwrap(),
                },
                message,
            ),
            RouterTarget::PointToPoint(to) => self.send_message(
                to,
                MessageHeader {
                    id: {
                        let mut hasher = HasherType::new();
                        hasher.update(message);
                        hasher.hash().0
                    },
                    creator: self.config.me.0.bytes(),
                    broadcast: false,
                    message_len: message.len().try_into().unwrap(),
                },
                message,
            ),
        }
    }

    fn handle_gossip_message(&mut self, time: Duration, from: PeerId, gossip_message: &[u8]) {
        self.current_tick = time;
        let (_buffer_status, read_buffer) = self.read_buffers.entry(from).or_default();
        read_buffer.extend(gossip_message.iter());
        loop {
            let (buffer_status, read_buffer) = self.read_buffers.entry(from).or_default();
            match buffer_status {
                BufferStatus::None => {
                    if read_buffer.len() >= MESSAGE_HEADER_LEN_LEN {
                        *buffer_status =
                            BufferStatus::HeaderLen(MessageHeaderLenType::from_le_bytes(
                                read_buffer
                                    .iter()
                                    .copied()
                                    .take(MESSAGE_HEADER_LEN_LEN)
                                    .collect::<Vec<_>>()
                                    .try_into()
                                    .unwrap(),
                            ));
                        read_buffer.drain(0..MESSAGE_HEADER_LEN_LEN);
                        continue;
                    }
                }
                BufferStatus::HeaderLen(header_len) => {
                    let header_len = *header_len as usize;
                    if read_buffer.len() >= header_len {
                        *buffer_status = BufferStatus::Header(
                            bincode::deserialize(&read_buffer[..header_len]).unwrap(),
                        );
                        read_buffer.drain(0..header_len);
                        continue;
                    }
                }
                BufferStatus::Header(header) => {
                    if read_buffer.len() >= header.message_len as usize {
                        let remaining = read_buffer.split_off(header.message_len as usize);
                        let message = std::mem::replace(read_buffer, remaining);
                        let header = header.clone();
                        *buffer_status = BufferStatus::None;
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

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Vec<u8>>> {
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

    use super::{super::testutil::Swarm, UnsafeGossipsub, UnsafeGossipsubConfig};
    use crate::testutil::{test_broadcast, test_direct};

    #[test]
    fn test_framed_messages() {
        let peers: Vec<_> = (1..=100_u8)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = KeyPair::from_bytes(&mut key).unwrap();
                PeerId(keypair.pubkey())
            })
            .collect();
        let mut swarm: Swarm<UnsafeGossipsub> =
            Swarm::new(peers.iter().enumerate().map(|(idx, peer_id)| {
                (
                    *peer_id,
                    UnsafeGossipsubConfig {
                        seed: [idx.try_into().unwrap(); 32],
                        me: *peer_id,
                        all_peers: peers.clone(),
                        fanout: 7,
                    },
                    vec![BytesTransformer::Latency(LatencyTransformer(
                        Duration::from_millis(5),
                    ))],
                )
            }));

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(&mut rng, &mut swarm, Duration::from_secs(1), 0.95);
        test_direct(&mut rng, &mut swarm, Duration::from_secs(1));
    }

    #[test]
    fn test_split_messages() {
        let peers: Vec<_> = (1..=100_u8)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = KeyPair::from_bytes(&mut key).unwrap();
                PeerId(keypair.pubkey())
            })
            .collect();
        let mut swarm: Swarm<UnsafeGossipsub> =
            Swarm::new(peers.iter().enumerate().map(|(idx, peer_id)| {
                (
                    *peer_id,
                    UnsafeGossipsubConfig {
                        seed: [idx.try_into().unwrap(); 32],
                        me: *peer_id,
                        all_peers: peers.clone(),
                        fanout: 7,
                    },
                    vec![
                        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(5))),
                        BytesTransformer::BytesSplitter(BytesSplitterTransformer::new()),
                    ],
                )
            }));

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(&mut rng, &mut swarm, Duration::from_secs(1), 0.95);
        test_direct(&mut rng, &mut swarm, Duration::from_secs(1));
    }
}
