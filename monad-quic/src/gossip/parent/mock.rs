use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Duration,
};

use monad_executor_glue::{PeerId, RouterTarget};
use monad_proto::error::ProtoError;
use monad_types::{Deserializable, Serializable};
use sha2::Digest;

use super::{Backup, Primary};
use crate::gossip::GossipEvent;

pub struct MockPrimary {
    config: MockPrimaryConfig,
    pending_events: VecDeque<(Duration, GossipEvent<MockPrimaryGossipMessage>)>,
}
pub struct MockPrimaryConfig {
    pub peers: Vec<PeerId>,
}
impl Primary for MockPrimary {
    type Config = MockPrimaryConfig;
    type GossipMessage = MockPrimaryGossipMessage;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            pending_events: Default::default(),
        }
    }

    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]) {
        assert!(
            time >= self
                .pending_events
                .back()
                .map(|(tick, _)| *tick)
                .unwrap_or(Duration::ZERO)
        );
        match to {
            RouterTarget::Broadcast => {
                for tx_peer in &self.config.peers {
                    self.pending_events.push_back((
                        time,
                        GossipEvent::Send(
                            *tx_peer,
                            MockPrimaryGossipMessage {
                                message: message.to_vec(),
                            },
                        ),
                    ))
                }
            }
            RouterTarget::PointToPoint(tx_peer) => self.pending_events.push_back((
                time,
                GossipEvent::Send(
                    tx_peer,
                    MockPrimaryGossipMessage {
                        message: message.to_vec(),
                    },
                ),
            )),
        }
    }

    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: PeerId,
        gossip_message: Self::GossipMessage,
    ) {
        assert!(
            time >= self
                .pending_events
                .back()
                .map(|(tick, _)| *tick)
                .unwrap_or(Duration::ZERO)
        );
        self.pending_events
            .push_back((time, GossipEvent::Emit(from, gossip_message.message)))
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.pending_events.front().map(|(tick, _)| *tick)
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::GossipMessage>> {
        let next_tick = self.peek_tick()?;
        if time >= next_tick {
            self.pending_events.pop_front().map(|(_, event)| event)
        } else {
            None
        }
    }
}

pub struct MockPrimaryGossipMessage {
    message: Vec<u8>,
}

impl Serializable<Vec<u8>> for MockPrimaryGossipMessage {
    fn serialize(&self) -> Vec<u8> {
        self.message.clone()
    }
}

impl Deserializable<[u8]> for MockPrimaryGossipMessage {
    type ReadError = ProtoError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        Ok(Self {
            message: message.to_vec(),
        })
    }
}

type Hash = [u8; 32];

pub struct MockBackup {
    config: MockBackupConfig,

    message_pool: BTreeMap<(PeerId, Hash), Vec<u8>>,
    pending_events: VecDeque<(Duration, GossipEvent<MockBackupGossipMessage>)>,
    pending_retransmits: BTreeMap<Duration, BTreeSet<(PeerId, Hash)>>,
}
pub struct MockBackupConfig {
    pub peers: Vec<PeerId>,
    pub retransmit_delay: Duration,
}
impl Backup for MockBackup {
    type Config = MockBackupConfig;
    type MessageId = Hash;
    type GossipMessage = MockBackupGossipMessage;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            message_pool: Default::default(),
            pending_events: Default::default(),
            pending_retransmits: Default::default(),
        }
    }

    fn message_id(message: &[u8]) -> <Self as Backup>::MessageId {
        let mut hasher = sha2::Sha256::new();
        hasher.update(message);
        hasher.finalize().into()
    }

    fn sent(&mut self, time: Duration, to: RouterTarget, message: &[u8]) -> Self::MessageId {
        let message_id = Self::message_id(message);
        let entry = self
            .pending_retransmits
            .entry(time + self.config.retransmit_delay)
            .or_default();

        match to {
            RouterTarget::Broadcast => {
                for to in &self.config.peers {
                    entry.insert((*to, message_id));
                    self.message_pool
                        .insert((*to, message_id), message.to_vec());
                }
            }
            RouterTarget::PointToPoint(to) => {
                entry.insert((to, message_id));
                self.message_pool.insert((to, message_id), message.to_vec());
            }
        };

        message_id
    }

    fn forget(&mut self, time: Duration, to: RouterTarget, message_id: Self::MessageId) {
        match to {
            RouterTarget::Broadcast => {
                for to in &self.config.peers {
                    self.message_pool.remove(&(*to, message_id));
                }
            }
            RouterTarget::PointToPoint(to) => {
                self.message_pool.remove(&(to, message_id));
            }
        };
    }

    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: PeerId,
        gossip_message: Self::GossipMessage,
    ) {
        match gossip_message {
            MockBackupGossipMessage::Ack { message_id } => {
                self.message_pool.remove(&(from, message_id));
            }

            // is it ok to emit duplicate messages?
            MockBackupGossipMessage::Retransmit { message } => {
                self.pending_events
                    .push_back((time, GossipEvent::Emit(from, message)));
            }
        }
    }

    fn ack_message(&mut self, time: Duration, source: PeerId, message_id: Self::MessageId) {
        self.pending_events.push_back((
            time,
            GossipEvent::Send(source, MockBackupGossipMessage::Ack { message_id }),
        ));
    }

    fn peek_tick(&self) -> Option<Duration> {
        std::iter::empty()
            .chain(self.pending_events.front().map(|(tick, _)| tick))
            .chain(
                self.pending_retransmits
                    .first_key_value()
                    .map(|(tick, _)| tick),
            )
            .copied()
            .min()
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::GossipMessage>> {
        let next_tick = self.peek_tick()?;
        if time < next_tick {
            return None;
        }
        if self.pending_events.front().map(|(tick, _)| tick) == Some(&next_tick) {
            self.pending_events.pop_front().map(|(_, event)| event)
        } else {
            let mut entry = self
                .pending_retransmits
                .first_entry()
                .expect("invariant broken");
            let queue = entry.get_mut();

            let key = queue.pop_first().expect("must be nonempty");
            if queue.is_empty() {
                self.pending_retransmits.pop_first();
            }

            self.message_pool.get(&key).map(|message| {
                self.pending_retransmits
                    .entry(time + self.config.retransmit_delay)
                    .or_default()
                    .insert(key);
                GossipEvent::Send(
                    key.0,
                    MockBackupGossipMessage::Retransmit {
                        message: message.to_vec(),
                    },
                )
            })
        }
    }
}

pub enum MockBackupGossipMessage {
    Ack { message_id: Hash },
    Retransmit { message: Vec<u8> },
}

impl Serializable<Vec<u8>> for MockBackupGossipMessage {
    fn serialize(&self) -> Vec<u8> {
        match self {
            MockBackupGossipMessage::Ack { message_id } => std::iter::once(0_u8)
                .chain(message_id.as_slice().iter().copied())
                .collect(),
            MockBackupGossipMessage::Retransmit { message } => std::iter::once(1_u8)
                .chain(message.iter().copied())
                .collect(),
        }
    }
}

impl Deserializable<[u8]> for MockBackupGossipMessage {
    type ReadError = ProtoError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        match message.first() {
            Some(0) => {
                let message_id = message
                    .iter()
                    .skip(1)
                    .copied()
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| ProtoError::DeserializeError("invalid ack".to_owned()))?;
                Ok(Self::Ack { message_id })
            }
            Some(1) => Ok(Self::Retransmit {
                message: message.iter().copied().skip(1).collect(),
            }),
            _ => Err(ProtoError::DeserializeError("failed to deser".to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_crypto::secp256k1::KeyPair;
    use monad_executor_glue::PeerId;
    use monad_mock_swarm::transformer::{
        BytesSplitterTransformer, BytesTransformer, DropTransformer, LatencyTransformer,
        PeriodicTransformer,
    };
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;

    use crate::gossip::{
        parent::Parent,
        testutil::{test_broadcast, test_direct, Swarm},
    };

    use super::{MockBackup, MockBackupConfig, MockPrimary, MockPrimaryConfig};

    #[test]
    fn test_framed_messages() {
        let peers: Vec<_> = (1..=10_u8)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = KeyPair::from_bytes(&mut key).unwrap();
                PeerId(keypair.pubkey())
            })
            .collect();
        let mut swarm: Swarm<Parent<MockPrimary, MockBackup>> =
            Swarm::new(peers.iter().map(|peer_id| {
                (
                    *peer_id,
                    (
                        MockPrimaryConfig {
                            peers: peers.clone(),
                        },
                        MockBackupConfig {
                            peers: peers.clone(),
                            retransmit_delay: Duration::from_millis(500),
                        },
                    ),
                    vec![BytesTransformer::Latency(LatencyTransformer(
                        Duration::from_millis(5),
                    ))],
                )
            }));

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(&mut rng, &mut swarm, Duration::from_secs(10));
        test_direct(&mut rng, &mut swarm, Duration::from_secs(10));
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
        let mut swarm: Swarm<Parent<MockPrimary, MockBackup>> =
            Swarm::new(peers.iter().map(|peer_id| {
                (
                    *peer_id,
                    (
                        MockPrimaryConfig {
                            peers: peers.clone(),
                        },
                        MockBackupConfig {
                            peers: peers.clone(),
                            retransmit_delay: Duration::from_millis(500),
                        },
                    ),
                    vec![
                        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(5))),
                        BytesTransformer::BytesSplitter(BytesSplitterTransformer::new()),
                    ],
                )
            }));

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(&mut rng, &mut swarm, Duration::from_secs(10));
        test_direct(&mut rng, &mut swarm, Duration::from_secs(10));
    }

    #[test]
    fn test_dropped_messages() {
        let peers: Vec<_> = (1..=10_u8)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = KeyPair::from_bytes(&mut key).unwrap();
                PeerId(keypair.pubkey())
            })
            .collect();
        let mut swarm: Swarm<Parent<MockPrimary, MockBackup>> =
            Swarm::new(peers.iter().map(|peer_id| {
                (
                    *peer_id,
                    (
                        MockPrimaryConfig {
                            peers: peers.clone(),
                        },
                        MockBackupConfig {
                            peers: peers.clone(),
                            retransmit_delay: Duration::from_millis(500),
                        },
                    ),
                    vec![
                        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(5))),
                        BytesTransformer::Periodic(PeriodicTransformer::new(
                            Duration::ZERO,
                            Duration::from_secs(2),
                        )),
                        BytesTransformer::Drop(DropTransformer()),
                    ],
                )
            }));

        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        test_broadcast(&mut rng, &mut swarm, Duration::from_secs(10));
        test_direct(&mut rng, &mut swarm, Duration::from_secs(10));
    }
}
