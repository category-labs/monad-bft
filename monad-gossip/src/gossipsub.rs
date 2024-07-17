use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use bytes::Buf;
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hash, Hasher, HasherType},
};
use monad_types::{NodeId, RouterTarget};
use rand::{seq::IteratorRandom, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, GossipMessage};

pub struct UnsafeGossipsubConfig<PT: PubKey> {
    pub seed: [u8; 32],
    pub me: NodeId<PT>,
    pub all_peers: Vec<NodeId<PT>>,
    pub fanout: usize,
}

impl<PT: PubKey> UnsafeGossipsubConfig<PT> {
    pub fn build(self) -> UnsafeGossipsub<PT> {
        UnsafeGossipsub {
            rng: ChaCha20Rng::from_seed(self.seed),
            config: self,

            events: VecDeque::default(),
            current_tick: Duration::ZERO,

            message_cache: Default::default(),
        }
    }
}

pub struct UnsafeGossipsub<PT: PubKey> {
    config: UnsafeGossipsubConfig<PT>,
    rng: ChaCha20Rng,

    events: VecDeque<GossipEvent<PT>>,
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

#[derive(Serialize, Deserialize)]
struct OuterHeader(u32);
const OUTER_HEADER_SIZE: usize = std::mem::size_of::<OuterHeader>();

impl<PT: PubKey> UnsafeGossipsub<PT> {
    /// to must not be self
    fn send_message(&mut self, to: NodeId<PT>, header: MessageHeader, message: AppMessage) {
        assert_eq!(header.message_len, message.len() as u32);
        let header = bincode::serialize(&header).unwrap();
        let outer_header = bincode::serialize(&OuterHeader(header.len() as u32)).unwrap();
        let gossip_message = std::iter::empty()
            .chain(std::iter::once(outer_header.into()))
            .chain(std::iter::once(header.into()))
            .chain(std::iter::once(message))
            .collect();
        self.events.push_back(GossipEvent::Send(to, gossip_message));
    }

    fn handle_message(&mut self, from: NodeId<PT>, header: MessageHeader, message: AppMessage) {
        if self.message_cache.contains(&Hash(header.id)) {
            return;
        }
        self.message_cache.insert(Hash(header.id));
        let creator = NodeId::new(
            PT::from_bytes(&header.creator).expect("invalid pubkey in GossipSub message"),
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

impl<PT: PubKey> Gossip for UnsafeGossipsub<PT> {
    type NodeIdPubKey = PT;

    fn send(&mut self, time: Duration, to: RouterTarget<Self::NodeIdPubKey>, message: AppMessage) {
        self.current_tick = time;
        match to {
            // when self.handle_message is called, the broadcast actually happens
            RouterTarget::Broadcast(_, _) | RouterTarget::Raptorcast(_, _) => self.handle_message(
                self.config.me,
                MessageHeader {
                    id: {
                        let mut hasher = HasherType::new();
                        hasher.update(&message);
                        hasher.hash().0
                    },
                    creator: self.config.me.pubkey().bytes(),
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
                            creator: self.config.me.pubkey().bytes(),
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
        from: NodeId<Self::NodeIdPubKey>,
        mut gossip_message: GossipMessage,
    ) {
        let outer_header: OuterHeader =
            bincode::deserialize(&gossip_message.copy_to_bytes(OUTER_HEADER_SIZE)).unwrap();
        let message_header: MessageHeader =
            bincode::deserialize(&gossip_message.copy_to_bytes(outer_header.0 as usize)).unwrap();
        assert_eq!(
            gossip_message.remaining() as u32,
            message_header.message_len
        );
        let message = gossip_message.copy_to_bytes(gossip_message.remaining());
        self.current_tick = time;
        self.handle_message(from, message_header, message);
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.current_tick)
        } else {
            None
        }
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::NodeIdPubKey>> {
        assert!(time >= self.current_tick);
        self.events.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_crypto::{
        certificate_signature::{CertificateSignaturePubKey, PubKey},
        hasher::{Hasher, HasherType},
        NopSignature,
    };
    use monad_transformer::{BytesTransformer, LatencyTransformer};
    use monad_types::NodeId;
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;

    use super::UnsafeGossipsubConfig;
    use crate::testutil::{make_swarm, test_broadcast, test_direct};

    const NUM_NODES: u16 = 100;
    const PAYLOAD_SIZE_BYTES: usize = 1024;
    const FANOUT: usize = 7;

    #[test]
    fn test_framed_messages() {
        let mut swarm = make_swarm::<NopSignature, _>(
            NUM_NODES,
            |all_peers, me: &NodeId<CertificateSignaturePubKey<NopSignature>>| {
                UnsafeGossipsubConfig {
                    seed: {
                        let mut hasher = HasherType::new();
                        hasher.update(me.pubkey().bytes());
                        hasher.hash().0
                    },
                    me: *me,
                    all_peers: all_peers.to_vec(),
                    fanout: FANOUT,
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
