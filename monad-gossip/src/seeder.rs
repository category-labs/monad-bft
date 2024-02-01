use std::{collections::VecDeque, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, RouterTarget};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, FragmentedGossipMessage, GossipMessage};

pub struct SeederConfig<PT: PubKey> {
    pub all_peers: Vec<NodeId<PT>>,
    pub me: NodeId<PT>,
}

impl<PT: PubKey> SeederConfig<PT> {
    pub fn build(self) -> Seeder<PT> {
        Seeder {
            config: self,

            events: VecDeque::default(),
            current_tick: Duration::ZERO,
        }
    }
}

pub struct Seeder<PT: PubKey> {
    config: SeederConfig<PT>,

    events: VecDeque<GossipEvent<PT>>,
    current_tick: Duration,
}

impl<PT: PubKey> Seeder<PT> {
    fn prepare_message(message_type: MessageType, data: Bytes) -> FragmentedGossipMessage {
        let mut inner_header_buf = BytesMut::new().writer();
        bincode::serialize_into(
            &mut inner_header_buf,
            &Header {
                data_len: data.len().try_into().unwrap(),
                message_type,
            },
        )
        .expect("serializing gossip header should succeed");
        let inner_header_buf: Bytes = inner_header_buf.into_inner().into();

        let outer_header = OuterHeader(inner_header_buf.len() as u32);
        let mut outer_header_buf = BytesMut::new().writer();
        bincode::serialize_into(&mut outer_header_buf, &outer_header)
            .expect("serializing outer header should succeed");
        let outer_header_buf: Bytes = outer_header_buf.into_inner().into();

        std::iter::once(outer_header_buf)
            .chain(std::iter::once(inner_header_buf))
            .chain(std::iter::once(data))
            .collect()
    }
}

impl<PT: PubKey> Gossip for Seeder<PT> {
    type NodeIdPubKey = PT;

    fn send(&mut self, time: Duration, to: RouterTarget<Self::NodeIdPubKey>, message: AppMessage) {
        self.current_tick = time;
        match to {
            RouterTarget::Broadcast => {
                for to in &self.config.all_peers {
                    if to == &self.config.me {
                        self.events
                            .push_back(GossipEvent::Emit(self.config.me, message.clone()))
                    } else {
                        todo!()
                    }
                }
            }
            RouterTarget::PointToPoint(to) => {
                if to == self.config.me {
                    self.events
                        .push_back(GossipEvent::Emit(self.config.me, message))
                } else {
                    self.events.push_back(GossipEvent::Send(
                        to,
                        Self::prepare_message(MessageType::Direct, message),
                    ))
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
        // FIXME we don't do ANY input sanitization right now
        // It's trivial for any node to crash any other node by sending malformed input
        self.current_tick = time;

        let outer_header: OuterHeader =
            bincode::deserialize(&gossip_message.copy_to_bytes(OUTER_HEADER_SIZE)).unwrap();
        let header: Header =
            bincode::deserialize(&gossip_message.copy_to_bytes(outer_header.0 as usize)).unwrap();
        let data = gossip_message.copy_to_bytes(header.data_len as usize);
        assert!(
            gossip_message.is_empty(),
            "header data_len should match data section size"
        );

        match header.message_type {
            MessageType::Direct => self.events.push_back(GossipEvent::Emit(from, data)),
            MessageType::BroadcastProtocol(header) => {
                todo!()
            }
        }
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

#[derive(Deserialize, Serialize)]
struct OuterHeader(u32);
const OUTER_HEADER_SIZE: usize = std::mem::size_of::<OuterHeader>();

#[derive(Clone, Deserialize, Serialize)]
struct Header {
    data_len: u32,
    message_type: MessageType,
}

#[derive(Clone, Deserialize, Serialize)]
enum MessageType {
    Direct,
    BroadcastProtocol(BroadcastProtocolHeader),
}

#[derive(Clone, Deserialize, Serialize)]
struct BroadcastProtocolHeader {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_crypto::NopSignature;
    use monad_transformer::{BytesTransformer, LatencyTransformer};
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;

    use super::SeederConfig;
    use crate::testutil::{make_swarm, test_broadcast, test_direct};

    const NUM_NODES: u16 = 10;
    const PAYLOAD_SIZE_BYTES: usize = 1024;

    #[test]
    fn test_framed_messages() {
        let mut swarm = make_swarm::<NopSignature, _>(
            NUM_NODES,
            |all_peers, me| {
                SeederConfig {
                    all_peers: all_peers.to_vec(),
                    me: *me,
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
