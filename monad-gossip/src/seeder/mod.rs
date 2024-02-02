use std::{collections::VecDeque, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use monad_types::{NodeId, RouterTarget};

use self::chunker::{Meta, Policy};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, FragmentedGossipMessage, GossipMessage};

mod chunker;
use chunker::Chunker;

pub struct SeederConfig<C: Chunker> {
    pub all_peers: Vec<NodeId<C::NodeIdPubKey>>,
    pub me: NodeId<C::NodeIdPubKey>,
}

impl<C: Chunker> SeederConfig<C> {
    pub fn build(self) -> Seeder<C> {
        Seeder {
            config: self,

            policy: Default::default(),

            events: VecDeque::default(),
            current_tick: Duration::ZERO,
        }
    }
}

pub struct Seeder<C: Chunker> {
    config: SeederConfig<C>,

    policy: Policy<C>,

    events: VecDeque<GossipEvent<C::NodeIdPubKey>>,
    current_tick: Duration,
}

impl<C: Chunker> Seeder<C> {
    fn prepare_message(
        message_type: MessageType<C::Meta, C::Chunk>,
        data: Bytes,
    ) -> FragmentedGossipMessage {
        let mut inner_header_buf = BytesMut::new().writer();
        bincode::serialize_into(
            &mut inner_header_buf,
            &Header::<C::Meta, C::Chunk> {
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

    fn handle_protocol_message(
        &mut self,
        from: NodeId<C::NodeIdPubKey>,
        header: ProtocolHeader<C::Meta, C::Chunk>,
        data: Bytes,
    ) {
        match header {
            ProtocolHeader::Meta(meta) => {
                // TODO this block should probably be moved inside chunker
                let id = meta.id();
                if !self.policy.contains(&id) {
                    match C::try_new_from_meta(meta) {
                        Ok(chunker) => {
                            tracing::info!("initialized chunker for id: {:?}", id);
                            self.policy.insert(chunker);
                        }
                        Err(e) => {
                            tracing::warn!("failed to create chunker from meta: {:?}", e);
                        }
                    }
                } else {
                    tracing::trace!("received duplicate meta for id: {:?}", id);
                }
            }

            ProtocolHeader::Chunk(chunk) => self.policy.process_chunk(from, chunk, data),
        }
    }

    fn update_tick(&mut self, time: Duration) {
        assert!(time >= self.current_tick);
        self.current_tick = time;
        self.policy.update_tick(time);
    }
}

impl<C: Chunker> Gossip for Seeder<C> {
    type NodeIdPubKey = C::NodeIdPubKey;

    fn send(&mut self, time: Duration, to: RouterTarget<Self::NodeIdPubKey>, message: AppMessage) {
        self.update_tick(time);
        match to {
            RouterTarget::Broadcast => {
                self.events
                    .push_back(GossipEvent::Emit(self.config.me, message.clone()));

                // TODO this block should probably be moved inside chunker
                match C::try_new_from_message(message) {
                    Ok(chunker) => {
                        tracing::info!(
                            "initialized chunker on broadcast attempt: {:?}",
                            chunker.meta()
                        );
                        // this is safe because chunkers are guaranteed to be unique, even for
                        // same AppMessage.
                        self.policy.insert(chunker);
                    }
                    Err(e) => {
                        tracing::warn!("failed to create chunker on broadcast attempt: {:?}", e);
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
        self.update_tick(time);

        // FIXME we don't do ANY input sanitization right now
        // It's trivial for any node to crash any other node by sending malformed input

        let outer_header: OuterHeader =
            bincode::deserialize(&gossip_message.copy_to_bytes(OUTER_HEADER_SIZE)).unwrap();
        let header: Header<C::Meta, C::Chunk> =
            bincode::deserialize(&gossip_message.copy_to_bytes(outer_header.0 as usize)).unwrap();
        let data = gossip_message.copy_to_bytes(header.data_len as usize);
        assert!(
            gossip_message.is_empty(),
            "header data_len should match data section size"
        );

        match header.message_type {
            MessageType::Direct => self.events.push_back(GossipEvent::Emit(from, data)),
            MessageType::BroadcastProtocol(header) => {
                self.handle_protocol_message(from, header, data)
            }
        }
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.current_tick)
        } else {
            self.policy.peek_tick()
        }
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::NodeIdPubKey>> {
        self.update_tick(time);
        if !self.events.is_empty() {
            self.events.pop_front()
        } else {
            self.policy.poll()
        }
    }
}

#[derive(Deserialize, Serialize)]
struct OuterHeader(u32);
const OUTER_HEADER_SIZE: usize = std::mem::size_of::<OuterHeader>();

#[derive(Clone, Deserialize, Serialize)]
struct Header<Meta, Chunk> {
    data_len: u32,
    message_type: MessageType<Meta, Chunk>,
}

#[derive(Clone, Deserialize, Serialize)]
enum MessageType<Meta, Chunk> {
    Direct,
    BroadcastProtocol(ProtocolHeader<Meta, Chunk>),
}

#[derive(Clone, Deserialize, Serialize)]
enum ProtocolHeader<Meta, Chunk> {
    Meta(Meta),
    Chunk(Chunk),
}

// type PayloadHash = [u8; 32];
//
// #[derive(Clone, Deserialize, Serialize)]
// struct ProtocolMeta {
//     payload_hash: PayloadHash,
//     // TODO insert initial proposer signature over (payload hash, qc_timestamp) here
//     // This will be used in validation step
//
//     // this is only here
//     total_chunks: u32,
// }
//
// #[derive(Clone, Deserialize, Serialize)]
// struct ProtocolChunk {
//     payload_hash: PayloadHash,
//     // TODO insert proof that the chunk is a valid constituent of payload here
//     // This will be used in validation step
// }

// #[cfg(test)]
// mod tests {
//     use std::time::Duration;
//
//     use monad_crypto::NopSignature;
//     use monad_transformer::{BytesTransformer, LatencyTransformer};
//     use rand::SeedableRng;
//     use rand_chacha::ChaCha20Rng;
//
//     use super::SeederConfig;
//     use crate::testutil::{make_swarm, test_broadcast, test_direct};
//
//     const NUM_NODES: u16 = 10;
//     const PAYLOAD_SIZE_BYTES: usize = 1024;
//
//     #[test]
//     fn test_framed_messages() {
//         let mut swarm = make_swarm::<NopSignature, _>(
//             NUM_NODES,
//             |all_peers, me| {
//                 SeederConfig {
//                     all_peers: all_peers.to_vec(),
//                     me: *me,
//                 }
//                 .build()
//             },
//             |_all_peers, _me| {
//                 vec![BytesTransformer::Latency(LatencyTransformer::new(
//                     Duration::from_millis(5),
//                 ))]
//             },
//         );
//
//         let mut rng = ChaCha20Rng::from_seed([0; 32]);
//         test_broadcast(
//             &mut rng,
//             &mut swarm,
//             Duration::from_secs(1),
//             PAYLOAD_SIZE_BYTES,
//             usize::MAX,
//             1.0,
//         );
//         test_direct(
//             &mut rng,
//             &mut swarm,
//             Duration::from_secs(1),
//             PAYLOAD_SIZE_BYTES,
//         );
//     }
// }
