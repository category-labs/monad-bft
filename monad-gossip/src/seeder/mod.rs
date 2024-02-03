use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap, VecDeque},
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
};
use serde::{Deserialize, Serialize};

use monad_types::{NodeId, RouterTarget};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, FragmentedGossipMessage, GossipMessage};

mod chunker;
use chunker::{Chunk, Chunker, Meta};
mod tree;
pub use tree::Tree;

pub struct SeederConfig<'k, C: Chunker<'k>> {
    pub all_peers: Vec<NodeId<CertificateSignaturePubKey<C::SignatureType>>>,
    pub key: &'k <C::SignatureType as CertificateSignature>::KeyPairType,

    pub timeout: Duration,
    pub up_bandwidth_Mbps: u16,
    pub chunker_poll_interval: Duration,
}

impl<'k, C: Chunker<'k>> SeederConfig<'k, C> {
    pub fn build(self) -> Seeder<'k, C> {
        Seeder {
            me: NodeId::new(self.key.pubkey()),
            config: self,

            chunkers: Default::default(),
            chunker_timeouts: Default::default(),

            events: VecDeque::default(),
            current_tick: Duration::ZERO,
            next_chunker_poll: None,
        }
    }
}

pub struct Seeder<'k, C: Chunker<'k>> {
    /// convenience derived from config.key.pubkey()
    me: NodeId<CertificateSignaturePubKey<C::SignatureType>>,
    config: SeederConfig<'k, C>,

    chunkers: BTreeMap<C::PayloadId, ChunkerStatus<'k, C>>,
    /// Chunker is scheduled to be deleted `timeout` after C::Meta::created_at
    chunker_timeouts: BTreeMap<Duration, Vec<C::PayloadId>>,

    events: VecDeque<GossipEvent<CertificateSignaturePubKey<C::SignatureType>>>,
    current_tick: Duration,
    next_chunker_poll: Option<Duration>,
}

struct ChunkerStatus<'k, C: Chunker<'k>> {
    chunker: C,
    sent_metas: HashMap<NodeId<CertificateSignaturePubKey<C::SignatureType>>, MetaInfo<C::Meta>>,
}

impl<'k, C: Chunker<'k>> ChunkerStatus<'k, C> {
    fn new(chunker: C) -> Self {
        Self {
            chunker,
            sent_metas: Default::default(),
        }
    }

    fn sent_seeding(&self, peer: &NodeId<CertificateSignaturePubKey<C::SignatureType>>) -> bool {
        self.sent_metas
            .get(peer)
            .map(|meta| meta.seeding)
            .unwrap_or(false)
    }
}

impl<'k, C: Chunker<'k>> Seeder<'k, C> {
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
        from: NodeId<CertificateSignaturePubKey<C::SignatureType>>,
        header: ProtocolHeader<C::Meta, C::Chunk>,
        data: Bytes,
    ) {
        match header {
            ProtocolHeader::Meta(MetaInfo { meta, seeding }) => {
                let id = meta.id();
                if !self.chunkers.contains_key(&id) {
                    match C::try_new_from_meta(
                        self.current_tick,
                        &self.config.all_peers,
                        self.config.key,
                        meta,
                    ) {
                        Ok(chunker) => {
                            tracing::info!("initialized chunker for id: {:?}", id);
                            self.insert_chunker(chunker);
                        }
                        Err(e) => {
                            tracing::warn!("failed to create chunker from meta: {:?}", e);
                        }
                    }
                } else {
                    tracing::trace!("received duplicate meta for id: {:?}", id);
                }

                if seeding {
                    self.chunkers
                        .get_mut(&id)
                        .expect("invariant broken")
                        .chunker
                        .set_peer_seeder(from);
                }
            }

            ProtocolHeader::Chunk(chunk) => {
                let id = chunk.id();
                if let Some(status) = self.chunkers.get_mut(&id) {
                    if !status.chunker.is_seeder() {
                        let result = status.chunker.process_chunk(from, chunk, data);
                        match result {
                            Ok(None) => {}
                            Ok(Some(app_message)) => {
                                tracing::debug!("emitting app_message, len={}", app_message.len());
                                self.events.push_back(GossipEvent::Emit(
                                    status.chunker.creator(),
                                    app_message,
                                ));
                                assert!(status.chunker.is_seeder());
                            }
                            Err(e) => {
                                tracing::warn!("failed to process chunk: {:?}", e);
                            }
                        }
                    }
                    // chunker may be complete if event was emitted
                    if status.chunker.is_seeder() && !status.sent_seeding(&from) {
                        let meta_info = MetaInfo {
                            meta: status.chunker.meta().clone(),
                            seeding: true,
                        };
                        let msg =
                            MessageType::BroadcastProtocol(ProtocolHeader::Meta(meta_info.clone()));
                        self.events.push_back(GossipEvent::Send(
                            from,
                            Self::prepare_message(msg, Bytes::default()),
                        ));

                        // this shouldn't usually be dropped, because there must already be an
                        // outstanding connection
                        status.sent_metas.insert(from, meta_info);
                    }
                } else {
                    tracing::trace!("no chunker initialized for id: {:?}", id);
                }
            }
        }
    }

    fn insert_chunker(&mut self, chunker: C) {
        let created_at = chunker.created_at();
        self.chunker_timeouts
            .entry(created_at + self.config.timeout)
            .or_default()
            .push(chunker.meta().id());

        let removed = self
            .chunkers
            .insert(chunker.meta().id(), ChunkerStatus::new(chunker));
        assert!(removed.is_none());
    }

    fn update_tick(&mut self, time: Duration) {
        assert!(time >= self.current_tick);
        self.current_tick = time;
        self.next_chunker_poll = self.next_chunker_poll.map(|t| t.max(time));
    }
}

impl<'k, C: Chunker<'k>> Gossip for Seeder<'k, C> {
    type NodeIdPubKey = CertificateSignaturePubKey<C::SignatureType>;

    fn send(&mut self, time: Duration, to: RouterTarget<Self::NodeIdPubKey>, message: AppMessage) {
        self.update_tick(time);
        match to {
            RouterTarget::Broadcast => {
                if self.next_chunker_poll.is_none() {
                    self.next_chunker_poll = Some(self.current_tick);
                }
                self.events
                    .push_back(GossipEvent::Emit(self.me, message.clone()));

                let chunker =
                    C::new_from_message(time, &self.config.all_peers, self.config.key, message);
                tracing::info!(
                    "initialized chunker on broadcast attempt: {:?}",
                    chunker.meta()
                );
                // this is safe because chunkers are guaranteed to be unique, even for
                // same AppMessage.
                self.insert_chunker(chunker);
            }
            RouterTarget::PointToPoint(to) => {
                if to == self.me {
                    self.events.push_back(GossipEvent::Emit(self.me, message))
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
                if self.next_chunker_poll.is_none() {
                    self.next_chunker_poll = Some(self.current_tick);
                }
                self.handle_protocol_message(from, header, data)
            }
        }
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.current_tick)
        } else {
            self.next_chunker_poll
        }
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Self::NodeIdPubKey>> {
        self.update_tick(time);

        if self.next_chunker_poll.map(|t| t >= time).unwrap_or(false) {
            while time
                >= self
                    .chunker_timeouts
                    .keys()
                    .next()
                    .copied()
                    .unwrap_or(Duration::MAX)
            {
                let gc_ids = self.chunker_timeouts.pop_first().expect("must exist").1;
                for gc_id in gc_ids {
                    let removed = self.chunkers.remove(&gc_id);
                    if removed.is_some() {
                        tracing::debug!("garbage collected chunk with id: {:?}", gc_id);
                    }
                }
            }

            // TODO we can do more intelligent selection of chunkers here - eg time-weighted decay?
            //      stake-weighted selection?
            // TODO can we eliminate disconnected peers from selection here? or is that too jank?
            let mut chunkers: Vec<_> = self.chunkers.values_mut().collect();
            // TODO shuffle chunkers with deterministic RNG

            let mut chunk_bytes_generated: u64 = 0; // TODO should we include outbound meta bytes?
            let mut chunker_idx = 0;
            while {
                let up_bandwidth_Bps = self.config.up_bandwidth_Mbps as u64 * 125_000;
                let up_bandwidth_Bpms = up_bandwidth_Bps / 1_000;
                let exceeded_limit =
                    Duration::from_millis(chunk_bytes_generated / up_bandwidth_Bpms)
                        >= self.config.chunker_poll_interval;
                !chunkers.is_empty() && !exceeded_limit
            } {
                let status = &mut chunkers[chunker_idx];
                if let Some((to, chunk, data)) = status.chunker.generate_chunk() {
                    if let Entry::Vacant(e) = status.sent_metas.entry(to) {
                        let meta_info = MetaInfo {
                            meta: status.chunker.meta().clone(),
                            seeding: status.chunker.is_seeder(),
                        };
                        e.insert(meta_info.clone());
                        let meta_message = Self::prepare_message(
                            MessageType::BroadcastProtocol(ProtocolHeader::Meta(meta_info)),
                            Bytes::default(),
                        );
                        // Note that as currently constructed, this will always be dropped by the
                        // ConnectionManager if there isn't already an established connection. This
                        // should be fine for now - adding an extra timeout/retry mechanism seems
                        // unnecessary given latencies.
                        self.events.push_back(GossipEvent::Send(to, meta_message));
                    }

                    let chunk_message = Self::prepare_message(
                        MessageType::BroadcastProtocol(ProtocolHeader::Chunk(chunk)),
                        data,
                    );
                    chunk_bytes_generated += chunk_message.remaining() as u64;
                    self.events.push_back(GossipEvent::Send(to, chunk_message));
                    chunker_idx = (chunker_idx + 1) % chunkers.len();
                } else {
                    chunkers.swap_remove(chunker_idx);
                    if chunkers.is_empty() {
                        break;
                    }
                    chunker_idx %= chunkers.len();
                }
            }

            self.next_chunker_poll = if chunk_bytes_generated == 0 {
                None
            } else {
                Some(time + self.config.chunker_poll_interval)
            };
        }

        self.events.pop_front()
    }
}

#[derive(Deserialize, Serialize)]
struct OuterHeader(u32);
const OUTER_HEADER_SIZE: usize = std::mem::size_of::<OuterHeader>();

#[derive(Clone, Deserialize, Serialize)]
struct Header<M, C> {
    data_len: u32,
    message_type: MessageType<M, C>,
}

#[derive(Clone, Deserialize, Serialize)]
enum MessageType<M, C> {
    Direct,
    BroadcastProtocol(ProtocolHeader<M, C>),
}

#[derive(Clone, Deserialize, Serialize, Debug)]
enum ProtocolHeader<M, C> {
    Meta(MetaInfo<M>),
    Chunk(C),
}

#[derive(Clone, Deserialize, Serialize, Debug)]
struct MetaInfo<M> {
    meta: M,
    seeding: bool,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        hasher::{Hasher, HasherType},
        NopSignature,
    };
    use monad_transformer::{BytesTransformer, LatencyTransformer};
    use monad_types::NodeId;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use super::{tree::Tree, SeederConfig};
    use crate::testutil::{test_broadcast, test_direct, Swarm};

    const NUM_NODES: u16 = 20;
    const PAYLOAD_SIZE_BYTES: usize = 1024 * 1024;

    type SignatureType = NopSignature;

    #[test]
    fn test_framed_messages() {
        let keys: Vec<_> = (1_u32..)
            .take(NUM_NODES.into())
            .map(|idx| {
                let mut secret = {
                    let mut hasher = HasherType::new();
                    hasher.update(idx.to_le_bytes());
                    hasher.hash().0
                };
                <SignatureType as CertificateSignature>::KeyPairType::from_bytes(&mut secret)
                    .unwrap()
            })
            .collect();
        let mut swarm = {
            Swarm::new(keys.iter().map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SeederConfig::<Tree<SignatureType>> {
                        all_peers: keys.iter().map(|key| NodeId::new(key.pubkey())).collect(),
                        key,

                        timeout: Duration::from_millis(700),
                        up_bandwidth_Mbps: 1_000,
                        chunker_poll_interval: Duration::from_millis(10),
                    }
                    .build(),
                    vec![BytesTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(100),
                    ))],
                )
            }))
        };

        let mut rng = ChaCha8Rng::from_seed([0; 32]);
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
