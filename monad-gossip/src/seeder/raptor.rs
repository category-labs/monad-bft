use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    time::Duration,
};

use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hash, Hasher, HasherType},
};
use monad_merkle::{MerkleHash, MerkleProof, MerkleTree};
use monad_types::NodeId;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use serde::{Deserialize, Serialize};

use super::chunker::{Chunk, Chunker, Meta};
use crate::{connection_manager::MAX_DATAGRAM_SIZE, AppMessage};

pub struct Raptor<'k, ST: CertificateSignatureRecoverable> {
    me: NodeId<CertificateSignaturePubKey<ST>>,
    rng: ChaCha8Rng,

    meta: RaptorMeta<ST>,
    /// computed from meta signature
    creator: NodeId<CertificateSignaturePubKey<ST>>,

    role: Role<'k, ST>,

    non_seeders: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
}

enum Role<'k, ST: CertificateSignatureRecoverable> {
    Encoder {
        key: &'k ST::KeyPairType,
        encoder: Encoder,
        source_packets: Vec<EncodingPacket>,
        // index of last generated symbol id, for given chunk idx
        repair_idx: Vec<u32>,
        // pregenerated chunks that have yet to be sent
        pending_chunks: Vec<(RaptorChunk<ST>, Bytes)>,
    },
    Decoder {
        seeder: bool,
        decoder: Decoder,
        chunks: BTreeMap<raptorq::PayloadId, ChunkData<ST>>,

        // cached signatures for fast-path verification of merkle root
        // TODO cap size of this
        signature_cache: HashMap<ST, (MerkleHash, CertificateSignaturePubKey<ST>)>,
    },
}

struct ChunkData<ST: CertificateSignatureRecoverable> {
    chunk: RaptorChunk<ST>,
    data: Bytes,
    to_forward: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
}

const MERKLE_PACKET_BATCH: u16 = 8;

impl<'k, ST: CertificateSignatureRecoverable> Chunker<'k> for Raptor<'k, ST> {
    type SignatureType = ST;
    type PayloadId = RaptorPayloadId;
    type Meta = RaptorMeta<ST>;
    type Chunk = RaptorChunk<ST>;

    fn new_from_message(
        time: Duration,
        all_peers: &[NodeId<CertificateSignaturePubKey<Self::SignatureType>>],
        key: &'k <Self::SignatureType as CertificateSignature>::KeyPairType,
        message: AppMessage,
    ) -> Self {
        let me = NodeId::new(key.pubkey());
        // FIXME set this RNG non-jankly
        let rng = ChaCha8Rng::from_seed(me.pubkey().bytes()[..32].try_into().unwrap());

        // TODO size this properly without magic numbers - right now it's set overconservatively
        let raptor_symbol_size: u16 = MAX_DATAGRAM_SIZE as u16
            // buffer for header (signature + other metadata)
            - 150
            // merkle proof
            - std::mem::size_of::<MerkleHash>() as u16 * MERKLE_PACKET_BATCH.ilog2() as u16;

        let encoder = Encoder::with_defaults(&message, raptor_symbol_size);
        let meta = RaptorMeta::create(key, &message, time, encoder.get_config());

        let mut source_packets = Vec::new();
        let mut repair_idx = Vec::new();
        for encoder in encoder.get_block_encoders() {
            source_packets.extend(encoder.source_packets());
            repair_idx.push(0);
        }
        // reverse so that we can pop these off in order
        source_packets.reverse();

        Self {
            me,
            rng,
            meta,
            creator: me,

            role: Role::Encoder {
                key,
                encoder,
                source_packets,
                repair_idx,
                pending_chunks: Vec::new(),
            },
            non_seeders: all_peers.iter().copied().filter(|id| id != &me).collect(),
        }
    }

    /// Can be called in untrusted context
    fn try_new_from_meta(
        _time: Duration,
        all_peers: &[NodeId<CertificateSignaturePubKey<Self::SignatureType>>],
        key: &'k <Self::SignatureType as CertificateSignature>::KeyPairType,
        meta: Self::Meta,
    ) -> Result<Self, Box<dyn Error>> {
        let me = NodeId::new(key.pubkey());
        // FIXME set this RNG non-jankly
        let rng = ChaCha8Rng::from_seed(me.pubkey().bytes()[..32].try_into().unwrap());

        // TODO validate that fields in `meta` are valid
        let creator = NodeId::new(
            meta.signature
                .recover_pubkey(meta.id().0.as_slice())
                .map_err(|_| "failed to recover pubkey")?,
        );
        // TODO verify that creator is current leader
        // otherwise any non-leader validator can broadcast anything

        let decoder = Decoder::new(meta.raptor_meta);
        let chunker = Self {
            me,
            rng,
            meta,
            creator,
            role: Role::Decoder {
                seeder: false,
                decoder,
                chunks: Default::default(),
                signature_cache: Default::default(),
            },
            non_seeders: all_peers
                .iter()
                .copied()
                .filter(|id| id != &creator && id != &me)
                .collect(),
        };

        Ok(chunker)
    }

    fn meta(&self) -> &Self::Meta {
        &self.meta
    }

    fn creator(&self) -> NodeId<CertificateSignaturePubKey<Self::SignatureType>> {
        self.creator
    }

    fn created_at(&self) -> Duration {
        Duration::from_micros(self.meta.created_at_us)
    }

    fn is_seeder(&self) -> bool {
        match &self.role {
            Role::Encoder { .. } => true,
            Role::Decoder { seeder, .. } => *seeder,
        }
    }

    fn weight(&self) -> u64 {
        self.meta.raptor_meta.transfer_length() * self.non_seeders.len() as u64
    }

    /// Can be called in untrusted context
    fn process_chunk(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        chunk: Self::Chunk,
        data: Bytes,
    ) -> Result<Option<AppMessage>, Box<dyn Error>> {
        assert!(!self.is_seeder());
        let Role::Decoder {
            ref mut decoder,
            ref mut seeder,
            ref mut chunks,
            ref mut signature_cache,
        } = self.role
        else {
            unreachable!("invariant broken, not seeder");
        };

        // TODO validate that fields in `chunk` are valid
        let merkle_root = chunk
            .compute_merkle_root(&data)
            .ok_or("invalid merkle proof")?;

        if let Some((cached_root, author)) = signature_cache.get(&chunk.signature) {
            if cached_root != &merkle_root || author != &self.creator.pubkey() {
                return Err("chunk wasn't signed by publisher!".into());
            }
        } else {
            let signer = chunk
                .signature
                .recover_pubkey(&merkle_root)
                .map_err(|_| "failed to recover pubkey")?;
            if self.creator.pubkey() != signer {
                return Err("chunk wasn't signed by publisher!".into());
            }
            signature_cache.insert(chunk.signature, (merkle_root, signer));
        }

        let encoding_packet = EncodingPacket::deserialize(data.as_ref());

        if from == self.creator {
            // TODO this branch will never get hit once we become a Decoder { seeder }, this is
            // suboptimal
            chunks
                .entry(encoding_packet.payload_id().clone())
                .or_insert_with(|| ChunkData {
                    chunk,
                    data,
                    to_forward: self.non_seeders.clone(),
                });
        } else {
            // not responsible for forwarding
        }

        if let Some(app_mesage) = decoder.decode(encoding_packet) {
            *seeder = true;
            return Ok(Some(app_mesage.into()));
        }
        Ok(None)
    }

    fn generate_chunk(
        &mut self,
    ) -> Option<(
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        Self::Chunk,
        Bytes,
    )> {
        match &mut self.role {
            Role::Encoder {
                key,
                source_packets,
                encoder,
                repair_idx,
                pending_chunks,
            } => {
                let to_node = **self
                    .non_seeders
                    .iter()
                    .collect::<Vec<_>>()
                    .choose(&mut self.rng)?;
                if pending_chunks.is_empty() {
                    // generate merkle batch
                    let packets = {
                        let mut packets = Vec::new();
                        while packets.len() < MERKLE_PACKET_BATCH.into() {
                            let packet = source_packets.pop().unwrap_or_else(|| {
                                let idx = self.rng.gen_range(0..encoder.get_block_encoders().len());
                                let encoder = &encoder.get_block_encoders()[idx];
                                let repair_idx = &mut repair_idx[idx];
                                let mut repair_packet = encoder.repair_packets(*repair_idx, 1);
                                *repair_idx += 1;
                                repair_packet
                                    .pop()
                                    .expect("failed to generate repair packet")
                            });
                            let data: Bytes = packet.serialize().into();
                            packets.push(data);
                        }
                        packets
                    };
                    *pending_chunks = RaptorChunk::create(*key, self.meta.id(), packets);
                };
                let (chunk, data) = pending_chunks.pop().expect("must be existing chunks");
                Some((to_node, chunk, data))
            }
            Role::Decoder { chunks, .. } => loop {
                let mut chunks_vec = chunks.iter_mut().collect::<Vec<_>>();
                let (payload_id, chunk_data) = chunks_vec.choose_mut(&mut self.rng)?;
                let Some(to_node) = chunk_data
                    .to_forward
                    .iter()
                    .collect::<Vec<_>>()
                    .choose(&mut self.rng)
                    .map(|id| **id)
                else {
                    // noone left to forward the chunk to, so remove it and try a different chunk
                    let payload_id: raptorq::PayloadId = (*payload_id).clone();
                    chunks.remove(&payload_id);
                    continue;
                };
                chunk_data.to_forward.remove(&to_node);
                if !self.non_seeders.contains(&to_node) {
                    // to_node is seeder now, so remove it and try a different node
                    continue;
                }
                return Some((to_node, chunk_data.chunk.clone(), chunk_data.data.clone()));
            },
        }
    }

    fn set_peer_seeder(&mut self, peer: NodeId<CertificateSignaturePubKey<Self::SignatureType>>) {
        self.non_seeders.remove(&peer);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaptorMeta<ST: CertificateSignatureRecoverable> {
    #[serde(with = "HashDef")]
    message_hash: Hash,
    created_at_us: u64,
    raptor_meta: ObjectTransmissionInformation,

    #[serde(with = "signature_serde")]
    #[serde(bound = "")]
    signature: ST,
}

impl<ST: CertificateSignatureRecoverable> Meta for RaptorMeta<ST> {
    type PayloadId = RaptorPayloadId;

    fn id(&self) -> Self::PayloadId {
        RaptorPayloadId(Self::compute_id(
            &self.message_hash,
            self.created_at_us,
            self.raptor_meta,
        ))
    }
}

impl<ST: CertificateSignatureRecoverable> RaptorMeta<ST> {
    fn compute_id(
        message_hash: &Hash,
        created_at_us: u64,
        raptor_meta: ObjectTransmissionInformation,
    ) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(message_hash);
        hasher.update(created_at_us.to_le_bytes());
        hasher.update(raptor_meta.serialize());
        hasher.hash()
    }
    /// this function must only be called in trusted contexts!
    pub fn create(
        key: &ST::KeyPairType,
        message: &AppMessage,
        created_at: Duration,
        raptor_meta: ObjectTransmissionInformation,
    ) -> Self {
        let message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(message);
            hasher.hash()
        };
        let created_at_us = created_at.as_micros().try_into().unwrap();
        let signature = ST::sign(
            Self::compute_id(&message_hash, created_at_us, raptor_meta).as_slice(),
            key,
        );
        Self {
            message_hash,
            created_at_us,
            raptor_meta,

            signature,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaptorChunk<ST: CertificateSignatureRecoverable> {
    payload_id: RaptorPayloadId,

    merkle_proof: MerkleProof,

    #[serde(with = "signature_serde")]
    #[serde(bound = "")]
    signature: ST,
}

impl<ST: CertificateSignatureRecoverable> RaptorChunk<ST> {
    /// Compute merkle root of chunk given merkle proof and payload
    fn compute_merkle_root(&self, chunk: &Bytes) -> Option<MerkleHash> {
        let leaf = Self::compute_payload_hash(&self.payload_id, chunk);
        self.merkle_proof.compute_root(&leaf)
    }

    fn compute_payload_hash(payload_id: &RaptorPayloadId, chunk: &Bytes) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(payload_id.0);
        hasher.update(chunk);
        hasher.hash()
    }

    pub fn create(
        key: &ST::KeyPairType,
        payload_id: RaptorPayloadId,
        chunks: Vec<Bytes>,
    ) -> Vec<(Self, Bytes)> {
        assert!(!chunks.is_empty());

        let leaves = chunks
            .iter()
            .map(|chunk| Self::compute_payload_hash(&payload_id, chunk))
            .collect::<Vec<_>>();

        let merkle_tree = MerkleTree::new(&leaves);

        // TODO add prefix so this is safe and signature can't be re-used?
        let root_signature = ST::sign(merkle_tree.root(), key);

        chunks
            .into_iter()
            .enumerate()
            .map(|(idx, chunk)| {
                let merkle_proof =
                    merkle_tree.proof(idx.try_into().expect("number of chunks should fit in u8"));
                let raptor_chunk = Self {
                    payload_id,
                    merkle_proof,
                    signature: root_signature,
                };
                (raptor_chunk, chunk)
            })
            .collect()
    }
}

impl<ST: CertificateSignatureRecoverable> Chunk for RaptorChunk<ST> {
    type PayloadId = RaptorPayloadId;

    fn id(&self) -> Self::PayloadId {
        self.payload_id
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct RaptorPayloadId(#[serde(with = "HashDef")] Hash);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[serde(remote = "Hash")]
pub struct HashDef(pub [u8; 32]);

mod signature_serde {
    use monad_crypto::certificate_signature::CertificateSignature;
    use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

    pub fn deserialize<'de, D, ST>(deserializer: D) -> Result<ST, D::Error>
    where
        D: Deserializer<'de>,
        ST: CertificateSignature,
    {
        let bytes = Vec::deserialize(deserializer)?;
        ST::deserialize(&bytes).map_err(|e| D::Error::custom(format!("{:?}", e)))
    }

    pub fn serialize<S, ST>(signature: &ST, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        ST: CertificateSignature,
    {
        signature.serialize().serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use raptorq::Encoder;

    #[test]
    fn test_encoder() {
        const RAPTOR_SYMBOL_SIZE: u16 = 1024;
        let message = vec![0; 10_000 * 400];
        let _encoder = Encoder::with_defaults(&message, RAPTOR_SYMBOL_SIZE);
    }
}
