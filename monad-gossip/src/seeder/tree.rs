use std::{collections::BTreeMap, error::Error, time::Duration};

use bytes::{Buf, Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hash, Hasher, HasherType},
};
use monad_types::NodeId;
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::{Deserialize, Serialize};

use super::chunker::{Chunk, Chunker, Meta};
use crate::AppMessage;

const TREE_ARITY: usize = 6;

pub struct Tree<ST: CertificateSignatureRecoverable> {
    /// tree_peers should not include broadcaster!
    tree_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    me: NodeId<CertificateSignaturePubKey<ST>>,

    meta: TreeMeta<ST>,
    /// computed from meta signature
    creator: NodeId<CertificateSignaturePubKey<ST>>,

    chunks: BTreeMap<u16, ChunkStatus<ST>>,
}

struct ChunkStatus<ST: CertificateSignatureRecoverable> {
    chunk: TreeChunk<ST>,
    data: Bytes,

    children: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
}

impl<ST: CertificateSignatureRecoverable> ChunkStatus<ST> {
    /// tree_peers should not include broadcaster! it should also be sorted
    pub fn new(
        mut tree_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        me: NodeId<CertificateSignaturePubKey<ST>>,
        tree_arity: usize,
        chunk: TreeChunk<ST>,
        chunk_hash: Hash,
        data: Bytes,
    ) -> Self {
        let children = {
            let mut rng = ChaChaRng::from_seed(chunk_hash.0);
            tree_peers.shuffle(&mut rng);

            let maybe_my_idx = tree_peers.iter().position(|&x| x == me);

            match maybe_my_idx {
                None => tree_peers.into_iter().take(1).collect(),
                Some(my_idx) => {
                    // children of a node at index i are at indices (i*k)+1 through (i*k)+k
                    let start_idx = my_idx * tree_arity + 1;
                    tree_peers
                        .into_iter()
                        .skip(start_idx)
                        .take(tree_arity)
                        .collect()
                }
            }
        };

        Self {
            chunk,
            data,
            children,
        }
    }
}

impl<'k, ST: CertificateSignatureRecoverable> Chunker<'k> for Tree<ST> {
    type SignatureType = ST;
    type PayloadId = TreePayloadId;
    type Meta = TreeMeta<ST>;
    type Chunk = TreeChunk<ST>;

    fn new_from_message(
        time: Duration,
        all_peers: &[NodeId<CertificateSignaturePubKey<Self::SignatureType>>],
        key: &'k <Self::SignatureType as CertificateSignature>::KeyPairType,
        mut message: AppMessage,
    ) -> Self {
        let me = NodeId::new(key.pubkey());
        let mut tree_peers: Vec<_> = all_peers
            .iter()
            .copied()
            .filter(|peer| peer != &me)
            .collect();
        tree_peers.sort();

        const CHUNK_SIZE_BYTES: usize = 8192;
        let meta = TreeMeta::create(key, &message, time, CHUNK_SIZE_BYTES);
        let mut chunks = BTreeMap::default();
        let mut chunk_idx = 0;
        while message.has_remaining() {
            let chunk_data = message.copy_to_bytes(CHUNK_SIZE_BYTES.min(message.remaining()));
            let (chunk, chunk_hash) = TreeChunk::create(key, meta.id(), chunk_idx, &chunk_data);
            chunks.insert(
                chunk_idx,
                ChunkStatus::new(
                    tree_peers.clone(),
                    me,
                    TREE_ARITY,
                    chunk,
                    chunk_hash,
                    chunk_data,
                ),
            );
            chunk_idx += 1;
        }

        Self {
            tree_peers,
            me,
            meta,
            creator: me,
            chunks,
        }
    }

    /// Can be called in untrusted context
    fn try_new_from_meta(
        _time: Duration,
        all_peers: &[NodeId<CertificateSignaturePubKey<Self::SignatureType>>],
        key: &'k <Self::SignatureType as CertificateSignature>::KeyPairType,
        meta: Self::Meta,
    ) -> Result<Self, Box<dyn Error>> {
        // TODO validate that fields in `meta` are valid
        let creator = NodeId::new(
            meta.signature
                .recover_pubkey(meta.id().0.as_slice())
                .map_err(|_| "failed to recover pubkey")?,
        );
        // TODO verify that creator is current leader
        // otherwise any non-leader validator can broadcast anything

        let mut tree_peers: Vec<_> = all_peers
            .iter()
            .copied()
            .filter(|peer| peer != &creator)
            .collect();
        tree_peers.sort();

        let chunker = Self {
            tree_peers,
            me: NodeId::new(key.pubkey()),
            meta,
            creator,
            chunks: Default::default(),
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
        self.chunks.len() == self.meta.num_chunks.into()
    }

    fn weight(&self) -> u64 {
        self.meta.num_chunks as u64
            * self
                .chunks
                .values()
                .map(|chunk| chunk.children.len() as u64)
                .sum::<u64>()
    }

    /// Can be called in untrusted context
    fn process_chunk(
        &mut self,
        _from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        chunk: Self::Chunk,
        data: Bytes,
    ) -> Result<Option<AppMessage>, Box<dyn Error>> {
        assert!(!self.is_seeder());
        // TODO validate that fields in `chunk` are valid
        let chunk_hash = Self::Chunk::compute_hash(&self.meta.id(), &chunk.chunk_idx, &data);
        let signer = chunk
            .signature
            .recover_pubkey(chunk_hash.as_slice())
            .map_err(|_| "failed to recover pubkey")?;
        if self.creator.pubkey() != signer {
            return Err("chunk wasn't signed by publisher!".into());
        }
        self.chunks.insert(
            chunk.chunk_idx,
            ChunkStatus::new(
                self.tree_peers.clone(),
                self.me,
                TREE_ARITY,
                chunk,
                chunk_hash,
                data,
            ),
        );

        Ok(if self.chunks.len() == self.meta.num_chunks.into() {
            let message_size = self.chunks.values().map(|status| status.data.len()).sum();
            let message = self
                .chunks
                .values()
                .fold(BytesMut::with_capacity(message_size), |mut b, status| {
                    b.extend_from_slice(&status.data);
                    b
                })
                .freeze();
            Some(message)
        } else {
            None
        })
    }

    fn generate_chunk(
        &mut self,
    ) -> Option<(
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        Self::Chunk,
        Bytes,
    )> {
        let statuses: Vec<_> = self.chunks.values_mut().collect();
        // TODO shuffle chunkers with deterministic RNG
        for status in statuses {
            if let Some(to) = status.children.pop() {
                return Some((to, status.chunk.clone(), status.data.clone()));
            }
        }
        None
    }

    fn set_peer_seeder(&mut self, _peer: NodeId<CertificateSignaturePubKey<Self::SignatureType>>) {
        // There's no special encoding done on blocks, so there's no need to track seeders
        // Every node should receive each chunk only once with broadcast trees
        // Also, each node MUST receive every chunk in order to become a seeder, which implies that
        // we've already sent them the chunk they need, or they've received it through some
        // side-channel mechanism. Either way, tracking seeders is not required
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeMeta<ST: CertificateSignatureRecoverable> {
    #[serde(with = "HashDef")]
    message_hash: Hash,
    created_at_us: u64,
    num_chunks: u16,

    #[serde(with = "signature_serde")]
    #[serde(bound = "")]
    signature: ST,
}

impl<ST: CertificateSignatureRecoverable> Meta for TreeMeta<ST> {
    type PayloadId = TreePayloadId;

    fn id(&self) -> Self::PayloadId {
        TreePayloadId(Self::compute_id(
            &self.message_hash,
            self.created_at_us,
            self.num_chunks,
        ))
    }
}

impl<ST: CertificateSignatureRecoverable> TreeMeta<ST> {
    fn compute_id(message_hash: &Hash, created_at_us: u64, num_chunks: u16) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(message_hash);
        hasher.update(created_at_us.to_le_bytes());
        hasher.update(num_chunks.to_le_bytes());
        hasher.hash()
    }
    /// this function must only be called in trusted contexts!
    pub fn create(
        key: &ST::KeyPairType,
        message: &AppMessage,
        created_at: Duration,
        chunk_size_bytes: usize,
    ) -> Self {
        let message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(message);
            hasher.hash()
        };
        let num_chunks = message
            .len()
            .div_ceil(chunk_size_bytes)
            .try_into()
            .expect("can't generate more than u16 chunks");
        let created_at_us = created_at.as_micros().try_into().unwrap();
        let signature = ST::sign(
            Self::compute_id(&message_hash, created_at_us, num_chunks).as_slice(),
            key,
        );
        Self {
            message_hash,
            created_at_us,
            num_chunks,

            signature,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeChunk<ST: CertificateSignatureRecoverable> {
    payload_id: TreePayloadId,
    chunk_idx: u16,

    #[serde(with = "signature_serde")]
    #[serde(bound = "")]
    signature: ST,
}

impl<ST: CertificateSignatureRecoverable> TreeChunk<ST> {
    fn compute_hash(payload_id: &TreePayloadId, chunk_idx: &u16, chunk: &Bytes) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(payload_id.0);
        hasher.update(chunk_idx.to_le_bytes());
        hasher.update(chunk);
        hasher.hash()
    }

    pub fn create(
        key: &ST::KeyPairType,
        payload_id: TreePayloadId,
        chunk_idx: u16,
        chunk: &Bytes,
    ) -> (Self, Hash) {
        let hash = Self::compute_hash(&payload_id, &chunk_idx, chunk);
        let signature = ST::sign(hash.as_slice(), key);
        (
            Self {
                payload_id,
                chunk_idx,
                signature,
            },
            hash,
        )
    }
}

impl<ST: CertificateSignatureRecoverable> Chunk for TreeChunk<ST> {
    type PayloadId = TreePayloadId;

    fn id(&self) -> Self::PayloadId {
        self.payload_id
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct TreePayloadId(#[serde(with = "HashDef")] Hash);

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
