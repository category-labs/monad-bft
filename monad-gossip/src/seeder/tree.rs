use std::{error::Error, marker::PhantomData, time::Duration};

use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hash, Hasher, HasherType},
};
use monad_types::NodeId;

use serde::{Deserialize, Serialize};

use crate::AppMessage;

use super::chunker::{Chunk, Chunker, Meta};

pub struct Tree<ST: CertificateSignatureRecoverable> {
    is_root: bool,

    meta: TreeMeta<ST>,
}

impl<ST: CertificateSignatureRecoverable> Chunker for Tree<ST> {
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type PayloadId = TreePayloadId;
    type Meta = TreeMeta<ST>;
    type Chunk = TreeChunk<ST>;

    fn try_new_from_message(
        time: Duration,
        sender: NodeId<Self::NodeIdPubKey>,
        message: AppMessage,
    ) -> Result<Self, Box<dyn Error>> {
        todo!()
    }

    fn try_new_from_meta(meta: Self::Meta) -> Result<Self, Box<dyn Error>> {
        todo!()
    }

    fn meta(&self) -> &Self::Meta {
        todo!()
    }

    fn is_seeder(&self) -> bool {
        self.is_root
    }

    fn process_chunk(
        &mut self,
        from: NodeId<Self::NodeIdPubKey>,
        chunk: Self::Chunk,
        data: bytes::Bytes,
    ) -> Option<AppMessage> {
        todo!()
    }

    fn generate_chunk(
        &mut self,
    ) -> Option<(NodeId<Self::NodeIdPubKey>, Self::Chunk, bytes::Bytes)> {
        todo!()
    }

    fn set_peer_seeder(&mut self, peer: NodeId<Self::NodeIdPubKey>) {
        todo!()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeMeta<ST: CertificateSignatureRecoverable> {
    #[serde(with = "HashDef")]
    message_hash: Hash,
    created_at_us: u128,

    #[serde(with = "signature_serde")]
    #[serde(bound = "")]
    signature: ST,
}

impl<ST: CertificateSignatureRecoverable> Meta for TreeMeta<ST> {
    type PayloadId = TreePayloadId;

    fn id(&self) -> Self::PayloadId {
        TreePayloadId(Self::compute_id(&self.message_hash, self.created_at_us))
    }
}

impl<ST: CertificateSignatureRecoverable> TreeMeta<ST> {
    fn compute_id(message_hash: &Hash, created_at_us: u128) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(message_hash);
        hasher.update(created_at_us.to_le_bytes());
        hasher.hash()
    }
    pub fn create(key: &ST::KeyPairType, message: &AppMessage, created_at: Duration) -> Self {
        let message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(message);
            hasher.hash()
        };
        let created_at_us = created_at.as_micros();
        let signature = ST::sign(
            Self::compute_id(&message_hash, created_at_us).as_slice(),
            key,
        );
        Self {
            message_hash,
            created_at_us,
            signature,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeChunk<ST: CertificateSignatureRecoverable> {
    payload_id: TreePayloadId,

    /// this should be over payload_id and chunk data
    #[serde(with = "signature_serde")]
    #[serde(bound = "")]
    signature: ST,
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
