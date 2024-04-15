use std::{error::Error, fmt::Debug, time::Duration};

use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;
use serde::{de::DeserializeOwned, Serialize};

use crate::AppMessage;

/// Chunker is responsible for constructing a chunking scheme
/// Examples of different schemes:
/// 1) Splitting payload into N chunks
/// 2) RS encoding, splitting into N chunks
/// 3) Fountain encoding, splitting into N chunks
///
/// One other implicit responsiblity it has is validating `Meta` and `Chunk`.
///
/// Flow:
/// 1) Payload is constructed from AppMessage, sender, time, etc.
/// 2) Chunks are generated from payload
pub trait Chunker<'k>: Sized {
    type SignatureType: CertificateSignatureRecoverable;
    // payload includes AppMessage + created_at TS as entropy
    type PayloadId: Clone + Ord + Debug;
    type Meta: Meta<PayloadId = Self::PayloadId>;
    type Chunk: Chunk<PayloadId = Self::PayloadId>;

    /// This must generate a Chunker with a UNIQUE PayloadId
    /// This is to ensure that two separate chunkers are generated for two separate broadcasts,
    /// even if they are the same AppMessage
    fn new_from_message(
        time: Duration,
        all_peers: &[NodeId<CertificateSignaturePubKey<Self::SignatureType>>],
        key: &'k <Self::SignatureType as CertificateSignature>::KeyPairType,
        message: AppMessage,
    ) -> Self;
    fn try_new_from_meta(
        time: Duration,
        all_peers: &[NodeId<CertificateSignaturePubKey<Self::SignatureType>>],
        key: &'k <Self::SignatureType as CertificateSignature>::KeyPairType,
        meta: Self::Meta,
    ) -> Result<Self, Box<dyn Error>>;

    fn meta(&self) -> &Self::Meta;
    /// populated from meta
    fn creator(&self) -> NodeId<CertificateSignaturePubKey<Self::SignatureType>>;
    /// populated from meta
    fn created_at(&self) -> Duration;

    // Payload has been reconstructed - process_chunk should not be called if this returns true
    fn is_seeder(&self) -> bool;

    fn weight(&self) -> u64;

    /// Some(x) indicates that the chunker doesn't need to receive any more chunks, because it has
    /// successfully reconstructed payload. Chunker::is_complete MUST return true after this.
    fn process_chunk(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        chunk: Self::Chunk,
        data: Bytes,
    ) -> Result<Option<AppMessage>, Box<dyn Error>>;

    fn generate_chunk(
        &mut self,
    ) -> Option<(
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        Self::Chunk,
        Bytes,
    )>;

    /// Peer is now seeding - so we can stop sending them chunks
    fn set_peer_seeder(&mut self, peer: NodeId<CertificateSignaturePubKey<Self::SignatureType>>);
}

pub trait Meta: Clone + Debug + Serialize + DeserializeOwned {
    type PayloadId;
    fn id(&self) -> Self::PayloadId;
}

pub trait Chunk: Clone + Debug + Serialize + DeserializeOwned {
    type PayloadId;
    fn id(&self) -> Self::PayloadId;
}
