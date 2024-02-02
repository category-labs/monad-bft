use std::{error::Error, fmt::Debug, time::Duration};

use bytes::Bytes;
use monad_crypto::certificate_signature::PubKey;

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
pub trait Chunker: Sized {
    type NodeIdPubKey: PubKey;
    // payload includes AppMessage + created_at TS as entropy
    type PayloadId: Clone + Ord + Debug;
    type Meta: Meta<NodeIdPubKey = Self::NodeIdPubKey, PayloadId = Self::PayloadId>
        + Clone
        + Debug
        + Serialize
        + DeserializeOwned;
    type Chunk: Chunk<PayloadId = Self::PayloadId> + Clone + Debug + Serialize + DeserializeOwned;

    /// This must generate a Chunker with a UNIQUE PayloadId
    /// This is to ensure that two separate chunkers are generated for two separate broadcasts,
    /// even if they are the same AppMessage
    fn try_new_from_message(
        time: Duration,
        sender: NodeId<Self::NodeIdPubKey>,
        message: AppMessage,
    ) -> Result<Self, Box<dyn Error>>;
    fn try_new_from_meta(meta: Self::Meta) -> Result<Self, Box<dyn Error>>;

    fn meta(&self) -> &Self::Meta;

    // Payload has been reconstructed - process_chunk should not be called if this returns true
    fn is_seeder(&self) -> bool;

    /// Some(x) indicates that the chunker doesn't need to receive any more chunks, because it has
    /// successfully reconstructed payload. Chunker::is_complete MUST return true after this.
    fn process_chunk(
        &mut self,
        from: NodeId<Self::NodeIdPubKey>,
        chunk: Self::Chunk,
        data: Bytes,
    ) -> Option<AppMessage>;

    fn generate_chunk(&mut self) -> Option<(NodeId<Self::NodeIdPubKey>, Self::Chunk, Bytes)>;

    /// Peer is now seeding - so we can stop sending them chunks
    fn set_peer_seeder(&mut self, peer: NodeId<Self::NodeIdPubKey>);
}

pub trait Meta {
    type NodeIdPubKey: PubKey;
    type PayloadId;
    fn id(&self) -> Self::PayloadId;
    fn creator(&self) -> NodeId<Self::NodeIdPubKey>;
    fn created_at(&self) -> Duration;
}

pub trait Chunk: Clone + Debug + Serialize + DeserializeOwned {
    type PayloadId;
    fn id(&self) -> Self::PayloadId;
}
