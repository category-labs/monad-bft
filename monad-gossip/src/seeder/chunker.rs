use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt::Debug,
    time::Duration,
};

use bytes::Bytes;
use monad_crypto::certificate_signature::PubKey;

use monad_types::NodeId;
use serde::{de::DeserializeOwned, Serialize};

use crate::{AppMessage, GossipEvent};

/// Chunker is responsible for constructing a chunking scheme
/// Examples of different schemes:
/// 1) Splitting payload into N chunks
/// 2) RS encoding, splitting into N chunks
/// 3) Fountain encoding, splitting into N chunks
pub trait Chunker: Sized {
    type NodeIdPubKey: PubKey;
    type PayloadId: Clone + Ord + Debug; // payload includes AppMessage + created_at TS as entropy
    type Meta: Meta<NodeIdPubKey = Self::NodeIdPubKey, PayloadId = Self::PayloadId>
        + Clone
        + Debug
        + Serialize
        + DeserializeOwned;
    type Chunk: Chunk<PayloadId = Self::PayloadId> + Clone + Debug + Serialize + DeserializeOwned;

    /// This must generate a Chunker with a UNIQUE PayloadId
    /// This is to ensure that two separate chunkers are generated for two separate broadcasts,
    /// even if they are the same AppMessage
    fn try_new_from_message(message: AppMessage) -> Result<Self, Box<dyn Error>>;
    fn try_new_from_meta(meta: Self::Meta) -> Result<Self, Box<dyn Error>>;

    fn meta(&self) -> &Self::Meta;

    // Payload has been reconstructed - process_chunk should not be called if this returns true
    fn is_complete(&self) -> bool;

    /// Some(x) indicates that the chunker doesn't need to receive any more chunks, because it has
    /// successfully reconstructed payload. Chunker::is_complete MUST return true after this.
    fn process_chunk(
        &mut self,
        from: NodeId<Self::NodeIdPubKey>,
        chunk: Self::Chunk,
        data: Bytes,
    ) -> Option<AppMessage>;

    fn generate_chunk(&mut self) -> Option<(NodeId<Self::NodeIdPubKey>, Self::Chunk)>;

    // Peer's payload has been reconstructed - so we can stop sending them chunks
    fn peer_complete(&mut self);
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

pub struct Chunkers<C: Chunker> {
    chunkers: BTreeMap<C::PayloadId, C>,
    current_tick: Duration,

    /// Chunker is scheduled to be deleted `timeout` after C::Meta::created_at
    timeout: Duration,
    chunker_timeouts: BTreeMap<Duration, Vec<C::PayloadId>>,

    events: VecDeque<GossipEvent<C::NodeIdPubKey>>,
}

impl<C: Chunker> Default for Chunkers<C> {
    fn default() -> Self {
        Self {
            chunkers: Default::default(),
            current_tick: Duration::ZERO,

            timeout: Duration::from_millis(700),
            chunker_timeouts: Default::default(),

            events: Default::default(),
        }
    }
}

impl<C: Chunker> Chunkers<C> {
    pub fn contains(&self, id: &C::PayloadId) -> bool {
        self.chunkers.contains_key(id)
    }

    /// note that this must not overwrite an existing chunker!
    pub fn insert(&mut self, chunker: C) {
        let created_at = chunker.meta().created_at();
        self.chunker_timeouts
            .entry(created_at + self.timeout)
            .or_default()
            .push(chunker.meta().id());

        let removed = self.chunkers.insert(chunker.meta().id(), chunker);
        assert!(removed.is_none());
    }

    pub fn update_tick(&mut self, time: Duration) {
        self.current_tick = time;
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
    }

    pub fn process_chunk(&mut self, from: NodeId<C::NodeIdPubKey>, chunk: C::Chunk, data: Bytes) {
        let id = chunk.id();
        if let Some(chunker) = self.chunkers.get_mut(&id) {
            if !chunker.is_complete() {
                let maybe_app_message = chunker.process_chunk(from, chunk, data);
                if let Some(app_message) = maybe_app_message {
                    self.events
                        .push_back(GossipEvent::Emit(chunker.meta().creator(), app_message));
                    assert!(chunker.is_complete());
                }
            }
            // chunker may be complete if event was emitted
            if chunker.is_complete() {
                todo!("send P2P response telling peer to stop sending. only should send this ONCE per peer per chunker! peer should then call Chunker::peer_complete on receival")
            }
        } else {
            tracing::trace!("no chunker initialized for id: {:?}", id);
        }
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.current_tick)
        } else {
            None
        }
    }

    // TODO add ChunkersEvent type, which has ChunkersEvent::Emit, and ChunkersEvent::{SendChunk,
    // SendMeta, SendSeeding}
    pub fn poll(&mut self) -> Option<GossipEvent<C::NodeIdPubKey>> {
        self.events.pop_front()
        // TODO generate_chunk needs to be called on individual peers
        // TODO peek_tick needs to be updated after this is done
    }
}
