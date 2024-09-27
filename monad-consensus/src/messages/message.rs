use std::fmt::Debug;

use monad_consensus_types::{
    block::{Block, BlockIdRange, BlockType, FullBlock},
    payload::{Payload, PayloadId},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHashInfo,
    timeout::{Timeout, TimeoutCertificate},
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    hasher::{Hashable, Hasher, HasherType},
};
use monad_types::{EnumDiscriminant, NodeId};
use zerocopy::AsBytes;

/// Consensus protocol vote message
///
/// The signature is a protocol signature, can be collected into the
/// corresponding SignatureCollection type, used to create QC from the votes
#[derive(PartialEq, Eq, Clone)]
pub struct VoteMessage<SCT: SignatureCollection> {
    pub vote: Vote,
    pub sig: SCT::SignatureType,
}

/// Explicitly implementing Copy because derive macro can't resolve that
/// SCT::SignatureType is actually Copy
impl<SCT: SignatureCollection> Copy for VoteMessage<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for VoteMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteMessage")
            .field("vote", &self.vote)
            .field("sig", &self.sig)
            .finish()
    }
}

/// An integrity hash over all the fields
impl<SCT: SignatureCollection> Hashable for VoteMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.vote.hash(state);
        self.sig.hash(state);
    }
}

impl<SCT: SignatureCollection> VoteMessage<SCT> {
    pub fn new(vote: Vote, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let vote_hash = HasherType::hash_object(&vote);

        let sig = <SCT::SignatureType as CertificateSignature>::sign(vote_hash.as_ref(), key);

        Self { vote, sig }
    }
}

/// Consensus protocol timeout message
///
/// The signature is a protocol signature,can be collected into the
/// corresponding SignatureCollection type, used to create TC from the timeouts
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutMessage<SCT: SignatureCollection> {
    pub timeout: Timeout<SCT>,
    pub sig: SCT::SignatureType,
}

impl<SCT: SignatureCollection> TimeoutMessage<SCT> {
    pub fn new(timeout: Timeout<SCT>, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let tmo_hash = timeout.tminfo.timeout_digest();
        let sig = <SCT::SignatureType as CertificateSignature>::sign(tmo_hash.as_ref(), key);

        Self { timeout, sig }
    }
}

/// An integrity hash over all the fields
impl<SCT: SignatureCollection> Hashable for TimeoutMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.timeout.hash(state);
        self.sig.hash(state);
    }
}

/// Consensus protocol proposal message
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposalMessage<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub payload: Payload,
    pub last_round_tc: Option<TimeoutCertificate<SCT>>,
}

/// The last_round_tc can be independently verified. The message hash is over
/// the block only
impl<T: SignatureCollection> Hashable for ProposalMessage<T> {
    fn hash(&self, state: &mut impl Hasher) {
        self.block.hash(state);
    }
}

/// Request block sync message sent to a peer
///
/// The node sends the block sync request either missing blocks headers or
/// a single payload
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum BlockSyncRequestMessage {
    Headers(BlockIdRange),
    Payload(PayloadId),
}

impl Hashable for BlockSyncRequestMessage {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncRequestMessage::Headers(block_id_range) => {
                EnumDiscriminant(1).hash(state);
                block_id_range.hash(state);
            }
            BlockSyncRequestMessage::Payload(payload_id) => {
                EnumDiscriminant(2).hash(state);
                state.update(payload_id.0.as_bytes());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncHeadersResponse<SCT: SignatureCollection> {
    Found((BlockIdRange, Vec<Block<SCT>>)),
    NotAvailable(BlockIdRange),
}

impl<SCT: SignatureCollection> Hashable for BlockSyncHeadersResponse<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncHeadersResponse::Found((block_id_range, blocks)) => {
                EnumDiscriminant(1).hash(state);
                block_id_range.hash(state);
                for block in blocks {
                    block.hash(state);
                }
            }
            BlockSyncHeadersResponse::NotAvailable(block_id_range) => {
                EnumDiscriminant(2).hash(state);
                block_id_range.hash(state);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncPayloadResponse {
    Found((PayloadId, Payload)),
    NotAvailable(PayloadId),
}

impl Hashable for BlockSyncPayloadResponse {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncPayloadResponse::Found((payload_id, payload)) => {
                EnumDiscriminant(1).hash(state);
                state.update(payload_id.0.as_bytes());
                payload.hash(state);
            }
            BlockSyncPayloadResponse::NotAvailable(payload_id) => {
                EnumDiscriminant(2).hash(state);
                state.update(payload_id.0.as_bytes());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncResponseMessage<SCT: SignatureCollection> {
    HeadersResponse(BlockSyncHeadersResponse<SCT>),
    PayloadResponse(BlockSyncPayloadResponse),
}

impl<SCT: SignatureCollection> BlockSyncResponseMessage<SCT> {
    pub fn found_headers(block_id_range: BlockIdRange, headers: Vec<Block<SCT>>) -> Self {
        Self::HeadersResponse(BlockSyncHeadersResponse::Found((block_id_range, headers)))
    }

    pub fn headers_not_available(block_id_range: BlockIdRange) -> Self {
        Self::HeadersResponse(BlockSyncHeadersResponse::NotAvailable(block_id_range))
    }

    pub fn found_payload(payload_id: PayloadId, payload: Payload) -> Self {
        Self::PayloadResponse(BlockSyncPayloadResponse::Found((payload_id, payload)))
    }

    pub fn payload_not_available(payload_id: PayloadId) -> Self {
        Self::PayloadResponse(BlockSyncPayloadResponse::NotAvailable(payload_id))
    }
}

impl<SCT: SignatureCollection> Hashable for BlockSyncResponseMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                EnumDiscriminant(1).hash(state);
                headers_response.hash(state);
            }
            BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                EnumDiscriminant(2).hash(state);
                payload_response.hash(state)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerStateRootMessage<SCT: SignatureCollection> {
    pub peer: NodeId<SCT::NodeIdPubKey>,
    pub info: StateRootHashInfo,
    pub sig: SCT::SignatureType,
}
