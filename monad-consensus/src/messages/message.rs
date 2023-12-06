use monad_consensus_types::{
    block::{Block, BlockType, UnverifiedFullBlock},
    certificate_signature::CertificateSignature,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{Timeout, TimeoutCertificate},
    voting::Vote,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_types::BlockId;

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
    pub fn new<H: Hasher>(vote: Vote, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let vote_hash = H::hash_object(&vote);

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
    pub fn new<H: Hasher>(
        timeout: Timeout<SCT>,
        key: &SignatureCollectionKeyPairType<SCT>,
    ) -> Self {
        let tmo_hash = timeout.tminfo.timeout_digest::<H>();
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
pub struct ProposalMessage<T> {
    pub block: Block<T>,
    pub last_round_tc: Option<TimeoutCertificate<T>>,
}

/// The last_round_tc can be independently verified. The message hash is over
/// the block only
impl<T: SignatureCollection> Hashable for ProposalMessage<T> {
    fn hash(&self, state: &mut impl Hasher) {
        self.block.hash(state);
    }
}

/// Request block sync message
///
/// The node sends the block sync request to repair path from a block to the
/// root in the block tree
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestBlockSyncMessage {
    pub block_id: BlockId,
}

impl Hashable for RequestBlockSyncMessage {
    fn hash(&self, state: &mut impl Hasher) {
        self.block_id.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncResponseMessage<T> {
    BlockFound(UnverifiedFullBlock<T>),
    NotAvailable(BlockId),
}

impl<T: SignatureCollection> BlockSyncResponseMessage<T> {
    pub fn get_block_id(&self) -> BlockId {
        match self {
            BlockSyncResponseMessage::BlockFound(b) => b.block.get_id(),
            BlockSyncResponseMessage::NotAvailable(bid) => *bid,
        }
    }
}

/// FIXME-2, possible hash malleability for variants, similar for
/// [crate::messages::consensus_message::ConsensusMessage]
impl<T: SignatureCollection> Hashable for BlockSyncResponseMessage<T> {
    fn hash(&self, state: &mut impl Hasher) {
        match self {
            BlockSyncResponseMessage::BlockFound(unverified_full_block) => {
                unverified_full_block.hash(state)
            }
            BlockSyncResponseMessage::NotAvailable(bid) => bid.hash(state),
        }
    }
}
