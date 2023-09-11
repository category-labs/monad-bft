use monad_consensus_types::{
    block::Block,
    certificate_signature::CertificateSignature,
    message_signature::MessageSignature,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{Timeout, TimeoutCertificate},
    validation::{Hashable, Hasher},
    voting::Vote,
};
use monad_types::BlockId;
use zerocopy::AsBytes;

#[derive(PartialEq, Eq)]
pub struct VoteMessage<SCT: SignatureCollection> {
    pub vote: Vote,
    pub sig: SCT::SignatureType,
}

impl<SCT: SignatureCollection> Clone for VoteMessage<SCT> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<SCT: SignatureCollection> Copy for VoteMessage<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for VoteMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteMessage")
            .field("vote", &self.vote)
            .field("sig", &self.sig)
            .finish()
    }
}

impl<SCT: SignatureCollection> Hashable for VoteMessage<SCT> {
    fn hash<H: Hasher>(&self, state: &mut H) {
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
        let tmo_hash = timeout.timeout_digest();
        let sig = <SCT::SignatureType as CertificateSignature>::sign(tmo_hash.as_ref(), key);

        Self { timeout, sig }
    }
}

impl<SCT: SignatureCollection> Hashable for TimeoutMessage<SCT> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timeout.hash(state);
        self.sig.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposalMessage<S, T> {
    pub block: Block<T>,
    pub last_round_tc: Option<TimeoutCertificate<S>>,
}

impl<S: MessageSignature, T: SignatureCollection> Hashable for ProposalMessage<S, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.block.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestBlockSyncMessage {
    pub block_id: BlockId,
}

impl Hashable for RequestBlockSyncMessage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.block_id.0.as_bytes());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSyncMessage<T> {
    pub block: Block<T>,
}

impl<T: SignatureCollection> Hashable for BlockSyncMessage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.block.hash(state);
    }
}
