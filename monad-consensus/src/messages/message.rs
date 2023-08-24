use monad_consensus_types::{
    block::Block,
    certificate_signature::CertificateSignature,
    message_signature::MessageSignature,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{TimeoutCertificate, TimeoutInfo},
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
pub struct TimeoutMessage<S, T> {
    pub tminfo: TimeoutInfo<T>,
    pub last_round_tc: Option<TimeoutCertificate<S>>,
}

impl<S: MessageSignature, T: SignatureCollection> Hashable for TimeoutMessage<S, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.tminfo.round);
        state.update(self.tminfo.high_qc.info.vote.round);
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
