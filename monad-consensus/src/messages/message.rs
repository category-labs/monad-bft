use std::fmt::Debug;

use monad_consensus_types::{
    block::Block,
    payload::Payload,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHashInfo,
    timeout::{Timeout, TimeoutCertificate},
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    hasher::{Hashable, Hasher, HasherType},
};
use monad_types::NodeId;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerStateRootMessage<SCT: SignatureCollection> {
    pub peer: NodeId<SCT::NodeIdPubKey>,
    pub info: StateRootHashInfo,
    pub sig: SCT::SignatureType,
}
