use std::fmt::Debug;

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hashable, Hasher},
};
use monad_types::{EnumDiscriminant, Round};

use crate::{
    messages::message::{ProposalMessage, TimeoutMessage, VoteMessage},
    validation::signing::{Validated, Verified},
};

/// Consensus protocol messages
#[derive(Clone, PartialEq, Eq)]
pub enum ProtocolMessage<SCT: SignatureCollection> {
    /// Consensus protocol proposal message
    Proposal(ProposalMessage<SCT>),

    /// Consensus protocol vote message
    Vote(VoteMessage<SCT>),

    /// Consensus protocol timeout message
    Timeout(TimeoutMessage<SCT>),
}

impl<SCT: Debug + SignatureCollection> Debug for ProtocolMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolMessage::Proposal(p) => f.debug_tuple("").field(&p).finish(),
            ProtocolMessage::Vote(v) => f.debug_tuple("").field(&v).finish(),
            ProtocolMessage::Timeout(t) => f.debug_tuple("").field(&t).finish(),
        }
    }
}

/// Integrity hash
impl<SCT> Hashable for ProtocolMessage<SCT>
where
    SCT: SignatureCollection,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            ProtocolMessage::Proposal(m) => {
                EnumDiscriminant(1).hash(state);
                m.hash(state);
            }
            // FIXME-2:
            // it can be confusing as we are hashing only part of the message
            // in the signature refactoring, we might want a clean split between:
            //      integrity sig: sign over the entire serialized struct
            //      protocol sig: signatures outlined in the protocol
            ProtocolMessage::Vote(m) => {
                EnumDiscriminant(2).hash(state);
                m.hash(state);
            }
            ProtocolMessage::Timeout(m) => {
                EnumDiscriminant(3).hash(state);
                m.hash(state);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsensusMessage<SCT: SignatureCollection> {
    pub version: String,
    pub message: ProtocolMessage<SCT>,
}

impl<SCT> Hashable for ConsensusMessage<SCT>
where
    SCT: SignatureCollection,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(&self.version);
        self.message.hash(state);
    }
}

impl<SCT> ConsensusMessage<SCT>
where
    SCT: SignatureCollection,
{
    pub fn sign<ST>(
        self,
        keypair: &ST::KeyPairType,
    ) -> Verified<ST, Validated<ConsensusMessage<SCT>>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        Verified::new(Validated::new(self), keypair)
    }

    pub fn get_round(&self) -> Round {
        match &self.message {
            ProtocolMessage::Proposal(p) => p.block.round,
            ProtocolMessage::Vote(v) => v.vote.vote_info.round,
            ProtocolMessage::Timeout(t) => t.timeout.tminfo.round,
        }
    }
}
