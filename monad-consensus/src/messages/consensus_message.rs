use std::fmt::Debug;

use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_crypto::{
    hasher::{Hashable, Hasher},
    secp256k1::KeyPair,
};

use crate::{
    messages::message::{ProposalMessage, TimeoutMessage, VoteMessage},
    validation::signing::{Validated, Verified},
};

/// Consensus protocol messages
#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusMessage<SCT: SignatureCollection> {
    /// Consensus protocol proposal message
    Proposal(ProposalMessage<SCT>),

    /// Consensus protocol vote message
    Vote(VoteMessage<SCT>),

    /// Consensus protocol timeout message
    Timeout(TimeoutMessage<SCT>),
}

impl<SCT: Debug + SignatureCollection> Debug for ConsensusMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusMessage::Proposal(p) => f.debug_tuple("").field(&p).finish(),
            ConsensusMessage::Vote(v) => f.debug_tuple("").field(&v).finish(),
            ConsensusMessage::Timeout(t) => f.debug_tuple("").field(&t).finish(),
        }
    }
}

/// Integrity hash
impl<SCT> Hashable for ConsensusMessage<SCT>
where
    SCT: SignatureCollection,
{
    fn hash(&self, state: &mut impl Hasher) {
        match self {
            ConsensusMessage::Proposal(m) => m.hash(state),
            // FIXME-2:
            // it can be confusing as we are hashing only part of the message
            // in the signature refactoring, we might want a clean split between:
            //      integrity sig: sign over the entire serialized struct
            //      protocol sig: signatures outlined in the protocol
            ConsensusMessage::Vote(m) => m.hash(state),
            ConsensusMessage::Timeout(m) => m.hash(state),
        }
    }
}

impl<SCT> ConsensusMessage<SCT>
where
    SCT: SignatureCollection,
{
    pub fn sign<ST: MessageSignature>(
        self,
        keypair: &KeyPair,
    ) -> Verified<ST, Validated<ConsensusMessage<SCT>>> {
        Verified::new(Validated::new(self), keypair)
    }
}
