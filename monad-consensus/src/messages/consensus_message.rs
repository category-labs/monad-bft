use std::fmt::Debug;

use monad_consensus_types::signature::SignatureCollection;
use monad_consensus_types::validation::{Hashable, Hasher};
use monad_crypto::{secp256k1::KeyPair, Signature};

use crate::{
    messages::message::{ProposalMessage, TimeoutMessage, VoteMessage},
    validation::signing::Verified,
};

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusMessage<ST, SCT> {
    Proposal(ProposalMessage<ST, SCT>),
    Vote(VoteMessage),
    Timeout(TimeoutMessage<ST, SCT>),
}

impl<ST: Debug, SCT: Debug> Debug for ConsensusMessage<ST, SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusMessage::Proposal(p) => f.debug_tuple("").field(&p).finish(),
            ConsensusMessage::Vote(v) => f.debug_tuple("").field(&v).finish(),
            ConsensusMessage::Timeout(t) => f.debug_tuple("").field(&t).finish(),
        }
    }
}

impl<ST, SCT> Hashable for ConsensusMessage<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ConsensusMessage::Proposal(m) => m.hash(state),
            // FIXME:
            // implemented Hashable for VoteMsg, which only hash the ledger commit info
            // it can be confusing as we are hashing only part of the message
            // in the signature refactoring, we might want a clean split between:
            //      integrity sig: sign over the entire serialized struct
            //      protocol sig: signatures outlined in the protocol
            // TimeoutMsg doesn't have a protocol sig
            ConsensusMessage::Vote(m) => m.hash(state),
            ConsensusMessage::Timeout(m) => m.hash(state),
        }
    }
}

impl<ST, SCT> ConsensusMessage<ST, SCT>
where
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
{
    pub fn sign<H: Hasher>(self, keypair: &KeyPair) -> Verified<ST, ConsensusMessage<ST, SCT>> {
        Verified::new::<H>(self, keypair)
    }
}
