use std::fmt::Debug;

use monad_consensus_types::{
    message_signature::MessageSignature,
    signature_collection::SignatureCollection,
    validation::{Hashable, Hasher},
};
use monad_crypto::secp256k1::KeyPair;

use crate::{
    messages::message::{
        BlockSyncMessage, ProposalMessage, RequestBlockSyncMessage, TimeoutMessage, VoteMessage,
    },
    validation::signing::Verified,
};

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusMessage<ST, SCT: SignatureCollection> {
    Proposal(ProposalMessage<ST, SCT>),
    Vote(VoteMessage<SCT>),
    Timeout(TimeoutMessage<ST, SCT>),
    RequestBlockSync(RequestBlockSyncMessage),
    BlockSync(BlockSyncMessage<SCT>),
}

impl<ST: Debug, SCT: Debug + SignatureCollection> Debug for ConsensusMessage<ST, SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusMessage::Proposal(p) => f.debug_tuple("").field(&p).finish(),
            ConsensusMessage::Vote(v) => f.debug_tuple("").field(&v).finish(),
            ConsensusMessage::Timeout(t) => f.debug_tuple("").field(&t).finish(),
            ConsensusMessage::RequestBlockSync(s) => f.debug_tuple("").field(&s).finish(),
            ConsensusMessage::BlockSync(s) => f.debug_tuple("").field(&s).finish(),
        }
    }
}

impl<ST, SCT> Hashable for ConsensusMessage<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ConsensusMessage::Proposal(m) => m.hash(state),
            // FIXME:
            // it can be confusing as we are hashing only part of the message
            // in the signature refactoring, we might want a clean split between:
            //      integrity sig: sign over the entire serialized struct
            //      protocol sig: signatures outlined in the protocol
            // TimeoutMsg doesn't have a protocol sig
            ConsensusMessage::Vote(m) => m.hash(state),
            ConsensusMessage::Timeout(m) => m.hash(state),
            ConsensusMessage::RequestBlockSync(m) => m.hash(state),
            ConsensusMessage::BlockSync(m) => m.hash(state),
        }
    }
}

impl<ST, SCT> ConsensusMessage<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    pub fn sign<H: Hasher>(self, keypair: &KeyPair) -> Verified<ST, ConsensusMessage<ST, SCT>> {
        Verified::new::<H>(self, keypair)
    }
}
