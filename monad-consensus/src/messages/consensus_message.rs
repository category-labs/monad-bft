use std::fmt::Debug;

use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_crypto::{
    hasher::{Hashable, Hasher},
    secp256k1::KeyPair,
};

use crate::{
    messages::message::{
        BlockSyncResponseMessage, ProposalMessage, RequestBlockSyncMessage, TimeoutMessage,
        VoteMessage,
    },
    validation::signing::Verified,
};

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusMessage<SCT: SignatureCollection> {
    Proposal(ProposalMessage<SCT>),
    Vote(VoteMessage<SCT>),
    Timeout(TimeoutMessage<SCT>),
    RequestBlockSync(RequestBlockSyncMessage),
    BlockSync(BlockSyncResponseMessage<SCT>),
}

impl<SCT: Debug + SignatureCollection> Debug for ConsensusMessage<SCT> {
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
            // TimeoutMsg doesn't have a protocol sig
            ConsensusMessage::Vote(m) => m.hash(state),
            ConsensusMessage::Timeout(m) => m.hash(state),
            ConsensusMessage::RequestBlockSync(m) => m.hash(state),
            ConsensusMessage::BlockSync(m) => m.hash(state),
        }
    }
}

impl<SCT> ConsensusMessage<SCT>
where
    SCT: SignatureCollection,
{
    pub fn sign<H: Hasher, ST: MessageSignature>(
        self,
        keypair: &KeyPair,
    ) -> Verified<ST, ConsensusMessage<SCT>> {
        Verified::new::<H>(self, keypair)
    }
}
