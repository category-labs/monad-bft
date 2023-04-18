use crate::types::message::{ProposalMessage, TimeoutMessage, VoteMessage};
use crate::validation::signing::{Unverified, Verified};

#[derive(Debug, Clone)]
pub enum SignedConsensusMessage<S, T> {
    Proposal(Unverified<S, ProposalMessage<S, T>>),
    Vote(Unverified<S, VoteMessage>),
    Timeout(Unverified<S, TimeoutMessage<S, T>>),
}

#[derive(Debug, Clone)]
pub enum VerifiedConsensusMessage<S, T> {
    Proposal(Verified<S, ProposalMessage<S, T>>),
    Vote(Verified<S, VoteMessage>),
    Timeout(Verified<S, TimeoutMessage<S, T>>),
}

impl<S, T> From<VerifiedConsensusMessage<S, T>> for SignedConsensusMessage<S, T> {
    fn from(value: VerifiedConsensusMessage<S, T>) -> Self {
        match value {
            VerifiedConsensusMessage::Proposal(msg) => Self::Proposal(msg.into()),
            VerifiedConsensusMessage::Vote(msg) => Self::Vote(msg.into()),
            VerifiedConsensusMessage::Timeout(msg) => Self::Timeout(msg.into()),
        }
    }
}
