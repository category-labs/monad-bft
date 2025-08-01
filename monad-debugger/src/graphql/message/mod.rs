// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::ops::Deref;

use async_graphql::{Object, Union};
use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{
            AdvanceRoundMessage, NoEndorsementMessage, ProposalMessage, RoundRecoveryMessage,
            TimeoutMessage, VoteMessage,
        },
    },
    validation::signing::{Validated, Verified},
};
use monad_state::VerifiedMonadMessage;
use monad_types::Round;

use crate::graphql::{
    ExecutionProtocolType, GraphQLRound, GraphQLSeqNum, SignatureCollectionType, SignatureType,
};

type MonadMessageType =
    VerifiedMonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>;
type ConsensusMessageType =
    ConsensusMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>;
type VerifiedConsensusMessageType = Verified<SignatureType, Validated<ConsensusMessageType>>;

impl<'s> From<&'s MonadMessageType> for GraphQLMonadMessage<'s> {
    fn from(message: &'s MonadMessageType) -> Self {
        match message {
            MonadMessageType::Consensus(message) => {
                Self::Consensus(GraphQLConsensusMessage(message))
            }
            MonadMessageType::BlockSyncRequest(_) => todo!("BlockSyncRequest"),
            MonadMessageType::BlockSyncResponse(_) => todo!("BlockSyncResponse"),
            MonadMessageType::ForwardedTx(_) => todo!("ForwardedTx"),
            MonadMessageType::StateSyncMessage(_) => todo!("StateSyncMessage"),
        }
    }
}

#[derive(Union)]
pub(crate) enum GraphQLMonadMessage<'s> {
    Consensus(GraphQLConsensusMessage<'s>),
}

struct GraphQLConsensusMessage<'s>(&'s VerifiedConsensusMessageType);

#[Object]
impl<'s> GraphQLConsensusMessage<'s> {
    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.0.get_round())
    }
    async fn message(&self) -> GraphQLConsensusMessageType<'s> {
        self.0.into()
    }
}

impl<'s> From<&'s VerifiedConsensusMessageType> for GraphQLConsensusMessageType<'s> {
    fn from(message: &'s VerifiedConsensusMessageType) -> Self {
        match &message.deref().message {
            ProtocolMessage::Proposal(proposal) => Self::Proposal(GraphQLProposal(proposal)),
            ProtocolMessage::Vote(vote) => Self::Vote(GraphQLVote(vote)),
            ProtocolMessage::Timeout(timeout) => Self::Timeout(GraphQLTimeout(timeout)),
            ProtocolMessage::RoundRecovery(round_recovery) => {
                Self::RoundRecovery(GraphQLRoundRecovery(round_recovery))
            }
            ProtocolMessage::NoEndorsement(no_endorsement) => {
                Self::NoEndorsement(GraphQLNoEndorsement(no_endorsement))
            }
            ProtocolMessage::AdvanceRound(advance_round) => {
                Self::AdvanceRound(GraphQLAdvanceRound(advance_round))
            }
        }
    }
}

#[derive(Union)]
enum GraphQLConsensusMessageType<'s> {
    Proposal(GraphQLProposal<'s>),
    Vote(GraphQLVote<'s>),
    Timeout(GraphQLTimeout<'s>),
    RoundRecovery(GraphQLRoundRecovery<'s>),
    NoEndorsement(GraphQLNoEndorsement<'s>),
    AdvanceRound(GraphQLAdvanceRound<'s>),
}

struct GraphQLProposal<'s>(
    &'s ProposalMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl<'s> GraphQLProposal<'s> {
    async fn seq_num(&self) -> GraphQLSeqNum {
        GraphQLSeqNum::new(self.0.tip.block_header.seq_num)
    }
}

struct GraphQLVote<'s>(&'s VoteMessage<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLVote<'s> {
    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.0.vote.round)
    }
}

struct GraphQLTimeout<'s>(
    &'s TimeoutMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl<'s> GraphQLTimeout<'s> {
    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.0 .0.tminfo.round)
    }
}

struct GraphQLRoundRecovery<'s>(
    &'s RoundRecoveryMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl<'s> GraphQLRoundRecovery<'s> {
    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.0.round)
    }
}

struct GraphQLNoEndorsement<'s>(&'s NoEndorsementMessage<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLNoEndorsement<'s> {
    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.0.msg.round)
    }
}

struct GraphQLAdvanceRound<'s>(
    &'s AdvanceRoundMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl<'s> GraphQLAdvanceRound<'s> {
    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.0.last_round_certificate.round() + Round(1))
    }
}
