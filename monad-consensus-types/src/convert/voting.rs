use monad_proto::{error::ProtoError, proto::voting::*};

use crate::voting::{Vote, VoteInfo};

impl From<&VoteInfo> for ProtoVoteInfo {
    fn from(vi: &VoteInfo) -> Self {
        ProtoVoteInfo {
            id: Some((&vi.id).into()),
            round: Some((&vi.round).into()),
            parent_id: Some((&vi.parent_id).into()),
            parent_round: Some((&vi.parent_round).into()),
            seq_num: vi.seq_num,
        }
    }
}
impl TryFrom<ProtoVoteInfo> for VoteInfo {
    type Error = ProtoError;
    fn try_from(proto_vi: ProtoVoteInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            id: proto_vi
                .id
                .ok_or(Self::Error::MissingRequiredField("VoteInfo.id".to_owned()))?
                .try_into()?,
            round: proto_vi
                .round
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteInfo.round".to_owned(),
                ))?
                .try_into()?,
            parent_id: proto_vi
                .parent_id
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteInfo.parent_id".to_owned(),
                ))?
                .try_into()?,
            parent_round: proto_vi
                .parent_round
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteInfo.parent_round".to_owned(),
                ))?
                .try_into()?,
            seq_num: proto_vi.seq_num,
        })
    }
}

impl From<&Vote> for ProtoVote {
    fn from(value: &Vote) -> Self {
        ProtoVote {
            vote_info: Some((&value.vote_info).into()),
            ledger_commit_info: Some((&value.ledger_commit_info).into()),
        }
    }
}

impl TryFrom<ProtoVote> for Vote {
    type Error = ProtoError;

    fn try_from(value: ProtoVote) -> Result<Self, Self::Error> {
        Ok(Self {
            vote_info: value
                .vote_info
                .ok_or(Self::Error::MissingRequiredField(
                    "Vote.vote_info".to_owned(),
                ))?
                .try_into()?,
            ledger_commit_info: value
                .ledger_commit_info
                .ok_or(Self::Error::MissingRequiredField(
                    "Vote.ledger_commit_info".to_owned(),
                ))?
                .try_into()?,
        })
    }
}
