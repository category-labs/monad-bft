use crate::voting::VoteInfo;

use monad_proto::error::ProtoError;
use monad_proto::proto::voting::*;
use monad_types::Round;

impl From<&VoteInfo> for ProtoVoteInfo {
    fn from(vi: &VoteInfo) -> Self {
        ProtoVoteInfo {
            id: Some((&vi.id).into()),
            round: vi.round.0,
            parent_id: Some((&vi.parent_id).into()),
            parent_round: vi.parent_round.0,
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
            round: Round(proto_vi.round),
            parent_id: proto_vi
                .parent_id
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteInfo.parent_id".to_owned(),
                ))?
                .try_into()?,
            parent_round: Round(proto_vi.parent_round),
        })
    }
}
