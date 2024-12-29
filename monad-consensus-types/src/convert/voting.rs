use monad_proto::{error::ProtoError, proto::voting::*};

use crate::voting::Vote;

impl From<&Vote> for ProtoVote {
    fn from(vi: &Vote) -> Self {
        ProtoVote {
            id: Some((&vi.id).into()),
            epoch: Some((&vi.epoch).into()),
            round: Some((&vi.round).into()),
            parent_id: Some((&vi.parent_id).into()),
            parent_round: Some((&vi.parent_round).into()),
            seq_num: Some((&vi.seq_num).into()),
            timestamp: vi.timestamp,
            version: Some((&vi.version).into()),
        }
    }
}
impl TryFrom<ProtoVote> for Vote {
    type Error = ProtoError;
    fn try_from(proto_vi: ProtoVote) -> Result<Self, Self::Error> {
        Ok(Self {
            id: proto_vi
                .id
                .ok_or(Self::Error::MissingRequiredField("Vote.id".to_owned()))?
                .try_into()?,
            epoch: proto_vi
                .epoch
                .ok_or(Self::Error::MissingRequiredField("Vote.epoch".to_owned()))?
                .try_into()?,
            round: proto_vi
                .round
                .ok_or(Self::Error::MissingRequiredField("Vote.round".to_owned()))?
                .try_into()?,
            parent_id: proto_vi
                .parent_id
                .ok_or(Self::Error::MissingRequiredField(
                    "Vote.parent_id".to_owned(),
                ))?
                .try_into()?,
            parent_round: proto_vi
                .parent_round
                .ok_or(Self::Error::MissingRequiredField(
                    "Vote.parent_round".to_owned(),
                ))?
                .try_into()?,
            seq_num: proto_vi
                .seq_num
                .ok_or(Self::Error::MissingRequiredField("Vote.seq_num".to_owned()))?
                .try_into()?,
            timestamp: proto_vi.timestamp,
            version: proto_vi
                .version
                .ok_or(Self::Error::MissingRequiredField("Vote.version".to_owned()))?
                .try_into()?,
        })
    }
}
