use monad_proto::{error::ProtoError, proto::voting::*};

use crate::voting::Vote;

impl From<&Vote> for ProtoVote {
    fn from(vi: &Vote) -> Self {
        ProtoVote {
            id: Some((&vi.id).into()),
            epoch: Some((&vi.epoch).into()),
            round: Some((&vi.round).into()),
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
        })
    }
}
