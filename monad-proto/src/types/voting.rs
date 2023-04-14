use monad_consensus::types::voting::VoteInfo;
use monad_types::{BlockId, Round};

use crate::error::ProtoError;

include!(concat!(env!("OUT_DIR"), "/monad_proto.voting.rs"));

impl From<&VoteInfo> for ProtoVoteInfo {
    fn from(vi: &VoteInfo) -> Self {
        ProtoVoteInfo {
            id: vi.id.0.to_vec(),
            round: vi.round.0,
            parent_id: vi.parent_id.0.to_vec(),
            parent_round: vi.parent_round.0,
        }
    }
}
impl TryFrom<ProtoVoteInfo> for VoteInfo {
    type Error = ProtoError;
    fn try_from(proto_vi: ProtoVoteInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            id: BlockId(
                proto_vi
                    .id
                    .try_into()
                    .map_err(|e: Vec<_>| Self::Error::WrongHashLen(format!("id {}", e.len())))?,
            ),
            round: Round(proto_vi.round),
            parent_id: BlockId(proto_vi.parent_id.try_into().map_err(|e: Vec<_>| {
                Self::Error::WrongHashLen(format!("parent_id {}", e.len()))
            })?),
            parent_round: Round(proto_vi.parent_round),
        })
    }
}
