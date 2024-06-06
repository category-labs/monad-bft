use monad_proto::{error::ProtoError, proto::state_root_hash::*};

use crate::state_root_hash::{StateRootHash, StateRootHashInfo};

impl From<&StateRootHash> for ProtoStateRootHash {
    fn from(value: &StateRootHash) -> Self {
        ProtoStateRootHash {
            hash: Some((&value.0).into()),
        }
    }
}

impl TryFrom<ProtoStateRootHash> for StateRootHash {
    type Error = ProtoError;

    fn try_from(value: ProtoStateRootHash) -> Result<Self, Self::Error> {
        let h = value
            .hash
            .ok_or(ProtoError::MissingRequiredField(
                "ProtoStateRootHash.hash".to_owned(),
            ))?
            .try_into()?;

        Ok(StateRootHash(h))
    }
}

impl From<&StateRootHashInfo> for ProtoStateRootHashInfo {
    fn from(value: &StateRootHashInfo) -> Self {
        ProtoStateRootHashInfo {
            state_root_hash: Some((&value.state_root_hash).into()),
            seq_num: Some((&value.seq_num).into()),
        }
    }
}

impl TryFrom<ProtoStateRootHashInfo> for StateRootHashInfo {
    type Error = ProtoError;

    fn try_from(value: ProtoStateRootHashInfo) -> Result<Self, Self::Error> {
        let state_root_hash = value
            .state_root_hash
            .ok_or(ProtoError::MissingRequiredField(
                "ProtoStateRootHashVote.state_root_hash".to_owned(),
            ))?
            .try_into()?;

        let seq_num = value
            .seq_num
            .ok_or(ProtoError::MissingRequiredField(
                "ProtoStateRootHashVote.seq_num".to_owned(),
            ))?
            .try_into()?;

        Ok(StateRootHashInfo {
            state_root_hash,
            seq_num,
        })
    }
}
