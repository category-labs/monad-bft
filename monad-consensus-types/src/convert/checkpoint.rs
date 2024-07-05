use monad_proto::{error::ProtoError, proto::event::ProtoRootInfo};

use crate::checkpoint::RootInfo;

impl From<&RootInfo> for ProtoRootInfo {
    fn from(root: &RootInfo) -> Self {
        ProtoRootInfo {
            round: Some((&root.round).into()),
            seq_num: Some((&root.seq_num).into()),
            epoch: Some((&root.epoch).into()),
            block_id: Some((&root.block_id).into()),
            state_root: Some((&root.state_root).into()),
        }
    }
}

impl TryFrom<ProtoRootInfo> for RootInfo {
    type Error = ProtoError;

    fn try_from(root: ProtoRootInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            round: root
                .round
                .ok_or(ProtoError::MissingRequiredField(
                    "RootInfo::round".to_owned(),
                ))?
                .try_into()?,
            seq_num: root
                .seq_num
                .ok_or(ProtoError::MissingRequiredField(
                    "RootInfo::seq_num".to_owned(),
                ))?
                .try_into()?,
            epoch: root
                .epoch
                .ok_or(ProtoError::MissingRequiredField(
                    "RootInfo::seq_num".to_owned(),
                ))?
                .try_into()?,
            block_id: root
                .block_id
                .ok_or(ProtoError::MissingRequiredField(
                    "RootInfo::seq_num".to_owned(),
                ))?
                .try_into()?,
            state_root: root
                .state_root
                .ok_or(ProtoError::MissingRequiredField(
                    "RootInfo::state_root".to_owned(),
                ))?
                .try_into()?,
        })
    }
}
