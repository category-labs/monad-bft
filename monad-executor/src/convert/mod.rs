use monad_proto::error::ProtoError;
use monad_proto::proto::event::ProtoPeerId;

use crate::state::PeerId;

impl From<&PeerId> for ProtoPeerId {
    fn from(value: &PeerId) -> Self {
        Self {
            pubkey: Some((&(value.0)).into()),
        }
    }
}

impl TryFrom<ProtoPeerId> for PeerId {
    type Error = ProtoError;

    fn try_from(value: ProtoPeerId) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .pubkey
                .ok_or(ProtoError::MissingRequiredField("PeerId.pubkey".to_owned()))?
                .try_into()?,
        ))
    }
}
