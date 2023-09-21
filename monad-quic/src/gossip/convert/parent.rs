use monad_proto::{
    error::ProtoError,
    proto::gossip::{proto_parent_message, ProtoParentMessage},
};

use crate::gossip::parent::GossipMessage;

impl TryFrom<ProtoParentMessage> for GossipMessage {
    type Error = ProtoError;

    fn try_from(message: ProtoParentMessage) -> Result<Self, Self::Error> {
        match message.r#type {
            Some(proto_parent_message::Type::Primary(bytes)) => Ok(GossipMessage::Primary(bytes)),
            Some(proto_parent_message::Type::Backup(bytes)) => Ok(GossipMessage::Backup(bytes)),
            None => Err(ProtoError::MissingRequiredField(
                "ProtoParentMessage.type".to_owned(),
            )),
        }
    }
}

impl From<GossipMessage> for ProtoParentMessage {
    fn from(message: GossipMessage) -> Self {
        match message {
            GossipMessage::Primary(bytes) => ProtoParentMessage {
                r#type: Some(proto_parent_message::Type::Primary(bytes)),
            },
            GossipMessage::Backup(bytes) => ProtoParentMessage {
                r#type: Some(proto_parent_message::Type::Backup(bytes)),
            },
        }
    }
}
