use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_proto::{error::ProtoError, proto::message::*};

use crate::{MonadMessage, VerifiedMonadMessage};

impl<ST: MessageSignature, SCT: SignatureCollection> From<&VerifiedMonadMessage<ST, SCT>>
    for ProtoMonadMessage
{
    fn from(value: &VerifiedMonadMessage<ST, SCT>) -> Self {
        Self {
            oneof_message: Some(match value {
                VerifiedMonadMessage::Consensus(msg) => {
                    proto_monad_message::OneofMessage::Consensus(msg.into())
                }
                VerifiedMonadMessage::BlockSyncRequest(msg) => {
                    proto_monad_message::OneofMessage::BlockSyncRequest(msg.into())
                }
                VerifiedMonadMessage::BlockSyncResponse(msg) => {
                    proto_monad_message::OneofMessage::BlockSyncResponse(msg.into())
                }
            }),
        }
    }
}

impl<ST: MessageSignature, SCT: SignatureCollection> TryFrom<ProtoMonadMessage>
    for MonadMessage<ST, SCT>
{
    type Error = ProtoError;

    fn try_from(value: ProtoMonadMessage) -> Result<Self, Self::Error> {
        let msg = match value.oneof_message {
            Some(proto_monad_message::OneofMessage::Consensus(msg)) => {
                MonadMessage::Consensus(msg.try_into()?)
            }
            Some(proto_monad_message::OneofMessage::BlockSyncRequest(msg)) => {
                MonadMessage::BlockSyncRequest(msg.try_into()?)
            }
            Some(proto_monad_message::OneofMessage::BlockSyncResponse(msg)) => {
                MonadMessage::BlockSyncResponse(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadMessage.oneofmessage".to_owned(),
            ))?,
        };
        Ok(msg)
    }
}
