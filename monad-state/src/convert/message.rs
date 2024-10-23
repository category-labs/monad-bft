use bytes::Bytes;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{
    error::ProtoError,
    proto::message::{ProtoRouterMessage, *},
};
use prost::Message as _;

use crate::{DiscoveryMessage, InboundRouterMessage, MonadMessage, VerifiedMonadMessage};

impl<ST, SCT> From<&VerifiedMonadMessage<ST, SCT>> for ProtoMonadMessage
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
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
                VerifiedMonadMessage::PeerStateRootMessage(msg) => {
                    proto_monad_message::OneofMessage::PeerStateRoot(msg.into())
                }
                VerifiedMonadMessage::ForwardedTx(msg) => {
                    proto_monad_message::OneofMessage::ForwardedTx(ProtoForwardedTx {
                        tx: (*msg).clone(),
                    })
                }
                VerifiedMonadMessage::StateSyncMessage(msg) => {
                    proto_monad_message::OneofMessage::StateSyncMessage(msg.into())
                }
            }),
        }
    }
}

impl<ST, SCT> TryFrom<ProtoMonadMessage> for MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
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
            Some(proto_monad_message::OneofMessage::PeerStateRoot(msg)) => {
                MonadMessage::PeerStateRoot(msg.try_into()?)
            }
            Some(proto_monad_message::OneofMessage::ForwardedTx(msg)) => {
                MonadMessage::ForwardedTx(msg.tx)
            }
            Some(proto_monad_message::OneofMessage::StateSyncMessage(msg)) => {
                MonadMessage::StateSyncMessage(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadMessage.oneofmessage".to_owned(),
            ))?,
        };
        Ok(msg)
    }
}

impl<M> TryFrom<ProtoRouterMessage> for InboundRouterMessage<M> {
    type Error = ProtoError;

    fn try_from(value: ProtoRouterMessage) -> Result<Self, Self::Error> {
        match value.message.ok_or(ProtoError::MissingRequiredField(
            "ProtoRouterMessage.message".to_owned(),
        ))? {
            proto_router_message::Message::AppMessage(app_message) => {
                Ok(InboundRouterMessage::Application(app_message.try_into()?))
            }
            proto_router_message::Message::DiscoveryMessage(discovery_message) => {
                Ok(InboundRouterMessage::Discovery(DiscoveryMessage))
            }
        }
    }
}

impl<M> monad_types::Deserializable<Bytes> for InboundRouterMessage<M> {
    type ReadError = ProtoError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        ProtoRouterMessage::decode(message.clone())?.try_into()
    }
}
