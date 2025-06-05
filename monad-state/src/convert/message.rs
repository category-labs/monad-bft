use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{error::ProtoError, proto::message::*};
use monad_types::ExecutionProtocol;

use crate::{MonadMessage, VerifiedMonadMessage};

impl<ST, SCT, EPT> From<&VerifiedMonadMessage<ST, SCT, EPT>> for ProtoMonadMessage
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &VerifiedMonadMessage<ST, SCT, EPT>) -> Self {
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
                VerifiedMonadMessage::ForwardedTx(msg) => {
                    proto_monad_message::OneofMessage::ForwardedTx(ProtoForwardedTx {
                        tx: (*msg).clone(),
                    })
                }
                VerifiedMonadMessage::StateSyncMessage(msg) => {
                    proto_monad_message::OneofMessage::StateSyncMessage(msg.into())
                }
                VerifiedMonadMessage::PingRequest(msg) => {
                    proto_monad_message::OneofMessage::PingRequest(msg.into())
                }
                VerifiedMonadMessage::PingResponse(msg) => {
                    proto_monad_message::OneofMessage::PingResponse(msg.into())
                }
            }),
        }
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoMonadMessage> for MonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
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
            Some(proto_monad_message::OneofMessage::ForwardedTx(msg)) => {
                MonadMessage::ForwardedTx(msg.tx)
            }
            Some(proto_monad_message::OneofMessage::StateSyncMessage(msg)) => {
                MonadMessage::StateSyncMessage(msg.try_into()?)
            }
            Some(proto_monad_message::OneofMessage::PingRequest(msg)) => {
                MonadMessage::PingRequest(msg.try_into()?)
            }
            Some(proto_monad_message::OneofMessage::PingResponse(msg)) => {
                MonadMessage::PingResponse(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadMessage.oneofmessage".to_owned(),
            ))?,
        };
        Ok(msg)
    }
}
