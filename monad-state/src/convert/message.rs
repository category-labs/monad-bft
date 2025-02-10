use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{error::ProtoError, proto::message::*};
use monad_types::PingSequence;

use crate::{MonadMessage, VerifiedMonadMessage};

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
                VerifiedMonadMessage::PingRequest(msg) => {
                    proto_monad_message::OneofMessage::PingRequest(ProtoPingRequest {
                        sequence: msg.0,
                    })
                }
                VerifiedMonadMessage::PingResponse(msg) => {
                    proto_monad_message::OneofMessage::PingResponse(ProtoPingResponse {
                        sequence: msg.0,
                    })
                }
                VerifiedMonadMessage::ProposalPing(msg) => {
                    proto_monad_message::OneofMessage::ProposalPing(ProtoProposalPing {
                        round: msg.0,
                    })
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
            Some(proto_monad_message::OneofMessage::PingRequest(msg)) => {
                MonadMessage::PingRequest(PingSequence(msg.sequence))
            }
            Some(proto_monad_message::OneofMessage::PingResponse(msg)) => {
                MonadMessage::PingResponse(PingSequence(msg.sequence))
            }
            Some(proto_monad_message::OneofMessage::ProposalPing(msg)) => {
                MonadMessage::ProposalPing(monad_types::Round(msg.round))
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadMessage.oneofmessage".to_owned(),
            ))?,
        };
        Ok(msg)
    }
}
