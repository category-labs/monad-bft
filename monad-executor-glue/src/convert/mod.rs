use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{error::ProtoError, proto::event::*};

use crate::ConsensusEvent;

pub mod event;
pub mod interface;

impl<S: CertificateSignatureRecoverable, SCT: SignatureCollection> From<&ConsensusEvent<S, SCT>>
    for ProtoConsensusEvent
{
    fn from(value: &ConsensusEvent<S, SCT>) -> Self {
        let event = match value {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => proto_consensus_event::Event::Message(ProtoMessageWithSender {
                sender: Some(sender.into()),
                unverified_message: Some(unverified_message.into()),
            }),
            ConsensusEvent::Timeout => {
                proto_consensus_event::Event::Timeout(ProtoPaceMakerTimeout {})
            }
            ConsensusEvent::BlockSync { block, payload } => {
                proto_consensus_event::Event::BlockSync(ProtoBlockSyncWithPayload {
                    block: Some(block.into()),
                    payload: Some(payload.into()),
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<S: CertificateSignatureRecoverable, SCT: SignatureCollection> TryFrom<ProtoConsensusEvent>
    for ConsensusEvent<S, SCT>
{
    type Error = ProtoError;

    fn try_from(value: ProtoConsensusEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_consensus_event::Event::Message(msg)) => ConsensusEvent::Message {
                sender: msg
                    .sender
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::message.sender".to_owned(),
                    ))?
                    .try_into()?,
                unverified_message: msg
                    .unverified_message
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::message.unverified_message".to_owned(),
                    ))?
                    .try_into()?,
            },
            Some(proto_consensus_event::Event::Timeout(_tmo)) => ConsensusEvent::Timeout,
            Some(proto_consensus_event::Event::BlockSync(event)) => {
                //ConsensusEvent::BlockSync(block.try_into()?)
                ConsensusEvent::BlockSync {
                    block: event
                        .block
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::blocksync.block".to_owned(),
                        ))?
                        .try_into()?,
                    payload: event
                        .payload
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::blocksync.payload".to_owned(),
                        ))?
                        .try_into()?,
                }
            }
            None => Err(ProtoError::MissingRequiredField(
                "ConsensusEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}
