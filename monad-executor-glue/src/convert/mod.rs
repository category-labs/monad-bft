use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{error::ProtoError, proto::event::*};
use monad_types::TimeoutVariant;

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
            ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                TimeoutVariant::Pacemaker => {
                    proto_consensus_event::Event::Timeout(ProtoScheduleTimeout {
                        event: Some(proto_schedule_timeout::Event::Pacemaker(
                            ProtoPaceMakerTimeout {},
                        )),
                    })
                }
                TimeoutVariant::BlockSync(bid) => {
                    proto_consensus_event::Event::Timeout(ProtoScheduleTimeout {
                        event: Some(proto_schedule_timeout::Event::BlockSync(
                            ProtoBlockSyncTimeout {
                                block_id: Some((bid).into()),
                            },
                        )),
                    })
                }
            },
            ConsensusEvent::BlockSyncResponse {
                sender,
                unvalidated_response,
            } => proto_consensus_event::Event::BlockSyncResp(ProtoBlockSyncResponseWithSender {
                sender: Some(sender.into()),
                response: Some(unvalidated_response.into()),
            }),
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
            Some(proto_consensus_event::Event::Timeout(tmo)) => {
                ConsensusEvent::Timeout(match tmo.event {
                    Some(proto_schedule_timeout::Event::Pacemaker(_)) => TimeoutVariant::Pacemaker,
                    Some(proto_schedule_timeout::Event::BlockSync(bsync_tmo)) => {
                        TimeoutVariant::BlockSync(
                            bsync_tmo
                                .block_id
                                .ok_or(ProtoError::MissingRequiredField(
                                    "ConsensusEvent::Timeout.bsync_tmo.block_id".to_owned(),
                                ))?
                                .try_into()?,
                        )
                    }
                    None => Err(ProtoError::MissingRequiredField(
                        "ConsensusEvent.event".to_owned(),
                    ))?,
                })
            }
            Some(proto_consensus_event::Event::BlockSyncResp(event)) => {
                let sender = event
                    .sender
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::BlockSyncResp::sender".to_owned(),
                    ))?
                    .try_into()?;

                let unvalidated_response = event
                    .response
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::BlockSyncResp::response".to_owned(),
                    ))?
                    .try_into()?;

                ConsensusEvent::BlockSyncResponse {
                    sender,
                    unvalidated_response,
                }
            }
            None => Err(ProtoError::MissingRequiredField(
                "ConsensusEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}
