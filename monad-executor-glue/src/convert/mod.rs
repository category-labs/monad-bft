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
            ConsensusEvent::BlockSync {
                block_range,
                full_blocks,
            } => proto_consensus_event::Event::BlockSync(ProtoBlockSyncFullBlocks {
                block_range: Some(block_range.into()),
                full_blocks: full_blocks.iter().map(|b| b.into()).collect::<Vec<_>>(),
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
            Some(proto_consensus_event::Event::Timeout(_tmo)) => ConsensusEvent::Timeout,
            Some(proto_consensus_event::Event::BlockSync(blocks)) => {
                //ConsensusEvent::BlockSync(block.try_into()?)
                ConsensusEvent::BlockSync {
                    block_range: blocks
                        .block_range
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::blocksync.block_range".to_owned(),
                        ))?
                        .try_into()?,
                    full_blocks: blocks
                        .full_blocks
                        .into_iter()
                        .map(|b| b.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                }
            }
            None => Err(ProtoError::MissingRequiredField(
                "ConsensusEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}
