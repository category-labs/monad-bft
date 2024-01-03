use monad_consensus_types::{
    message_signature::MessageSignature,
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
    validator_data::ValidatorData,
};
use monad_proto::{
    error::ProtoError,
    proto::{basic::ProtoPubkey, event::*, validator_data::ProtoValidatorData},
};

use crate::{BlockSyncEvent, FetchedBlock, MonadEvent, ValidatorEvent};

impl<SCT: SignatureCollection> From<&FetchedBlock<SCT>> for ProtoFetchedBlock {
    fn from(value: &FetchedBlock<SCT>) -> Self {
        ProtoFetchedBlock {
            requester: Some((&value.requester).into()),
            block_id: Some((&value.block_id).into()),
            unverified_block: value.unverified_block.as_ref().map(|b| b.into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoFetchedBlock> for FetchedBlock<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoFetchedBlock) -> Result<Self, Self::Error> {
        Ok(FetchedBlock {
            requester: value
                .requester
                .ok_or(ProtoError::MissingRequiredField(
                    "FetchedBlock.requester".to_owned(),
                ))?
                .try_into()?,
            block_id: value
                .block_id
                .ok_or(ProtoError::MissingRequiredField(
                    "FetchedBlock.requester".to_owned(),
                ))?
                .try_into()?,
            unverified_block: Some(
                value
                    .unverified_block
                    .ok_or(ProtoError::MissingRequiredField(
                        "FetchedBlock.unverified_block".to_owned(),
                    ))?
                    .try_into()?,
            ),
        })
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> From<&MonadEvent<S, SCT>> for ProtoMonadEvent
where
    for<'a> &'a SignatureCollectionPubKeyType<SCT>: Into<ProtoPubkey>,
{
    fn from(value: &MonadEvent<S, SCT>) -> Self {
        let event = match value {
            MonadEvent::ConsensusEvent(event) => {
                proto_monad_event::Event::ConsensusEvent(event.into())
            }
            MonadEvent::BlockSyncEvent(event) => {
                proto_monad_event::Event::BlockSyncEvent(event.into())
            }
            MonadEvent::ValidatorEvent(event) => {
                proto_monad_event::Event::ValidatorEvent(event.into())
            }
        };
        Self { event: Some(event) }
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> TryFrom<ProtoMonadEvent> for MonadEvent<S, SCT>
where
    ProtoPubkey: TryInto<SignatureCollectionPubKeyType<SCT>, Error = ProtoError>,
{
    type Error = ProtoError;
    fn try_from(value: ProtoMonadEvent) -> Result<Self, Self::Error> {
        let event: MonadEvent<S, SCT> = match value.event {
            Some(proto_monad_event::Event::ConsensusEvent(event)) => {
                MonadEvent::ConsensusEvent(event.try_into()?)
            }
            Some(proto_monad_event::Event::BlockSyncEvent(event)) => {
                MonadEvent::BlockSyncEvent(event.try_into()?)
            }
            Some(proto_monad_event::Event::ValidatorEvent(event)) => {
                MonadEvent::ValidatorEvent(event.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}

impl<SCT: SignatureCollection> From<&BlockSyncEvent<SCT>> for ProtoBlockSyncEvent {
    fn from(value: &BlockSyncEvent<SCT>) -> Self {
        let event = match value {
            BlockSyncEvent::BlockSyncRequest {
                sender,
                unvalidated_request,
            } => proto_block_sync_event::Event::BlockSyncReq(ProtoBlockSyncRequestWithSender {
                sender: Some(sender.into()),
                request: Some(unvalidated_request.into()),
            }),
            BlockSyncEvent::FetchedBlock(fetched_block) => {
                proto_block_sync_event::Event::FetchedBlock(fetched_block.into())
            }
        };
        Self { event: Some(event) }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoBlockSyncEvent> for BlockSyncEvent<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(event) => match event {
                proto_block_sync_event::Event::BlockSyncReq(event) => {
                    let sender = event
                        .sender
                        .ok_or(ProtoError::MissingRequiredField(
                            "BlockSyncEvent.block_sync_req.sender".to_owned(),
                        ))?
                        .try_into()?;
                    let unvalidated_request = event
                        .request
                        .ok_or(ProtoError::MissingRequiredField(
                            "BlockSyncEvent.block_sync_req.req".to_owned(),
                        ))?
                        .try_into()?;
                    BlockSyncEvent::BlockSyncRequest {
                        sender,
                        unvalidated_request,
                    }
                }
                proto_block_sync_event::Event::FetchedBlock(event) => {
                    BlockSyncEvent::FetchedBlock(event.try_into()?)
                }
            },
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncEvent.event".to_owned(),
            ))?,
        };

        Ok(event)
    }
}

impl<SCT: SignatureCollection> From<&ValidatorEvent<SCT>> for ProtoValidatorEvent
where
    for<'a> &'a SignatureCollectionPubKeyType<SCT>: Into<ProtoPubkey>,
{
    fn from(value: &ValidatorEvent<SCT>) -> Self {
        let event = match value {
            ValidatorEvent::UpdateValidators((validator_data, epoch)) => {
                proto_validator_event::Event::UpdateValidators(ProtoUpdateValidatorsEvent {
                    validator_data: Some(validator_data.into()),
                    epoch: Some(epoch.into()),
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoValidatorEvent> for ValidatorEvent<SCT>
where
    ValidatorData<SCT>: TryFrom<ProtoValidatorData, Error = ProtoError>,
{
    type Error = ProtoError;

    fn try_from(value: ProtoValidatorEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_validator_event::Event::UpdateValidators(event)) => {
                let vs = event
                    .validator_data
                    .ok_or(ProtoError::MissingRequiredField(
                        "ValidatorEvent::update_validators::validator_data".to_owned(),
                    ))?
                    .try_into()?;
                let e = event
                    .epoch
                    .ok_or(ProtoError::MissingRequiredField(
                        "ValidatorEvent::update_validators::epoch".to_owned(),
                    ))?
                    .try_into()?;
                ValidatorEvent::UpdateValidators((vs, e))
            }
            None => Err(ProtoError::MissingRequiredField(
                "ValidatorEvent.event".to_owned(),
            ))?,
        };

        Ok(event)
    }
}
