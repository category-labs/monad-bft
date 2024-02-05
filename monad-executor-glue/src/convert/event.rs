use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{error::ProtoError, proto::event::*};

use crate::{
    BlockSyncEvent, FetchedBlock, MempoolEvent, MonadEvent, RecordMetricsEvent, ValidatorEvent,
};

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

impl<S: CertificateSignatureRecoverable, SCT: SignatureCollection> From<&MonadEvent<S, SCT>>
    for ProtoMonadEvent
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
            MonadEvent::MempoolEvent(event) => proto_monad_event::Event::MempoolEvent(event.into()),
            MonadEvent::RecordMetricsEvent(event) => {
                proto_monad_event::Event::RecordMetricsEvent(event.into())
            }
        };
        Self { event: Some(event) }
    }
}

impl<S: CertificateSignatureRecoverable, SCT: SignatureCollection> TryFrom<ProtoMonadEvent>
    for MonadEvent<S, SCT>
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
            Some(proto_monad_event::Event::MempoolEvent(event)) => {
                MonadEvent::MempoolEvent(event.try_into()?)
            }
            Some(proto_monad_event::Event::RecordMetricsEvent(event)) => {
                MonadEvent::RecordMetricsEvent(event.try_into()?)
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

impl<SCT: SignatureCollection> From<&ValidatorEvent<SCT>> for ProtoValidatorEvent {
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

impl<SCT: SignatureCollection> TryFrom<ProtoValidatorEvent> for ValidatorEvent<SCT> {
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

impl<SCT: SignatureCollection> From<&MempoolEvent<SCT>> for ProtoMempoolEvent {
    fn from(value: &MempoolEvent<SCT>) -> Self {
        let event = match value {
            MempoolEvent::UserTxns(tx) => {
                proto_mempool_event::Event::Usertx(ProtoUserTx { tx: tx.clone() })
            }
            MempoolEvent::CascadeTxns { sender, txns } => {
                proto_mempool_event::Event::CascadeTxns(ProtoCascadeTxnsWithSender {
                    sender: Some(sender.into()),
                    cascade: Some(txns.into()),
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoMempoolEvent> for MempoolEvent<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoMempoolEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_mempool_event::Event::Usertx(tx)) => MempoolEvent::UserTxns(tx.tx),
            Some(proto_mempool_event::Event::CascadeTxns(msg)) => MempoolEvent::CascadeTxns {
                sender: msg
                    .sender
                    .ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent.cascade_txns.sender".to_owned(),
                    ))?
                    .try_into()?,
                txns: msg
                    .cascade
                    .ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent.cascade.txns".to_owned(),
                    ))?
                    .try_into()?,
            },
            None => Err(ProtoError::MissingRequiredField(
                "MempoolEvent.event".to_owned(),
            ))?,
        };

        Ok(event)
    }
}

impl From<&RecordMetricsEvent> for ProtoRecordMetricsEvent {
    fn from(_: &RecordMetricsEvent) -> Self {
        Self {}
    }
}

impl TryFrom<ProtoRecordMetricsEvent> for RecordMetricsEvent {
    type Error = ProtoError;

    fn try_from(_: ProtoRecordMetricsEvent) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use monad_crypto::NopSignature;
    use monad_multi_sig::MultiSig;
    use monad_types::{Deserializable, Serializable};
    use reth_primitives::hex_literal::hex;

    use super::*;

    type MessageSignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;

    #[test]
    fn test_mempool_event_roundtrip() {
        // https://etherscan.io/tx/0xc97438c9ac71f94040abec76967bcaf16445ff747bcdeb383e5b94033cbed201
        let tx = hex!("02f871018302877a8085070adf56b2825208948880bb98e7747f73b52a9cfa34dab9a4a06afa3887eecbb1ada2fad280c080a0d5e6f03b507cc86b59bed88c201f98c9ca6514dc5825f41aa923769cf0402839a0563f21850c0c212ce6f402f140acdcebbb541c9bb6a051070851efec99e4dd8d").as_slice().into();

        let mempool_event =
            MonadEvent::<MessageSignatureType, SignatureCollectionType>::MempoolEvent(
                MempoolEvent::UserTxns(tx),
            );

        let mempool_event_bytes: Bytes = mempool_event.serialize();
        assert_eq!(
            mempool_event_bytes,
            <MonadEvent::<MessageSignatureType, SignatureCollectionType> as Serializable<Bytes>>::serialize(&MonadEvent::<MessageSignatureType,SignatureCollectionType>::deserialize(mempool_event_bytes.as_ref()).expect("deserialization to succeed")
        )
        )
    }
}
