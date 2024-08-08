use bytes::Bytes;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{CertificateSignatureRecoverable, PubKey};
use monad_proto::{error::ProtoError, proto::event::*};

use crate::{
    AsyncStateVerifyEvent, BlockSyncEvent, BlockSyncSelfRequester, ControlPanelEvent, MempoolEvent,
    MonadEvent, StateSyncEvent, StateSyncNetworkMessage, StateSyncRequest, StateSyncResponse,
    ValidatorEvent,
};

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
            MonadEvent::StateRootEvent(info) => {
                proto_monad_event::Event::StateRootEvent(ProtoStateUpdateEvent {
                    info: Some(info.into()),
                })
            }
            MonadEvent::AsyncStateVerifyEvent(event) => {
                proto_monad_event::Event::AsyncStateVerifyEvent(event.into())
            }
            MonadEvent::ControlPanelEvent(event) => {
                proto_monad_event::Event::ControlPanelEvent(event.into())
            }
            MonadEvent::TimestampUpdateEvent(event) => {
                proto_monad_event::Event::TimestampUpdateEvent(ProtoTimestampUpdate {
                    update: *event,
                })
            }
            MonadEvent::StateSyncEvent(event) => {
                proto_monad_event::Event::StateSyncEvent(event.into())
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
            Some(proto_monad_event::Event::StateRootEvent(event)) => {
                let info = event
                    .info
                    .ok_or(ProtoError::MissingRequiredField(
                        "StateUpdateEvent::info".to_owned(),
                    ))?
                    .try_into()?;

                MonadEvent::StateRootEvent(info)
            }
            Some(proto_monad_event::Event::AsyncStateVerifyEvent(event)) => {
                MonadEvent::AsyncStateVerifyEvent(event.try_into()?)
            }
            Some(proto_monad_event::Event::ControlPanelEvent(e)) => {
                MonadEvent::ControlPanelEvent(e.try_into()?)
            }
            Some(proto_monad_event::Event::TimestampUpdateEvent(event)) => {
                MonadEvent::TimestampUpdateEvent(event.update)
            }
            Some(proto_monad_event::Event::StateSyncEvent(event)) => {
                MonadEvent::StateSyncEvent(event.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}

impl From<&BlockSyncSelfRequester> for i32 {
    fn from(requester: &BlockSyncSelfRequester) -> Self {
        match requester {
            BlockSyncSelfRequester::Consensus => 0,
            BlockSyncSelfRequester::StateSync => 1,
        }
    }
}

impl TryFrom<i32> for BlockSyncSelfRequester {
    type Error = ProtoError;
    fn try_from(requester: i32) -> Result<Self, Self::Error> {
        match requester {
            0 => Ok(BlockSyncSelfRequester::Consensus),
            1 => Ok(BlockSyncSelfRequester::StateSync),
            _ => Err(ProtoError::DeserializeError(
                "unknown blocksync requester".to_owned(),
            )),
        }
    }
}

impl<SCT: SignatureCollection> From<&BlockSyncEvent<SCT>> for ProtoBlockSyncEvent {
    fn from(value: &BlockSyncEvent<SCT>) -> Self {
        let event = match value {
            BlockSyncEvent::Request { sender, request } => {
                proto_block_sync_event::Event::Request(ProtoBlockSyncRequestWithSender {
                    sender: Some(sender.into()),
                    request: Some(request.into()),
                })
            }
            BlockSyncEvent::SelfRequest { requester, request } => {
                proto_block_sync_event::Event::SelfRequest(ProtoBlockSyncSelfRequest {
                    requester: requester.into(),
                    request: Some(request.into()),
                })
            }
            BlockSyncEvent::SelfCancelRequest { requester, request } => {
                proto_block_sync_event::Event::SelfCancelRequest(ProtoBlockSyncSelfRequest {
                    requester: requester.into(),
                    request: Some(request.into()),
                })
            }
            BlockSyncEvent::Response { sender, response } => {
                proto_block_sync_event::Event::Response(ProtoBlockSyncResponseWithSender {
                    sender: Some(sender.into()),
                    response: Some(response.into()),
                })
            }
            BlockSyncEvent::SelfResponse { response } => {
                proto_block_sync_event::Event::SelfResponse(response.into())
            }
            BlockSyncEvent::Timeout(request) => {
                proto_block_sync_event::Event::Timeout(request.into())
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
                proto_block_sync_event::Event::Request(event) => {
                    let sender = event
                        .sender
                        .ok_or(ProtoError::MissingRequiredField(
                            "BlockSyncRequest.sender".to_owned(),
                        ))?
                        .try_into()?;
                    let request = event
                        .request
                        .ok_or(ProtoError::MissingRequiredField(
                            "BlockSyncRequest.request".to_owned(),
                        ))?
                        .try_into()?;
                    BlockSyncEvent::Request { sender, request }
                }
                proto_block_sync_event::Event::SelfRequest(self_request) => {
                    BlockSyncEvent::SelfRequest {
                        requester: self_request.requester.try_into()?,
                        request: self_request
                            .request
                            .ok_or(ProtoError::MissingRequiredField(
                                "BlockSyncSelfRequest.request".to_owned(),
                            ))?
                            .try_into()?,
                    }
                }
                proto_block_sync_event::Event::SelfCancelRequest(self_request) => {
                    BlockSyncEvent::SelfCancelRequest {
                        requester: self_request.requester.try_into()?,
                        request: self_request
                            .request
                            .ok_or(ProtoError::MissingRequiredField(
                                "BlockSyncCancelRequest.request".to_owned(),
                            ))?
                            .try_into()?,
                    }
                }
                proto_block_sync_event::Event::Response(event) => {
                    let sender = event
                        .sender
                        .ok_or(ProtoError::MissingRequiredField(
                            "BlockSyncResponse.sender".to_owned(),
                        ))?
                        .try_into()?;
                    let response = event
                        .response
                        .ok_or(ProtoError::MissingRequiredField(
                            "BlockSyncResponse.response".to_owned(),
                        ))?
                        .try_into()?;
                    BlockSyncEvent::Response { sender, response }
                }
                proto_block_sync_event::Event::SelfResponse(response) => {
                    BlockSyncEvent::SelfResponse {
                        response: response.try_into()?,
                    }
                }
                proto_block_sync_event::Event::Timeout(request) => {
                    BlockSyncEvent::Timeout(request.try_into()?)
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
            ValidatorEvent::UpdateValidators((validator_set_data, epoch)) => {
                proto_validator_event::Event::UpdateValidators(ProtoUpdateValidatorsEvent {
                    validator_set_data: Some(validator_set_data.into()),
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
                    .validator_set_data
                    .ok_or(ProtoError::MissingRequiredField(
                        "ValidatorEvent::update_validators::validator_set_data".to_owned(),
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

impl<PT: PubKey> From<&MempoolEvent<PT>> for ProtoMempoolEvent {
    fn from(value: &MempoolEvent<PT>) -> Self {
        let event = match value {
            MempoolEvent::UserTxns(tx) => {
                proto_mempool_event::Event::Usertx(ProtoUserTx { tx: tx.clone() })
            }
            MempoolEvent::ForwardedTxns { sender, txns } => {
                proto_mempool_event::Event::ForwardedTxs(ProtoForwardedTxs {
                    sender: Some(sender.into()),
                    forwarded_tx: Some(monad_proto::proto::message::ProtoForwardedTx {
                        tx: txns.clone(),
                    }),
                })
            }
            MempoolEvent::Clear => proto_mempool_event::Event::Clear(ProtoClearMempool {}),
        };
        Self { event: Some(event) }
    }
}

impl<PT: PubKey> TryFrom<ProtoMempoolEvent> for MempoolEvent<PT> {
    type Error = ProtoError;

    fn try_from(value: ProtoMempoolEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_mempool_event::Event::Usertx(tx)) => MempoolEvent::UserTxns(tx.tx),
            Some(proto_mempool_event::Event::ForwardedTxs(forwarded)) => {
                MempoolEvent::ForwardedTxns {
                    sender: forwarded
                        .sender
                        .ok_or(ProtoError::MissingRequiredField(
                            "MempoolEvent::ForwardedTxns.sender".to_owned(),
                        ))?
                        .try_into()?,
                    txns: forwarded
                        .forwarded_tx
                        .ok_or(ProtoError::MissingRequiredField(
                            "MempoolEvent::ForwardedTxns.forwarded_tx".to_owned(),
                        ))?
                        .tx,
                }
            }
            Some(proto_mempool_event::Event::Clear(_)) => MempoolEvent::Clear,
            None => Err(ProtoError::MissingRequiredField(
                "MempoolEvent.event".to_owned(),
            ))?,
        };

        Ok(event)
    }
}

impl<SCT: SignatureCollection> From<&AsyncStateVerifyEvent<SCT>> for ProtoAsyncStateVerifyEvent {
    fn from(value: &AsyncStateVerifyEvent<SCT>) -> Self {
        let event = match value {
            AsyncStateVerifyEvent::PeerStateRoot {
                sender,
                unvalidated_message,
            } => proto_async_state_verify_event::Event::PeerStateRoot(
                ProtoPeerStateUpdateWithSender {
                    sender: Some(sender.into()),
                    message: Some(unvalidated_message.into()),
                },
            ),
            AsyncStateVerifyEvent::LocalStateRoot(info) => {
                proto_async_state_verify_event::Event::LocalStateRoot(ProtoStateUpdateEvent {
                    info: Some(info.into()),
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoAsyncStateVerifyEvent> for AsyncStateVerifyEvent<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoAsyncStateVerifyEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_async_state_verify_event::Event::PeerStateRoot(event)) => {
                let sender = event
                    .sender
                    .ok_or(ProtoError::MissingRequiredField(
                        "AsyncStateVerifyEvent.PeerStateRoot.sender".to_owned(),
                    ))?
                    .try_into()?;
                let unvalidated_message = event
                    .message
                    .ok_or(ProtoError::MissingRequiredField(
                        "AsyncStateVerifyEvent.PeerStateRoot.message".to_owned(),
                    ))?
                    .try_into()?;
                AsyncStateVerifyEvent::PeerStateRoot {
                    sender,
                    unvalidated_message,
                }
            }
            Some(proto_async_state_verify_event::Event::LocalStateRoot(event)) => {
                let info = event
                    .info
                    .ok_or(ProtoError::MissingRequiredField(
                        "AsyncStateVerifyEvent.LocalStateRoot.info".to_owned(),
                    ))?
                    .try_into()?;

                AsyncStateVerifyEvent::LocalStateRoot(info)
            }
            None => Err(ProtoError::MissingRequiredField(
                "AsyncStateVerifyEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}

impl From<&ControlPanelEvent> for ProtoControlPanelEvent {
    fn from(value: &ControlPanelEvent) -> Self {
        match value {
            ControlPanelEvent::GetValidatorSet => ProtoControlPanelEvent {
                event: Some(proto_control_panel_event::Event::GetValidatorSetEvent(
                    ProtoGetValidatorSetEvent {},
                )),
            },
            ControlPanelEvent::ClearMetricsEvent => ProtoControlPanelEvent {
                event: Some(proto_control_panel_event::Event::ClearMetricsEvent(
                    ProtoClearMetricsEvent {},
                )),
            },
        }
    }
}

impl TryFrom<ProtoControlPanelEvent> for ControlPanelEvent {
    type Error = ProtoError;

    fn try_from(e: ProtoControlPanelEvent) -> Result<Self, Self::Error> {
        Ok({
            let ProtoControlPanelEvent { event } = e;
            match event.ok_or(ProtoError::MissingRequiredField(
                "ControlPanelEvent::GetValidatorSetEvent".to_owned(),
            ))? {
                proto_control_panel_event::Event::GetValidatorSetEvent(_) => {
                    ControlPanelEvent::GetValidatorSet
                }
                proto_control_panel_event::Event::ClearMetricsEvent(_) => {
                    ControlPanelEvent::ClearMetricsEvent
                }
            }
        })
    }
}

impl From<&StateSyncRequest> for monad_proto::proto::message::ProtoStateSyncRequest {
    fn from(request: &StateSyncRequest) -> Self {
        monad_proto::proto::message::ProtoStateSyncRequest {
            prefix: request.prefix,
            prefix_bytes: request.prefix_bytes.into(),
            target: request.target,
            from: request.from,
            until: request.until,
            old_target: request.old_target,
        }
    }
}

impl From<&StateSyncResponse> for monad_proto::proto::message::ProtoStateSyncResponse {
    fn from(response: &StateSyncResponse) -> Self {
        monad_proto::proto::message::ProtoStateSyncResponse {
            request: Some((&response.request).into()),
            upserts: response
                .response
                .iter()
                .map(
                    |(code, key, value)| monad_proto::proto::message::ProtoStateSyncUpsert {
                        code: *code,
                        key: Bytes::copy_from_slice(key),
                        value: Bytes::copy_from_slice(value),
                    },
                )
                .collect(),
            n: response.response_n,
        }
    }
}

impl From<&StateSyncNetworkMessage> for monad_proto::proto::message::ProtoStateSyncNetworkMessage {
    fn from(value: &StateSyncNetworkMessage) -> Self {
        use monad_proto::proto::message::proto_state_sync_network_message::OneofMessage;
        match value {
            StateSyncNetworkMessage::Request(request) => Self {
                oneof_message: Some(OneofMessage::Request(request.into())),
            },
            StateSyncNetworkMessage::Response(response) => Self {
                oneof_message: Some(OneofMessage::Response(response.into())),
            },
        }
    }
}

impl<SCT: SignatureCollection> From<&StateSyncEvent<SCT>> for ProtoStateSyncEvent {
    fn from(value: &StateSyncEvent<SCT>) -> Self {
        match value {
            StateSyncEvent::Inbound(from, message) => Self {
                event: Some(proto_state_sync_event::Event::Inbound(
                    ProtoInboundStateMessage {
                        sender: Some(from.into()),
                        message: Some(message.into()),
                    },
                )),
            },
            StateSyncEvent::Outbound(to, message) => Self {
                event: Some(proto_state_sync_event::Event::Outbound(
                    ProtoOutboundStateMessage {
                        recipient: Some(to.into()),
                        message: Some(message.into()),
                    },
                )),
            },
            StateSyncEvent::DoneSync(n) => Self {
                event: Some(proto_state_sync_event::Event::DoneSync(ProtoDoneSync {
                    seq_num: Some(n.into()),
                })),
            },
            StateSyncEvent::RequestSync { root, high_qc } => Self {
                event: Some(proto_state_sync_event::Event::RequestSync(
                    ProtoRequestSync {
                        root: Some(root.into()),
                        high_qc: Some(high_qc.into()),
                    },
                )),
            },
            StateSyncEvent::BlockSync(block) => Self {
                event: Some(proto_state_sync_event::Event::BlockSync(block.into())),
            },
        }
    }
}

impl TryFrom<monad_proto::proto::message::ProtoStateSyncRequest> for StateSyncRequest {
    type Error = ProtoError;

    fn try_from(
        request: monad_proto::proto::message::ProtoStateSyncRequest,
    ) -> Result<Self, Self::Error> {
        Ok(StateSyncRequest {
            prefix: request.prefix,
            prefix_bytes: request
                .prefix_bytes
                .try_into()
                .map_err(|_| ProtoError::DeserializeError("prefix_bytes too big".to_owned()))?,
            target: request.target,
            from: request.from,
            until: request.until,
            old_target: request.old_target,
        })
    }
}

impl TryFrom<monad_proto::proto::message::ProtoStateSyncNetworkMessage>
    for StateSyncNetworkMessage
{
    type Error = ProtoError;

    fn try_from(
        value: monad_proto::proto::message::ProtoStateSyncNetworkMessage,
    ) -> Result<Self, Self::Error> {
        use monad_proto::proto::message::proto_state_sync_network_message::OneofMessage;
        match value.oneof_message.ok_or(ProtoError::MissingRequiredField(
            "StateSyncNetworkMessage::oneof_message".to_owned(),
        ))? {
            OneofMessage::Request(request) => {
                Ok(StateSyncNetworkMessage::Request(request.try_into()?))
            }
            OneofMessage::Response(response) => {
                Ok(StateSyncNetworkMessage::Response(StateSyncResponse {
                    request: response
                        .request
                        .ok_or(ProtoError::MissingRequiredField(
                            "StateSyncResponse::request".to_owned(),
                        ))?
                        .try_into()?,
                    response: response
                        .upserts
                        .into_iter()
                        .map(|upsert| (upsert.code, upsert.key.into(), upsert.value.into()))
                        .collect(),
                    response_n: response.n,
                }))
            }
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoStateSyncEvent> for StateSyncEvent<SCT> {
    type Error = ProtoError;

    fn try_from(e: ProtoStateSyncEvent) -> Result<Self, Self::Error> {
        Ok({
            match e.event.ok_or(ProtoError::MissingRequiredField(
                "StateSyncEvent::event".to_owned(),
            ))? {
                proto_state_sync_event::Event::Inbound(inbound) => StateSyncEvent::Inbound(
                    inbound
                        .sender
                        .ok_or(ProtoError::MissingRequiredField(
                            "InboundStateMessage::sender".to_owned(),
                        ))?
                        .try_into()?,
                    inbound
                        .message
                        .ok_or(ProtoError::MissingRequiredField(
                            "InboundStateMessage::message".to_owned(),
                        ))?
                        .try_into()?,
                ),
                proto_state_sync_event::Event::Outbound(outbound) => StateSyncEvent::Outbound(
                    outbound
                        .recipient
                        .ok_or(ProtoError::MissingRequiredField(
                            "InboundStateMessage::recipient".to_owned(),
                        ))?
                        .try_into()?,
                    outbound
                        .message
                        .ok_or(ProtoError::MissingRequiredField(
                            "OutboundStateMessage::message".to_owned(),
                        ))?
                        .try_into()?,
                ),
                proto_state_sync_event::Event::DoneSync(done_sync) => StateSyncEvent::DoneSync(
                    done_sync
                        .seq_num
                        .ok_or(ProtoError::MissingRequiredField(
                            "DoneSync::seq_num".to_owned(),
                        ))?
                        .try_into()?,
                ),
                proto_state_sync_event::Event::RequestSync(request_sync) => {
                    StateSyncEvent::RequestSync {
                        root: request_sync
                            .root
                            .ok_or(ProtoError::MissingRequiredField(
                                "RequestSync::root".to_owned(),
                            ))?
                            .try_into()?,
                        high_qc: request_sync
                            .high_qc
                            .ok_or(ProtoError::MissingRequiredField(
                                "RequestSync::high_qc".to_owned(),
                            ))?
                            .try_into()?,
                    }
                }
                proto_state_sync_event::Event::BlockSync(block) => {
                    StateSyncEvent::BlockSync(block.try_into()?)
                }
            }
        })
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
                MempoolEvent::UserTxns(vec![tx]),
            );

        let mempool_event_bytes: Bytes = mempool_event.serialize();
        assert_eq!(
            mempool_event_bytes,
            <MonadEvent::<MessageSignatureType, SignatureCollectionType> as Serializable<Bytes>>::serialize(&MonadEvent::<MessageSignatureType,SignatureCollectionType>::deserialize(mempool_event_bytes.as_ref()).expect("deserialization to succeed")
        )
        )
    }
}
