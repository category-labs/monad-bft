use std::{net::SocketAddr, str::FromStr};

use bytes::Bytes;
use monad_consensus_types::{
    signature_collection::SignatureCollection, validator_data::ValidatorSetDataWithEpoch,
};
use monad_crypto::certificate_signature::{CertificateSignatureRecoverable, PubKey};
use monad_proto::{
    error::ProtoError,
    proto::{blocksync::ProtoBlockSyncSelfRequest, event::*},
};

use crate::{
    AsyncStateVerifyEvent, BlockSyncEvent, ControlPanelEvent, GetFullNodes, GetPeers, MempoolEvent,
    MonadEvent, StateSyncEvent, StateSyncNetworkMessage, StateSyncRequest, StateSyncResponse,
    StateSyncUpsertType, StateSyncVersion, UpdateFullNodes, UpdatePeers, ValidatorEvent,
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

impl<SCT: SignatureCollection> From<&BlockSyncEvent<SCT>> for ProtoBlockSyncEvent {
    fn from(value: &BlockSyncEvent<SCT>) -> Self {
        let event = match value {
            BlockSyncEvent::Request { sender, request } => {
                proto_block_sync_event::Event::Request(ProtoBlockSyncRequestWithSender {
                    sender: Some(sender.into()),
                    request: Some(request.into()),
                })
            }
            BlockSyncEvent::SelfRequest {
                requester,
                block_range,
            } => proto_block_sync_event::Event::SelfRequest(ProtoBlockSyncSelfRequest {
                requester: requester.into(),
                block_range: Some(block_range.into()),
            }),
            BlockSyncEvent::SelfCancelRequest {
                requester,
                block_range,
            } => proto_block_sync_event::Event::SelfCancelRequest(ProtoBlockSyncSelfRequest {
                requester: requester.into(),
                block_range: Some(block_range.into()),
            }),
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
                        block_range: self_request
                            .block_range
                            .ok_or(ProtoError::MissingRequiredField(
                                "BlockSyncSelfRequest.block_range".to_owned(),
                            ))?
                            .try_into()?,
                    }
                }
                proto_block_sync_event::Event::SelfCancelRequest(self_request) => {
                    BlockSyncEvent::SelfCancelRequest {
                        requester: self_request.requester.try_into()?,
                        block_range: self_request
                            .block_range
                            .ok_or(ProtoError::MissingRequiredField(
                                "BlockSyncCancelRequest.block_range".to_owned(),
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

fn socket_addr_to_proto(socket_addr: &SocketAddr) -> ProtoSockAddr {
    let serialized = socket_addr.to_string();
    ProtoSockAddr { addr: serialized }
}

fn proto_to_socket_addr(proto_socket_addr: ProtoSockAddr) -> Result<SocketAddr, ProtoError> {
    SocketAddr::from_str(&proto_socket_addr.addr)
        .map_err(|e| ProtoError::DeserializeError(format!("{}", e)))
}

impl<SCT> From<&ControlPanelEvent<SCT>> for ProtoControlPanelEvent
where
    SCT: SignatureCollection,
{
    fn from(value: &ControlPanelEvent<SCT>) -> Self {
        match value {
            ControlPanelEvent::GetValidatorSet => ProtoControlPanelEvent {
                event: Some(proto_control_panel_event::Event::GetValidatorSetEvent(
                    ProtoGetValidatorSetEvent {},
                )),
            },
            ControlPanelEvent::GetMetricsEvent => ProtoControlPanelEvent {
                event: Some(proto_control_panel_event::Event::GetMetricsEvent(
                    ProtoGetMetricsEvent {},
                )),
            },
            ControlPanelEvent::ClearMetricsEvent => ProtoControlPanelEvent {
                event: Some(proto_control_panel_event::Event::ClearMetricsEvent(
                    ProtoClearMetricsEvent {},
                )),
            },
            ControlPanelEvent::UpdateValidators(ValidatorSetDataWithEpoch {
                epoch,
                validators,
                ..
            }) => ProtoControlPanelEvent {
                event: Some(proto_control_panel_event::Event::UpdateValidatorsEvent(
                    ProtoUpdateValidatorsEvent {
                        validator_set_data: Some(validators.into()),
                        epoch: Some(epoch.into()),
                    },
                )),
            },
            ControlPanelEvent::UpdateLogFilter(filter) => ProtoControlPanelEvent {
                event: Some(proto_control_panel_event::Event::UpdateLogFilter(
                    ProtoUpdateLogFilter {
                        filter: filter.clone(),
                    },
                )),
            },
            ControlPanelEvent::GetPeers(get_peer_list) => match get_peer_list {
                GetPeers::Request => ProtoControlPanelEvent {
                    event: Some(proto_control_panel_event::Event::GetPeersEvent(
                        ProtoGetPeersEvent {
                            req_resp: Some(proto_get_peers_event::ReqResp::Req(
                                ProtoGetPeersReq {},
                            )),
                        },
                    )),
                },
                GetPeers::Response(vec) => {
                    let records = vec
                        .iter()
                        .map(|(node_id, addr)| ProtoPeerRecord {
                            node_id: Some(node_id.into()),
                            addr: Some(socket_addr_to_proto(addr)),
                        })
                        .collect();
                    ProtoControlPanelEvent {
                        event: Some(proto_control_panel_event::Event::GetPeersEvent(
                            ProtoGetPeersEvent {
                                req_resp: Some(proto_get_peers_event::ReqResp::Resp(
                                    ProtoGetPeersResp { records },
                                )),
                            },
                        )),
                    }
                }
            },
            ControlPanelEvent::UpdatePeers(update_peers) => match update_peers {
                crate::UpdatePeers::Request(vec) => {
                    let records = vec
                        .iter()
                        .map(|(node_id, addr)| ProtoPeerRecord {
                            node_id: Some(node_id.into()),
                            addr: Some(socket_addr_to_proto(addr)),
                        })
                        .collect();
                    ProtoControlPanelEvent {
                        event: Some(proto_control_panel_event::Event::UpdatePeersEvent(
                            ProtoUpdatePeersEvent {
                                req_resp: Some(proto_update_peers_event::ReqResp::Req(
                                    ProtoUpdatePeersReq { records },
                                )),
                            },
                        )),
                    }
                }
                crate::UpdatePeers::Response => ProtoControlPanelEvent {
                    event: Some(proto_control_panel_event::Event::UpdatePeersEvent(
                        ProtoUpdatePeersEvent {
                            req_resp: Some(proto_update_peers_event::ReqResp::Resp(
                                ProtoUpdatePeersResp {},
                            )),
                        },
                    )),
                },
            },
            ControlPanelEvent::GetFullNodes(get_full_nodes) => match get_full_nodes {
                GetFullNodes::Request => ProtoControlPanelEvent {
                    event: Some(proto_control_panel_event::Event::GetFullNodesEvent(
                        ProtoGetFullNodesEvent {
                            req_resp: Some(proto_get_full_nodes_event::ReqResp::Req(
                                ProtoGetFullNodeReq {},
                            )),
                        },
                    )),
                },
                GetFullNodes::Response(vec) => {
                    let node_ids = vec.iter().map(Into::into).collect();
                    ProtoControlPanelEvent {
                        event: Some(proto_control_panel_event::Event::GetFullNodesEvent(
                            ProtoGetFullNodesEvent {
                                req_resp: Some(proto_get_full_nodes_event::ReqResp::Resp(
                                    ProtoGetFullNodeResp { node_ids },
                                )),
                            },
                        )),
                    }
                }
            },
            ControlPanelEvent::UpdateFullNodes(update_full_nodes) => match update_full_nodes {
                UpdateFullNodes::Request(vec) => {
                    let node_ids = vec.iter().map(Into::into).collect();
                    ProtoControlPanelEvent {
                        event: Some(proto_control_panel_event::Event::UpdateFullNodesEvent(
                            ProtoUpdateFullNodesEvent {
                                req_resp: Some(proto_update_full_nodes_event::ReqResp::Req(
                                    ProtoUpdateFullNodesReq { node_ids },
                                )),
                            },
                        )),
                    }
                }
                UpdateFullNodes::Response => ProtoControlPanelEvent {
                    event: Some(proto_control_panel_event::Event::UpdateFullNodesEvent(
                        ProtoUpdateFullNodesEvent {
                            req_resp: Some(proto_update_full_nodes_event::ReqResp::Resp(
                                ProtoUpdateFullNodesResp {},
                            )),
                        },
                    )),
                },
            },
        }
    }
}

impl<SCT> TryFrom<ProtoControlPanelEvent> for ControlPanelEvent<SCT>
where
    SCT: SignatureCollection,
{
    type Error = ProtoError;

    fn try_from(e: ProtoControlPanelEvent) -> Result<Self, Self::Error> {
        Ok({
            let ProtoControlPanelEvent { event } = e;
            match event.ok_or(ProtoError::MissingRequiredField(
                "ControlPanelEvent::event".to_owned(),
            ))? {
                proto_control_panel_event::Event::GetValidatorSetEvent(_) => {
                    ControlPanelEvent::GetValidatorSet
                }
                proto_control_panel_event::Event::GetMetricsEvent(_) => {
                    ControlPanelEvent::GetMetricsEvent
                }
                proto_control_panel_event::Event::ClearMetricsEvent(_) => {
                    ControlPanelEvent::ClearMetricsEvent
                }
                proto_control_panel_event::Event::UpdateValidatorsEvent(v) => {
                    ControlPanelEvent::UpdateValidators(ValidatorSetDataWithEpoch {
                        epoch: v
                            .epoch
                            .ok_or(ProtoError::MissingRequiredField(
                                "ProtoUpdateValidatorsEvent.epoch".to_owned(),
                            ))?
                            .try_into()?,
                        round: None,
                        validators: v
                            .validator_set_data
                            .ok_or(ProtoError::MissingRequiredField(
                                "ProtoUpdateValidatorsEvent.epoch".to_owned(),
                            ))?
                            .try_into()?,
                    })
                }
                proto_control_panel_event::Event::UpdateLogFilter(update_log_filter) => {
                    ControlPanelEvent::UpdateLogFilter(update_log_filter.filter)
                }
                proto_control_panel_event::Event::GetPeersEvent(proto_get_peers_event) => {
                    let req_resp =
                        proto_get_peers_event
                            .req_resp
                            .ok_or(ProtoError::MissingRequiredField(
                                "ControlPanel::GetPeersEvent.req_resp".to_owned(),
                            ))?;
                    match req_resp {
                        proto_get_peers_event::ReqResp::Req(_proto_get_peers_req) => {
                            ControlPanelEvent::GetPeers(GetPeers::Request)
                        }
                        proto_get_peers_event::ReqResp::Resp(proto_get_peers_resp) => {
                            let mut records =
                                Vec::with_capacity(proto_get_peers_resp.records.len());
                            for proto_record in proto_get_peers_resp.records.into_iter() {
                                let record = (
                                    proto_record
                                        .node_id
                                        .ok_or(ProtoError::MissingRequiredField(
                                            "ControlPanel::GetPeersEvent.resp.record.node_id"
                                                .to_owned(),
                                        ))?
                                        .try_into()?,
                                    proto_to_socket_addr(
                                        proto_record.addr.ok_or(
                                            ProtoError::MissingRequiredField(
                                                "ControlPanel::GetPeersEvent.resp.record.addr"
                                                    .to_owned(),
                                            ),
                                        )?,
                                    )?,
                                );
                                records.push(record);
                            }

                            ControlPanelEvent::GetPeers(GetPeers::Response(records))
                        }
                    }
                }
                proto_control_panel_event::Event::UpdatePeersEvent(proto_update_peers_event) => {
                    let req_resp = proto_update_peers_event.req_resp.ok_or(
                        ProtoError::MissingRequiredField(
                            "ControlPanel::UpdatePeersEvent.req_resp".to_owned(),
                        ),
                    )?;
                    match req_resp {
                        proto_update_peers_event::ReqResp::Req(proto_update_peers_req) => {
                            let mut records =
                                Vec::with_capacity(proto_update_peers_req.records.len());
                            for proto_record in proto_update_peers_req.records.into_iter() {
                                let record = (
                                    proto_record
                                        .node_id
                                        .ok_or(ProtoError::MissingRequiredField(
                                            "ControlPanel::GetPeersEvent.resp.record.node_id"
                                                .to_owned(),
                                        ))?
                                        .try_into()?,
                                    proto_to_socket_addr(
                                        proto_record.addr.ok_or(
                                            ProtoError::MissingRequiredField(
                                                "ControlPanel::GetPeersEvent.resp.record.addr"
                                                    .to_owned(),
                                            ),
                                        )?,
                                    )?,
                                );
                                records.push(record);
                            }

                            ControlPanelEvent::UpdatePeers(UpdatePeers::Request(records))
                        }
                        proto_update_peers_event::ReqResp::Resp(_proto_update_peers_resp) => {
                            ControlPanelEvent::UpdatePeers(UpdatePeers::Response)
                        }
                    }
                }
                proto_control_panel_event::Event::GetFullNodesEvent(proto_get_full_nodes_event) => {
                    let req_resp = proto_get_full_nodes_event.req_resp.ok_or(
                        ProtoError::MissingRequiredField(
                            "ControlPanel::GetFullNodesEvent.req_resp".to_owned(),
                        ),
                    )?;
                    match req_resp {
                        proto_get_full_nodes_event::ReqResp::Req(proto_get_full_node_req) => {
                            ControlPanelEvent::GetFullNodes(GetFullNodes::Request)
                        }
                        proto_get_full_nodes_event::ReqResp::Resp(proto_get_full_node_resp) => {
                            let mut node_ids =
                                Vec::with_capacity(proto_get_full_node_resp.node_ids.len());
                            for proto_node_id in proto_get_full_node_resp.node_ids {
                                node_ids.push(proto_node_id.try_into()?);
                            }
                            ControlPanelEvent::GetFullNodes(GetFullNodes::Response(node_ids))
                        }
                    }
                }
                proto_control_panel_event::Event::UpdateFullNodesEvent(
                    proto_update_full_nodes_event,
                ) => {
                    let req_resp = proto_update_full_nodes_event.req_resp.ok_or(
                        ProtoError::MissingRequiredField(
                            "ControlPanel::UpdateFullNodesEvent.req_resp".to_owned(),
                        ),
                    )?;
                    match req_resp {
                        proto_update_full_nodes_event::ReqResp::Req(
                            proto_update_full_nodes_req,
                        ) => {
                            let mut node_ids =
                                Vec::with_capacity(proto_update_full_nodes_req.node_ids.len());
                            for proto_node_id in proto_update_full_nodes_req.node_ids {
                                node_ids.push(proto_node_id.try_into()?);
                            }
                            ControlPanelEvent::UpdateFullNodes(UpdateFullNodes::Request(node_ids))
                        }
                        proto_update_full_nodes_event::ReqResp::Resp(
                            proto_update_full_nodes_resp,
                        ) => ControlPanelEvent::UpdateFullNodes(UpdateFullNodes::Response),
                    }
                }
            }
        })
    }
}

impl From<&StateSyncRequest> for monad_proto::proto::message::ProtoStateSyncRequest {
    fn from(request: &StateSyncRequest) -> Self {
        Self {
            version: StateSyncVersion::to_u32(&request.version),
            prefix: request.prefix,
            prefix_bytes: request.prefix_bytes.into(),
            target: request.target,
            from: request.from,
            until: request.until,
            old_target: request.old_target,
        }
    }
}

impl From<&StateSyncUpsertType> for monad_proto::proto::message::ProtoStateSyncUpsertType {
    fn from(upsert_type: &StateSyncUpsertType) -> Self {
        match upsert_type {
            StateSyncUpsertType::Code => {
                monad_proto::proto::message::ProtoStateSyncUpsertType::Code
            }
            StateSyncUpsertType::Account => {
                monad_proto::proto::message::ProtoStateSyncUpsertType::Account
            }
            StateSyncUpsertType::Storage => {
                monad_proto::proto::message::ProtoStateSyncUpsertType::Storage
            }
            StateSyncUpsertType::AccountDelete => {
                monad_proto::proto::message::ProtoStateSyncUpsertType::AccountDelete
            }
            StateSyncUpsertType::StorageDelete => {
                monad_proto::proto::message::ProtoStateSyncUpsertType::StorageDelete
            }
        }
    }
}

impl From<&StateSyncResponse> for monad_proto::proto::message::ProtoStateSyncResponse {
    fn from(response: &StateSyncResponse) -> Self {
        Self {
            version: StateSyncVersion::to_u32(&response.version),
            nonce: response.nonce,
            response_index: response.response_index,
            request: Some((&response.request).into()),
            upserts: response
                .response
                .iter()
                .map(
                    |(upsert_type, data)| monad_proto::proto::message::ProtoStateSyncUpsert {
                        r#type: monad_proto::proto::message::ProtoStateSyncUpsertType::from(
                            upsert_type,
                        )
                        .into(),
                        data: Bytes::copy_from_slice(data),
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
            StateSyncEvent::BlockSync {
                block_range,
                full_blocks,
            } => Self {
                event: Some(proto_state_sync_event::Event::BlockSync(
                    ProtoBlockSyncFullBlocks {
                        block_range: Some(block_range.into()),
                        full_blocks: full_blocks.iter().map(|b| b.into()).collect::<Vec<_>>(),
                    },
                )),
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
            version: StateSyncVersion::from_u32(request.version),
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

impl From<monad_proto::proto::message::ProtoStateSyncUpsertType> for StateSyncUpsertType {
    fn from(upsert_type: monad_proto::proto::message::ProtoStateSyncUpsertType) -> Self {
        match upsert_type {
            monad_proto::proto::message::ProtoStateSyncUpsertType::Code => {
                StateSyncUpsertType::Code
            }
            monad_proto::proto::message::ProtoStateSyncUpsertType::Account => {
                StateSyncUpsertType::Account
            }
            monad_proto::proto::message::ProtoStateSyncUpsertType::Storage => {
                StateSyncUpsertType::Storage
            }
            monad_proto::proto::message::ProtoStateSyncUpsertType::AccountDelete => {
                StateSyncUpsertType::AccountDelete
            }
            monad_proto::proto::message::ProtoStateSyncUpsertType::StorageDelete => {
                StateSyncUpsertType::StorageDelete
            }
        }
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
                    version: StateSyncVersion::from_u32(response.version),
                    nonce: response.nonce,
                    response_index: response.response_index,
                    request: response
                        .request
                        .ok_or(ProtoError::MissingRequiredField(
                            "StateSyncResponse::request".to_owned(),
                        ))?
                        .try_into()?,
                    response: response
                        .upserts
                        .into_iter()
                        .map(|upsert| {
                            let upsert_type =
                                monad_proto::proto::message::ProtoStateSyncUpsertType::try_from(
                                    upsert.r#type,
                                )
                                .map_err(|_| {
                                    ProtoError::DeserializeError("unknown upsert type".to_owned())
                                })?
                                .into();
                            Ok((upsert_type, Vec::from(upsert.data)))
                        })
                        .collect::<Result<_, ProtoError>>()?,
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
                proto_state_sync_event::Event::BlockSync(blocks) => StateSyncEvent::BlockSync {
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
                },
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
            <MonadEvent::<MessageSignatureType, SignatureCollectionType> as Serializable<Bytes>>::serialize(&MonadEvent::<MessageSignatureType, SignatureCollectionType>::deserialize(mempool_event_bytes.as_ref()).expect("deserialization to succeed")
            )
        )
    }
}
