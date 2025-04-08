use std::{net::SocketAddr, str::FromStr};

use alloy_rlp::{Decodable, Encodable};
use bytes::BytesMut;
use monad_consensus_types::{
    convert::signing::{certificate_signature_to_proto, proto_to_certificate_signature},
    payload::RoundSignature,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{
    error::ProtoError,
    proto::{blocksync::ProtoBlockSyncSelfRequest, event::*},
};
use monad_types::ExecutionProtocol;

use crate::{
    BlockSyncEvent, ConfigEvent, ConfigUpdate, ControlPanelEvent, GetFullNodes, GetPeers,
    KnownPeersUpdate, MempoolEvent, MonadEvent, ReloadConfig, StateSyncBadVersion, StateSyncEvent,
    StateSyncNetworkMessage, StateSyncRequest, StateSyncResponse, StateSyncUpsertType,
    StateSyncUpsertV1, StateSyncVersion, ValidatorEvent,
};

impl<ST, SCT, EPT> From<&MonadEvent<ST, SCT, EPT>> for ProtoMonadEvent
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &MonadEvent<ST, SCT, EPT>) -> Self {
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
            MonadEvent::ExecutionResultEvent(event) => {
                proto_monad_event::Event::ExecutionResultEvent(event.into())
            }
            MonadEvent::ControlPanelEvent(event) => {
                proto_monad_event::Event::ControlPanelEvent(event.into())
            }
            MonadEvent::TimestampUpdateEvent(event) => {
                proto_monad_event::Event::TimestampUpdateEvent(ProtoTimestampUpdate {
                    update: (*event) as u64, // TODO: this is wrong but protobuf is not used in
                                             // protocol and will be deleted
                })
            }
            MonadEvent::StateSyncEvent(event) => {
                proto_monad_event::Event::StateSyncEvent(event.into())
            }
            MonadEvent::ConfigEvent(event) => proto_monad_event::Event::ConfigEvent(event.into()),
        };
        Self { event: Some(event) }
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoMonadEvent> for MonadEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;
    fn try_from(value: ProtoMonadEvent) -> Result<Self, Self::Error> {
        let event: MonadEvent<ST, SCT, EPT> = match value.event {
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
            Some(proto_monad_event::Event::ExecutionResultEvent(event)) => {
                MonadEvent::ExecutionResultEvent(event.try_into()?)
            }
            Some(proto_monad_event::Event::ControlPanelEvent(e)) => {
                MonadEvent::ControlPanelEvent(e.try_into()?)
            }
            Some(proto_monad_event::Event::TimestampUpdateEvent(event)) => {
                MonadEvent::TimestampUpdateEvent(event.update as u128)
            }
            Some(proto_monad_event::Event::StateSyncEvent(event)) => {
                MonadEvent::StateSyncEvent(event.try_into()?)
            }
            Some(proto_monad_event::Event::ConfigEvent(event)) => {
                MonadEvent::ConfigEvent(event.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}

impl<ST, SCT, EPT> From<&BlockSyncEvent<ST, SCT, EPT>> for ProtoBlockSyncEvent
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &BlockSyncEvent<ST, SCT, EPT>) -> Self {
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

impl<ST, SCT, EPT> TryFrom<ProtoBlockSyncEvent> for BlockSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
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

impl<SCT: SignatureCollection, EPT: ExecutionProtocol> From<&MempoolEvent<SCT, EPT>>
    for ProtoMempoolEvent
{
    fn from(value: &MempoolEvent<SCT, EPT>) -> Self {
        let event = match value {
            MempoolEvent::Proposal {
                epoch,
                round,
                seq_num,
                high_qc,
                timestamp_ns,
                round_signature,
                delayed_execution_results,
                proposed_execution_inputs,
                last_round_tc,
            } => proto_mempool_event::Event::Proposal(ProtoProposal {
                epoch: Some(epoch.into()),
                round: Some(round.into()),
                seq_num: Some(seq_num.into()),
                high_qc: Some(high_qc.into()),
                timestamp_ns: *timestamp_ns as u64,
                round_signature: Some(certificate_signature_to_proto(&round_signature.0)),
                delayed_execution_results: delayed_execution_results
                    .iter()
                    .map(|result| {
                        let mut buf = BytesMut::new();
                        result.encode(&mut buf);
                        buf.into()
                    })
                    .collect(),
                proposed_execution_inputs: Some(proposed_execution_inputs.into()),
                last_round_tc: last_round_tc.as_ref().map(Into::into),
            }),
            MempoolEvent::ForwardedTxs { sender, txs } => {
                proto_mempool_event::Event::ForwardedTxs(ProtoForwardedTxs {
                    sender: Some(sender.into()),
                    forwarded_tx: Some(monad_proto::proto::message::ProtoForwardedTx {
                        tx: txs.clone(),
                    }),
                })
            }
            MempoolEvent::ForwardTxs(txs) => {
                proto_mempool_event::Event::ForwardTxs(ProtoForwardTxs {
                    txs: txs.to_owned(),
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<SCT: SignatureCollection, EPT: ExecutionProtocol> TryFrom<ProtoMempoolEvent>
    for MempoolEvent<SCT, EPT>
{
    type Error = ProtoError;

    fn try_from(value: ProtoMempoolEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_mempool_event::Event::Proposal(ProtoProposal {
                epoch,
                round,
                seq_num,
                high_qc,
                timestamp_ns,
                round_signature,
                delayed_execution_results,
                proposed_execution_inputs,
                last_round_tc,
            })) => MempoolEvent::Proposal {
                epoch: epoch
                    .ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent::Proposal.epoch".to_owned(),
                    ))?
                    .try_into()?,
                round: round
                    .ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent::Proposal.round".to_owned(),
                    ))?
                    .try_into()?,
                seq_num: seq_num
                    .ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent::Proposal.seq_num".to_owned(),
                    ))?
                    .try_into()?,
                high_qc: high_qc
                    .ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent::Proposal.high_qc".to_owned(),
                    ))?
                    .try_into()?,
                timestamp_ns: timestamp_ns as u128,
                round_signature: RoundSignature(proto_to_certificate_signature(
                    round_signature.ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent::Proposal.round_signature".to_owned(),
                    ))?,
                )?),
                delayed_execution_results: delayed_execution_results
                    .into_iter()
                    .map(|delayed_execution_result| {
                        EPT::FinalizedHeader::decode(&mut delayed_execution_result.as_ref())
                            .map_err(|_err| {
                                ProtoError::DeserializeError(
                                    "MempoolEvent::Proposal.delayed_execution_results".to_owned(),
                                )
                            })
                    })
                    .collect::<Result<_, _>>()?,
                proposed_execution_inputs: proposed_execution_inputs
                    .ok_or(ProtoError::MissingRequiredField(
                        "MempoolEvent::Proposal.proposed_execution_inputs".to_owned(),
                    ))?
                    .try_into()?,
                last_round_tc: last_round_tc.map(TryInto::try_into).transpose()?,
            },
            Some(proto_mempool_event::Event::ForwardedTxs(forwarded)) => {
                MempoolEvent::ForwardedTxs {
                    sender: forwarded
                        .sender
                        .ok_or(ProtoError::MissingRequiredField(
                            "MempoolEvent::ForwardedTxns.sender".to_owned(),
                        ))?
                        .try_into()?,
                    txs: forwarded
                        .forwarded_tx
                        .ok_or(ProtoError::MissingRequiredField(
                            "MempoolEvent::ForwardedTxns.forwarded_tx".to_owned(),
                        ))?
                        .tx,
                }
            }
            Some(proto_mempool_event::Event::ForwardTxs(ProtoForwardTxs { txs })) => {
                MempoolEvent::ForwardTxs(txs)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MempoolEvent.event".to_owned(),
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
            ControlPanelEvent::ReloadConfig(reload_config) => match reload_config {
                ReloadConfig::Request => ProtoControlPanelEvent {
                    event: Some(proto_control_panel_event::Event::ReloadConfigEvent(
                        ProtoReloadConfigEvent {
                            req_resp: Some(proto_reload_config_event::ReqResp::Req(
                                ProtoReloadConfigReq {},
                            )),
                        },
                    )),
                },
                ReloadConfig::Response(msg) => ProtoControlPanelEvent {
                    event: Some(proto_control_panel_event::Event::ReloadConfigEvent(
                        ProtoReloadConfigEvent {
                            req_resp: Some(proto_reload_config_event::ReqResp::Resp(
                                ProtoReloadConfigResp { msg: msg.into() },
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
                proto_control_panel_event::Event::GetMetricsEvent(_) => {
                    ControlPanelEvent::GetMetricsEvent
                }
                proto_control_panel_event::Event::ClearMetricsEvent(_) => {
                    ControlPanelEvent::ClearMetricsEvent
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
                proto_control_panel_event::Event::ReloadConfigEvent(proto_reload_config_event) => {
                    let req_resp = proto_reload_config_event.req_resp.ok_or(
                        ProtoError::MissingRequiredField(
                            "ControlPanel::ReloadConfigEvent.req_resp".to_owned(),
                        ),
                    )?;

                    match req_resp {
                        proto_reload_config_event::ReqResp::Req(_proto_reload_config_req) => {
                            ControlPanelEvent::ReloadConfig(ReloadConfig::Request)
                        }
                        proto_reload_config_event::ReqResp::Resp(proto_reload_config_resp) => {
                            ControlPanelEvent::ReloadConfig(ReloadConfig::Response(
                                proto_reload_config_resp.msg,
                            ))
                        }
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
            StateSyncUpsertType::Header => {
                monad_proto::proto::message::ProtoStateSyncUpsertType::Header
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
                .map(|upsert| monad_proto::proto::message::ProtoStateSyncUpsert {
                    r#type: monad_proto::proto::message::ProtoStateSyncUpsertType::from(
                        &upsert.upsert_type,
                    )
                    .into(),
                    data: upsert.data.clone(),
                })
                .collect(),
            n: response.response_n,
        }
    }
}

impl From<&StateSyncBadVersion> for monad_proto::proto::message::ProtoStateSyncBadVersion {
    fn from(value: &StateSyncBadVersion) -> Self {
        Self {
            min_version: StateSyncVersion::to_u32(&value.min_version),
            max_version: StateSyncVersion::to_u32(&value.max_version),
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
            StateSyncNetworkMessage::BadVersion(bad_version) => Self {
                oneof_message: Some(OneofMessage::BadVersion(bad_version.into())),
            },
        }
    }
}

impl<ST, SCT, EPT> From<&StateSyncEvent<ST, SCT, EPT>> for ProtoStateSyncEvent
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &StateSyncEvent<ST, SCT, EPT>) -> Self {
        match value {
            StateSyncEvent::Inbound(from, message) => Self {
                event: Some(proto_state_sync_event::Event::Inbound(
                    ProtoInboundStateMessage {
                        sender: Some(from.into()),
                        message: Some(message.into()),
                    },
                )),
            },
            StateSyncEvent::Outbound(
                to,
                message,
                // a serialized completion doesn't mean anything
                _completion,
            ) => Self {
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
            monad_proto::proto::message::ProtoStateSyncUpsertType::Header => {
                StateSyncUpsertType::Header
            }
        }
    }
}

impl From<monad_proto::proto::message::ProtoStateSyncBadVersion> for StateSyncBadVersion {
    fn from(value: monad_proto::proto::message::ProtoStateSyncBadVersion) -> Self {
        Self {
            min_version: StateSyncVersion::from_u32(value.min_version),
            max_version: StateSyncVersion::from_u32(value.max_version),
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
                            Ok(StateSyncUpsertV1 {
                                upsert_type,
                                data: upsert.data,
                            })
                        })
                        .collect::<Result<_, ProtoError>>()?,
                    response_n: response.n,
                }))
            }
            OneofMessage::BadVersion(bad_version) => {
                Ok(StateSyncNetworkMessage::BadVersion(bad_version.into()))
            }
        }
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoStateSyncEvent> for StateSyncEvent<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
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
                    None, // a deserialized completion doesn't mean anything
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

impl<SCT: SignatureCollection> From<&ConfigEvent<SCT>> for ProtoConfigEvent {
    fn from(value: &ConfigEvent<SCT>) -> Self {
        match value {
            ConfigEvent::ConfigUpdate(config_update) => {
                let full_nodes = config_update.full_nodes.iter().map(Into::into).collect();

                let blocksync_override_peers = config_update
                    .blocksync_override_peers
                    .iter()
                    .map(Into::into)
                    .collect();

                Self {
                    event: Some(proto_config_event::Event::Update(ProtoConfigUpdate {
                        full_nodes,
                        blocksync_override_peers,
                    })),
                }
            }
            ConfigEvent::LoadError(msg) => Self {
                event: Some(proto_config_event::Event::Error(ProtoConfigLoadError {
                    msg: msg.into(),
                })),
            },
            ConfigEvent::KnownPeersUpdate(known_peers_update) => Self {
                event: Some(proto_config_event::Event::KnownPeersUpdate(
                    ProtoKnownPeersUpdate {
                        known_peers: known_peers_update
                            .known_peers
                            .iter()
                            .map(|(node_id, addr)| ProtoPeerRecord {
                                node_id: Some(node_id.into()),
                                addr: Some(socket_addr_to_proto(addr)),
                            })
                            .collect(),
                    },
                )),
            },
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoConfigEvent> for ConfigEvent<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoConfigEvent) -> Result<Self, Self::Error> {
        let event = match value.event.ok_or(ProtoError::MissingRequiredField(
            "ConfigEvent::event".to_owned(),
        ))? {
            proto_config_event::Event::Update(proto_config_update) => {
                let mut full_nodes = Vec::with_capacity(proto_config_update.full_nodes.len());
                for proto_node_id in proto_config_update.full_nodes {
                    full_nodes.push(proto_node_id.try_into()?);
                }

                let mut blocksync_override_peers =
                    Vec::with_capacity(proto_config_update.blocksync_override_peers.len());
                for proto_node_id in proto_config_update.blocksync_override_peers {
                    blocksync_override_peers.push(proto_node_id.try_into()?);
                }

                ConfigEvent::ConfigUpdate(ConfigUpdate {
                    full_nodes,
                    blocksync_override_peers,
                })
            }
            proto_config_event::Event::Error(proto_config_load_error) => {
                ConfigEvent::LoadError(proto_config_load_error.msg)
            }
            proto_config_event::Event::KnownPeersUpdate(proto_known_peers_update) => {
                let mut known_peers =
                    Vec::with_capacity(proto_known_peers_update.known_peers.len());
                for proto_record in proto_known_peers_update.known_peers {
                    let record = (
                        proto_record
                            .node_id
                            .ok_or(ProtoError::MissingRequiredField(
                                "ControlPanel::GetPeersEvent.resp.record.node_id".to_owned(),
                            ))?
                            .try_into()?,
                        proto_to_socket_addr(proto_record.addr.ok_or(
                            ProtoError::MissingRequiredField(
                                "ControlPanel::GetPeersEvent.resp.record.addr".to_owned(),
                            ),
                        )?)?,
                    );
                    known_peers.push(record);
                }
                ConfigEvent::KnownPeersUpdate(KnownPeersUpdate { known_peers })
            }
        };
        Ok(event)
    }
}
