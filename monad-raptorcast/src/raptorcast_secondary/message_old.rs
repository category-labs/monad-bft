use bytes::{Bytes, BytesMut};
use monad_crypto::certificate_signature::{PubKey, CertificateSignatureRecoverable,
    CertificateSignature, CertificateSignaturePubKey};
use monad_proto::{
    error::ProtoError,
    proto::message::{
        proto_discovery_message::Message, ProtoConfirmGroup, ProtoDiscoveryMessage,
        ProtoDiscoveryRequest, ProtoDiscoveryResponse, ProtoFullNodesGroupMessage,
        ProtoPrepareGroup, ProtoPrepareGroupResponse, ProtoRouterMessage,
    },
};
use monad_types::{Deserializable, NodeId, Round, Serializable};

//use prost::Message as _;

use monad_peer_discovery::MonadNameRecord;

#[derive(Debug, PartialEq, Clone)]
pub struct PrepareGroup<ST: CertificateSignatureRecoverable> {
    pub validator_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub max_group_size: usize,
    pub start_round: Round,
    pub end_round: Round,
}

#[derive(Debug, Clone)]
pub struct PrepareGroupResponse<ST: CertificateSignatureRecoverable> {
    pub req: PrepareGroup<ST>,
    pub node_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub accept: bool,
}

#[derive(Debug, Clone)]
pub struct ConfirmGroup<ST: CertificateSignatureRecoverable> {
    pub prepare: PrepareGroup<ST>,
    pub peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    pub name_records: Option<Vec<MonadNameRecord<ST>>>,
}

#[derive(Debug, Clone)]
pub enum FullNodesGroupMessage<ST: CertificateSignatureRecoverable> {
    PrepareGroup(PrepareGroup<ST>),
    PrepareGroupResponse(PrepareGroupResponse<ST>),
    ConfirmGroup(ConfirmGroup<ST>),
}

// Outbound, Serialization
impl<ST: CertificateSignatureRecoverable> From<&PrepareGroup<ST>> for ProtoPrepareGroup {
    fn from(value: &PrepareGroup<ST>) -> Self {
        let PrepareGroup {
            validator_id,
            max_group_size,
            start_round,
            end_round,
        } = value;
        Self {
            validator_id: Some(validator_id.into()),
            max_group_size: *max_group_size as u64,
            start_round: Some(start_round.into()),
            end_round: Some(end_round.into()),
        }
    }
}

// Inbound, Deserialization
impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoPrepareGroup> for PrepareGroup<ST> {
    type Error = ProtoError;

    fn try_from(value: ProtoPrepareGroup) -> Result<Self, Self::Error> {
        Ok(Self {
            validator_id: value
                .validator_id
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoPrepareGroup.validator_id".to_owned(),
                ))?
                .try_into()?,
            max_group_size: value.max_group_size as usize,
            start_round: value
                .start_round
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoMonadNameRecord.start_round".to_owned(),
                ))?
                .try_into()?,
            end_round: value
                .end_round
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoMonadNameRecord.end_round".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

// Outbound, Serialization
impl<ST: CertificateSignatureRecoverable> From<&PrepareGroupResponse<ST>> for ProtoPrepareGroupResponse {
    fn from(value: &PrepareGroupResponse<ST>) -> Self {
        let PrepareGroupResponse {
            req,
            node_id,
            accept,
        } = value;
        Self {
            req: Some(req.into()),
            node_id: Some(node_id.into()),
            accept: *accept,
        }
    }
}

// Inbound, Deserialization
impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoPrepareGroupResponse> for PrepareGroupResponse<ST> {
    type Error = ProtoError;

    fn try_from(value: ProtoPrepareGroupResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            req: value
                .req
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoPrepareGroupResponse.req".to_owned(),
                ))?
                .try_into()?,
            node_id: value
                .node_id
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoPrepareGroupResponse.node_id".to_owned(),
                ))?
                .try_into()?,
            accept: value.accept,
        })
    }
}

// Outbound, Serialization
impl<ST: CertificateSignatureRecoverable> From<&ConfirmGroup<ST>> for ProtoConfirmGroup {
    fn from(value: &ConfirmGroup<ST>) -> Self {
        let ConfirmGroup {
            prepare,
            peers,
            name_records,
        } = value;
        let node_ids: Vec<_> = peers.iter().map(Into::into).collect();
        let recs: Vec<_> = name_records
            .as_ref()
            .map(|recs| recs.iter().map(Into::into).collect())
            .unwrap_or_default();
        Self {
            prepare: Some(prepare.into()),
            peers: node_ids,
            name_records: recs,
        }
    }
}

// Inbound, Deserialization
impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoConfirmGroup> for ConfirmGroup<ST> {
    type Error = ProtoError;

    fn try_from(value: ProtoConfirmGroup) -> Result<Self, Self::Error> {
        let mut peers = Vec::with_capacity(value.peers.len());
        for proto_node_id in value.peers {
            peers.push(proto_node_id.try_into()?);
        }

        let mut name_records = Vec::with_capacity(value.name_records.len());
        for rec in value.name_records {
            name_records.push(rec.try_into()?);
        }

        Ok(Self {
            prepare: value
                .prepare
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoConfirmGroup.prepare".to_owned(),
                ))?
                .try_into()?,
            peers,
            name_records: Some(name_records),
        })
    }
}

// Outbound, Serialization
impl<ST: CertificateSignatureRecoverable> From<&FullNodesGroupMessage<ST>> for ProtoFullNodesGroupMessage {
    fn from(value: &FullNodesGroupMessage<ST>) -> Self {
        match value {
            FullNodesGroupMessage::PrepareGroup(msg) => ProtoFullNodesGroupMessage {
                message: Some(monad_proto::proto::message::proto_full_nodes_group_message::Message::PrepareGroup(msg.into())),
            },

            FullNodesGroupMessage::PrepareGroupResponse(msg) => ProtoFullNodesGroupMessage {
                message: Some(monad_proto::proto::message::proto_full_nodes_group_message::Message::PrepareGroupResponse(msg.into())),
            },

            FullNodesGroupMessage::ConfirmGroup(msg) => ProtoFullNodesGroupMessage {
                message: Some(monad_proto::proto::message::proto_full_nodes_group_message::Message::ConfirmGroup(msg.into())),
            },
        }
    }
}

// Inbound, Deserialization
impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoFullNodesGroupMessage> for FullNodesGroupMessage<ST> {
    type Error = ProtoError;

    fn try_from(value: ProtoFullNodesGroupMessage) -> Result<Self, Self::Error> {
        match value.message.ok_or(ProtoError::MissingRequiredField(
            "ProtoFullNodesGroupMessage.message".to_owned(),
        ))?
        {
            monad_proto::proto::message::proto_full_nodes_group_message::Message::PrepareGroup(msg)
                => Ok(FullNodesGroupMessage::PrepareGroup(msg.try_into()?)),

            monad_proto::proto::message::proto_full_nodes_group_message::Message::PrepareGroupResponse(msg)
                => Ok(FullNodesGroupMessage::PrepareGroupResponse(msg.try_into()?)),

            monad_proto::proto::message::proto_full_nodes_group_message::Message::ConfirmGroup(msg)
                => Ok(FullNodesGroupMessage::ConfirmGroup(msg.try_into()?)),
        }
    }
}

//pub enum OutboundRouterMessage<'a, OM, ST: CertificateSignatureRecoverable> {
//    Application(&'a OM),
//    FullNodesGroup(FullNodesGroupMessage<ST>),
//}

//impl<'a, OM, PT: PubKey> From<&OutboundRouterMessage<'a, OM, PT>> for ProtoRouterMessage
//where
//    OM: Serializable<Bytes>,
//{
//    fn from(value: &OutboundRouterMessage<'a, OM, PT>) -> Self {
//        match value {
//            OutboundRouterMessage::Application(app_message) => {
//                let serialized_app_message: Bytes = (*app_message).serialize();
//                ProtoRouterMessage {
//                    message: Some(
//                        monad_proto::proto::message::proto_router_message::Message::AppMessage(
//                            serialized_app_message,
//                        ),
//                    ),
//                }
//            }
//
//            OutboundRouterMessage::Discovery(discovery) => ProtoRouterMessage {
//                message: Some(
//                    monad_proto::proto::message::proto_router_message::Message::DiscoveryMessage(
//                        discovery.into(),
//                    ),
//                ),
//            },
//
//            OutboundRouterMessage::FullNodesGroup(fn_grp_msg) => {
//                //panic!("not implemented");
//                ProtoRouterMessage {
//                    message: Some(
//                        monad_proto::proto::message::proto_router_message::Message::FullNodesGroupMessage(
//                            fn_grp_msg.into(),
//                        ),
//                    ),
//                }
//            }
//        }
//    }
//}
//
//impl<OM: Serializable<Bytes>, PT: PubKey> Serializable<Bytes>
//    for OutboundRouterMessage<'_, OM, PT>
//{
//    fn serialize(&self) -> Bytes {
//        let msg: ProtoRouterMessage = self.into();
//
//        let mut buf = BytesMut::new();
//        msg.encode(&mut buf)
//            .expect("message serialization shouldn't fail");
//        buf.into()
//    }
//}

//pub enum InboundRouterMessage<M, PT: PubKey> {
//    Application(M),
//    Discovery(DiscoveryMessage<PT>),
//    FullNodesGroup(FullNodesGroupMessage<PT>),
//}
//
//impl<M: Deserializable<Bytes>, PT: PubKey> TryFrom<ProtoRouterMessage>
//    for InboundRouterMessage<M, PT>
//{
//    type Error = ProtoError;
//
//    fn try_from(value: ProtoRouterMessage) -> Result<Self, Self::Error> {
//        match value.message.ok_or(ProtoError::MissingRequiredField(
//            "ProtoRouterMessage.message".to_owned(),
//        ))? {
//            monad_proto::proto::message::proto_router_message::Message::AppMessage(app_message) => {
//                let app_message = M::deserialize(&app_message).map_err(|_| {
//                    /*
//                        TODO(rene): This map_err is not ideal because it effectively drops a future
//                        error type for an opaque string error. We can remove this map_err and
//                        convert to ProtoError using ?, but then we would have to specify the
//                        ReadError generic associated type in the Deserializable<Bytes> bound on M
//                        like so
//
//                        M: Deserializable<Bytes, ReadError = ProtoError>
//
//                        but then this bound has to propagate upwards to the RaptorCast type, which
//                        is also not ideal.
//                    */
//                    ProtoError::DeserializeError("unknown deserialization error".to_owned())
//                })?;
//                Ok(InboundRouterMessage::Application(app_message))
//            }
//
//            monad_proto::proto::message::proto_router_message::Message::DiscoveryMessage(
//                discovery_message,
//            ) => Ok(InboundRouterMessage::Discovery(
//                discovery_message.try_into()?,
//            )),
//
//            monad_proto::proto::message::proto_router_message::Message::FullNodesGroupMessage(
//                full_nodes_group_message,
//            ) => Ok(InboundRouterMessage::FullNodesGroup(
//                full_nodes_group_message.try_into()?,
//            )),
//        }
//    }
//}

//impl<M: Deserializable<Bytes>, PT: PubKey> Deserializable<Bytes> for InboundRouterMessage<M, PT> {
//    type ReadError = ProtoError;
//
//    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
//        ProtoRouterMessage::decode(message.clone())?.try_into()
//    }
//}
