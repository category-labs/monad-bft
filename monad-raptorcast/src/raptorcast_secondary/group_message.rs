use alloy_rlp::{Decodable, RlpDecodable, RlpEncodable};
use bytes::{Bytes, BytesMut};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_peer_discovery::MonadNameRecord;
use monad_types::{NodeId, Round};

use super::super::rlp::{
    try_deserialize_rlp_message, try_serialize_rlp_message, DeserializeError,
    NetworkMessageVersion, SerializeError,
};

#[derive(RlpEncodable, RlpDecodable, Debug, PartialEq, Clone)]
pub struct PrepareGroup<ST: CertificateSignatureRecoverable> {
    pub validator_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub max_group_size: usize,
    pub start_round: Round,
    pub end_round: Round,
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct PrepareGroupResponse<ST: CertificateSignatureRecoverable> {
    pub req: PrepareGroup<ST>,
    pub node_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub accept: bool,
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ConfirmGroup<ST: CertificateSignatureRecoverable> {
    pub prepare: PrepareGroup<ST>,
    pub peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    pub name_records: Option<Vec<MonadNameRecord<ST>>>,
}

const MESSAGE_TYPE_PREP_REQ: u8 = 1;
const MESSAGE_TYPE_PREP_RES: u8 = 2;
const MESSAGE_TYPE_CONF_GRP: u8 = 3;

#[derive(Debug, Clone)]
pub enum FullNodesGroupMessage<ST: CertificateSignatureRecoverable> {
    PrepareGroup(PrepareGroup<ST>), // MESSAGE_TYPE_PREP_REQ
    PrepareGroupResponse(PrepareGroupResponse<ST>), // MESSAGE_TYPE_PREP_RES
    ConfirmGroup(ConfirmGroup<ST>), // MESSAGE_TYPE_CONF_GRP
}

impl<ST: CertificateSignatureRecoverable> FullNodesGroupMessage<ST> {
    pub fn try_serialize_inner(
        self,
        version: NetworkMessageVersion,
    ) -> Result<Bytes, SerializeError> {
        let mut buf = BytesMut::new();
        match self {
            Self::PrepareGroup(prep_req) => {
                try_serialize_rlp_message(MESSAGE_TYPE_PREP_REQ, prep_req, version, &mut buf)?;
            }
            Self::PrepareGroupResponse(prep_res) => {
                try_serialize_rlp_message(MESSAGE_TYPE_PREP_RES, prep_res, version, &mut buf)?;
            }
            Self::ConfirmGroup(conf_grp) => {
                try_serialize_rlp_message(MESSAGE_TYPE_CONF_GRP, conf_grp, version, &mut buf)?;
            }
        };
        Ok(buf.into())
    }

    pub fn try_deserialize_inner(
        version: NetworkMessageVersion,
        payload: &mut &[u8],
    ) -> Result<Self, DeserializeError> {
        let message_type = u8::decode(payload).map_err(DeserializeError::from)?;
        match message_type {
            MESSAGE_TYPE_PREP_REQ => {
                let obj: PrepareGroup<ST> = try_deserialize_rlp_message(version, payload)?;
                Ok(Self::PrepareGroup(obj))
            }
            MESSAGE_TYPE_PREP_RES => {
                let obj: PrepareGroupResponse<ST> = try_deserialize_rlp_message(version, payload)?;
                Ok(Self::PrepareGroupResponse(obj))
            }
            MESSAGE_TYPE_CONF_GRP => {
                let obj: ConfirmGroup<ST> = try_deserialize_rlp_message(version, payload)?;
                Ok(Self::ConfirmGroup(obj))
            }
            _ => Err(DeserializeError(
                "Unknown FullNodesGroupMessage enum variant".into(),
            )),
        }
    }
}
