use alloy_rlp::{Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::BufMut;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_peer_discovery::MonadNameRecord;
use monad_types::{NodeId, Round};

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

const GROUP_MSG_VERSION: u8 = 1;

const MESSAGE_TYPE_PREP_REQ: u8 = 1;
const MESSAGE_TYPE_PREP_RES: u8 = 2;
const MESSAGE_TYPE_CONF_GRP: u8 = 3;

#[derive(Debug, Clone)]
pub enum FullNodesGroupMessage<ST: CertificateSignatureRecoverable> {
    PrepareGroup(PrepareGroup<ST>), // MESSAGE_TYPE_PREP_REQ
    PrepareGroupResponse(PrepareGroupResponse<ST>), // MESSAGE_TYPE_PREP_RES
    ConfirmGroup(ConfirmGroup<ST>), // MESSAGE_TYPE_CONF_GRP
}

impl<ST: CertificateSignatureRecoverable> Encodable for FullNodesGroupMessage<ST> {
    fn length(&self) -> usize {
        size_of_val(&GROUP_MSG_VERSION)
            + size_of_val(&MESSAGE_TYPE_PREP_REQ)
            + match self {
                Self::PrepareGroup(prep_req) => prep_req.length(),
                Self::PrepareGroupResponse(prep_res) => prep_res.length(),
                Self::ConfirmGroup(conf_grp) => conf_grp.length(),
            }
    }

    fn encode(&self, out: &mut dyn BufMut) {
        GROUP_MSG_VERSION.encode(out);
        match self {
            Self::PrepareGroup(prep_req) => {
                MESSAGE_TYPE_PREP_REQ.encode(out);
                prep_req.encode(out);
            }
            Self::PrepareGroupResponse(prep_res) => {
                MESSAGE_TYPE_PREP_RES.encode(out);
                prep_res.encode(out);
            }
            Self::ConfirmGroup(conf_grp) => {
                MESSAGE_TYPE_CONF_GRP.encode(out);
                conf_grp.encode(out);
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Decodable for FullNodesGroupMessage<ST> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let version = u8::decode(&mut payload)?;
        if version > GROUP_MSG_VERSION {
            return Err(alloy_rlp::Error::Custom("Unknown group message version"));
        }
        match u8::decode(&mut payload)? {
            MESSAGE_TYPE_PREP_REQ => Ok(Self::PrepareGroup(PrepareGroup::decode(&mut payload)?)),
            MESSAGE_TYPE_PREP_RES => Ok(Self::PrepareGroupResponse(PrepareGroupResponse::decode(
                &mut payload,
            )?)),
            MESSAGE_TYPE_CONF_GRP => Ok(Self::ConfirmGroup(ConfirmGroup::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "Unknown FullNodesGroupMessage enum variant",
            )),
        }
    }
}
