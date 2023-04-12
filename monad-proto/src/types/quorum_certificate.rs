use crate::error::ProtoError;
use monad_consensus::types::quorum_certificate::QcInfo;
use prost::Message;

include!(concat!(
    env!("OUT_DIR"),
    "/monad_proto.quorum_certificate.rs"
));

impl From<&QcInfo> for ProtoQcInfo {
    fn from(qcinfo: &QcInfo) -> Self {
        ProtoQcInfo {
            vote: Some((&qcinfo.vote).into()),
            ledger_commit: Some((&qcinfo.ledger_commit).into()),
        }
    }
}
impl TryFrom<ProtoQcInfo> for QcInfo {
    type Error = ProtoError;
    fn try_from(proto_qci: ProtoQcInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: proto_qci
                .vote
                .map(|v| v.try_into())
                .transpose()?
                .ok_or(ProtoError::MissingRequiredField("qcinfo.vote".to_owned()))?,
            ledger_commit: proto_qci
                .ledger_commit
                .map(|v| v.try_into())
                .transpose()?
                .ok_or(ProtoError::MissingRequiredField(
                    "qcinfo.ledger_commit".to_owned(),
                ))?,
        })
    }
}
