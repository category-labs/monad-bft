use crate::error::ProtoError;
use monad_consensus::types::quorum_certificate::QcInfo;

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
                .ok_or(Self::Error::MissingRequiredField("qcinfo.vote".to_owned()))?
                .try_into()?,
            ledger_commit: proto_qci
                .ledger_commit
                .ok_or(Self::Error::MissingRequiredField(
                    "qcinfo.ledger_commit".to_owned(),
                ))?
                .try_into()?,
        })
    }
}
