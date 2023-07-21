use monad_crypto::{GenericSignature, Signature};
use monad_proto::{error::ProtoError, proto::quorum_certificate::*};

use crate::{
    multi_sig::MultiSig,
    quorum_certificate::{QcInfo, QuorumCertificate as ConsensusQC},
    validation::Hashable,
};

type QuorumCertificate<S> = ConsensusQC<MultiSig<S>>;

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

// TODO: generalize over signature collection
impl<S: Signature + GenericSignature + Hashable> From<&QuorumCertificate<S>>
    for ProtoQuorumCertificateAggSig
{
    fn from(value: &QuorumCertificate<S>) -> Self {
        Self {
            info: Some((&value.info).into()),
            signatures: Some((&value.signatures).into()),
        }
    }
}

impl<S: Signature + GenericSignature + Hashable> TryFrom<ProtoQuorumCertificateAggSig>
    for QuorumCertificate<S>
{
    type Error = ProtoError;

    fn try_from(value: ProtoQuorumCertificateAggSig) -> Result<Self, Self::Error> {
        Ok(QuorumCertificate::new(
            value
                .info
                .ok_or(Self::Error::MissingRequiredField(
                    "QC<AggSig>.info".to_owned(),
                ))?
                .try_into()?,
            value
                .signatures
                .ok_or(Self::Error::MissingRequiredField(
                    "QC<AggSig>.signatures".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}
