use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{ExecutionProtocol, Round};
use serde::{Deserialize, Serialize};

use crate::{
    quorum_certificate::QuorumCertificate, signature_collection::SignatureCollection,
    timeout::TimeoutCertificate,
};

pub mod block;
pub mod block_validator;
pub mod checkpoint;
pub mod metrics;
pub mod no_endorsement;
pub mod payload;
pub mod quorum_certificate;
pub mod signature_collection;
pub mod timeout;
pub mod tip;
pub mod validation;
pub mod validator_data;
pub mod voting;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = ""))]
pub enum RoundCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Qc(QuorumCertificate<SCT>),
    Tc(TimeoutCertificate<ST, SCT, EPT>),
}

impl<ST, SCT, EPT> RoundCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn round(&self) -> Round {
        match &self {
            Self::Qc(qc) => qc.info.round,
            Self::Tc(tc) => tc.round,
        }
    }

    pub fn tc(&self) -> Option<&TimeoutCertificate<ST, SCT, EPT>> {
        match &self {
            Self::Qc(_) => None,
            Self::Tc(tc) => Some(tc),
        }
    }

    pub fn qc(&self) -> &QuorumCertificate<SCT> {
        match &self {
            Self::Qc(qc) => qc,
            Self::Tc(tc) => tc.high_extend.qc(),
        }
    }
}
