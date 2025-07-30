use alloy_rlp::{Decodable, Encodable};
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

impl<ST, SCT, EPT> Encodable for RoundCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match &self {
            Self::Qc(qc) => {
                let enc: [&dyn Encodable; 2] = [&1u8, qc];
                alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Tc(tc) => {
                let enc: [&dyn Encodable; 2] = [&2u8, tc];
                alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for RoundCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let qc = QuorumCertificate::decode(&mut payload)?;
                Ok(Self::Qc(qc))
            }
            2 => {
                let tc = TimeoutCertificate::decode(&mut payload)?;
                Ok(Self::Tc(tc))
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown RoundCertificate",
            )),
        }
    }
}
