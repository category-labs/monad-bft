// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use alloy_rlp::{Decodable, Encodable};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{ExecutionProtocol, Round};
use monad_validator::signature_collection::SignatureCollection;
use serde::{Deserialize, Serialize};

use crate::{quorum_certificate::QuorumCertificate, timeout::TimeoutCertificate};

pub mod block;
pub mod block_validator;
pub mod checkpoint;
pub mod metrics;
pub mod no_endorsement;
pub mod payload;
pub mod quorum_certificate;
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
        let result = match u8::decode(&mut payload)? {
            1 => Self::Qc(QuorumCertificate::decode(&mut payload)?),
            2 => Self::Tc(TimeoutCertificate::decode(&mut payload)?),
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown RoundCertificate",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}
