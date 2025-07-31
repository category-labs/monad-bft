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
