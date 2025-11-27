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

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    signing_domain,
};
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;
use serde::{Deserialize, Serialize};

use crate::{block::ConsensusBlockHeader, no_endorsement::FreshProposalCertificate};

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ConsensusTip<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub block_header: ConsensusBlockHeader<ST, SCT, EPT>,
    /// block_header.block_round leader signature over block_header
    signature: ST,

    pub fresh_certificate: Option<FreshProposalCertificate<SCT>>,
}

impl<ST, SCT, EPT> ConsensusTip<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        keypair: &ST::KeyPairType,
        block_header: ConsensusBlockHeader<ST, SCT, EPT>,
        fresh_certificate: Option<FreshProposalCertificate<SCT>>,
    ) -> Self {
        let rlp_block_header = alloy_rlp::encode(&block_header);
        let signature = ST::sign::<signing_domain::Tip>(&rlp_block_header, keypair);
        Self {
            block_header,
            signature,
            fresh_certificate,
        }
    }

    pub fn signature_author(&self) -> Result<CertificateSignaturePubKey<ST>, ST::Error> {
        let rlp_block_header = alloy_rlp::encode(&self.block_header);
        self.signature
            .recover_pubkey::<signing_domain::Tip>(&rlp_block_header)
    }

    pub fn from_raw(
        block_header: ConsensusBlockHeader<ST, SCT, EPT>,
        signature: ST,
        fresh_certificate: Option<FreshProposalCertificate<SCT>>,
    ) -> Self {
        Self {
            block_header,
            signature,
            fresh_certificate,
        }
    }

    pub fn signature(&self) -> &ST {
        &self.signature
    }
}

impl<ST, SCT, EPT> Serialize for ConsensusTip<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = alloy_rlp::encode(self);
        let bytes_hex = hex::encode(&bytes);
        serializer.serialize_str(&bytes_hex)
    }
}

impl<'de, ST, SCT, EPT> Deserialize<'de> for ConsensusTip<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes_hex = String::deserialize(deserializer)?;
        let bytes = hex::decode(&bytes_hex).map_err(|err| {
            serde::de::Error::custom(format!("tip hex decoding error: {:?}", err))
        })?;
        let tip = alloy_rlp::decode_exact(&bytes).map_err(|err| {
            serde::de::Error::custom(format!("tip rlp decoding error: {:?}", err))
        })?;
        Ok(tip)
    }
}
