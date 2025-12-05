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
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, Epoch, ExecutionProtocol, Round, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use serde::{Deserialize, Serialize};

use crate::RoundCertificate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootInfo {
    pub round: Round,
    pub seq_num: SeqNum,
    pub epoch: Epoch,
    pub block_id: BlockId,
    pub timestamp_ns: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(serialize = "", deserialize = ""))]
pub struct Checkpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub root: BlockId,
    pub high_certificate: RoundCertificate<ST, SCT, EPT>,

    // TODO can we get rid of this by including an epoch_start_block_id in every block?
    pub validator_sets: Vec<LockedEpoch>,
}

impl<ST, SCT, EPT> Checkpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn try_to_toml_string(&self) -> Result<String, Box<dyn std::error::Error>> {
        let toml_str = toml::to_string_pretty(self)?;
        Ok(toml_str)
    }

    pub fn to_rlp_bytes(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn try_parse_bytes(path: &str, bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if path.ends_with(".toml") {
            let toml_str = std::str::from_utf8(bytes)?;
            let checkpoint: Checkpoint<ST, SCT, EPT> = toml::from_str(toml_str)?;
            Ok(checkpoint)
        } else {
            let rlp_bytes = bytes;
            let checkpoint: Checkpoint<ST, SCT, EPT> = alloy_rlp::decode_exact(rlp_bytes)?;
            Ok(checkpoint)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
pub struct LockedEpoch {
    /// Validator set are active for this epoch
    pub epoch: Epoch,
    /// By the end of epoch - 1, the next epoch is scheduled to start on round.
    pub round: Round,
}
