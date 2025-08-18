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

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_types::{Epoch, Round};
use monad_validator::signature_collection::SignatureCollection;

use crate::timeout::NoTipCertificate;

#[derive(PartialEq, Eq, Clone, Debug, RlpEncodable, RlpDecodable)]
pub struct NoEndorsement {
    /// The epoch this message was generated in
    pub epoch: Epoch,

    /// The round this message was generated
    pub round: Round,

    pub tip_qc_round: Round,
}

#[derive(PartialEq, Eq, Clone, Debug, RlpEncodable, RlpDecodable)]
pub struct NoEndorsementCertificate<SCT: SignatureCollection> {
    pub msg: NoEndorsement,

    pub signatures: SCT,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum FreshProposalCertificate<SCT: SignatureCollection> {
    Nec(NoEndorsementCertificate<SCT>),
    NoTip(NoTipCertificate<SCT>),
}

impl<SCT> Encodable for FreshProposalCertificate<SCT>
where
    SCT: SignatureCollection,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match &self {
            Self::Nec(nec) => {
                let enc: [&dyn Encodable; 2] = [&1u8, nec];
                alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NoTip(no_tip) => {
                let enc: [&dyn Encodable; 2] = [&2u8, no_tip];
                alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<SCT> Decodable for FreshProposalCertificate<SCT>
where
    SCT: SignatureCollection,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let nec = NoEndorsementCertificate::decode(&mut payload)?;
                Ok(Self::Nec(nec))
            }
            2 => {
                let no_tip = NoTipCertificate::decode(&mut payload)?;
                Ok(Self::NoTip(no_tip))
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown FreshProposalCertificate",
            )),
        }
    }
}
