use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_types::{Epoch, Round};

use crate::{signature_collection::SignatureCollection, timeout::NoTipCertificate};

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
