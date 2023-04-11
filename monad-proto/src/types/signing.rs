use zerocopy::AsBytes;

use monad_consensus::signatures::aggregate_signature::AggregateSignatures;
use monad_crypto::secp256k1::SecpSignature;

use crate::error::ProtoError;

pub(crate) type AggSecpSignature = AggregateSignatures<SecpSignature>;

include!(concat!(env!("OUT_DIR"), "/monad_proto.signing.rs"));

impl From<&SecpSignature> for ProtoSecpSignature {
    fn from(value: &SecpSignature) -> Self {
        ProtoSecpSignature {
            sig: value.serialize().to_vec(),
        }
    }
}

impl TryFrom<ProtoSecpSignature> for SecpSignature {
    type Error = ProtoError;

    fn try_from(value: ProtoSecpSignature) -> Result<Self, Self::Error> {
        Ok(SecpSignature::deserialize(value.sig.as_bytes())?)
    }
}

impl From<&AggSecpSignature> for ProtoAggSig {
    fn from(value: &AggSecpSignature) -> Self {
        Self {
            sigs: value.sigs.iter().map(|v| v.into()).collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<ProtoAggSig> for AggSecpSignature {
    type Error = ProtoError;

    fn try_from(value: ProtoAggSig) -> Result<Self, Self::Error> {
        Ok(Self {
            sigs: value
                .sigs
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}
