use monad_crypto::secp256k1::SecpSignature;
use monad_proto::error::ProtoError;
use monad_proto::proto::signing::*;

use crate::signatures::aggregate_signature::AggregateSignatures;

pub(super) type AggSecpSignature = AggregateSignatures<SecpSignature>;

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
