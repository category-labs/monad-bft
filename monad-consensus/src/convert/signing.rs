use monad_crypto::convert::{proto_to_signature, signature_to_proto};
use monad_crypto::Signature;
use monad_proto::error::ProtoError;
use monad_proto::proto::signing::*;

use crate::signatures::aggregate_signature::AggregateSignatures;

impl<S: Signature> From<&AggregateSignatures<S>> for ProtoAggSig {
    fn from(value: &AggregateSignatures<S>) -> Self {
        Self {
            sigs: value
                .sigs
                .iter()
                .map(|v| signature_to_proto(v))
                .collect::<Vec<_>>(),
        }
    }
}

impl<S: Signature> TryFrom<ProtoAggSig> for AggregateSignatures<S> {
    type Error = ProtoError;

    fn try_from(value: ProtoAggSig) -> Result<Self, Self::Error> {
        Ok(Self {
            sigs: value
                .sigs
                .into_iter()
                .map(|v| proto_to_signature(v))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}
