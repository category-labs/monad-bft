use monad_crypto::{
    convert::{proto_to_signature, signature_to_proto},
    Signature,
};
use monad_proto::{error::ProtoError, proto::signing::*};

use crate::multi_sig::MultiSig;

impl<S: Signature> From<&MultiSig<S>> for ProtoMultiSig {
    fn from(value: &MultiSig<S>) -> Self {
        Self {
            sigs: value
                .sigs
                .iter()
                .map(|v| signature_to_proto(v))
                .collect::<Vec<_>>(),
        }
    }
}

impl<S: Signature> TryFrom<ProtoMultiSig> for MultiSig<S> {
    type Error = ProtoError;

    fn try_from(value: ProtoMultiSig) -> Result<Self, Self::Error> {
        Ok(Self {
            sigs: value
                .sigs
                .into_iter()
                .map(|v| proto_to_signature(v))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}
