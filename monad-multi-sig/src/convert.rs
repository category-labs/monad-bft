use monad_consensus_types::convert::signing::{
    certificate_signature_to_proto, proto_to_certificate_signature,
};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{error::ProtoError, proto::signing::*};

use crate::MultiSig;

impl<S: CertificateSignatureRecoverable> From<&MultiSig<S>> for ProtoMultiSig {
    fn from(value: &MultiSig<S>) -> Self {
        Self {
            sigs: value
                .sigs
                .iter()
                .map(|v| certificate_signature_to_proto(v))
                .collect::<Vec<_>>(),
        }
    }
}

impl<S: CertificateSignatureRecoverable> TryFrom<ProtoMultiSig> for MultiSig<S> {
    type Error = ProtoError;

    fn try_from(value: ProtoMultiSig) -> Result<Self, Self::Error> {
        Ok(Self {
            sigs: value
                .sigs
                .into_iter()
                .map(|v| proto_to_certificate_signature(v))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}
