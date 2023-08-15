use monad_proto::{error::ProtoError, proto::signing::*};

use crate::{
    certificate_signature::{CertificateSignature, CertificateSignatureRecoverable},
    message_signature::MessageSignature,
    multi_sig::MultiSig,
};

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

pub fn message_signature_to_proto(signature: &impl MessageSignature) -> ProtoSignature {
    ProtoSignature {
        sig: signature.serialize(),
    }
}

pub fn proto_to_message_signature<S: MessageSignature>(
    proto: ProtoSignature,
) -> Result<S, ProtoError> {
    S::deserialize(&proto.sig).map_err(|e| ProtoError::CryptoError(format!("{}", e)))
}

pub fn certificate_signature_to_proto(signature: &impl CertificateSignature) -> ProtoSignature {
    ProtoSignature {
        sig: signature.serialize(),
    }
}

pub fn proto_to_certificate_signature<S: CertificateSignature>(
    proto: ProtoSignature,
) -> Result<S, ProtoError> {
    S::deserialize(&proto.sig).map_err(|e| ProtoError::CryptoError(format!("{}", e)))
}
