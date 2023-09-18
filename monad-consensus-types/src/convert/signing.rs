use monad_crypto::bls12_381::BlsAggregateSignature;
use monad_proto::{error::ProtoError, proto::signing::*};

use crate::{
    bls::BlsSignatureCollection,
    certificate_signature::{CertificateSignature, CertificateSignatureRecoverable},
    message_signature::MessageSignature,
    multi_sig::MultiSig,
    signature_collection::{SignatureCollection, SignatureCollectionError},
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

impl From<&BlsSignatureCollection> for ProtoBlsSignatureCollection {
    fn from(value: &BlsSignatureCollection) -> Self {
        Self {
            signers: serde_cbor::to_vec(&value.signers).expect("serialization success"),
            sig: value.sig.serialize(),
        }
    }
}

impl TryFrom<ProtoBlsSignatureCollection> for BlsSignatureCollection {
    type Error = ProtoError;

    fn try_from(value: ProtoBlsSignatureCollection) -> Result<Self, Self::Error> {
        Ok(Self {
            signers: serde_cbor::from_slice(value.signers.as_slice())
                .map_err(|e| ProtoError::DeserializeError(format!("{}", e)))?,
            sig: BlsAggregateSignature::deserialize(value.sig.as_slice())
                .map_err(|e| ProtoError::CryptoError(format!("{}", e)))?,
        })
    }
}

pub fn signature_collection_to_proto(sc: &impl SignatureCollection) -> ProtoSignatureCollection {
    ProtoSignatureCollection {
        data: sc.serialize(),
    }
}

pub fn proto_to_signature_collection<SCT: SignatureCollection>(
    proto: ProtoSignatureCollection,
) -> Result<SCT, ProtoError> {
    SCT::deserialize(&proto.data).map_err(|e| match e {
        SignatureCollectionError::DeserializeError(e) => ProtoError::DeserializeError(e),
        _ => panic!("invalid err type during deserialization"),
    })
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
