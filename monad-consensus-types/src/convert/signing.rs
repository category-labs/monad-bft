use monad_crypto::certificate_signature::CertificateSignature;
use monad_proto::{error::ProtoError, proto::signing::*};

use crate::signature_collection::{SignatureCollection, SignatureCollectionError};

pub fn signature_collection_to_proto(sc: &impl SignatureCollection) -> ProtoSignatureCollection {
    ProtoSignatureCollection {
        data: sc.serialize().into(),
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

pub fn certificate_signature_to_proto(signature: &impl CertificateSignature) -> ProtoSignature {
    ProtoSignature {
        sig: signature.serialize().into(),
    }
}

pub fn proto_to_certificate_signature<S: CertificateSignature>(
    proto: ProtoSignature,
) -> Result<S, ProtoError> {
    S::deserialize(&proto.sig).map_err(|e| ProtoError::CryptoError(format!("{}", e)))
}
