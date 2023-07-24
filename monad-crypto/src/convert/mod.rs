use monad_proto::{
    error::ProtoError,
    proto::{basic::ProtoPubkey, signing::ProtoSignature},
};
use zerocopy::AsBytes;

use crate::{PubKey, Signature};

impl From<&PubKey> for ProtoPubkey {
    fn from(value: &PubKey) -> Self {
        Self {
            pubkey: value.bytes(),
        }
    }
}

impl TryFrom<ProtoPubkey> for PubKey {
    type Error = ProtoError;

    fn try_from(value: ProtoPubkey) -> Result<Self, Self::Error> {
        PubKey::from_slice(value.pubkey.as_bytes())
            .map_err(|e| ProtoError::Secp256k1Error(format!("{}", e)))
    }
}

pub fn signature_to_proto(signature: &impl Signature) -> ProtoSignature {
    ProtoSignature {
        sig: signature.serialize(),
    }
}

pub fn proto_to_signature<S: Signature>(proto: ProtoSignature) -> Result<S, ProtoError> {
    S::deserialize(&proto.sig).map_err(|e| ProtoError::Secp256k1Error(format!("{}", e)))
}
