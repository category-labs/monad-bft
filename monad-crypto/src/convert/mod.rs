use zerocopy::AsBytes;

use monad_proto::error::ProtoError;
use monad_proto::proto::basic::ProtoPubkey;
use monad_proto::proto::signing::ProtoSignature;

use crate::secp256k1::SecpPubKey;
use crate::Signature;

impl From<&SecpPubKey> for ProtoPubkey {
    fn from(value: &SecpPubKey) -> Self {
        Self {
            pubkey: value.bytes(),
        }
    }
}

impl TryFrom<ProtoPubkey> for SecpPubKey {
    type Error = ProtoError;

    fn try_from(value: ProtoPubkey) -> Result<Self, Self::Error> {
        Self::from_slice(value.pubkey.as_bytes())
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
