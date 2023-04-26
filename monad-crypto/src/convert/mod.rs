use zerocopy::AsBytes;

use monad_proto::error::ProtoError;
use monad_proto::proto::basic::ProtoPubkey;
use monad_proto::proto::signing::ProtoSecpSignature;

use crate::{secp256k1::SecpSignature, PubKey};

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
        SecpSignature::deserialize(value.sig.as_bytes())
            .map_err(|e| ProtoError::Secp256k1Error(format!("{}", e)))
    }
}
