use zerocopy::AsBytes;

use monad_crypto::secp256k1::SecpSignature;

use crate::error::ProtoError;

include!(concat!(env!("OUT_DIR"), "/monad_proto.signing.rs"));

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
        Ok(SecpSignature::deserialize(value.sig.as_bytes())?)
    }
}
