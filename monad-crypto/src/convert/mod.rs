use monad_proto::{error::ProtoError, proto::basic::ProtoPubkey};
use zerocopy::AsBytes;

use crate::PubKey;

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
            .map_err(|e| ProtoError::CryptoError(format!("{}", e)))
    }
}
