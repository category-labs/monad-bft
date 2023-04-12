use zerocopy::AsBytes;

use monad_crypto::secp256k1::PubKey;
use monad_types::NodeId;

use crate::error::ProtoError;

include!(concat!(env!("OUT_DIR"), "/monad_proto.basic.rs"));

impl From<&NodeId> for ProtoNodeId {
    fn from(nodeid: &NodeId) -> Self {
        ProtoNodeId {
            id: nodeid.0.into_bytes(),
        }
    }
}

impl TryFrom<ProtoNodeId> for NodeId {
    type Error = ProtoError;
    fn try_from(value: ProtoNodeId) -> Result<Self, Self::Error> {
        Ok(Self(PubKey::from_slice(value.id.as_bytes())?))
    }
}
