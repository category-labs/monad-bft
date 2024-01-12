use monad_proto::{error::ProtoError, proto::basic::ProtoHash};

use crate::hasher::Hash;

impl From<&Hash> for ProtoHash {
    fn from(value: &Hash) -> Self {
        Self {
            hash: value.0.to_vec().into(),
        }
    }
}

impl TryFrom<ProtoHash> for Hash {
    type Error = ProtoError;
    fn try_from(value: ProtoHash) -> Result<Self, Self::Error> {
        Ok(Self(value.hash.to_vec().try_into().map_err(
            |e: Vec<_>| Self::Error::WrongHashLen(format!("{}", e.len())),
        )?))
    }
}
