use monad_types::NodeId;

use crate::types::signature::ConsensusSignature;

#[derive(Clone, Debug)]
pub struct Verified<M> {
    pub obj: M,
    pub author: NodeId,
    pub author_signature: ConsensusSignature,
}

#[derive(Clone, Debug)]
pub struct Unverified<M> {
    pub obj: M,
    pub author_signature: ConsensusSignature,
}
