use monad_types::NodeId;

use crate::types::signature::ConsensusSignature;

#[derive(Clone, Debug)]
pub struct Signed<M, const VERIFIED: bool> {
    pub obj: M,
    pub author: NodeId,
    pub author_signature: ConsensusSignature,
}

#[derive(Clone, Debug)]
pub struct Verified<M>(pub Signed<M, true>);

#[derive(Clone, Debug)]
pub struct Unverified<M>(pub Signed<M, false>);
