use monad_types::NodeId;

use crate::types::signature::ConsensusSignature;

pub trait Signable {
    type Output;

    fn signed_object(self, author: NodeId, signature: ConsensusSignature) -> Self::Output;
}

#[derive(Clone, Debug)]
pub struct Signed<M: Signable, const VERIFIED: bool> {
    pub obj: M,
    pub author: NodeId,
    pub author_signature: ConsensusSignature,
}

#[derive(Clone, Debug)]
pub struct Verified<M: Signable>(pub Signed<M, true>);

#[derive(Clone, Debug)]
pub struct Unverified<M: Signable>(pub Signed<M, false>);
