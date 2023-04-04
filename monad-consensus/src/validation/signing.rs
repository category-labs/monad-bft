use crate::types::signature::ConsensusSignature;
use crate::*;

pub trait Signable {
    type Output;

    fn signed_object(self, author: NodeId, signature: ConsensusSignature) -> Self::Output;
}

#[derive(Clone, Debug)]
pub struct Signed<M: Signable> {
    pub obj: M,
    pub author: NodeId,
    pub author_signature: ConsensusSignature,
}

pub trait ConsensusSigned {}
impl<M> ConsensusSigned for Signed<M> where M: Signable {}

pub struct Verified<S: ConsensusSigned> {
    pub obj: S,
}
