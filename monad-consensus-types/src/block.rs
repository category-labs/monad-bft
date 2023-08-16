use monad_types::{BlockId, Hash as HashType, NodeId, Round};
use zerocopy::AsBytes;

use crate::{
    payload::Payload,
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    validation::{Hashable, Hasher},
};

pub trait BlockType: Clone {
    fn get_id(&self) -> BlockId;
    fn get_parent_id(&self) -> BlockId;
}

#[derive(Clone, PartialEq, Eq)]
pub struct Block<T> {
    pub author: NodeId,
    pub round: Round,
    pub payload: Payload,
    pub qc: QuorumCertificate<T>,
    id: BlockId,
}

impl<T> std::fmt::Debug for Block<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("author", &self.author)
            .field("round", &self.round)
            .field("qc_info", &self.qc.info)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<T: SignatureCollection> Hashable for Block<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.author.0.bytes());
        state.update(self.round.as_bytes());
        self.payload.hash(state);
        state.update(self.qc.info.vote.id.0.as_bytes());
        state.update(self.qc.get_hash().as_bytes());
    }
}

impl<T: SignatureCollection> Block<T> {
    pub fn new<H: Hasher>(
        author: NodeId,
        round: Round,
        payload: &Payload,
        qc: &QuorumCertificate<T>,
    ) -> Self {
        let mut b = Block {
            author,
            round,
            payload: payload.clone(),
            qc: qc.clone(),
            id: BlockId(HashType([0x00_u8; 32])),
        };
        // FIXME make this less jank
        b.id = BlockId(H::hash_object(&b));
        b
    }
}

impl<T: SignatureCollection> BlockType for Block<T> {
    fn get_id(&self) -> BlockId {
        self.id
    }

    fn get_parent_id(&self) -> BlockId {
        self.qc.info.vote.id
    }
}
