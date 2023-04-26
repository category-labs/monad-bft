use zerocopy::AsBytes;

use monad_types::{BlockId, Hash as HashType, NodeId, Round};

use crate::types::quorum_certificate::QuorumCertificate;
use crate::types::signature::SignatureCollection;
use crate::validation::hashing::{Hashable, Hasher};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactionList(pub Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block<T> {
    pub author: NodeId,
    pub round: Round,
    pub payload: TransactionList,
    pub qc: QuorumCertificate<T>,
    id: BlockId,
}

impl<T: SignatureCollection> Hashable for Block<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(&self.author.0.bytes());
        state.update(self.round.as_bytes());
        state.update(self.payload.0.as_bytes());
        state.update(self.qc.info.vote.id.0.as_bytes());
        state.update(self.qc.get_hash().as_bytes());
    }
}

impl<T: SignatureCollection> Block<T> {
    pub fn new<H: Hasher>(
        author: NodeId,
        round: Round,
        txns: &TransactionList,
        qc: &QuorumCertificate<T>,
    ) -> Self {
        let mut b = Block {
            author,
            round,
            payload: txns.clone(),
            qc: qc.clone(),
            id: BlockId(HashType([0x00_u8; 32])),
        };
        // FIXME make this less jank
        b.id = BlockId(H::hash_object(&b));
        b
    }

    pub fn get_id(&self) -> BlockId {
        self.id
    }
}
