use crate::types::quorum_certificate::QuorumCertificate;
use crate::types::voting::VotingQuorum;
use crate::*;

#[derive(Clone, Debug, Default)]
pub struct TransactionList(Vec<u8>);

pub struct Block<T>
where
    T: VotingQuorum,
{
    pub author: NodeId,
    pub round: Round,
    pub payload: TransactionList,
    pub qc: QuorumCertificate<T>,
    id: Hash,
}

use sha2::Digest;

impl<T: VotingQuorum> Block<T> {
    pub fn new(
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
            id: Default::default(),
        };
        b.hash();
        b
    }

    pub fn get_id(&self) -> Hash {
        self.id
    }

    fn hash(&mut self) {
        let mut hasher = sha2::Sha256::new();
        hasher.update(self.author);
        hasher.update(self.round);
        hasher.update(&self.payload.0);
        hasher.update(self.qc.info.vote.id.0);
        hasher.update(self.qc.signatures.get_hash());

        self.id = hasher.finalize().into();
    }
}
