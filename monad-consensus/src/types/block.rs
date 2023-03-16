use crate::types::quorum_certificate::QuorumCertificate;
use crate::types::voting::VotingQuorum;
use crate::validation::signing::Hashable;
use crate::*;

#[derive(Clone, Debug, Default)]
pub struct TransactionList(pub Vec<u8>);

#[derive(Clone, Debug)]
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

pub struct BlockIter<'a, T>
where
    T: VotingQuorum,
{
    pub b: &'a Block<T>,
    pub index: usize,
}

impl<'a, T> Iterator for BlockIter<'a, T>
where
    T: VotingQuorum,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
            0 => Some(self.b.author.as_bytes()),
            1 => Some(self.b.round.as_bytes()),
            2 => Some(self.b.payload.0.as_bytes()),
            3 => Some(self.b.qc.info.vote.id.0.as_bytes()),
            4 => Some(self.b.qc.signature_hash.as_bytes()),
            _ => None,
        };

        self.index += 1;
        result
    }
}

impl<'a, T> Hashable<'a> for &'a Block<T>
where
    T: VotingQuorum,
{
    type DataIter = BlockIter<'a, T>;

    fn msg_parts(&self) -> Self::DataIter {
        BlockIter { b: self, index: 0 }
    }
}

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
        // TODO: new() can take in a Hasher and populate id
        b
    }

    pub fn get_id(&self) -> Hash {
        self.id
    }
}

#[cfg(test)]
pub mod tests {
    use crate::types::signature::ConsensusSignature;
    use crate::types::voting::VotingQuorum;
    use crate::*;
    use crate::{types::quorum_certificate::QuorumCertificate, validation::signing::Hashable};

    use super::{Block, TransactionList};
    use sha2::Digest;

    //TODO: use the common lib version of this once it is merged
    #[derive(Clone, Default, Debug)]
    struct MockSignatures();
    impl VotingQuorum for MockSignatures {
        fn verify_quorum(&self) -> bool {
            true
        }

        fn current_voting_power(&self) -> i64 {
            0
        }

        fn get_hash(&self) -> crate::Hash {
            Default::default()
        }

        fn add_signature(&mut self, _s: ConsensusSignature) {}
    }

    pub fn hash<T: VotingQuorum>(b: &Block<T>) -> Hash {
        let mut hasher = sha2::Sha256::new();
        hasher.update(b.author);
        hasher.update(b.round);
        hasher.update(&b.payload.0);
        hasher.update(b.qc.info.vote.id.0);
        hasher.update(b.qc.signatures.get_hash());

        hasher.finalize().into()
    }

    #[test]
    fn block_hash_id() {
        let txns = TransactionList(vec![1, 2, 3, 4]);
        let author = NodeId(12);
        let round = Round(234);
        let qc = QuorumCertificate::<MockSignatures>::new(Default::default(), MockSignatures());

        let block = Block::<MockSignatures>::new(author, round, &txns, &qc);

        let mut hasher = sha2::Sha256::new();
        for m in (&block).msg_parts() {
            hasher.update(m);
        }

        let h1 = hasher.finalize_reset();
        let h2 = hash(&block);

        assert_eq!(h1, h2.into());
    }
}
