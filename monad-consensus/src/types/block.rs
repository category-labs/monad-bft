use sha2::Digest;

use crate::types::quorum_certificate::QuorumCertificate;
use crate::types::signature::SignatureCollection;
use crate::validation::hashing::Hashable;
use crate::*;

#[derive(Clone, Debug, Default)]
pub struct TransactionList(pub Vec<u8>);

#[derive(Clone, Default, Debug)]
pub struct Block<T>
where
    T: SignatureCollection,
{
    pub author: NodeId,
    pub round: Round,
    pub payload: TransactionList,
    pub qc: QuorumCertificate<T>,
    id: BlockId,
}

pub struct BlockIter<'a, T>
where
    T: SignatureCollection,
{
    pub b: &'a Block<T>,
    pub index: usize,
}

impl<'a, T> Iterator for BlockIter<'a, T>
where
    T: SignatureCollection,
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
    T: SignatureCollection,
{
    type DataIter = BlockIter<'a, T>;

    fn msg_parts(&self) -> Self::DataIter {
        BlockIter { b: self, index: 0 }
    }
}

impl<T: SignatureCollection> Block<T> {
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
        let mut hasher = sha2::Sha256::new();
        for m in (&b).msg_parts() {
            hasher.update(m);
        }
        b.id = BlockId(hasher.finalize().into());

        b
    }

    pub fn get_id(&self) -> BlockId {
        self.id
    }
}
