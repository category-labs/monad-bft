use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::validation::hashing::{Hasher, Sha256Hash};
use monad_consensus::*;
use monad_testutil::signing::{hash, node_id, MockSignatures};

#[test]
fn block_hash_id() {
    let txns = TransactionList(vec![1, 2, 3, 4]);
    let author = node_id();
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new(Default::default(), MockSignatures);

    let block = Block::<MockSignatures>::new::<Sha256Hash>(author, round, &txns, &qc);

    let h1 = Sha256Hash::hash_object(&block);
    let h2: Hash = hash(&block);

    assert_eq!(h1, h2);
}
