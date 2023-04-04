use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::validation::hashing::Hashable;
use monad_consensus::*;
use monad_testutil::signing::{hash, MockSignatures};

use sha2::Digest;

#[test]
fn block_hash_id() {
    let txns = TransactionList(vec![1, 2, 3, 4]);
    let author = NodeId(12);
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new(Default::default(), MockSignatures);

    let block = Block::<MockSignatures>::new(author, round, &txns, &qc);

    let mut hasher = sha2::Sha256::new();
    for m in (&block).msg_parts() {
        hasher.update(m);
    }

    let h1 = hasher.finalize_reset();
    let h2 = hash(&block);

    assert_eq!(h1, h2.into());
}
