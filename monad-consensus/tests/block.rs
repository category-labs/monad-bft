use monad_consensus_types::{
    block::Block,
    ledger::LedgerCommitInfo,
    payload::{ExecutionArtifacts, Payload, TransactionList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature::SignatureCollection,
    validation::{Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_testutil::signing::{hash, node_id, MockSignatures};
use monad_types::*;

#[test]
fn block_hash_id() {
    let txns = TransactionList(vec![1, 2, 3, 4]);
    let author = node_id();
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new(
        QcInfo {
            vote: VoteInfo {
                id: BlockId(Hash([0x00_u8; 32])),
                parent_id: BlockId(Hash([0x00_u8; 32])),
                round: Round(0),
                parent_round: Round(0),
            },
            ledger_commit: LedgerCommitInfo::default(),
        },
        MockSignatures::new(),
    );

    let block = Block::<MockSignatures>::new::<Sha256Hash>(
        author,
        round,
        &Payload {
            txns,
            header: ExecutionArtifacts::zero(),
        },
        &qc,
    );

    let h1 = Sha256Hash::hash_object(&block);
    let h2: Hash = hash(&block);

    assert_eq!(h1, h2);
}
