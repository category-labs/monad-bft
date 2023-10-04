use monad_consensus_types::{
    block::Block,
    ledger::LedgerCommitInfo,
    payload::{ExecutionArtifacts, Payload, TransactionList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    validation::{Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_eth_types::EthAddress;
use monad_testutil::signing::{hash, node_id, MockSignatures};
use monad_types::*;

#[test]
fn block_hash_id() {
    let txns = TransactionList(vec![1, 2, 3, 4]);
    let author = node_id();
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new::<Sha256Hash>(
        QcInfo {
            vote: VoteInfo {
                id: BlockId(Hash([0x00_u8; 32])),
                parent_id: BlockId(Hash([0x00_u8; 32])),
                round: Round(0),
                parent_round: Round(0),
                seq_num: 0,
            },
            ledger_commit: LedgerCommitInfo::default(),
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let block = Block::<MockSignatures>::new::<Sha256Hash>(
        author,
        round,
        &Payload {
            txns,
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
        },
        &qc,
    );

    let h1 = Sha256Hash::hash_object(&block);
    let h2: Hash = hash(&block);

    assert_eq!(h1, h2);
}
