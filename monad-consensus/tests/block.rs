use monad_consensus_types::{
    block::Block,
    ledger::LedgerCommitInfo,
    payload::{ExecutionArtifacts, Payload, RandaoReveal, TransactionHashList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    voting::{Vote, VoteInfo},
};
use monad_crypto::hasher::{Hash, Hasher, HasherType};
use monad_eth_types::EthAddress;
use monad_testutil::signing::{hash, node_id, MockSignatures};
use monad_types::*;

#[test]
fn block_hash_id() {
    let txns = TransactionHashList::new(vec![1, 2, 3, 4]);
    let author = node_id();
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new::<HasherType>(
        QcInfo {
            vote: Vote {
                vote_info: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_round: Round(0),
                    seq_num: SeqNum(0),
                },
                ledger_commit_info: LedgerCommitInfo::empty(),
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let block = Block::<MockSignatures>::new::<HasherType>(
        author,
        round,
        &Payload {
            txns,
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        },
        &qc,
    );

    let h1 = HasherType::hash_object(&block);
    let h2: Hash = hash(&block);

    assert_eq!(h1, h2);
}
