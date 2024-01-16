use monad_consensus_types::{
    block::Block,
    ledger::CommitResult,
    payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
    quorum_certificate::{QcInfo, QuorumCertificate},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    hasher::{Hash, Hasher, HasherType},
    NopSignature,
};
use monad_eth_types::EthAddress;
use monad_testutil::signing::{hash, node_id, MockSignatures};
use monad_types::*;

type SignatureType = NopSignature;

#[test]
fn block_hash_id() {
    let txns = FullTransactionList::new(vec![1, 2, 3, 4].into());
    let author = node_id::<SignatureType>();
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        QcInfo {
            vote: Vote {
                vote_info: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_round: Round(0),
                    seq_num: SeqNum(0),
                },
                ledger_commit_info: CommitResult::NoCommit,
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let block = Block::<MockSignatures<SignatureType>>::new(
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
