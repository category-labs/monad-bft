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
use monad_testutil::signing::{block_hash, node_id, MockSignatures};
use monad_types::*;

type SignatureType = NopSignature;

#[test]
fn block_hash_id() {
    let txns = FullTransactionList::new(vec![1, 2, 3, 4].into());
    let author = node_id::<SignatureType>();
    let epoch = Epoch(1);
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        QcInfo {
            vote: Vote {
                vote_info: VoteInfo {
                    ..DontCare::dont_care()
                },
                ledger_commit_info: CommitResult::NoCommit,
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );

    let block = Block::<MockSignatures<SignatureType>>::new(
        author,
        0,
        epoch,
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
    let h2: Hash = block_hash(&block);

    assert_eq!(h1, h2);
}
