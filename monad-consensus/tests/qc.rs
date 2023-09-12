use monad_consensus_types::{
    ledger::LedgerCommitInfo,
    quorum_certificate::{QcInfo, QuorumCertificate, Rank},
    validation::Sha256Hash,
    voting::VoteInfo,
};
use monad_testutil::signing::MockSignatures;
use monad_types::*;

extern crate monad_testutil;

#[test]
fn comparison() {
    let ci = LedgerCommitInfo::default();

    let vi_1 = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(2),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
        seq_num: 0,
    };

    let vi_2 = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(3),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
        seq_num: 0,
    };

    let qc_1 = QuorumCertificate::<MockSignatures>::new::<Sha256Hash>(
        QcInfo {
            vote: vi_1,
            ledger_commit: ci,
        },
        MockSignatures::with_pubkeys(&[]),
    );
    let mut qc_2 = QuorumCertificate::<MockSignatures>::new::<Sha256Hash>(
        QcInfo {
            vote: vi_2,
            ledger_commit: ci,
        },
        MockSignatures::with_pubkeys(&[]),
    );

    assert!(Rank(qc_1.info) < Rank(qc_2.info));
    assert!(Rank(qc_2.info) > Rank(qc_1.info));

    qc_2.info.vote.round = Round(2);

    assert_eq!(Rank(qc_1.info), Rank(qc_2.info));
}
