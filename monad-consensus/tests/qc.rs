use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::voting::VoteInfo;
use monad_testutil::signing::MockSignatures;
use monad_types::*;

use monad_consensus::types::quorum_certificate::QcInfo;
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::types::quorum_certificate::Rank;

extern crate monad_testutil;

#[test]
fn comparison() {
    let ci = LedgerCommitInfo::default();

    let vi_1 = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(2),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
    };

    let vi_2 = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(3),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
    };

    let qc_1 = QuorumCertificate::<monad_testutil::signing::MockSignatures>::new(
        QcInfo {
            vote: vi_1,
            ledger_commit: ci,
        },
        MockSignatures,
    );
    let mut qc_2 = QuorumCertificate::<MockSignatures>::new(
        QcInfo {
            vote: vi_2,
            ledger_commit: ci,
        },
        MockSignatures,
    );

    assert!(Rank(qc_1.info) < Rank(qc_2.info));
    assert!(Rank(qc_2.info) > Rank(qc_1.info));

    qc_2.info.vote.round = Round(2);

    assert_eq!(Rank(qc_1.info), Rank(qc_2.info));
}
