use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::voting::VoteInfo;
use monad_consensus::*;
use monad_testutil::signing::MockSignatures;

use monad_consensus::types::quorum_certificate::QcInfo;
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::types::quorum_certificate::Rank;

extern crate monad_testutil;

#[test]
fn comparison() {
    let ci = LedgerCommitInfo::default();

    let mut vi_1 = VoteInfo::default();
    vi_1.round = Round(2);

    let mut vi_2 = VoteInfo::default();
    vi_2.round = Round(3);

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
