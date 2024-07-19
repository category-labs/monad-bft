use monad_consensus_types::{
    ledger::CommitResult,
    quorum_certificate::{QcInfo, QuorumCertificate, Rank},
    voting::{Vote, VoteInfo},
};
use monad_crypto::NopSignature;
use monad_testutil::signing::MockSignatures;
use monad_types::*;

extern crate monad_testutil;

#[test]
fn comparison() {
    let ci = CommitResult::NoCommit;

    let vi_1 = VoteInfo {
        round: Round(2),
        ..DontCare::dont_care()
    };

    let vi_2 = VoteInfo {
        round: Round(3),
        ..DontCare::dont_care()
    };

    let qc_1 = QuorumCertificate::<MockSignatures<NopSignature>>::new(
        QcInfo {
            vote: Vote {
                vote_info: vi_1,
                ledger_commit_info: ci,
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );
    let mut qc_2 = QuorumCertificate::<MockSignatures<NopSignature>>::new(
        QcInfo {
            vote: Vote {
                vote_info: vi_2,
                ledger_commit_info: ci,
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );

    assert!(Rank(qc_1.info) < Rank(qc_2.info));
    assert!(Rank(qc_2.info) > Rank(qc_1.info));

    qc_2.info.vote.vote_info.round = Round(2);

    assert_eq!(Rank(qc_1.info), Rank(qc_2.info));
}
