use monad_consensus_types::{
    ledger::CommitResult,
    quorum_certificate::{QcInfo, QuorumCertificate, Rank},
    voting::{Vote, VoteInfo},
};
use monad_crypto::{certificate_signature::CertificateSignaturePubKey, hasher::Hash, NopSignature};
use monad_testutil::signing::MockSignatures;
use monad_types::*;

extern crate monad_testutil;

#[test]
fn comparison() {
    let ci = CommitResult::NoCommit;

    let vi_1 = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(2),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
    };

    let vi_2 = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(3),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
    };

    let qc_1 = QuorumCertificate::<MockSignatures<CertificateSignaturePubKey<NopSignature>>>::new(
        QcInfo {
            vote: Vote {
                vote_info: vi_1,
                ledger_commit_info: ci,
            },
        },
        MockSignatures::with_pubkeys(&[]),
    );
    let mut qc_2 =
        QuorumCertificate::<MockSignatures<CertificateSignaturePubKey<NopSignature>>>::new(
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
