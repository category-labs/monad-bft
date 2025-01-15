use monad_consensus_types::{
    quorum_certificate::{QuorumCertificate, Rank},
    voting::Vote,
};
use monad_crypto::NopSignature;
use monad_testutil::signing::MockSignatures;
use monad_types::*;

extern crate monad_testutil;

#[test]
fn comparison() {
    let v_1 = Vote {
        round: Round(2),
        ..DontCare::dont_care()
    };

    let v_2 = Vote {
        round: Round(3),
        ..DontCare::dont_care()
    };

    let qc_1 = QuorumCertificate::<MockSignatures<NopSignature>>::new(
        v_1,
        MockSignatures::with_pubkeys(&[]),
    );
    let mut qc_2 = QuorumCertificate::<MockSignatures<NopSignature>>::new(
        v_2,
        MockSignatures::with_pubkeys(&[]),
    );

    assert!(Rank(qc_1.info) < Rank(qc_2.info));
    assert!(Rank(qc_2.info) > Rank(qc_1.info));

    qc_2.info.round = Round(2);

    assert_eq!(Rank(qc_1.info), Rank(qc_2.info));
}
