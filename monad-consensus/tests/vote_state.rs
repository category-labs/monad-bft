use std::io::Write;

use test_case::test_case;

use monad_consensus::signatures::aggregate_signature::AggregateSignatures;
use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::message::VoteMessage;
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::types::signature::SignatureCollection;
use monad_consensus::validation::hashing::{Hasher, Sha256Hash};
use monad_consensus::validation::signing::Unverified;
use monad_consensus::validation::signing::Verified;
use monad_consensus::vote_state::VoteState;
use monad_crypto::secp256k1::{KeyPair, SecpSignature};
use monad_testutil::signing::*;
use monad_testutil::validators::MockLeaderElection;
use monad_validator::validator::Validator;
use monad_validator::validator_set::ValidatorSet;

fn create_signed_vote_message(
    vote_hash: &str,
    keypair: &KeyPair,
) -> Unverified<SecpSignature, VoteMessage> {
    let mut vote_info_hash = [0; 32];
    let mut b: &mut [u8] = &mut vote_info_hash;

    b.write(vote_hash.as_bytes()).unwrap();

    let lci = LedgerCommitInfo {
        commit_state_hash: Some(Default::default()),
        vote_info_hash: vote_info_hash,
    };

    let vm = VoteMessage {
        vote_info: Default::default(),
        ledger_commit_info: lci,
    };

    let msg = Sha256Hash::hash_object(&vm.ledger_commit_info);
    let svm = TestSigner::sign_object(vm, &msg, keypair);

    svm
}

fn setup_ctx(
    num_nodes: u32,
) -> (
    Vec<KeyPair>,
    ValidatorSet<MockLeaderElection>,
    Vec<Verified<SecpSignature, VoteMessage>>,
) {
    let keys = create_keys(num_nodes);

    let mut nodes = Vec::new();
    for i in 0..num_nodes {
        nodes.push(Validator {
            pubkey: keys[i as usize].pubkey().clone(),
            stake: 1,
        });
    }

    let valset = ValidatorSet::<MockLeaderElection>::new(nodes).unwrap();

    let mut votes = Vec::new();
    for i in 0..num_nodes {
        let svm = create_signed_vote_message("foobar", &keys[i as usize]);
        let vm = svm
            .verify::<Sha256Hash>(&valset.get_members(), &keys[i as usize].pubkey())
            .unwrap();

        votes.push(vm);
    }

    (keys, valset, votes)
}

fn verify_qcs(
    qcs: Vec<&Option<QuorumCertificate<AggregateSignatures<SecpSignature>>>>,
    expected_qcs: u32,
    expected_sigs: u32,
) {
    assert_eq!(qcs.len(), expected_qcs as usize);

    for i in 0..expected_qcs {
        assert_eq!(
            qcs[i as usize]
                .as_ref()
                .unwrap()
                .signatures
                .num_signatures(),
            expected_sigs as usize
        );
    }
}

#[test_case(4 ; "min nodes")]
#[test_case(15 ; "multiple nodes")]
fn test_votes(num_nodes: u32) {
    let (_, valset, votes) = setup_ctx(num_nodes);

    let mut voteset = VoteState::<AggregateSignatures<SecpSignature>>::new();
    let mut qcs = Vec::new();
    for i in 0..num_nodes {
        let qc =
            voteset.process_vote::<MockLeaderElection, Sha256Hash>(&votes[i as usize], &valset);
        qcs.push(qc);
    }
    let valid_qc: Vec<&Option<QuorumCertificate<AggregateSignatures<SecpSignature>>>> =
        qcs.iter().filter(|a| a.is_some()).collect();

    // number of expected signatures is 2/3 + 1 for num_nodes because all weights were equal
    let num_expected_sigs = 2 * num_nodes / 3 + 1;

    // no reset of voteset is done, so expect only one qc regardless of validator count
    verify_qcs(valid_qc, 1, num_expected_sigs);
}

#[test_case(4, 2 ; "min nodes 1 reset")]
#[test_case(8, 2 ; "multiple nodes 1 reset")]
#[test_case(20, 5 ; "multiple nodes multiple resets")]
fn test_reset(num_nodes: u32, num_rounds: u32) {
    let (_, valset, votes) = setup_ctx(num_nodes);

    let mut voteset = VoteState::<AggregateSignatures<SecpSignature>>::new();
    let mut qcs = Vec::new();

    for _ in 0..num_rounds {
        for i in 0..num_nodes {
            let qc =
                voteset.process_vote::<MockLeaderElection, Sha256Hash>(&votes[i as usize], &valset);
            qcs.push(qc);
        }

        voteset.start_new_round();
    }

    let valid_qc: Vec<&Option<QuorumCertificate<AggregateSignatures<SecpSignature>>>> =
        qcs.iter().filter(|a| a.is_some()).collect();

    let num_expected_sigs = 2 * num_nodes / 3 + 1;

    verify_qcs(valid_qc, num_rounds, num_expected_sigs);
}

#[test_case(4 ; "min nodes")]
#[test_case(15 ; "multiple nodes")]
fn test_minority(num_nodes: u32) {
    let (_, valset, votes) = setup_ctx(num_nodes);

    let mut voteset = VoteState::<AggregateSignatures<SecpSignature>>::new();
    let mut qcs = Vec::new();

    let majority = 2 * num_nodes / 3 + 1;

    for i in 0..majority - 1 {
        let qc =
            voteset.process_vote::<MockLeaderElection, Sha256Hash>(&votes[i as usize], &valset);
        qcs.push(qc);
    }

    let valid_qc: Vec<&Option<QuorumCertificate<AggregateSignatures<SecpSignature>>>> =
        qcs.iter().filter(|a| a.is_some()).collect();

    assert_eq!(valid_qc.len(), 0);
}
