use test_case::test_case;

use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::message::VoteMessage;
use monad_consensus::types::message::{ProposalMessage, TimeoutMessage};
use monad_consensus::types::quorum_certificate::{QcInfo, QuorumCertificate};
use monad_consensus::types::timeout::{HighQcRound, TimeoutCertificate, TimeoutInfo};
use monad_consensus::validation::hashing::*;
use monad_consensus::*;
use monad_consensus::{types::ledger::LedgerCommitInfo, validation::hashing::Hashable, Hash};
use monad_crypto::secp256k1::KeyPair;
use monad_testutil::signing::*;
use sha2::Digest;

#[test_case(None ; "None commit_state")]
#[test_case(Some(Default::default()) ; "Some commit_state")]
fn vote_msg_hash(cs: Option<Hash>) {
    let lci = LedgerCommitInfo {
        commit_state_hash: cs,
        vote_info_hash: Default::default(),
    };

    let vm = VoteMessage {
        vote_info: Default::default(),
        ledger_commit_info: lci,
    };

    let mut hasher = sha2::Sha256::new();
    hasher.update(vm.ledger_commit_info.vote_info_hash);
    if vm.ledger_commit_info.commit_state_hash.is_some() {
        hasher.update(vm.ledger_commit_info.commit_state_hash.as_ref().unwrap());
    }
    let h1 = hasher.finalize_reset();

    for v in (&vm).msg_parts() {
        hasher.update(v);
    }
    let h2 = hasher.finalize();

    assert_eq!(h1, h2);
}

#[test]
fn timeout_msg_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: Default::default(),
                ledger_commit: Default::default(),
            },
            MockSignatures,
        ),
    };

    let tm = TimeoutMessage {
        tminfo: ti,
        last_round_tc: None,
    };

    let mut hasher = sha2::Sha256::new();
    hasher.update(tm.tminfo.round);
    hasher.update(tm.tminfo.high_qc.info.vote.round);
    let h1 = hasher.finalize_reset();

    for m in (&tm).msg_parts() {
        hasher.update(m);
    }
    let h2 = hasher.finalize();

    assert_eq!(h1, h2);
}

#[test]
fn proposal_msg_hash() {
    use monad_testutil::signing::hash;

    let txns = TransactionList(vec![1, 2, 3, 4]);
    let author = NodeId(12);
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new(Default::default(), MockSignatures);

    let block = Block::<MockSignatures>::new(author, round, &txns, &qc);

    let proposal = ProposalMessage {
        block: block.clone(),
        last_round_tc: None,
    };

    let mut hasher = sha2::Sha256::new();
    for p in (&proposal).msg_parts() {
        hasher.update(p);
    }
    let h1 = hasher.finalize_reset();
    let h2 = hash(&block);

    assert_eq!(h1, h2.into());
}

#[test]
fn max_high_qc() {
    let high_qc_rounds = vec![
        HighQcRound { qc_round: Round(1) },
        HighQcRound { qc_round: Round(3) },
        HighQcRound { qc_round: Round(1) },
    ]
    .iter()
    .map(|x| {
        let hasher = Sha256Hash;
        let msg = hasher.hash_object(x);
        let keypair = get_key("a");

        Signer::sign_object(*x, &msg, keypair)
    })
    .collect();

    let tc = TimeoutCertificate {
        round: Round(2),
        high_qc_rounds,
    };

    assert_eq!(tc.max_round(), Round(3));
}

#[test]
fn test_vote_message() {
    let lci = LedgerCommitInfo {
        commit_state_hash: Some(Default::default()),
        vote_info_hash: Default::default(),
    };

    let vm = VoteMessage {
        vote_info: Default::default(),
        ledger_commit_info: lci,
    };

    let privkey =
        hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530").unwrap();
    let keypair = KeyPair::from_slice(&privkey).unwrap();

    let expected_vote_info_hash = vm.ledger_commit_info.vote_info_hash.clone();

    let msg = monad_testutil::signing::Hasher::hash_object(&vm);
    let svm = Signer::sign_object(vm, &msg, keypair);

    assert_eq!(svm.0.author, NodeId(0));
    assert_eq!(
        svm.0.obj.ledger_commit_info.vote_info_hash,
        expected_vote_info_hash
    );
}
