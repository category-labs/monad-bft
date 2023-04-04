use test_case::test_case;

use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::message::VoteMessage;
use monad_consensus::types::message::{ProposalMessage, TimeoutMessage};
use monad_consensus::types::quorum_certificate::{QcInfo, QuorumCertificate};
use monad_consensus::types::timeout::TimeoutInfo;
use monad_consensus::*;
use monad_consensus::{types::ledger::LedgerCommitInfo, validation::signing::Hashable, Hash};
use monad_testutil::signing::MockSignatures;
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
