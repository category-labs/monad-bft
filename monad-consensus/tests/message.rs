use monad_consensus::types::voting::VoteInfo;
use test_case::test_case;

use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::message::VoteMessage;
use monad_consensus::types::message::{ProposalMessage, TimeoutMessage};
use monad_consensus::types::quorum_certificate::{QcInfo, QuorumCertificate};
use monad_consensus::types::timeout::{
    HighQcRound, HighQcRoundSigTuple, TimeoutCertificate, TimeoutInfo,
};
use monad_consensus::validation::hashing::*;
use monad_crypto::secp256k1::{SecpKeyPair, SecpSignature};
use monad_crypto::{KeyPair, Signature};
use monad_testutil::signing::*;
use monad_types::*;
use sha2::Digest;
use zerocopy::AsBytes;

#[test_case(None ; "None commit_state")]
#[test_case(Some(Default::default()) ; "Some commit_state")]
fn vote_msg_hash(cs: Option<Hash>) {
    let lci = LedgerCommitInfo {
        commit_state_hash: cs,
        vote_info_hash: Default::default(),
    };

    let vm = VoteMessage {
        vote_info: VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        },
        ledger_commit_info: lci,
    };

    let mut hasher = sha2::Sha256::new();
    hasher.update(vm.vote_info.id.0.as_bytes());
    hasher.update(vm.vote_info.round.as_bytes());
    hasher.update(vm.vote_info.parent_id.0.as_bytes());
    hasher.update(vm.vote_info.parent_round.as_bytes());
    let h1: Hash = Hash(hasher.finalize_reset().into());

    let h2 = Sha256Hash::hash_object(&vm.vote_info);

    assert_eq!(h1, h2);
}

#[test]
fn timeout_msg_hash() {
    let ti = TimeoutInfo {
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x00_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x00_u8; 32])),
                    parent_round: Round(0),
                },
                ledger_commit: Default::default(),
            },
            MockSignatures,
        ),
    };

    let tm: TimeoutMessage<SecpSignature, MockSignatures> = TimeoutMessage {
        tminfo: ti,
        last_round_tc: None,
    };

    let mut hasher = sha2::Sha256::new();
    hasher.update(tm.tminfo.round);
    hasher.update(tm.tminfo.high_qc.info.vote.round);
    let h1: Hash = Hash(hasher.finalize_reset().into());

    let h2 = Sha256Hash::hash_object(&tm);

    assert_eq!(h1, h2);
}

#[test]
fn proposal_msg_hash() {
    use monad_testutil::signing::hash;

    let txns = TransactionList(vec![1, 2, 3, 4]);

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = SecpKeyPair::from_bytes(&mut privkey).unwrap();
    let author = NodeId(keypair.pubkey());
    let round = Round(234);
    let qc = QuorumCertificate::<MockSignatures>::new(
        QcInfo {
            vote: VoteInfo {
                id: BlockId(Hash([0x00_u8; 32])),
                round: Round(0),
                parent_id: BlockId(Hash([0x00_u8; 32])),
                parent_round: Round(0),
            },
            ledger_commit: LedgerCommitInfo::default(),
        },
        MockSignatures,
    );

    let block = Block::<MockSignatures>::new::<Sha256Hash>(author, round, &txns, &qc);

    let proposal: ProposalMessage<SecpSignature, MockSignatures> = ProposalMessage {
        block: block.clone(),
        last_round_tc: None,
    };

    let h1 = Sha256Hash::hash_object(&proposal);
    let h2 = hash(&block);

    assert_eq!(h1, h2);
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
        let msg = Sha256Hash::hash_object(x);
        let keypair = get_key(0);
        HighQcRoundSigTuple {
            high_qc_round: *x,
            author_signature: keypair.sign(msg.as_ref()),
        }
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
        vote_info: VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        },
        ledger_commit_info: lci,
    };

    let mut privkey: [u8; 32] = [127; 32];
    let keypair = SecpKeyPair::from_bytes(&mut privkey).unwrap();

    let expected_vote_info_hash = vm.ledger_commit_info.vote_info_hash;

    let msg = Sha256Hash::hash_object(&vm.vote_info);
    let svm = TestSigner::sign_object(vm, msg.as_ref(), &keypair);

    assert_eq!(
        svm.author_signature().recover_pubkey(msg.as_ref()).unwrap(),
        keypair.pubkey()
    );

    // TODO fix this test.. would be best if we could do this as a unit test
    // assert_eq!(
    //     svm.obj.ledger_commit_info.vote_info_hash,
    //     expected_vote_info_hash
    // );
}
