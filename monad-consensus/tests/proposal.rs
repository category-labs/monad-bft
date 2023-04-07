use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::message::ProposalMessage;
use monad_consensus::types::quorum_certificate::QuorumCertificate;
use monad_consensus::validation::hashing::*;
use monad_consensus::validation::protocol::{verify_proposal, ValidatorMember};
use monad_consensus::*;
use monad_testutil::signing::{get_key, node_id, MockSignatures, Signer};
use monad_validator::validator::Validator;

fn setup_block(author: NodeId, block_round: u64, qc_round: u64) -> Block<MockSignatures> {
    let txns = TransactionList(vec![1, 2, 3, 4]);
    let round = Round(block_round);
    let mut qc = QuorumCertificate::<MockSignatures>::new(Default::default(), MockSignatures);
    qc.info.vote.round = Round(qc_round);

    Block::<MockSignatures>::new::<Sha256Hash>(author, round, &txns, &qc)
}

#[test]
fn test_proposal_hash() {
    let mut vset = ValidatorMember::new();

    let author = node_id();
    let proposal = ProposalMessage {
        block: setup_block(author, 234, 233),
        last_round_tc: None,
    };

    let keypair = get_key("a");

    vset.insert(
        keypair.pubkey(),
        Validator {
            pubkey: keypair.pubkey(),
            stake: 0,
        },
    );

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = Signer::sign_object(proposal, &msg, &keypair);

    assert!(verify_proposal::<Sha256Hash, _>(&vset, sp).is_ok());
}

#[test]
fn test_proposal_missing_tc() {
    let mut vset = ValidatorMember::new();

    let author = node_id();
    let proposal = ProposalMessage {
        block: setup_block(author, 234, 232),
        last_round_tc: None,
    };

    let keypair = get_key("6");

    vset.insert(
        keypair.pubkey(),
        Validator {
            pubkey: keypair.pubkey(),
            stake: 0,
        },
    );

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = Signer::sign_object(proposal, &msg, &keypair);

    assert!(verify_proposal::<Sha256Hash, _>(&vset, sp).is_err());
}

#[test]
fn test_proposal_invalid_qc() {
    let mut vset = ValidatorMember::new();

    let author = node_id();
    let proposal = ProposalMessage {
        block: setup_block(author, 234, 233),
        last_round_tc: None,
    };

    let keypair = get_key("6");

    vset.insert(
        keypair.pubkey(),
        Validator {
            pubkey: keypair.pubkey(),
            stake: 0,
        },
    );

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = Signer::sign_object(proposal, &msg, &get_key("7"));

    assert!(verify_proposal::<Sha256Hash, _>(&vset, sp).is_err());
}
