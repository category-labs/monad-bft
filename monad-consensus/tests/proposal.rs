use monad_consensus::types::block::{Block, TransactionList};
use monad_consensus::types::ledger::LedgerCommitInfo;
use monad_consensus::types::message::ProposalMessage;
use monad_consensus::types::quorum_certificate::{QcInfo, QuorumCertificate};
use monad_consensus::types::voting::VoteInfo;
use monad_consensus::validation::error::Error;
use monad_consensus::validation::hashing::*;
use monad_consensus::validation::signing::ValidatorMember;
use monad_crypto::secp256k1::SecpSignature;
use monad_crypto::KeyPair;
use monad_testutil::signing::{get_key, node_id, MockSignatures, TestSigner};
use monad_types::*;
use monad_validator::validator::Validator;

fn setup_block(author: NodeId, block_round: u64, qc_round: u64) -> Block<MockSignatures> {
    let txns = TransactionList(vec![1, 2, 3, 4]);
    let round = Round(block_round);
    let vi = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: Round(qc_round),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
    };
    let qc = QuorumCertificate::<MockSignatures>::new(
        QcInfo {
            vote: vi,
            ledger_commit: LedgerCommitInfo::new::<Sha256Hash>(Some(Default::default()), &vi),
        },
        MockSignatures,
    );

    Block::<MockSignatures>::new::<Sha256Hash>(author, round, &txns, &qc)
}

#[test]
fn test_proposal_hash() {
    let mut vset = ValidatorMember::new();

    let author = node_id();
    let proposal: ProposalMessage<SecpSignature, MockSignatures> = ProposalMessage {
        block: setup_block(author, 234, 233),
        last_round_tc: None,
    };

    let keypair = get_key(0);

    vset.insert(
        NodeId(keypair.pubkey()),
        Validator {
            pubkey: keypair.pubkey(),
            stake: 0,
        },
    );

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &keypair);

    assert!(sp.verify::<Sha256Hash>(&vset, &keypair.pubkey()).is_ok());
}

#[test]
fn test_proposal_missing_tc() {
    let mut vset = ValidatorMember::new();

    let author = node_id();
    let proposal = ProposalMessage {
        block: setup_block(author, 234, 232),
        last_round_tc: None,
    };

    let keypair = get_key(6);

    vset.insert(
        NodeId(keypair.pubkey()),
        Validator {
            pubkey: keypair.pubkey(),
            stake: 0,
        },
    );

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &keypair);

    assert_eq!(
        sp.verify::<Sha256Hash>(&vset, &keypair.pubkey())
            .unwrap_err(),
        Error::NotWellFormed
    );
}

#[test]
fn test_proposal_invalid_qc() {
    let mut vset = ValidatorMember::new();

    let author = node_id();
    let proposal = ProposalMessage {
        block: setup_block(author, 234, 233),
        last_round_tc: None,
    };

    let keypair = get_key(6);

    vset.insert(
        NodeId(keypair.pubkey()),
        Validator {
            pubkey: keypair.pubkey(),
            stake: 0,
        },
    );

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &get_key(7));

    assert_eq!(
        sp.verify::<Sha256Hash>(&vset, &keypair.pubkey())
            .unwrap_err(),
        Error::InvalidAuthor
    );
}
