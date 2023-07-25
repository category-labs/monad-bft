use monad_consensus::messages::message::ProposalMessage;
use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    quorum_certificate::{QcInfo, QuorumCertificate},
    validation::{Error, Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::secp256k1::{PubKey, SecpSignature};
use monad_testutil::{
    signing::{get_key, MockSignatures, TestSigner},
    validators::create_keys_w_validators,
};
use monad_types::*;

fn setup_block(
    author: NodeId,
    block_round: u64,
    qc_round: u64,
    signers: &[PubKey],
) -> Block<MockSignatures> {
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
        MockSignatures::with_pubkeys(signers),
    );

    Block::<MockSignatures>::new::<Sha256Hash>(author, round, &txns, &qc)
}

#[test]
fn test_proposal_hash() {
    let (keypairs, _blskeys, vset, vprop) = create_keys_w_validators(1);
    let author = NodeId(keypairs[0].pubkey());

    let proposal: ProposalMessage<SecpSignature, MockSignatures> = ProposalMessage {
        block: setup_block(
            author,
            234,
            233,
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    };
    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &keypairs[0]);

    assert!(sp
        .verify::<Sha256Hash, _, _>(&vset, &vprop, &keypairs[0].pubkey())
        .is_ok());
}

#[test]
fn test_proposal_missing_tc() {
    let (keypairs, _blskeys, vset, vprop) = create_keys_w_validators(1);
    let author = NodeId(keypairs[0].pubkey());

    let proposal = ProposalMessage {
        block: setup_block(
            author,
            234,
            232,
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    };

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &keypairs[0]);

    assert_eq!(
        sp.verify::<Sha256Hash, _, _>(&vset, &vprop, &keypairs[0].pubkey())
            .unwrap_err(),
        Error::NotWellFormed
    );
}

#[test]
fn test_proposal_invalid_qc() {
    let (keypairs, _blskeys, vset, vprop) = create_keys_w_validators(1);
    let author = NodeId(keypairs[0].pubkey());

    let proposal = ProposalMessage {
        block: setup_block(
            author,
            234,
            233,
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    };

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &get_key(100));

    assert_eq!(
        sp.verify::<Sha256Hash, _, _>(&vset, &vprop, &keypairs[0].pubkey())
            .unwrap_err(),
        Error::InvalidAuthor
    );
}
