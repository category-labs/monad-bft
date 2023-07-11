use monad_consensus::messages::message::ProposalMessage;
use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    quorum_certificate::{QcInfo, QuorumCertificate},
    validation::{Error, Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::secp256k1::{PubKey, SecpSignature};
use monad_testutil::signing::{get_key, node_id, MockSignatures, TestSigner};
use monad_types::*;
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

fn setup_block(
    author: NodeId,
    block_round: Round,
    qc_round: Round,
    signers: &[PubKey],
) -> Block<MockSignatures> {
    let txns = TransactionList(vec![1, 2, 3, 4]);
    let vi = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: qc_round,
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

    Block::<MockSignatures>::new::<Sha256Hash>(author, block_round, &txns, &qc)
}

#[test]
fn test_proposal_hash() {
    let mut vlist = Vec::new();
    let keypair = get_key(6);

    vlist.push((NodeId(keypair.pubkey()), Stake(1)));

    let author = node_id();
    let proposal: ProposalMessage<SecpSignature, MockSignatures> = ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            vlist
                .iter()
                .map(|(node_id, _)| node_id.0)
                .collect::<Vec<_>>()
                .as_ref(),
        ),
        last_round_tc: None,
    };

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &keypair);

    let vset = ValidatorSet::new(vlist).unwrap();
    assert!(sp.verify::<Sha256Hash, _>(&vset, &keypair.pubkey()).is_ok());
}

#[test]
fn test_proposal_missing_tc() {
    let mut vlist = Vec::new();
    let keypair = get_key(6);

    vlist.push((NodeId(keypair.pubkey()), Stake(0)));

    let author = node_id();
    let proposal = ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(232),
            vlist
                .iter()
                .map(|(node_id, _)| node_id.0)
                .collect::<Vec<_>>()
                .as_ref(),
        ),
        last_round_tc: None,
    };

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &keypair);

    let vset = ValidatorSet::new(vlist).unwrap();
    assert_eq!(
        sp.verify::<Sha256Hash, _>(&vset, &keypair.pubkey())
            .unwrap_err(),
        Error::NotWellFormed
    );
}

#[test]
fn test_proposal_invalid_qc() {
    let mut vlist = Vec::new();

    let keypair = get_key(6);

    vlist.push((NodeId(keypair.pubkey()), Stake(0)));

    let author = node_id();
    let proposal = ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            vlist
                .iter()
                .map(|(node_id, _)| node_id.0)
                .collect::<Vec<_>>()
                .as_ref(),
        ),
        last_round_tc: None,
    };

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &get_key(7));

    let vset = ValidatorSet::new(vlist).unwrap();
    assert_eq!(
        sp.verify::<Sha256Hash, _>(&vset, &keypair.pubkey())
            .unwrap_err(),
        Error::InvalidAuthor
    );
}
