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
fn test_proposal_author_not_sender() {
    let mut vlist = Vec::new();

    let author_keypair = get_key(6);
    let sender_keypair = get_key(7);

    vlist.push((NodeId(author_keypair.pubkey()), Stake(0)));
    vlist.push((NodeId(sender_keypair.pubkey()), Stake(0)));

    let author = NodeId(author_keypair.pubkey());
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
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &author_keypair);

    let vset = ValidatorSet::new(vlist).unwrap();
    assert_eq!(
        sp.verify::<Sha256Hash, _>(&vset, &sender_keypair.pubkey())
            .unwrap_err(),
        Error::AuthorNotSender
    );
}

#[test]
fn test_proposal_invalid_author() {
    let mut vlist = Vec::new();
    let author_keypair = get_key(6);
    let non_valdiator_keypair = get_key(7);

    vlist.push((NodeId(author_keypair.pubkey()), Stake(0)));

    let author = NodeId(author_keypair.pubkey());
    let proposal = ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            &[author_keypair.pubkey(), non_valdiator_keypair.pubkey()],
        ),
        last_round_tc: None,
    };

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &non_valdiator_keypair);

    let vset = ValidatorSet::new(vlist).unwrap();
    assert_eq!(
        sp.verify::<Sha256Hash, _>(&vset, &author.0).unwrap_err(),
        Error::InvalidAuthor
    );
}

#[test]
fn test_proposal_invalid_qc() {
    let mut vlist = Vec::new();
    let non_staked_keypair = get_key(6);
    let staked_keypair = get_key(7);

    vlist.push((NodeId(non_staked_keypair.pubkey()), Stake(0)));
    vlist.push((NodeId(staked_keypair.pubkey()), Stake(1)));

    let author = NodeId(non_staked_keypair.pubkey());
    let proposal = ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            &[non_staked_keypair.pubkey()],
        ),
        last_round_tc: None,
    };

    let msg = Sha256Hash::hash_object(&proposal);
    let sp = TestSigner::sign_object(proposal, msg.as_ref(), &non_staked_keypair);

    let vset = ValidatorSet::new(vlist).unwrap();
    assert_eq!(
        sp.verify::<Sha256Hash, _>(&vset, &non_staked_keypair.pubkey())
            .unwrap_err(),
        Error::InsufficientStake
    );
}
