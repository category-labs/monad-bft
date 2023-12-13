use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::ProposalMessage},
    validation::signing::Unvalidated,
};
use monad_consensus_types::{
    block::Block,
    ledger::LedgerCommitInfo,
    payload::{ExecutionArtifacts, Payload, RandaoReveal, TransactionHashList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    validation::Error,
    voting::{ValidatorMapping, VoteInfo},
};
use monad_crypto::{hasher::Hash, secp256k1::PubKey};
use monad_eth_types::EthAddress;
use monad_testutil::{
    signing::{get_key, MockSignatures, TestSigner},
    validators::create_keys_w_validators,
};
use monad_types::*;
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

type SignatureCollectionType = MockSignatures;

fn setup_block(
    author: NodeId,
    block_round: Round,
    qc_round: Round,
    signers: &[PubKey],
) -> Block<MockSignatures> {
    let txns = TransactionHashList::new(vec![1, 2, 3, 4].into());
    let vi = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        round: qc_round,
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
    };
    let qc = QuorumCertificate::<MockSignatures>::new(
        QcInfo {
            vote: vi,
            ledger_commit: LedgerCommitInfo::new(Some(Default::default()), &vi),
        },
        MockSignatures::with_pubkeys(signers),
    );

    Block::<MockSignatures>::new(
        author,
        block_round,
        &Payload {
            txns,
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(1),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        },
        &qc,
    )
}

#[test]
fn test_proposal_hash() {
    let (keypairs, _certkeys, vset, _vmap) = create_keys_w_validators::<SignatureCollectionType>(1);
    let author = NodeId(keypairs[0].pubkey());

    let proposal = ConsensusMessage::Proposal(ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    });

    let sp = TestSigner::sign_object(proposal, &keypairs[0]);

    assert!(sp.verify(&vset, &keypairs[0].pubkey()).is_ok());
}

#[test]
fn test_proposal_missing_tc() {
    let (keypairs, _certkeys, vset, vmap) = create_keys_w_validators::<SignatureCollectionType>(1);
    let author = NodeId(keypairs[0].pubkey());

    let proposal = Unvalidated::new(ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(232),
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    });

    assert!(matches!(
        proposal.validate(&vset, &vmap),
        Err(Error::NotWellFormed)
    ));
}

#[test]
fn test_proposal_author_not_sender() {
    let (keypairs, _certkeys, vset, vmap) = create_keys_w_validators::<SignatureCollectionType>(2);

    let author_keypair = &keypairs[0];
    let sender_keypair = &keypairs[1];
    let author = NodeId(author_keypair.pubkey());

    let proposal = ConsensusMessage::Proposal(ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            keypairs
                .iter()
                .map(|keypair| keypair.pubkey())
                .collect::<Vec<_>>()
                .as_ref(),
        ),
        last_round_tc: None,
    });

    let sp = TestSigner::sign_object(proposal, author_keypair);
    assert_eq!(
        sp.verify(&vset, &sender_keypair.pubkey()).unwrap_err(),
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
    let proposal = ConsensusMessage::Proposal(ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            &[author_keypair.pubkey(), non_valdiator_keypair.pubkey()],
        ),
        last_round_tc: None,
    });

    let sp = TestSigner::sign_object(proposal, &non_valdiator_keypair);

    let vset = ValidatorSet::new(vlist).unwrap();
    assert_eq!(
        sp.verify(&vset, &author.0).unwrap_err(),
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
    let proposal = Unvalidated::new(ProposalMessage {
        block: setup_block(
            author,
            Round(234),
            Round(233),
            &[non_staked_keypair.pubkey()],
        ),
        last_round_tc: None,
    });

    let vset = ValidatorSet::new(vlist).unwrap();
    let vmap = ValidatorMapping::new(vec![
        (NodeId(staked_keypair.pubkey()), staked_keypair.pubkey()),
        (
            NodeId(non_staked_keypair.pubkey()),
            non_staked_keypair.pubkey(),
        ),
    ]);

    let validate_result = proposal.validate(&vset, &vmap);

    assert!(matches!(validate_result, Err(Error::InsufficientStake)));
}
