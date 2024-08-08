use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::ProposalMessage,
    },
    validation::signing::Unvalidated,
};
use monad_consensus_types::{
    block::Block,
    ledger::CommitResult,
    payload::{ExecutionProtocol, FullTransactionList, Payload, RandaoReveal, TransactionPayload},
    quorum_certificate::{QcInfo, QuorumCertificate},
    validation::Error,
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    hasher::Hash,
    NopSignature,
};
use monad_eth_types::EthAddress;
use monad_testutil::{
    signing::{get_key, MockSignatures, TestSigner},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, Stake};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

type SignatureType = NopSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;
type SignatureCollectionType = MockSignatures<SignatureType>;

fn setup_block(
    author: NodeId<PubKeyType>,
    block_epoch: Epoch,
    block_round: Round,
    qc_epoch: Epoch,
    qc_round: Round,
    signers: &[PubKeyType],
) -> Block<MockSignatures<SignatureType>> {
    let txns = TransactionPayload::List(FullTransactionList::new(vec![1, 2, 3, 4].into()));
    let vi = VoteInfo {
        id: BlockId(Hash([0x00_u8; 32])),
        epoch: qc_epoch,
        round: qc_round,
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
        timestamp: 0,
    };
    let qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        QcInfo {
            vote: Vote {
                vote_info: vi,
                ledger_commit_info: CommitResult::Commit,
            },
        },
        MockSignatures::with_pubkeys(signers),
    );

    Block::<MockSignatures<SignatureType>>::new(
        author,
        0,
        block_epoch,
        block_round,
        &Payload {
            txns,
            header: ExecutionProtocol::zero(),
            seq_num: SeqNum(1),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        },
        &qc,
    )
}

#[test]
fn test_proposal_hash() {
    let (keypairs, _certkeys, vset, vmap) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        _,
    >(1, ValidatorSetFactory::default());
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map: ValidatorsEpochMapping<ValidatorSetFactory<_>, SignatureCollectionType> =
        ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        vmap,
    );
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: setup_block(
            author,
            epoch_manager.get_epoch(Round(234)).expect("epoch exists"),
            Round(234),
            epoch_manager.get_epoch(Round(233)).expect("epoch exists"),
            Round(233),
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, &keypairs[0]);

    assert!(sp
        .verify(&epoch_manager, &val_epoch_map, &keypairs[0].pubkey())
        .is_ok());
}

#[test]
fn test_proposal_missing_tc() {
    let (keypairs, _certkeys, vset, vmap) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        _,
    >(1, ValidatorSetFactory::default());
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        vmap,
    );
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = Unvalidated::new(ProposalMessage {
        block: setup_block(
            author,
            epoch_manager.get_epoch(Round(234)).expect("epoch exists"),
            Round(234),
            epoch_manager.get_epoch(Round(232)).expect("epoch exists"),
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
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::NotWellFormed)
    ));
}

#[test]
fn test_proposal_author_not_sender() {
    let (keypairs, _certkeys, vset, vmap) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        _,
    >(2, ValidatorSetFactory::default());
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map: ValidatorsEpochMapping<_, SignatureCollectionType> =
        ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        vmap,
    );

    let author_keypair = &keypairs[0];
    let sender_keypair = &keypairs[1];
    let author = NodeId::new(author_keypair.pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: setup_block(
            author,
            epoch_manager.get_epoch(Round(234)).expect("epoch exists"),
            Round(234),
            epoch_manager.get_epoch(Round(233)).expect("epoch exists"),
            Round(233),
            keypairs
                .iter()
                .map(|keypair| keypair.pubkey())
                .collect::<Vec<_>>()
                .as_ref(),
        ),
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, author_keypair);
    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &sender_keypair.pubkey())
            .unwrap_err(),
        Error::AuthorNotSender
    );
}

#[test]
fn test_proposal_invalid_author() {
    let mut vlist = Vec::new();
    let author_keypair = get_key::<SignatureType>(6);
    let non_valdiator_keypair = get_key::<SignatureType>(7);

    vlist.push((NodeId::new(author_keypair.pubkey()), Stake(0)));

    let vset = ValidatorSetFactory::default().create(vlist).unwrap();
    let vmap: ValidatorMapping<PubKeyType, _> = ValidatorMapping::new(vec![(
        NodeId::new(author_keypair.pubkey()),
        author_keypair.pubkey(),
    )]);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map: ValidatorsEpochMapping<_, SignatureCollectionType> =
        ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        vmap,
    );

    let author = NodeId::new(author_keypair.pubkey());
    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: setup_block(
            author,
            epoch_manager.get_epoch(Round(234)).expect("epoch exists"),
            Round(234),
            epoch_manager.get_epoch(Round(233)).expect("epoch exists"),
            Round(233),
            &[author_keypair.pubkey(), non_valdiator_keypair.pubkey()],
        ),
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, &non_valdiator_keypair);

    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &author.pubkey())
            .unwrap_err(),
        Error::InvalidAuthor
    );
}

#[test]
fn test_proposal_invalid_qc() {
    let mut vlist = Vec::new();
    let non_staked_keypair = get_key::<SignatureType>(6);
    let staked_keypair = get_key::<SignatureType>(7);

    vlist.push((NodeId::new(non_staked_keypair.pubkey()), Stake(0)));
    vlist.push((NodeId::new(staked_keypair.pubkey()), Stake(1)));

    let vset = ValidatorSetFactory::default().create(vlist).unwrap();
    let vmap = ValidatorMapping::new(vec![
        (
            NodeId::new(staked_keypair.pubkey()),
            staked_keypair.pubkey(),
        ),
        (
            NodeId::new(non_staked_keypair.pubkey()),
            non_staked_keypair.pubkey(),
        ),
    ]);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        vmap,
    );

    let author = NodeId::new(non_staked_keypair.pubkey());
    let proposal = Unvalidated::new(ProposalMessage {
        block: setup_block(
            author,
            epoch_manager.get_epoch(Round(234)).expect("epoch exists"),
            Round(234),
            epoch_manager.get_epoch(Round(233)).expect("epoch exists"),
            Round(233),
            &[non_staked_keypair.pubkey()],
        ),
        last_round_tc: None,
    });

    let validate_result = proposal.validate(&epoch_manager, &val_epoch_map);

    assert!(matches!(validate_result, Err(Error::InsufficientStake)));
}
