use std::marker::PhantomData;

use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::ProposalMessage,
    },
    validation::signing::Unvalidated,
};
use monad_consensus_types::{
    block::{
        ConsensusBlockHeader, MockExecutionBody, MockExecutionProposedHeader, MockExecutionProtocol,
    },
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigColTuple, TimeoutCertificate, TimeoutInfo},
    validation::Error,
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey},
    hasher::Hash,
    NopKeyPair, NopPubKey, NopSignature,
};
use monad_multi_sig::MultiSig;
use monad_testutil::{
    signing::{MockSignatures, TestSigner},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetFactory, ValidatorSetType},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use test_case::test_case;

type SignatureType = NopSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;
type MockSignatureCollectionType = MockSignatures<SignatureType>;
type SignatureCollectionType = MultiSig<NopSignature>;
type ExecutionProtocolType = MockExecutionProtocol;

static NUM_NODES: u32 = 4;
static VAL_SET_UPDATE_INTERVAL: SeqNum = SeqNum(2000);
static EPOCH_START_DELAY: Round = Round(50);

fn setup_block(
    author: NodeId<PubKeyType>,
    block_epoch: Epoch,
    block_round: Round,
    block_seq_num: SeqNum,
    qc_epoch: Epoch,
    qc_round: Round,
    qc_parent_round: Round,
    seq_num: SeqNum,
    signers: &[PubKeyType],
) -> (
    ConsensusBlockHeader<SignatureType, MockSignatures<SignatureType>, ExecutionProtocolType>,
    ConsensusBlockBody<ExecutionProtocolType>,
) {
    let execution_body = MockExecutionBody {
        data: vec![1, 2, 3, 4].into(),
    };
    let vote = Vote {
        id: BlockId(Hash([0x00_u8; 32])),
        epoch: qc_epoch,
        round: qc_round,
    };

    let qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        vote,
        MockSignatures::with_pubkeys(signers),
    );

    let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner { execution_body });

    (
        ConsensusBlockHeader::new(
            author,
            block_epoch,
            block_round,
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            payload.get_id(),
            qc,
            block_seq_num,
            0,
            RoundSignature::new(
                block_round,
                &NopKeyPair::from_bytes(&mut [1_u8; 32]).unwrap(),
            ),
        ),
        payload,
    )
}
fn setup_val_state<SCT>(
    known_epoch: Epoch,
    known_round: Round,
    val_epoch: Epoch,
) -> (
    Vec<<SignatureType as CertificateSignature>::KeyPairType>,
    Vec<SignatureCollectionKeyPairType<SCT>>,
    EpochManager,
    ValidatorsEpochMapping<ValidatorSetFactory<CertificateSignaturePubKey<SignatureType>>, SCT>,
)
where
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<SignatureType>>,
{
    let (keypairs, certkeys, vset, vmap) = create_keys_w_validators::<SignatureType, SCT, _>(
        NUM_NODES,
        ValidatorSetFactory::default(),
    );

    let epoch_manager = EpochManager::new(
        VAL_SET_UPDATE_INTERVAL,
        EPOCH_START_DELAY,
        &[(known_epoch, known_round)],
    );
    let mut val_epoch_map: ValidatorsEpochMapping<_, SCT> =
        ValidatorsEpochMapping::new(ValidatorSetFactory::default());

    val_epoch_map.insert(
        val_epoch,
        vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        vmap,
    );

    (keypairs, certkeys, epoch_manager, val_epoch_map)
}

fn define_proposal_with_tc(
    known_epoch: Epoch,
    known_round: Round,
    val_epoch: Epoch,
    block_epoch: Epoch,
    block_round: Round,
    block_seq_num: SeqNum,
    qc_epoch: Epoch,
    qc_round: Round,
    qc_parent_round: Round,
    qc_seq_num: SeqNum,
    tc_epoch: Epoch,
    tc_round: Round,
) -> (
    Vec<NopKeyPair>,
    Vec<NopKeyPair>,
    EpochManager,
    ValidatorsEpochMapping<ValidatorSetFactory<NopPubKey>, MockSignatureCollectionType>,
    ProposalMessage<SignatureType, MockSignatureCollectionType, ExecutionProtocolType>,
) {
    let (keys, cert_keys, vset, vmap) = create_keys_w_validators::<
        SignatureType,
        MockSignatureCollectionType,
        _,
    >(NUM_NODES, ValidatorSetFactory::default());

    // create valid QC
    let vote = Vote {
        id: BlockId(Hash([0x09_u8; 32])),
        epoch: qc_epoch,
        round: qc_round,
    };

    let qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        vote,
        MockSignatures::with_pubkeys(
            keys.iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    );

    let high_qc_sig_tuple = HighQcRoundSigColTuple {
        high_qc_round: HighQcRound {
            qc_round: qc.get_round(),
        },
        sigs: MockSignatures::with_pubkeys(
            keys.iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    };

    let tc = TimeoutCertificate {
        epoch: tc_epoch, // wrong epoch here
        round: tc_round,
        high_qc_rounds: vec![high_qc_sig_tuple],
        _phantom: PhantomData,
    };

    // moved here because of valmap ownership
    let epoch_manager = EpochManager::new(
        VAL_SET_UPDATE_INTERVAL,
        EPOCH_START_DELAY,
        &[(known_epoch, known_round)],
    );
    let mut val_epoch_map: ValidatorsEpochMapping<
        ValidatorSetFactory<_>,
        MockSignatureCollectionType,
    > = ValidatorsEpochMapping::new(ValidatorSetFactory::default());

    val_epoch_map.insert(
        val_epoch,
        vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        vmap,
    );

    let author = NodeId::new(keys[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[keys[0].pubkey(), keys[1].pubkey(), keys[2].pubkey()],
    );

    let proposal = ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: Some(tc),
    };

    (keys, cert_keys, epoch_manager, val_epoch_map, proposal)
}

// test_verify tests hit all error messages in order of appearence
// in the verify function. The error messages and their
// related tests are the following:
//  - test_verify_incorrect_block_epoch - Error::InvalidEpoch
//  - test_verify_incorrect_validator_epoch - Error::ValidatorSetDataUnavailable
//  - test_verify_invalid_author - Error::InvalidAuthor
//  - test_verify_invalid_signature - Error::InvalidSignature
//  - test_verify_author_not_sender - Error::AuthorNotSender
//  - test_verify_proposal_happy - Success

// epoch determined by block round does not exist in epoch manager
#[test_case(Round(300), Round(234))]
#[test_case(Round(9889), Round(8888))]
fn test_verify_incorrect_block_epoch(known_round: Round, block_round: Round) {
    let known_epoch = Epoch(2);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_seq_num = SeqNum(110);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = Round(0);
    let qc_seq_num = SeqNum(0);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);

    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: 1,
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, &keypairs[0]);

    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &keypairs[0].pubkey()),
        Err(Error::InvalidEpoch)
    );
}

// sender is not equivalent to signer
#[test]
fn test_verify_author_not_sender() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(234);
    let block_seq_num = SeqNum(110);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = Round(0);
    let qc_seq_num = SeqNum(0);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);

    let author_keypair = &keypairs[0];
    let sender_keypair = &keypairs[1]; // this causes error
    let author = NodeId::new(author_keypair.pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    let conmsg = ConsensusMessage {
        version: 1,
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, author_keypair);
    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &sender_keypair.pubkey())
            .unwrap_err(),
        Error::AuthorNotSender
    );
}
// signed message is different than message to verify
#[test]
fn test_verify_invalid_signature() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(234);
    let block_seq_num = SeqNum(110);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = Round(0);
    let qc_seq_num = SeqNum(0);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);

    let author_keypair = &keypairs[0];
    let author = NodeId::new(author_keypair.pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    let (other_block, other_payload) = setup_block(
        author,
        Epoch(3),
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let other_proposal = ProtocolMessage::Proposal(ProposalMessage {
        block_header: other_block,
        block_body: other_payload,
        last_round_tc: None,
    });

    let conmsg = ConsensusMessage {
        version: 1,
        message: proposal,
    };
    let other_msg = ConsensusMessage {
        version: 1,
        message: other_proposal,
    };
    // this causes error
    let sp = TestSigner::<SignatureType>::sign_incorrect_object(other_msg, conmsg, author_keypair);
    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &author_keypair.pubkey())
            .unwrap_err(),
        Error::InvalidSignature
    );
}
// happy path for verification (fuzz target)
#[test]
fn test_verify_proposal_happy() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = Epoch(1);
    let block_round = known_round + Round(2344);
    let block_seq_num = SeqNum(110);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = Round(0);
    let qc_seq_num = SeqNum(0);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);
    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    let conmsg = ConsensusMessage {
        version: 1,
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, &keypairs[0]);

    assert!(sp
        .verify(&epoch_manager, &val_epoch_map, &keypairs[0].pubkey())
        .is_ok());
}

// The test_validate set hits all error messages in order appearence
// in the validate function in the case there exists no TC.
//
// The error messages and their related tests are the following:
//  - test_validate_missing_tc - Error::NotWellFormed
//  - test_validate_incorrect_block_epoch - Error::InvalidEpoch
//  - test_validate_qc_epoch - Error::InvalidEpoch
//  - test_validate_mismatch_qc_epoch - Error::InvalidEpoch
//  - test_proposal_invalid_qc_validator_set - Error::ValidatorSetDataUnavailable
//  - test_validate_insufficient_qc_stake - Error::InsufficientStake
//  - test_validate_qc_happy - happy path
//
// These tests do not reach Error::InvalidSignature due to mocksignature collection
// when a proposal message contains a QC.

// block round is not either QC seq num + 1 or TC block round + 1
#[test_case(Round(20))]
#[test_case(Round(233))]
fn test_validate_missing_tc(qc_round: Round) {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);
    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::NotWellFormed)
    );
}

// block epoch is not equal to epoch determined by block round
#[test_case(Epoch(4), Epoch(1))]
#[test_case(Epoch(11), Epoch(3))]
fn test_validate_incorrect_block_epoch(known_epoch: Epoch, block_epoch: Epoch) {
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(1);
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);

    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidEpoch)
    );
}
// epoch corresponding to qc round does not exist
#[test]
fn test_validate_qc_epoch() {
    let known_epoch = Epoch(4);
    let known_round = Round(30);
    let val_epoch = Epoch(1);

    let block_epoch = known_epoch;
    let block_round = known_round;
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(4);
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);
    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidEpoch)
    );
}
// qc epoch is not equal to local epoch corresponding to qc round
#[test]
fn test_validate_mismatch_qc_epoch() {
    let known_epoch = Epoch(1); // this causes error
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = known_epoch;
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(2); // this causes error
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);
    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidEpoch)
    );
}
// validators do not exist for correspond qc epoch
#[test]
fn test_proposal_invalid_qc_validator_set() {
    let known_epoch = Epoch(1); // this causes error
    let known_round = Round(0);
    let val_epoch = Epoch(2); // this causes error

    let block_epoch = known_epoch;
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);
    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::ValidatorSetDataUnavailable)
    );
}
// QC has insufficient stake
#[test]
fn test_validate_insufficient_qc_stake() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);
    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[keypairs[0].pubkey()],
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InsufficientStake)
    );
}
// validate happy path for empty TC
#[test]
fn test_validate_qc_happy() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);

    let author = NodeId::new(keypairs[0].pubkey());

    let (block, payload) = setup_block(
        author,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        &[
            keypairs[0].pubkey(),
            keypairs[1].pubkey(),
            keypairs[2].pubkey(),
        ],
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: None,
    });

    assert!(proposal.validate(&epoch_manager, &val_epoch_map).is_ok());
}

// The test_validate_tc set hits all error messages in sequential order
// of the validate function in the case there exists a TC.
//
// The error messages and their related tests are the following:
//  - test_validate_tc_invalid_round_block - Error::NotWellFormed
//  - test_validate_tc_invalid_epoch - Error::InvalidEpoch
//  - test_validate_tc_incorrect_epoch - Error::InvalidEpoch
//  - test_validate_tc_invalid_val_set - Error::ValidatorSetDataUnavailable
//  - test_validate_tc_invalid_round - Error::InvalidTcRound
//  - test_validate_tc_invalid_tc_signature - Error::InvalidSignature
//  - test_validate_tc_happy - happy path

// TC round is not 1 behind block round
#[test_case(Round(3), Round(1))]
#[test_case(Round(123), Round(321))]
fn test_validate_tc_invalid_round_block(block_round: Round, tc_round: Round) {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = Round(0);
    let qc_parent_round = block_round - Round(3);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(3);

    let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
        known_epoch,
        known_round,
        val_epoch,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        tc_epoch,
        tc_round,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::NotWellFormed)
    );
}
// TC Epoch does not exist as determined by tc.round
#[test]
fn test_validate_tc_invalid_epoch() {
    let known_epoch = Epoch(10);
    let known_round = Round(10);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(10);
    let tc_round = Round(9);

    let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
        known_epoch,
        known_round,
        val_epoch,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        tc_epoch,
        tc_round,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidEpoch)
    );
}
// TC Epoch does not match local Epoch
#[test]
fn test_validate_tc_incorrect_epoch() {
    let known_epoch = Epoch(1);
    let known_round = Round(1);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(3);
    let tc_round = Round(1);

    let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
        known_epoch,
        known_round,
        val_epoch,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        tc_epoch,
        tc_round,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidEpoch)
    );
}
// TC Epoch does not determine a validator set
#[test]
fn test_validate_tc_invalid_val_set() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(0);

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = val_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = known_epoch;
    let tc_round = Round(0);

    let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
        known_epoch,
        known_round,
        val_epoch,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        tc_epoch,
        tc_round,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::ValidatorSetDataUnavailable)
    );
}
// High QC round is larger than TC round
#[test]
fn test_validate_tc_invalid_round() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(1);
    let tc_round = Round(0);

    let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
        known_epoch,
        known_round,
        val_epoch,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        tc_epoch,
        tc_round,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidTcRound)
    );
}
// validate happy path for TC case
#[test]
fn test_validate_tc_happy() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(2);
    let qc_parent_round = block_round - Round(3);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(1);
    let tc_round = block_round - Round(1);

    let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
        known_epoch,
        known_round,
        val_epoch,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        tc_epoch,
        tc_round,
    );

    let proposal = Unvalidated::new(proposal);
    assert!(proposal.validate(&epoch_manager, &val_epoch_map).is_ok());
}

// Mismatch between TC and tminfo
#[test]
fn test_validate_tc_invalid_tc_signature() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(1);
    let block_seq_num = SeqNum(1);

    // a malformed tmo_info
    let tc_epoch = Epoch(1);
    let tc_round = Round(1);
    let tc_epoch_signed = Epoch(2);
    let tc_round_signed = Round(2);

    let tmo_info = TimeoutInfo::<SignatureCollectionType> {
        epoch: tc_epoch_signed,
        round: tc_round_signed,
        high_qc: QuorumCertificate::genesis_qc(),
    };

    let tmo_digest = alloy_rlp::encode(tmo_info.timeout_digest());

    let (keys, certkeys, epoch_manager, val_epoch_map) =
        setup_val_state(known_epoch, known_round, val_epoch);

    let val_map = val_epoch_map.get_cert_pubkeys(&known_epoch).unwrap();

    let mut sigs = Vec::new();

    for (keypair, certkey) in keys.iter().zip(certkeys.iter()) {
        let s = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(
           tmo_digest.as_ref(),
           certkey,
       );
        sigs.push((NodeId::new(keypair.pubkey()), s));
    }

    let sigcol = SignatureCollectionType::new(sigs, val_map, tmo_digest.as_ref()).unwrap();

    let tc = TimeoutCertificate {
        epoch: tc_epoch,
        round: tc_round,
        high_qc_rounds: vec![HighQcRoundSigColTuple {
            high_qc_round: HighQcRound {
                qc_round: QuorumCertificate::<SignatureCollectionType>::genesis_qc().get_round(),
            },
            sigs: sigcol,
        }],
        _phantom: PhantomData,
    };

    let author = NodeId::new(keys[0].pubkey());

    let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
        execution_body: MockExecutionBody {
            data: vec![1, 2, 3, 4].into(),
        },
    });
    let block: ConsensusBlockHeader<NopSignature, MultiSig<NopSignature>, MockExecutionProtocol> =
        ConsensusBlockHeader::new(
            author,
            block_epoch,
            block_round,
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            payload.get_id(),
            QuorumCertificate::genesis_qc(),
            block_seq_num,
            0,
            RoundSignature::new(
                block_round,
                &NopKeyPair::from_bytes(&mut [1_u8; 32]).unwrap(),
            ),
        );

    let proposal = ProposalMessage {
        block_header: block,
        block_body: payload,
        last_round_tc: Some(tc),
    };
    let proposal = Unvalidated::new(proposal);

    assert!(matches!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidSignature)
    ));
}

// This test has a proposal message whose round corresponds
// to genesis but does not contain Genesis QC
#[test]
fn test_validate_genesis_sig() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = Round(0);
    let qc_parent_round = block_round - Round(3);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(1);
    let tc_round = block_round - Round(1);

    let (_, _, epoch_manager, val_epoch_map, proposal) = define_proposal_with_tc(
        known_epoch,
        known_round,
        val_epoch,
        block_epoch,
        block_round,
        block_seq_num,
        qc_epoch,
        qc_round,
        qc_parent_round,
        qc_seq_num,
        tc_epoch,
        tc_round,
    );
    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidSignature)
    );
}
