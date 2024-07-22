use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::ProposalMessage,
    },
    validation::signing::Unvalidated,
};
use monad_consensus_types::{
    ledger::CommitResult,
    quorum_certificate::{QcInfo, QuorumCertificate},
    timeout::{HighQcRound, HighQcRoundSigColTuple, TimeoutCertificate},
    validation::Error,
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{certificate_signature::CertificateKeyPair, hasher::Hash, NopSignature};
use monad_testutil::{
    block::set_block_and_qc,
    proposal::define_proposal_with_tc,
    signing::{get_key, MockSignatures, TestSigner},
    validators::{create_keys_w_validators, setup_val_state},
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, Stake};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;

static _NUM_NODES: u32 = 4;
static _VAL_SET_UPDATE_INTERVAL: SeqNum = SeqNum(2000);
static _EPOCH_START_DELAY: Round = Round(50);

// epoch determined by block round does not exist in epoch manager
#[test]
fn test_verify_incorrect_block_epoch() {
    let known_epoch = Epoch(2);
    let known_round = Round(300);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(234);
    let block_seq_num = SeqNum(110);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = Round(0);
    let qc_seq_num = SeqNum(0);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
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

    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &keypairs[0].pubkey()),
        Err(Error::InvalidEpoch)
    );
}
// validator set determined by block round does not exist
#[test]
fn test_verify_incorrect_validator_epoch() {
    let known_epoch = Epoch(2);
    let known_round = Round(200);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(234);
    let block_seq_num = SeqNum(110);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = Round(0);
    let qc_seq_num = SeqNum(0);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
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

    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &keypairs[0].pubkey()),
        Err(Error::ValidatorSetDataUnavailable)
    );
}
//block proposer is not a valid signer
#[test]
fn test_verify_invalid_author() {
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

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let non_valdiator_keypair = get_key::<SignatureType>(7);
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            &[keypairs[0].pubkey(), non_valdiator_keypair.pubkey()],
        ),
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, &non_valdiator_keypair);

    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &keypairs[0].pubkey())
            .unwrap_err(),
        Error::InvalidAuthor
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

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author_keypair = &keypairs[0];
    let sender_keypair = &keypairs[1];
    let author = NodeId::new(author_keypair.pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
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
// signed message is different then message to verify
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

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let author_keypair = &keypairs[0];
    let author = NodeId::new(author_keypair.pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            keypairs
                .iter()
                .map(|keypair| keypair.pubkey())
                .collect::<Vec<_>>()
                .as_ref(),
        ),
        last_round_tc: None,
    });

    let other_proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            Epoch(3),
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
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
    let other_msg = ConsensusMessage {
        version: "TEST".into(),
        message: other_proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_incorrect_object(other_msg, conmsg, author_keypair);
    assert_eq!(
        sp.verify(&epoch_manager, &val_epoch_map, &author_keypair.pubkey())
            .unwrap_err(),
        Error::InvalidSignature
    );
}
// happy path for verification. can be used to debug bad proposals
#[test]
fn test_verify_proposal_hash() {
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

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
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
// block seq num is not QC seq num + 1
#[test]
fn test_validate_invalid_seq_num() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(234);
    let block_seq_num = SeqNum(110);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = Round(0);
    let qc_seq_num = block_seq_num - SeqNum(2);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidSeqNum)
    );
}
// block round is not either QC seq num + 1 or TC block round + 1
#[test]
fn test_validate_missing_tc() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(1);
    let qc_round = Round(233);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::NotWellFormed)
    );
}
// block epoch is not equal to epoch determined by block round
#[test]
fn test_validate_incorrect_block_epoch() {
    let known_epoch = Epoch(4);
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(1);
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());

    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            &[keypairs[0].pubkey()],
        ),
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
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = Epoch(1);
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(1);
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());
    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            &[keypairs[0].pubkey()],
        ),
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
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(1);

    let block_epoch = known_epoch;
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = Epoch(2);
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());
    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            &[keypairs[0].pubkey()],
        ),
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
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = Epoch(2);

    let block_epoch = known_epoch;
    let block_round = Round(23);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());
    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            &[keypairs[0].pubkey()],
        ),
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::ValidatorSetDataUnavailable)
    );
}
// QC has insufficent stake
#[test]
fn test_validate_insufficent_qc_stake() {
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

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            NodeId::new(keypairs[0].pubkey()),
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            &[keypairs[0].pubkey()],
        ),
        last_round_tc: None,
    });

    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InsufficientStake)
    );
}
// validate happy path for empty TC. can be used to debug bad proposals
#[test]
fn test_validate_qc_fuzz() {
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

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(ProposalMessage {
        block: set_block_and_qc(
            NodeId::new(keypairs[0].pubkey()),
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
                keypairs[3].pubkey(),
            ],
        ),
        last_round_tc: None,
    });

    assert!(proposal.validate(&epoch_manager, &val_epoch_map).is_ok());
}
// QC has invalid seq_num
#[test]
fn test_validate_tc_invalid_seq_num() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num;

    let tc_epoch = Epoch(3);
    let tc_round = Round(2);
    let tc_epoch_signed = Epoch(1);
    let tc_round_signed = known_round + Round(3);

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidSeqNum)
    );
}
// TC round is not 1 behind block round
#[test]
fn test_validate_tc_invalid_round_block() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(3);
    let qc_parent_round = block_round - Round(3);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(3);
    let tc_round = Round(1);
    let tc_epoch_signed = Epoch(1);
    let tc_round_signed = known_round + Round(3);

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::NotWellFormed)
    );
}
// TC Epoch does not exist
#[test]
fn test_validate_tc_incorrect_epoch() {
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

    let tc_epoch = Epoch(3);
    let tc_round = Round(2);
    let tc_epoch_signed = Epoch(1);
    let tc_round_signed = known_round + Round(3);

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidEpoch)
    );
}
// TC Epoch does not match local Epoch
#[test]
fn test_validate_tc_invalid_epoch() {
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
    let tc_round = Round(0);
    let tc_epoch_signed = Epoch(1);
    let tc_round_signed = known_round + Round(3);

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
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

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(2);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(1);
    let tc_round = Round(0);
    let tc_epoch_signed = Epoch(1);
    let tc_round_signed = known_round + Round(3);

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::ValidatorSetDataUnavailable)
    );
}
// TC round does is smaller or equal to QC round
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
    let tc_epoch_signed = Epoch(1);
    let tc_round_signed = known_round + Round(3);

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidTcRound)
    );
}
// Signed TC message is not equivalent to TC message
#[test]
fn test_validate_tc_sig() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(3);
    let block_seq_num = SeqNum(2);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(3);
    let qc_parent_round = block_round - Round(3);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(1);
    let tc_round = block_round - Round(1);
    let tc_epoch_signed = Epoch(1);
    let tc_round_signed = known_round + Round(3);

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(proposal);
    assert_eq!(
        proposal.validate(&epoch_manager, &val_epoch_map),
        Err(Error::InvalidSignature)
    );
}
// validate happy path for TC case. can be used to debug bad proposal message
#[test]
fn test_validate_tc_fuzz() {
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
    let tc_epoch_signed = tc_epoch;
    let tc_round_signed = tc_round;

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
        tc_epoch_signed,
        tc_round_signed,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );

    let proposal = Unvalidated::new(proposal);
    assert!(proposal.validate(&epoch_manager, &val_epoch_map).is_ok());
}
// degenerate case: Valid QC with an Old TC
#[test]
fn test_validate_valid_qc_old_tc() {
    let known_epoch = Epoch(1);
    let known_round = Round(0);
    let val_epoch = known_epoch;

    let block_epoch = known_epoch;
    let block_round = known_round + Round(10);
    let block_seq_num = SeqNum(4);

    let qc_epoch = known_epoch;
    let qc_round = block_round - Round(1);
    let qc_parent_round = block_round - Round(5);
    let qc_seq_num = block_seq_num - SeqNum(1);

    let tc_epoch = Epoch(1);

    let (keys, _, _, _) = create_keys_w_validators::<SignatureType, SignatureCollectionType, _>(
        4,
        ValidatorSetFactory::default(),
    );

    let mut vlist = Vec::new();
    let mut vmap_vec = Vec::new();

    for keypair in &keys {
        let node_id = NodeId::new(keypair.pubkey());

        vlist.push((node_id, Stake(1)));
        vmap_vec.push((node_id, keypair.pubkey()));
    }

    let _vset = ValidatorSetFactory::default().create(vlist).unwrap();

    // create old QC
    let old_vi = VoteInfo {
        id: BlockId(Hash([0x09_u8; 32])),
        epoch: qc_epoch,
        round: qc_round - Round(3),
        parent_id: BlockId(Hash([0x00_u8; 32])),
        parent_round: qc_parent_round - Round(4),
        seq_num: qc_seq_num - SeqNum(2),
    };

    let old_qc = QuorumCertificate::<MockSignatures<SignatureType>>::new(
        QcInfo {
            vote: Vote {
                vote_info: old_vi,
                ledger_commit_info: CommitResult::Commit,
            },
        },
        MockSignatures::with_pubkeys(
            keys.iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    );

    let high_qc_sig_tuple = HighQcRoundSigColTuple {
        high_qc_round: HighQcRound {
            qc_round: old_qc.get_round(),
        },
        sigs: MockSignatures::with_pubkeys(
            keys.iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    };
    // Create Old TC
    let tc = TimeoutCertificate {
        epoch: tc_epoch,
        round: old_qc.get_round() + Round(1),
        high_qc_rounds: vec![high_qc_sig_tuple],
    };
    // moved here because of valmap ownership
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(known_epoch, known_round)]);
    let mut val_epoch_map: ValidatorsEpochMapping<ValidatorSetFactory<_>, SignatureCollectionType> =
        ValidatorsEpochMapping::new(ValidatorSetFactory::default());

    val_epoch_map.insert(
        val_epoch,
        _vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        ValidatorMapping::new(vmap_vec),
    );

    let author = NodeId::new(keys[0].pubkey());
    let block = set_block_and_qc(
        author,
        block_epoch,
        block_round + Round(1),
        block_seq_num,
        qc_epoch,
        qc_round + Round(1),
        qc_parent_round + Round(1),
        qc_seq_num,
        &[keys[0].pubkey(), keys[1].pubkey(), keys[2].pubkey()],
    );

    let proposal = ProposalMessage {
        block,
        last_round_tc: Some(tc),
    };
    let proposal = Unvalidated::new(proposal);
    assert!(proposal.validate(&epoch_manager, &val_epoch_map).is_ok());
}
