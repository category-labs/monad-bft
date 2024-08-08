use monad_consensus::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    },
    validation::signing::{Validated, Verified},
};
use monad_consensus_types::{
    ledger::CommitResult,
    payload::{ExecutionProtocol, FullTransactionList, TransactionPayload},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hash, Hasher, HasherType},
};
use monad_multi_sig::MultiSig;
use monad_state::{
    convert::interface::{deserialize_monad_message, serialize_verified_monad_message},
    MonadMessage, VerifiedMonadMessage,
};
use monad_testutil::{block::setup_block, validators::create_keys_w_validators};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetFactory, ValidatorSetType},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

fn make_tc<
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
>(
    tc_epoch: Epoch,
    tc_round: Round,
    high_qc_round: HighQcRound,
    keys: &[ST::KeyPairType],
    certkeys: &[SignatureCollectionKeyPairType<SCT>],
    validator_mapping: &ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
) -> TimeoutCertificate<SCT> {
    let mut hasher = HasherType::new();
    hasher.update(tc_epoch);
    hasher.update(tc_round);
    hasher.update(high_qc_round.qc_round);

    let tmo_digest = hasher.hash();

    let mut tc_sigs = Vec::new();
    for (key, certkey) in keys.iter().zip(certkeys.iter()) {
        let node_id = NodeId::new(key.pubkey());
        let sig = <SCT::SignatureType as CertificateSignature>::sign(tmo_digest.as_ref(), certkey);
        tc_sigs.push((node_id, sig));
    }

    let sig_col = SCT::new(tc_sigs, validator_mapping, tmo_digest.as_ref()).unwrap();

    TimeoutCertificate {
        epoch: tc_epoch,
        round: tc_round,
        high_qc_rounds: vec![HighQcRoundSigColTuple {
            high_qc_round,
            sigs: sig_col,
        }],
    }
}

macro_rules! test_all_combination {
    ($test_name:ident, $test_code:expr) => {
        mod $test_name {
            use monad_bls::BlsSignatureCollection;
            use monad_crypto::NopSignature;
            use monad_secp::SecpSignature;
            use test_case::test_case;

            use super::*;

            fn invoke<
                ST: CertificateSignatureRecoverable,
                SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
            >(
                num_keys: u32,
            ) {
                // to supress linter warning about unused ST
                let _: Option<ST> = None;
                $test_code(num_keys)
            }

            #[test_case(1; "1 sig")]
            #[test_case(5; "5 sigs")]
            #[test_case(100; "100 sigs")]
            fn secp_multi_sig_secp(num_keys: u32) {
                invoke::<SecpSignature, MultiSig<SecpSignature>>(num_keys);
            }

            #[test_case(1; "1 sig")]
            #[test_case(5; "5 sigs")]
            #[test_case(100; "100 sigs")]
            fn secp_bls(num_keys: u32) {
                invoke::<
                    SecpSignature,
                    BlsSignatureCollection<CertificateSignaturePubKey<SecpSignature>>,
                >(num_keys);
            }

            #[test_case(1; "1 sig")]
            #[test_case(5; "5 sigs")]
            #[test_case(100; "100 sigs")]
            fn nop_multi_sig_nop(num_keys: u32) {
                invoke::<NopSignature, MultiSig<NopSignature>>(num_keys);
            }

            #[test_case(1; "1 sig")]
            #[test_case(5; "5 sigs")]
            #[test_case(100; "100 sigs")]
            fn nop_bls(num_keys: u32) {
                invoke::<
                    NopSignature,
                    BlsSignatureCollection<CertificateSignaturePubKey<NopSignature>>,
                >(num_keys);
            }
        }
    };
}

// TODO-4: revisit to cleanup
test_all_combination!(test_vote_message, |num_keys| {
    let (keypairs, certkeys, validators, validator_mapping) =
        create_keys_w_validators::<ST, SCT, _>(num_keys, ValidatorSetFactory::default());
    let version = "TEST";
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        validators
            .get_members()
            .iter()
            .map(|(a, b)| (*a, *b))
            .collect(),
        validator_mapping,
    );

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        epoch: Epoch(1),
        round: Round(2),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(1),
        seq_num: SeqNum(0),
        timestamp: 0,
    };

    let vote = Vote {
        vote_info: vi,
        ledger_commit_info: CommitResult::Commit,
    };

    let votemsg = ProtocolMessage::Vote(VoteMessage::<SCT>::new(vote, &certkeys[0]));
    let conmsg = ConsensusMessage {
        version: version.into(),
        message: votemsg.clone(),
    };

    let author_keypair = &keypairs[0];

    let verified_votemsg = Verified::<ST, _>::new(Validated::new(conmsg), author_keypair);

    let verified_monad_message = VerifiedMonadMessage::Consensus(verified_votemsg);
    let monad_message: MonadMessage<_, _> = verified_monad_message.clone().into();

    let rx_buf = serialize_verified_monad_message(&verified_monad_message);
    let rx_msg = deserialize_monad_message(rx_buf).unwrap();

    assert_eq!(rx_msg, monad_message);
    assert!(matches!(rx_msg, MonadMessage::Consensus(_)));

    if let MonadMessage::Consensus(rx_vote) = rx_msg {
        let rx_vote = rx_vote
            .verify(&epoch_manager, &val_epoch_map, &author_keypair.pubkey())
            .unwrap()
            .destructure()
            .2
            .validate(&epoch_manager, &val_epoch_map, version)
            .unwrap()
            .into_inner();
        assert_eq!(rx_vote, votemsg);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_timeout_message, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<ST, SCT, _>(num_keys, ValidatorSetFactory::default());
    let version = "TEST";
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        validators
            .get_members()
            .iter()
            .map(|(a, b)| (*a, *b))
            .collect(),
        validator_mapping,
    );
    let validator_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();

    let author_keypair = &keypairs[0];
    let author_cert_key = &cert_keys[0];

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        epoch: epoch_manager.get_epoch(Round(1)).expect("epoch exists"),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
        timestamp: 0,
    };

    let qcinfo = QcInfo {
        vote: Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
        },
    };

    let qcinfo_hash = HasherType::hash_object(&qcinfo.vote);

    let mut sigs = Vec::new();

    for i in 0..cert_keys.len() {
        let node_id = NodeId::new(keypairs[i].pubkey());
        let sig =
            <SCT::SignatureType as CertificateSignature>::sign(qcinfo_hash.as_ref(), &cert_keys[i]);
        sigs.push((node_id, sig));
    }

    let sigcol = SCT::new(sigs, validator_mapping, qcinfo_hash.as_ref()).unwrap();

    let qc = QuorumCertificate::new(qcinfo, sigcol);

    // timeout certificate for Round(2)
    // timeout message for Round(3)
    // TODO-3: add more high_qc_rounds
    let tc = make_tc::<ST, SCT>(
        epoch_manager.get_epoch(Round(2)).expect("epoch exists"),
        Round(2),
        HighQcRound { qc_round: Round(1) },
        keypairs.as_slice(),
        cert_keys.as_slice(),
        validator_mapping,
    );

    let tmo_info = TimeoutInfo {
        epoch: epoch_manager.get_epoch(Round(3)).expect("epoch exists"),
        round: Round(3),
        high_qc: qc,
    };
    let tmo = Timeout {
        tminfo: tmo_info,
        last_round_tc: Some(tc),
    };

    let tmo_message = ProtocolMessage::Timeout(TimeoutMessage::new(tmo, author_cert_key));
    let con_msg = ConsensusMessage {
        version: version.into(),
        message: tmo_message.clone(),
    };

    let verified_tmo_message = Verified::<ST, _>::new(Validated::new(con_msg), author_keypair);

    let verified_monad_message = VerifiedMonadMessage::Consensus(verified_tmo_message);
    let monad_message: MonadMessage<_, _> = verified_monad_message.clone().into();

    let rx_buf = serialize_verified_monad_message(&verified_monad_message);
    let rx_msg = deserialize_monad_message(rx_buf).unwrap();

    assert_eq!(rx_msg, monad_message);
    assert!(matches!(rx_msg, MonadMessage::Consensus(_)));

    if let MonadMessage::Consensus(rx_tmo) = rx_msg {
        let rx_tmo = rx_tmo
            .verify(&epoch_manager, &val_epoch_map, &author_keypair.pubkey())
            .unwrap()
            .destructure()
            .2
            .validate(&epoch_manager, &val_epoch_map, version)
            .unwrap()
            .into_inner();
        assert_eq!(tmo_message, rx_tmo);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_proposal_qc, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<ST, SCT, _>(num_keys, ValidatorSetFactory::default());
    let version = "TEST";
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        validators
            .get_members()
            .iter()
            .map(|(a, b)| (*a, *b))
            .collect(),
        validator_mapping,
    );
    let validator_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();

    let author_keypair = &keypairs[0];
    let blk = setup_block::<ST, SCT>(
        NodeId::new(author_keypair.pubkey()),
        Round(233),
        Round(232),
        BlockId(Hash([43_u8; 32])),
        TransactionPayload::List(FullTransactionList::new(vec![1, 2, 3, 4].into())),
        ExecutionProtocol::zero(),
        cert_keys.as_slice(),
        validator_mapping,
    );
    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: blk,
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: version.into(),
        message: proposal.clone(),
    };
    let verified_msg = Verified::<ST, _>::new(Validated::new(conmsg), author_keypair);
    let verified_monad_message = VerifiedMonadMessage::Consensus(verified_msg);
    let monad_message: MonadMessage<_, _> = verified_monad_message.clone().into();

    let rx_buf = serialize_verified_monad_message(&verified_monad_message);
    let rx_msg = deserialize_monad_message(rx_buf).unwrap();

    assert_eq!(rx_msg, monad_message);
    assert!(matches!(rx_msg, MonadMessage::Consensus(_)));

    if let MonadMessage::Consensus(rx_prop) = rx_msg {
        let rx_prop = rx_prop
            .verify(&epoch_manager, &val_epoch_map, &author_keypair.pubkey())
            .unwrap()
            .destructure()
            .2
            .validate(&epoch_manager, &val_epoch_map, version)
            .unwrap()
            .into_inner();
        assert_eq!(proposal, rx_prop);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_proposal_tc, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<ST, SCT, _>(num_keys, ValidatorSetFactory::default());
    let version = "TEST";
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
    let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
    val_epoch_map.insert(
        Epoch(1),
        validators
            .get_members()
            .iter()
            .map(|(a, b)| (*a, *b))
            .collect(),
        validator_mapping,
    );
    let validator_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();

    let author_keypair = &keypairs[0];
    let blk = setup_block::<ST, SCT>(
        NodeId::new(author_keypair.pubkey()),
        Round(233),
        Round(231),
        BlockId(Hash([43_u8; 32])),
        TransactionPayload::List(FullTransactionList::new(vec![1, 2, 3, 4].into())),
        ExecutionProtocol::zero(),
        cert_keys.as_slice(),
        validator_mapping,
    );

    let tc_round = Round(232);
    let high_qc_round = HighQcRound {
        qc_round: Round(231),
    };

    let tc = make_tc::<ST, SCT>(
        epoch_manager.get_epoch(tc_round).expect("epoch exists"),
        tc_round,
        high_qc_round,
        keypairs.as_slice(),
        cert_keys.as_slice(),
        validator_mapping,
    );

    let proposal_msg = ProtocolMessage::Proposal(ProposalMessage {
        block: blk,
        last_round_tc: Some(tc),
    });
    let con_msg = ConsensusMessage {
        version: version.into(),
        message: proposal_msg.clone(),
    };
    let verified_msg = Verified::<ST, _>::new(Validated::new(con_msg), author_keypair);

    let verified_monad_message = VerifiedMonadMessage::Consensus(verified_msg);
    let monad_message: MonadMessage<_, _> = verified_monad_message.clone().into();

    let rx_buf = serialize_verified_monad_message(&verified_monad_message);
    let rx_msg = deserialize_monad_message(rx_buf).unwrap();

    assert_eq!(rx_msg, monad_message);
    assert!(matches!(rx_msg, MonadMessage::Consensus(_)));

    if let MonadMessage::Consensus(rx_prop) = rx_msg {
        let rx_prop = rx_prop
            .verify(&epoch_manager, &val_epoch_map, &author_keypair.pubkey())
            .unwrap()
            .destructure()
            .2
            .validate(&epoch_manager, &val_epoch_map, version)
            .unwrap()
            .into_inner();
        assert_eq!(proposal_msg, rx_prop);
    } else {
        unreachable!()
    }
});
