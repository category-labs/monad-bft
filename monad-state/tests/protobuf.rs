use bytes::Bytes;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{
            BlockSyncResponseMessage, ProposalMessage, RequestBlockSyncMessage, TimeoutMessage,
            VoteMessage,
        },
    },
    validation::signing::{Validated, Verified},
};
use monad_consensus_types::{
    block::UnverifiedFullBlock,
    certificate_signature::CertificateSignature,
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, FullTransactionList, TransactionHashList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    hasher::{Hash, Hasher, HasherType},
    secp256k1::KeyPair,
};
use monad_epoch::epoch_manager::EpochManager;
use monad_state::{
    convert::interface::{deserialize_monad_message, serialize_verified_monad_message},
    MonadMessage, VerifiedMonadMessage,
};
use monad_testutil::{block::setup_block, validators::create_keys_w_validators};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};
use monad_validator::validators_epoch_map::ValidatorsEpochMapping;

fn make_tc<SCT: SignatureCollection>(
    tc_round: Round,
    high_qc_round: HighQcRound,
    keys: &[KeyPair],
    certkeys: &[SignatureCollectionKeyPairType<SCT>],
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
) -> TimeoutCertificate<SCT> {
    let mut hasher = HasherType::new();
    hasher.update(tc_round);
    hasher.update(high_qc_round.qc_round);

    let tmo_digest = hasher.hash();

    let mut tc_sigs = Vec::new();
    for (key, certkey) in keys.iter().zip(certkeys.iter()) {
        let node_id = NodeId(key.pubkey());
        let sig = <SCT::SignatureType as CertificateSignature>::sign(tmo_digest.as_ref(), certkey);
        tc_sigs.push((node_id, sig));
    }

    let sig_col = SCT::new(tc_sigs, validator_mapping, tmo_digest.as_ref()).unwrap();

    TimeoutCertificate {
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
            use monad_consensus_types::{
                bls::BlsSignatureCollection, message_signature::MessageSignature,
            };
            use monad_crypto::{secp256k1::SecpSignature, NopSignature};
            use test_case::test_case;

            use super::*;

            fn invoke<ST: MessageSignature, SCT: SignatureCollection>(num_keys: u32) {
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
            fn secp_multi_sig_nop(num_keys: u32) {
                invoke::<SecpSignature, MultiSig<NopSignature>>(num_keys);
            }

            #[test_case(1; "1 sig")]
            #[test_case(5; "5 sigs")]
            #[test_case(100; "100 sigs")]
            fn secp_bls(num_keys: u32) {
                invoke::<SecpSignature, BlsSignatureCollection>(num_keys);
            }

            #[test_case(1; "1 sig")]
            #[test_case(5; "5 sigs")]
            #[test_case(100; "100 sigs")]
            fn nop_multi_sig_secp(num_keys: u32) {
                invoke::<NopSignature, MultiSig<SecpSignature>>(num_keys);
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
                invoke::<NopSignature, BlsSignatureCollection>(num_keys);
            }
        }
    };
}

// TODO-4: revisit to cleanup
test_all_combination!(test_vote_message, |num_keys| {
    let (keypairs, certkeys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50));
    let mut val_epoch_map = ValidatorsEpochMapping::new();
    val_epoch_map.insert(Epoch(1), validators, validator_mapping);

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
    };
    let lci = LedgerCommitInfo {
        commit_state_hash: None,
        vote_info_hash: Hash([42_u8; 32]),
    };

    let vote = Vote {
        vote_info: vi,
        ledger_commit_info: lci,
    };

    let votemsg = ConsensusMessage::Vote(VoteMessage::<SCT>::new(vote, &certkeys[0]));

    let author_keypair = &keypairs[0];

    let verified_votemsg =
        Verified::<NopSignature, _>::new(Validated::new(votemsg.clone()), author_keypair);

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
            .validate(&epoch_manager, &val_epoch_map)
            .unwrap()
            .into_inner();
        assert_eq!(rx_vote, votemsg);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_timeout_message, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50));
    let mut val_epoch_map = ValidatorsEpochMapping::new();
    val_epoch_map.insert(Epoch(1), validators, validator_mapping);
    let validator_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();

    let author_keypair = &keypairs[0];
    let author_cert_key = &cert_keys[0];

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(0),
        seq_num: SeqNum(0),
    };
    let lci = LedgerCommitInfo::new(None, &vi);

    let qcinfo = QcInfo {
        vote: vi,
        ledger_commit: lci,
    };

    let qcinfo_hash = HasherType::hash_object(&qcinfo.ledger_commit);

    let mut sigs = Vec::new();

    for i in 0..cert_keys.len() {
        let node_id = NodeId(keypairs[i].pubkey());
        let sig =
            <SCT::SignatureType as CertificateSignature>::sign(qcinfo_hash.as_ref(), &cert_keys[i]);
        sigs.push((node_id, sig));
    }

    let sigcol = SCT::new(sigs, validator_mapping, qcinfo_hash.as_ref()).unwrap();

    let qc = QuorumCertificate::new(qcinfo, sigcol);

    // timeout certificate for Round(2)
    // timeout message for Round(3)
    // TODO-3: add more high_qc_rounds
    let tc = make_tc::<SCT>(
        Round(2),
        HighQcRound { qc_round: Round(1) },
        keypairs.as_slice(),
        cert_keys.as_slice(),
        validator_mapping,
    );

    let tmo_info = TimeoutInfo {
        round: Round(3),
        high_qc: qc,
    };
    let tmo = Timeout {
        tminfo: tmo_info,
        last_round_tc: Some(tc),
    };

    let tmo_message = ConsensusMessage::Timeout(TimeoutMessage::new(tmo, author_cert_key));

    let verified_tmo_message =
        Verified::<NopSignature, _>::new(Validated::new(tmo_message.clone()), author_keypair);

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
            .validate(&epoch_manager, &val_epoch_map)
            .unwrap()
            .into_inner();
        assert_eq!(tmo_message, rx_tmo);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_proposal_qc, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50));
    let mut val_epoch_map = ValidatorsEpochMapping::new();
    val_epoch_map.insert(Epoch(1), validators, validator_mapping);
    let validator_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();

    let author_keypair = &keypairs[0];
    let blk = setup_block(
        NodeId(author_keypair.pubkey()),
        Round(233),
        Round(232),
        TransactionHashList::new(vec![1, 2, 3, 4].into()),
        ExecutionArtifacts::zero(),
        cert_keys.as_slice(),
        validator_mapping,
    );
    let proposal: ConsensusMessage<SCT> = ConsensusMessage::Proposal(ProposalMessage {
        block: blk,
        last_round_tc: None,
    });
    let verified_msg =
        Verified::<NopSignature, _>::new(Validated::new(proposal.clone()), author_keypair);
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
            .validate(&epoch_manager, &val_epoch_map)
            .unwrap()
            .into_inner();
        assert_eq!(proposal, rx_prop);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_proposal_tc, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50));
    let mut val_epoch_map = ValidatorsEpochMapping::new();
    val_epoch_map.insert(Epoch(1), validators, validator_mapping);
    let validator_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();

    let author_keypair = &keypairs[0];
    let blk = setup_block::<SCT>(
        NodeId(author_keypair.pubkey()),
        Round(233),
        Round(231),
        TransactionHashList::new(vec![1, 2, 3, 4].into()),
        ExecutionArtifacts::zero(),
        cert_keys.as_slice(),
        validator_mapping,
    );

    let tc_round = Round(232);
    let high_qc_round = HighQcRound {
        qc_round: Round(231),
    };

    let tc = make_tc::<SCT>(
        tc_round,
        high_qc_round,
        keypairs.as_slice(),
        cert_keys.as_slice(),
        validator_mapping,
    );

    let proposal_msg = ConsensusMessage::Proposal(ProposalMessage {
        block: blk,
        last_round_tc: Some(tc),
    });
    let verified_msg =
        Verified::<NopSignature, _>::new(Validated::new(proposal_msg.clone()), author_keypair);

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
            .validate(&epoch_manager, &val_epoch_map)
            .unwrap()
            .into_inner();
        assert_eq!(proposal_msg, rx_prop);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_block_sync_request, |_| {
    let bid = BlockId(Hash([0x01_u8; 32]));
    let block_sync_msg = RequestBlockSyncMessage { block_id: bid };

    let verified_monad_message = VerifiedMonadMessage::<NopSignature, SCT>::BlockSyncRequest(
        Validated::new(block_sync_msg.clone()),
    );
    let monad_message: MonadMessage<_, _> = verified_monad_message.clone().into();

    let rx_buf = serialize_verified_monad_message(&verified_monad_message);
    let rx_msg = deserialize_monad_message(rx_buf).unwrap();

    assert_eq!(rx_msg, monad_message);
    assert!(matches!(rx_msg, MonadMessage::BlockSyncRequest(_)));

    if let MonadMessage::BlockSyncRequest(request) = rx_msg {
        assert_eq!(request.validate().unwrap().into_inner(), block_sync_msg);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_block_sync_response_not_available, |num_keys| {
    let (_keypairs, _cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50));
    let mut val_epoch_map = ValidatorsEpochMapping::new();
    val_epoch_map.insert(Epoch(1), validators, validator_mapping);

    let bid = BlockId(Hash([0x01_u8; 32]));
    let block_sync_msg = BlockSyncResponseMessage::NotAvailable(bid);

    let verified_monad_message = VerifiedMonadMessage::<NopSignature, SCT>::BlockSyncResponse(
        Validated::new(block_sync_msg.clone()),
    );
    let monad_message: MonadMessage<_, _> = verified_monad_message.clone().into();

    let rx_buf = serialize_verified_monad_message(&verified_monad_message);
    let rx_msg = deserialize_monad_message(rx_buf).unwrap();

    assert_eq!(rx_msg, monad_message);
    assert!(matches!(rx_msg, MonadMessage::BlockSyncResponse(_)));

    if let MonadMessage::BlockSyncResponse(resp) = rx_msg {
        let validated = resp
            .validate(&epoch_manager, &val_epoch_map)
            .unwrap()
            .into_inner();

        assert_eq!(block_sync_msg, validated);
    } else {
        unreachable!()
    }
});

test_all_combination!(test_block_sync_response_found, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50));
    let mut val_epoch_map = ValidatorsEpochMapping::new();
    val_epoch_map.insert(Epoch(1), validators, validator_mapping);
    let validator_mapping = val_epoch_map.get_cert_pubkeys(&Epoch(1)).unwrap();

    let author_keypair = &keypairs[0];
    let blk = setup_block::<SCT>(
        NodeId(author_keypair.pubkey()),
        Round(233),
        Round(232),
        TransactionHashList::new(Bytes::from_static(&[1, 2, 3, 4])),
        ExecutionArtifacts::zero(),
        cert_keys.as_slice(),
        validator_mapping,
    );

    let full_blk = UnverifiedFullBlock::new(
        blk,
        FullTransactionList::new(Bytes::from_static(&[1, 2, 3, 4])),
    );

    let block_sync_msg = BlockSyncResponseMessage::BlockFound(full_blk);

    let verified_monad_message = VerifiedMonadMessage::<NopSignature, SCT>::BlockSyncResponse(
        Validated::new(block_sync_msg.clone()),
    );
    let monad_message: MonadMessage<_, _> = verified_monad_message.clone().into();

    let rx_buf = serialize_verified_monad_message(&verified_monad_message);
    let rx_msg = deserialize_monad_message(rx_buf).unwrap();

    assert_eq!(rx_msg, monad_message);
    assert!(matches!(rx_msg, MonadMessage::BlockSyncResponse(_)));

    if let MonadMessage::BlockSyncResponse(rx_blk) = rx_msg {
        let validated = rx_blk
            .validate(&epoch_manager, &val_epoch_map)
            .unwrap()
            .into_inner();

        assert_eq!(block_sync_msg, validated);
    } else {
        unreachable!()
    }
});
