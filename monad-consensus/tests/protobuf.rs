use monad_consensus::{
    convert::interface::{
        deserialize_unverified_consensus_message, serialize_verified_consensus_message,
    },
    messages::{
        consensus_message::ConsensusMessage,
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    },
    validation::signing::Verified,
};
use monad_consensus_types::{
    certificate_signature::CertificateSignature,
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, TransactionList},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    hasher::{Hash, Hashable, Hasher, HasherType},
    secp256k1::KeyPair,
};
use monad_testutil::{block::setup_block, validators::create_keys_w_validators};
use monad_types::{BlockId, NodeId, Round};
use zerocopy::AsBytes;

fn make_tc<SCT: SignatureCollection>(
    tc_round: Round,
    high_qc_round: HighQcRound,
    keys: &[KeyPair],
    certkeys: &[SignatureCollectionKeyPairType<SCT>],
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
) -> TimeoutCertificate<SCT> {
    let mut hasher = HasherType::new();
    hasher.update(tc_round.0.as_bytes());
    high_qc_round.hash(&mut hasher);
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

// TODO: revisit to cleanup
test_all_combination!(test_vote_message, |num_keys| {
    let (keypairs, certkeys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: 0,
    };
    let lci = LedgerCommitInfo {
        commit_state_hash: None,
        vote_info_hash: Hash([42_u8; 32]),
    };

    let vote = Vote {
        vote_info: vi,
        ledger_commit_info: lci,
    };

    let votemsg = ConsensusMessage::Vote(VoteMessage::<SCT>::new::<HasherType>(vote, &certkeys[0]));

    let author_keypair = &keypairs[0];

    let verified_votemsg = Verified::<NopSignature, _>::new::<HasherType>(votemsg, author_keypair);

    let rx_buf = serialize_verified_consensus_message(&verified_votemsg);
    let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

    let verified_rx_vote = rx_msg
        .verify::<HasherType, _>(&validators, &validator_mapping, &author_keypair.pubkey())
        .unwrap();

    assert_eq!(verified_votemsg, verified_rx_vote);
});

test_all_combination!(test_timeout_message, |num_keys| {
    let (keypairs, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_keys);

    let author_keypair = &keypairs[0];
    let author_cert_key = &cert_keys[0];

    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(0),
        seq_num: 0,
    };
    let lci = LedgerCommitInfo::new::<HasherType>(None, &vi);

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

    let sigcol = SCT::new(sigs, &validator_mapping, qcinfo_hash.as_ref()).unwrap();

    let qc = QuorumCertificate::new::<HasherType>(qcinfo, sigcol);

    // timeout certificate for Round(2)
    // timeout message for Round(3)
    // TODO: add more high_qc_rounds
    let tc = make_tc::<SCT>(
        Round(2),
        HighQcRound { qc_round: Round(1) },
        keypairs.as_slice(),
        cert_keys.as_slice(),
        &validator_mapping,
    );

    let tmo_info = TimeoutInfo {
        round: Round(3),
        high_qc: qc,
    };
    let tmo = Timeout {
        tminfo: tmo_info,
        last_round_tc: Some(tc),
    };

    let tmo_message =
        ConsensusMessage::Timeout(TimeoutMessage::new::<HasherType>(tmo, author_cert_key));

    let verified_tmo_message =
        Verified::<NopSignature, _>::new::<HasherType>(tmo_message, author_keypair);

    let rx_buf = serialize_verified_consensus_message(&verified_tmo_message);
    let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

    let verified_rx_tmo_messaage =
        rx_msg.verify::<HasherType, _>(&validators, &validator_mapping, &author_keypair.pubkey());

    assert_eq!(verified_tmo_message, verified_rx_tmo_messaage.unwrap());
});

test_all_combination!(test_proposal_qc, |num_keys| {
    let (keypairs, cert_keys, validators, validator_map) =
        create_keys_w_validators::<SCT>(num_keys);

    let author_keypair = &keypairs[0];
    let blk = setup_block(
        NodeId(author_keypair.pubkey()),
        Round(233),
        Round(232),
        TransactionList(vec![1, 2, 3, 4]),
        ExecutionArtifacts::zero(),
        cert_keys.as_slice(),
        &validator_map,
    );
    let proposal: ConsensusMessage<SCT> = ConsensusMessage::Proposal(ProposalMessage {
        block: blk,
        last_round_tc: None,
    });
    let verified_msg = Verified::<NopSignature, _>::new::<HasherType>(proposal, author_keypair);

    let rx_buf = serialize_verified_consensus_message(&verified_msg);
    let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

    let verified_rx_msg =
        rx_msg.verify::<HasherType, _>(&validators, &validator_map, &author_keypair.pubkey());

    assert_eq!(verified_msg, verified_rx_msg.unwrap());
});

test_all_combination!(test_proposal_tc, |num_keys| {
    let (keypairs, cert_keys, validators, validator_map) =
        create_keys_w_validators::<SCT>(num_keys);

    let author_keypair = &keypairs[0];
    let blk = setup_block::<SCT>(
        NodeId(author_keypair.pubkey()),
        Round(233),
        Round(231),
        TransactionList(vec![1, 2, 3, 4]),
        ExecutionArtifacts::zero(),
        cert_keys.as_slice(),
        &validator_map,
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
        &validator_map,
    );

    let msg = ConsensusMessage::Proposal(ProposalMessage {
        block: blk,
        last_round_tc: Some(tc),
    });
    let verified_msg = Verified::<NopSignature, _>::new::<HasherType>(msg, author_keypair);

    let rx_buf = serialize_verified_consensus_message(&verified_msg);
    let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

    let verified_rx_msg =
        rx_msg.verify::<HasherType, _>(&validators, &validator_map, &author_keypair.pubkey());

    assert_eq!(verified_msg, verified_rx_msg.unwrap());
});
