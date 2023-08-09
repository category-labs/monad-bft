#[cfg(all(test, feature = "proto"))]
mod test {
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
        signature_collection::SignatureCollection,
        timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate, TimeoutInfo},
        validation::{Hasher, Sha256Hash},
        voting::VoteInfo,
    };
    use monad_crypto::secp256k1::SecpSignature;
    use monad_testutil::{block::setup_block, validators::create_keys_w_validators};
    use monad_types::{BlockId, Hash, NodeId, Round};

    type SignatureCollectionType = MultiSig<SecpSignature>;
    // TODO: revisit to cleanup
    #[test]
    fn test_vote_message() {
        let vi = VoteInfo {
            id: BlockId(Hash([42_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([43_u8; 32])),
            parent_round: Round(2),
        };
        let lci = LedgerCommitInfo {
            commit_state_hash: None,
            vote_info_hash: Hash([42_u8; 32]),
        };
        let votemsg: ConsensusMessage<SecpSignature, SignatureCollectionType> =
            ConsensusMessage::Vote(VoteMessage {
                vote_info: vi,
                ledger_commit_info: lci,
            });

        let (keypairs, _blskeys, validators, validator_mapping) =
            create_keys_w_validators::<SignatureCollectionType>(1);

        let author_keypair = &keypairs[0];

        let verified_votemsg = Verified::new::<Sha256Hash>(votemsg, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_votemsg);
        let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

        let verified_rx_vote = rx_msg
            .verify::<Sha256Hash, _>(&validators, &validator_mapping, &author_keypair.pubkey())
            .unwrap();

        assert_eq!(verified_votemsg, verified_rx_vote);
    }

    #[test]
    fn test_timeout_message() {
        let (keypairs, cert_keys, validators, validator_mapping) =
            create_keys_w_validators::<SignatureCollectionType>(2);

        let author_keypair = &keypairs[0];

        let vi = VoteInfo {
            id: BlockId(Hash([42_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([43_u8; 32])),
            parent_round: Round(2),
        };
        let lci = LedgerCommitInfo::new::<Sha256Hash>(None, &vi);

        let qcinfo = QcInfo {
            vote: vi,
            ledger_commit: lci,
        };

        let qcinfo_hash = Sha256Hash::hash_object(&qcinfo.ledger_commit);

        let mut sigs = Vec::new();

        for i in 0..cert_keys.len() {
            let node_id = NodeId(keypairs[i].pubkey());
            let sig =<< SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(qcinfo_hash.as_ref(), &cert_keys[i]);
            sigs.push((node_id, sig));
        }

        let multisig = MultiSig::new(sigs, &validator_mapping, qcinfo_hash.as_ref()).unwrap();

        let qc = QuorumCertificate::new::<Sha256Hash>(qcinfo, multisig);

        let tmo_info = TimeoutInfo {
            round: Round(3),
            high_qc: qc,
        };

        let high_qc_round = HighQcRound { qc_round: Round(1) };
        // FIXME: is there a cleaner way to do the high qc hash?
        let tc_round = Round(2);
        let mut hasher = Sha256Hash::new();
        hasher.update(tc_round);
        hasher.update(high_qc_round.qc_round);
        let high_qc_round_hash = hasher.hash();

        let mut high_qc_rounds = Vec::new();
        for keypair in keypairs.iter() {
            high_qc_rounds.push(HighQcRoundSigTuple {
                high_qc_round,
                author_signature: keypair.sign(high_qc_round_hash.as_ref()),
            });
        }

        let tc = TimeoutCertificate {
            round: tc_round,
            high_qc_rounds,
        };

        let tmo_message = ConsensusMessage::Timeout(TimeoutMessage {
            tminfo: tmo_info,
            last_round_tc: Some(tc),
        });
        let verified_tmo_message = Verified::new::<Sha256Hash>(tmo_message, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_tmo_message);
        let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

        let verified_rx_tmo_messaage = rx_msg.verify::<Sha256Hash, _>(
            &validators,
            &validator_mapping,
            &author_keypair.pubkey(),
        );

        assert_eq!(verified_tmo_message, verified_rx_tmo_messaage.unwrap());
    }

    #[test]
    fn test_proposal_qc() {
        let (keypairs, cert_keys, validators, validator_map) =
            create_keys_w_validators::<SignatureCollectionType>(2);

        let author_keypair = &keypairs[0];
        let blk = setup_block(
            NodeId(author_keypair.pubkey()),
            Round(233),
            Round(232),
            TransactionList(vec![1, 2, 3, 4]),
            ExecutionArtifacts::zero(),
            &cert_keys,
            &validator_map,
        );
        let proposal: ConsensusMessage<SecpSignature, SignatureCollectionType> =
            ConsensusMessage::Proposal(ProposalMessage {
                block: blk,
                last_round_tc: None,
            });
        let verified_msg = Verified::new::<Sha256Hash>(proposal, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_msg);
        let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

        let verified_rx_msg =
            rx_msg.verify::<Sha256Hash, _>(&validators, &validator_map, &author_keypair.pubkey());

        assert_eq!(verified_msg, verified_rx_msg.unwrap());
    }

    #[test]
    fn test_unverified_proposal_tc() {
        let (keypairs, cert_keys, validators, validator_map) =
            create_keys_w_validators::<SignatureCollectionType>(2);

        let author_keypair = &keypairs[0];
        let blk = setup_block(
            NodeId(author_keypair.pubkey()),
            Round(233),
            Round(231),
            TransactionList(vec![1, 2, 3, 4]),
            ExecutionArtifacts::zero(),
            &cert_keys,
            &validator_map,
        );

        let tc_round = Round(232);
        let high_qc_round = HighQcRound {
            qc_round: Round(231),
        };
        let mut hasher = Sha256Hash::new();
        hasher.update(tc_round);
        hasher.update(high_qc_round.qc_round);
        let high_qc_round_hash = hasher.hash();

        let mut high_qc_rounds = Vec::new();

        for keypair in keypairs.iter() {
            high_qc_rounds.push(HighQcRoundSigTuple {
                high_qc_round,
                author_signature: keypair.sign(high_qc_round_hash.as_ref()),
            });
        }

        let tc = TimeoutCertificate {
            round: Round(232),
            high_qc_rounds,
        };

        let msg = ConsensusMessage::Proposal(ProposalMessage {
            block: blk,
            last_round_tc: Some(tc),
        });
        let verified_msg = Verified::new::<Sha256Hash>(msg, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_msg);
        let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

        let verified_rx_msg =
            rx_msg.verify::<Sha256Hash, _>(&validators, &validator_map, &author_keypair.pubkey());

        assert_eq!(verified_msg, verified_rx_msg.unwrap());
    }
}
