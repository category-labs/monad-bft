#[cfg(test)]
mod test {
    use monad_consensus::{
        signatures::aggregate_signature::AggregateSignatures,
        types::{
            ledger::LedgerCommitInfo,
            message::{TimeoutMessage, VoteMessage},
            quorum_certificate::{QcInfo, QuorumCertificate},
            signature::SignatureCollection,
            timeout::{HighQcRound, TimeoutCertificate, TimeoutInfo},
            voting::VoteInfo,
        },
        validation::{
            hashing::{Hasher, Sha256Hash},
            signing::{Unverified, ValidatorMember, Verified},
        },
    };
    use monad_crypto::secp256k1::KeyPair;
    use monad_proto::types::message::{
        deserialize_unverified_timeout_message, deserialize_unverified_vote_message,
        serialize_verified_timeout_message, serialize_verified_vote_message,
    };
    use monad_types::{BlockId, NodeId, Round};
    use monad_validator::validator::Validator;

    // TODO: revisit to cleanup
    #[test]
    fn test_verified_vote_message() {
        let vi = VoteInfo {
            id: BlockId(vec![42; 32].try_into().unwrap()),
            round: Round(1),
            parent_id: BlockId(vec![43; 32].try_into().unwrap()),
            parent_round: Round(2),
        };
        let lci = LedgerCommitInfo {
            commit_state_hash: None,
            vote_info_hash: vec![42; 32].try_into().unwrap(),
        };
        let votemsg = VoteMessage {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();
        let author = NodeId(keypair.pubkey());
        let mut validators = ValidatorMember::new();
        validators.insert(
            author,
            Validator {
                pubkey: keypair.pubkey(),
                stake: 1,
            },
        );

        let hash = Sha256Hash::hash_object(&votemsg.ledger_commit_info);
        let sig = keypair.sign(&hash);

        let signed_votemsg = Unverified::new(votemsg, sig);
        let verified_votemsg = signed_votemsg
            .clone()
            .verify::<Sha256Hash>(&validators, &author.0)
            .unwrap();

        let buf = serialize_verified_vote_message(&verified_votemsg);
        let de_votemsg = deserialize_unverified_vote_message(buf.as_ref());

        assert_eq!(signed_votemsg, de_votemsg.unwrap());
    }

    #[test]
    fn test_unverified_timeout_message() {
        let vi = VoteInfo {
            id: BlockId(vec![42; 32].try_into().unwrap()),
            round: Round(1),
            parent_id: BlockId(vec![43; 32].try_into().unwrap()),
            parent_round: Round(2),
        };
        let lci = LedgerCommitInfo {
            commit_state_hash: None,
            vote_info_hash: vec![42; 32].try_into().unwrap(),
        };

        let qcinfo = QcInfo {
            vote: vi,
            ledger_commit: lci,
        };

        let qcinfo_hash = Sha256Hash::hash_object(&qcinfo.ledger_commit);

        let privkey1 =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let privkey2 =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90531")
                .unwrap();

        let keypair1 = KeyPair::from_slice(&privkey1).unwrap();
        let keypair2 = KeyPair::from_slice(&privkey2).unwrap();

        let mut validators = ValidatorMember::new();
        validators.insert(
            NodeId(keypair1.pubkey()),
            Validator {
                pubkey: keypair1.pubkey(),
                stake: 1,
            },
        );
        validators.insert(
            NodeId(keypair2.pubkey()),
            Validator {
                pubkey: keypair2.pubkey(),
                stake: 1,
            },
        );

        let qcinfo_sig1 = keypair1.sign(&qcinfo_hash);
        let qcinfo_sig2 = keypair2.sign(&qcinfo_hash);

        let mut aggsig = AggregateSignatures::new();
        aggsig.add_signature(qcinfo_sig1);
        aggsig.add_signature(qcinfo_sig2);

        let qc = QuorumCertificate::new(qcinfo, aggsig);

        let tmo_info = TimeoutInfo {
            round: Round(3),
            high_qc: qc,
        };

        let high_qc_round = HighQcRound { qc_round: Round(1) };
        // is there a cleaner way to do the high qc hash?
        let tc_round = Round(2);
        let mut hasher = Sha256Hash::new();
        hasher.update(tc_round);
        hasher.update(high_qc_round.qc_round);
        let high_qc_round_hash = hasher.hash();

        let mut high_qc_rounds = Vec::new();

        high_qc_rounds.push((high_qc_round, keypair1.sign(&high_qc_round_hash)));
        high_qc_rounds.push((high_qc_round, keypair2.sign(&high_qc_round_hash)));

        let tc = TimeoutCertificate {
            round: tc_round,
            high_qc_rounds: high_qc_rounds,
        };

        let tmo_message = TimeoutMessage {
            tminfo: tmo_info,
            last_round_tc: Some(tc),
        };
        let verified_tmo_message = Verified::new::<Sha256Hash>(tmo_message, &keypair1);

        let buf = serialize_verified_timeout_message(&verified_tmo_message);
        let de_tmo_message = deserialize_unverified_timeout_message(buf.as_ref());

        let verified_de_tmo_messaage = de_tmo_message
            .unwrap()
            .verify::<Sha256Hash>(&validators, &keypair1.pubkey());

        assert_eq!(verified_tmo_message, verified_de_tmo_messaage.unwrap());
    }
}
