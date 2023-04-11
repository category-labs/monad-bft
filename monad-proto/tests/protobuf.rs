#[cfg(test)]
mod test {
    use monad_consensus::{
        signatures::aggregate_signature::AggregateSignatures,
        types::{
            block::{Block, TransactionList},
            ledger::LedgerCommitInfo,
            message::{ProposalMessage, TimeoutMessage, VoteMessage},
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
    use monad_crypto::secp256k1::{KeyPair, SecpSignature};
    use monad_proto::types::message::{
        deserialize_unverified_proposal_message, deserialize_unverified_timeout_message,
        deserialize_unverified_vote_message, serialize_verified_proposal_message,
        serialize_verified_timeout_message, serialize_verified_vote_message,
    };
    use monad_testutil::signing::{create_keys, get_key};
    use monad_types::{BlockId, NodeId, Round};
    use monad_validator::validator::Validator;

    fn setup_validator_member(keypairs: &Vec<KeyPair>) -> ValidatorMember {
        let mut vmember = ValidatorMember::new();
        for keypair in keypairs.iter() {
            vmember.insert(
                NodeId(keypair.pubkey()),
                Validator {
                    pubkey: keypair.pubkey(),
                    stake: 1,
                },
            );
        }
        vmember
    }
    // TODO: revisit to cleanup
    #[test]
    fn test_vote_message() {
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
        let keypairs = vec![get_key("a")];
        let author_keypair = &keypairs[0];
        let validators = setup_validator_member(&keypairs);

        let hash = Sha256Hash::hash_object(&votemsg.ledger_commit_info);
        let sig = author_keypair.sign(&hash);

        let signed_votemsg = Unverified::new(votemsg, sig);
        let verified_votemsg = signed_votemsg
            .clone()
            .verify::<Sha256Hash>(&validators, &author_keypair.pubkey())
            .unwrap();

        let buf = serialize_verified_vote_message(&verified_votemsg);
        let de_votemsg = deserialize_unverified_vote_message(buf.as_ref());

        assert_eq!(signed_votemsg, de_votemsg.unwrap());
    }

    #[test]
    fn test_timeout_message() {
        let keypairs = create_keys(2);
        let vmember = setup_validator_member(&keypairs);
        let author_keypair = &keypairs[0];

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

        let mut aggsig = AggregateSignatures::new();
        for keypair in keypairs.iter() {
            aggsig.add_signature(keypair.sign(&qcinfo_hash));
        }

        let qc = QuorumCertificate::new(qcinfo, aggsig);

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
            high_qc_rounds.push((high_qc_round, keypair.sign(&high_qc_round_hash)));
        }

        let tc = TimeoutCertificate {
            round: tc_round,
            high_qc_rounds: high_qc_rounds,
        };

        let tmo_message = TimeoutMessage {
            tminfo: tmo_info,
            last_round_tc: Some(tc),
        };
        let verified_tmo_message = Verified::new::<Sha256Hash>(tmo_message, &author_keypair);

        let buf = serialize_verified_timeout_message(&verified_tmo_message);
        let de_tmo_message = deserialize_unverified_timeout_message(buf.as_ref());

        let verified_de_tmo_messaage = de_tmo_message
            .unwrap()
            .verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_tmo_message, verified_de_tmo_messaage.unwrap());
    }

    fn setup_block(
        author: NodeId,
        block_round: u64,
        qc_round: u64,
        keypairs: &Vec<KeyPair>,
    ) -> Block<AggregateSignatures<SecpSignature>> {
        let txns = TransactionList(vec![1, 2, 3, 4]);
        let round = Round(block_round);

        let vi = VoteInfo {
            id: BlockId(vec![42; 32].try_into().unwrap()),
            round: Round(qc_round),
            parent_id: BlockId(vec![43; 32].try_into().unwrap()),
            parent_round: Round(0),
        };
        let lci = LedgerCommitInfo {
            commit_state_hash: None,
            vote_info_hash: Sha256Hash::hash_object(&vi),
        };

        let qcinfo = QcInfo {
            vote: vi,
            ledger_commit: lci,
        };
        let qcinfo_hash = Sha256Hash::hash_object(&qcinfo.ledger_commit);

        let mut aggsig = AggregateSignatures::new();
        for keypair in keypairs.iter() {
            aggsig.add_signature(keypair.sign(&qcinfo_hash));
        }

        let qc = QuorumCertificate::<AggregateSignatures<SecpSignature>>::new(qcinfo, aggsig);

        Block::<AggregateSignatures<SecpSignature>>::new::<Sha256Hash>(author, round, &txns, &qc)
    }

    #[test]
    fn test_proposal_qc() {
        let keypairs = create_keys(2);
        let author_keypair = &keypairs[0];
        let vmember = setup_validator_member(&keypairs);
        let blk = setup_block(NodeId(author_keypair.pubkey()), 233, 232, &keypairs);
        let proposal = ProposalMessage {
            block: blk,
            last_round_tc: None,
        };
        let verified_proposal = Verified::new::<Sha256Hash>(proposal, author_keypair);

        let buf = serialize_verified_proposal_message(&verified_proposal);
        let de_proposal = deserialize_unverified_proposal_message(buf.as_ref()).unwrap();
        let verified_de_proposal =
            de_proposal.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_proposal, verified_de_proposal.unwrap());
    }

    #[test]
    fn test_unverified_proposal_tc() {
        let keypairs = create_keys(2);
        let vmember = setup_validator_member(&keypairs);
        let author_keypair = &keypairs[0];
        let blk = setup_block(NodeId(author_keypair.pubkey()), 233, 231, &keypairs);

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
            high_qc_rounds.push((high_qc_round, keypair.sign(&high_qc_round_hash)));
        }

        let tc = TimeoutCertificate {
            round: Round(232),
            high_qc_rounds: high_qc_rounds,
        };

        let proposal = ProposalMessage {
            block: blk,
            last_round_tc: Some(tc),
        };
        let verified_proposal = Verified::new::<Sha256Hash>(proposal, &author_keypair);

        let buf = serialize_verified_proposal_message(&verified_proposal);
        let de_proposal = deserialize_unverified_proposal_message(buf.as_ref()).unwrap();
        let verified_de_proposal =
            de_proposal.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_proposal, verified_de_proposal.unwrap());
    }
}
