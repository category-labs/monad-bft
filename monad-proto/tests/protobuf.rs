#[cfg(test)]
mod test {

    use monad_consensus::{
        signatures::aggregate_signature::AggregateSignatures,
        types::{
            block::{Block, TransactionList},
            consensus_message::{SignedConsensusMessage, VerifiedConsensusMessage},
            ledger::LedgerCommitInfo,
            message::{ProposalMessage, TimeoutMessage, VoteMessage},
            quorum_certificate::{QcInfo, QuorumCertificate},
            signature::SignatureCollection,
            timeout::{HighQcRound, TimeoutCertificate, TimeoutInfo},
            voting::VoteInfo,
        },
        validation::{
            hashing::{Hasher, Sha256Hash},
            signing::{ValidatorMember, Verified},
        },
    };
    use monad_crypto::secp256k1::{KeyPair, SecpSignature};
    use monad_proto::types::message::{
        deserialize_unverified_consensus_message, serialize_verified_consensus_message,
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
        let keypairs = vec![get_key(0)];
        let author_keypair = &keypairs[0];
        let validators = setup_validator_member(&keypairs);

        let verified_votemsg = Verified::new::<Sha256Hash>(votemsg.clone(), author_keypair);
        let verified_consensus_msg: VerifiedConsensusMessage<
            SecpSignature,
            AggregateSignatures<SecpSignature>,
        > = VerifiedConsensusMessage::Vote(verified_votemsg.clone());

        let rx_buf = serialize_verified_consensus_message(&verified_consensus_msg);
        let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

        let rx_vote = match rx_msg {
            SignedConsensusMessage::Vote(msg) => msg,
            _ => panic!("Expect Vote variant"),
        };
        let verified_rx_vote = rx_vote
            .verify::<Sha256Hash>(&validators, &author_keypair.pubkey())
            .unwrap();

        assert_eq!(verified_votemsg, verified_rx_vote);
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
        let lci = LedgerCommitInfo::new::<Sha256Hash>(None, &vi);

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
        let verified_consensus_msg =
            VerifiedConsensusMessage::Timeout(verified_tmo_message.clone());

        let rx_buf = serialize_verified_consensus_message(&verified_consensus_msg);
        let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

        let rx_tmo_msg = match rx_msg {
            SignedConsensusMessage::Timeout(msg) => msg,
            _ => panic!("Expect Timeout Variant"),
        };

        let verified_rx_tmo_messaage =
            rx_tmo_msg.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_tmo_message, verified_rx_tmo_messaage.unwrap());
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
        let verified_msg = VerifiedConsensusMessage::Proposal(verified_proposal.clone());

        let rx_buf = serialize_verified_consensus_message(&verified_msg);
        let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

        let rx_proposal = match rx_msg {
            SignedConsensusMessage::Proposal(msg) => msg,
            _ => panic!("Expected Proposal variant"),
        };

        let verified_rx_proposal =
            rx_proposal.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_proposal, verified_rx_proposal.unwrap());
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

        let verified_msg = VerifiedConsensusMessage::Proposal(verified_proposal.clone());

        let rx_buf = serialize_verified_consensus_message(&verified_msg);
        let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

        let rx_proposal = match rx_msg {
            SignedConsensusMessage::Proposal(msg) => msg,
            _ => panic!("Expected Proposal variant"),
        };

        let verified_rx_proposal =
            rx_proposal.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_proposal, verified_rx_proposal.unwrap());
    }
}
