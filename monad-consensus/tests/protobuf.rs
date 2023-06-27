#[cfg(all(test, feature = "proto"))]
mod test {
    use monad_consensus::convert::interface::{
        deserialize_unverified_consensus_message, serialize_verified_consensus_message,
    };
    use monad_consensus::types::timeout::HighQcRoundSigTuple;
    use monad_consensus::{
        signatures::aggregate_signature::AggregateSignatures,
        types::{
            block::TransactionList,
            consensus_message::ConsensusMessage,
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
    use monad_testutil::block::setup_block;
    use monad_testutil::signing::{create_keys, get_key};
    use monad_types::{BlockId, Hash, NodeId, Round, Stake};

    fn setup_validator_member(keypairs: &[KeyPair]) -> ValidatorMember {
        let mut vmember = ValidatorMember::new();
        for keypair in keypairs.iter() {
            vmember.insert(NodeId(keypair.pubkey()), Stake(1));
        }
        vmember
    }
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
        let votemsg: ConsensusMessage<SecpSignature, AggregateSignatures<SecpSignature>> =
            ConsensusMessage::Vote(VoteMessage {
                vote_info: vi,
                ledger_commit_info: lci,
            });
        let keypairs = vec![get_key(0)];
        let author_keypair = &keypairs[0];
        let validators = setup_validator_member(&keypairs);

        let verified_votemsg = Verified::new::<Sha256Hash>(votemsg, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_votemsg);
        let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

        let verified_rx_vote = rx_msg
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

        let mut aggsig = AggregateSignatures::new();
        for keypair in keypairs.iter() {
            aggsig.add_signature(keypair.sign(qcinfo_hash.as_ref()));
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

        let verified_rx_tmo_messaage =
            rx_msg.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_tmo_message, verified_rx_tmo_messaage.unwrap());
    }

    #[test]
    fn test_proposal_qc() {
        let keypairs = create_keys(2);
        let author_keypair = &keypairs[0];
        let vmember = setup_validator_member(&keypairs);
        let blk = setup_block(
            NodeId(author_keypair.pubkey()),
            233,
            232,
            TransactionList(vec![1, 2, 3, 4]),
            &keypairs,
        );
        let proposal: ConsensusMessage<SecpSignature, AggregateSignatures<SecpSignature>> =
            ConsensusMessage::Proposal(ProposalMessage {
                block: blk,
                last_round_tc: None,
            });
        let verified_msg = Verified::new::<Sha256Hash>(proposal, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_msg);
        let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

        let verified_rx_msg = rx_msg.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_msg, verified_rx_msg.unwrap());
    }

    #[test]
    fn test_unverified_proposal_tc() {
        let keypairs = create_keys(2);
        let vmember = setup_validator_member(&keypairs);
        let author_keypair = &keypairs[0];
        let blk = setup_block(
            NodeId(author_keypair.pubkey()),
            233,
            231,
            TransactionList(vec![1, 2, 3, 4]),
            &keypairs,
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

        let verified_rx_msg = rx_msg.verify::<Sha256Hash>(&vmember, &author_keypair.pubkey());

        assert_eq!(verified_msg, verified_rx_msg.unwrap());
    }
}
