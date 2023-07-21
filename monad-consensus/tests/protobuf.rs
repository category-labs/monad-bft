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
        block::TransactionList,
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature::{SignatureBuilder, SignatureCollection},
        timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate, TimeoutInfo},
        validation::{Hasher, Sha256Hash},
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{secp256k1::SecpSignature, GenericKeyPair, GenericSignature};
    use monad_testutil::{block::setup_block, validators::create_keys_w_validators};
    use monad_types::{BlockId, Hash, NodeId, Round};
    use monad_validator::validator_property::ValidatorSetPropertyType;

    type SignatureCollectionType = MultiSig<SecpSignature>;

    // TODO: revisit to cleanup
    #[test]
    fn test_vote_message() {
        let (keypairs, _blskeys, vote_keys, validators, vprop) =
            create_keys_w_validators::<SignatureCollectionType>(1);
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

        let vote = Vote {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let vote_hash = Sha256Hash::hash_object(&vote);
        let sig =
            <<SignatureCollectionType as SignatureCollection>::SignatureType as GenericSignature>::sign(
                vote_hash.as_ref(),
                &vote_keys[0],
            );

        let votemsg = ConsensusMessage::Vote(VoteMessage::<SignatureCollectionType> { vote, sig });

        let author_keypair = &keypairs[0];

        let verified_votemsg = Verified::new::<Sha256Hash>(votemsg, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_votemsg);
        let rx_msg = deserialize_unverified_consensus_message(rx_buf.as_ref()).unwrap();

        let verified_rx_vote = rx_msg
            .verify::<Sha256Hash, _, _>(&validators, &vprop, &author_keypair.pubkey())
            .unwrap();

        assert_eq!(verified_votemsg, verified_rx_vote);
    }

    #[test]
    fn test_timeout_message() {
        let (keypairs, _blskeys, _vote_keys, validators, validators_prop) =
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

        let mut builder = SignatureBuilder::new();
        for keypair in keypairs.iter() {
            let idx = validators_prop
                .get_index(&NodeId(keypair.pubkey()))
                .unwrap();
            let sig = keypair.sign(qcinfo_hash.as_ref());
            builder.add_signature(idx, sig);
        }

        let multisig = MultiSig::new(
            builder,
            validators_prop.get_voting_list(),
            qcinfo_hash.as_ref(),
        )
        .unwrap();

        let qc = QuorumCertificate::new(qcinfo, multisig);

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

        let verified_rx_tmo_messaage = rx_msg.verify::<Sha256Hash, _, _>(
            &validators,
            &validators_prop,
            &author_keypair.pubkey(),
        );

        assert_eq!(verified_tmo_message, verified_rx_tmo_messaage.unwrap());
    }

    #[test]
    fn test_proposal_qc() {
        let (keypairs, _blskeys, votekeys, validators, validators_prop) =
            create_keys_w_validators::<SignatureCollectionType>(2);

        let author_keypair = &keypairs[0];
        let voters = keypairs
            .iter()
            .map(|key| NodeId(key.pubkey()))
            .zip(votekeys.iter());

        let blk = setup_block(
            NodeId(author_keypair.pubkey()),
            233,
            232,
            TransactionList(vec![1, 2, 3, 4]),
            voters.into_iter(),
            &validators_prop,
        );
        let proposal: ConsensusMessage<SecpSignature, SignatureCollectionType> =
            ConsensusMessage::Proposal(ProposalMessage {
                block: blk,
                last_round_tc: None,
            });
        let verified_msg = Verified::new::<Sha256Hash>(proposal, author_keypair);

        let rx_buf = serialize_verified_consensus_message(&verified_msg);
        let rx_msg = deserialize_unverified_consensus_message(&rx_buf).unwrap();

        let verified_rx_msg = rx_msg.verify::<Sha256Hash, _, _>(
            &validators,
            &validators_prop,
            &author_keypair.pubkey(),
        );

        assert_eq!(verified_msg, verified_rx_msg.unwrap());
    }

    #[test]
    fn test_unverified_proposal_tc() {
        let (keypairs, _blskeys, votekeys, validators, validators_prop) =
            create_keys_w_validators::<SignatureCollectionType>(2);

        let author_keypair = &keypairs[0];
        let voters = keypairs
            .iter()
            .map(|key| NodeId(key.pubkey()))
            .zip(votekeys.iter());

        let blk = setup_block(
            NodeId(author_keypair.pubkey()),
            233,
            231,
            TransactionList(vec![1, 2, 3, 4]),
            voters.into_iter(),
            &validators_prop,
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

        let verified_rx_msg = rx_msg.verify::<Sha256Hash, _, _>(
            &validators,
            &validators_prop,
            &author_keypair.pubkey(),
        );

        assert_eq!(verified_msg, verified_rx_msg.unwrap());
    }
}
