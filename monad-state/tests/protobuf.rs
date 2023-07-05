#[cfg(all(test, feature = "proto"))]
mod test {
    use monad_consensus::messages::{consensus_message::ConsensusMessage, message::VoteMessage};
    use monad_consensus::{pacemaker::PacemakerTimerExpire, validation::signing::Unverified};
    use monad_consensus_types::{
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        validation::{Hasher, Sha256Hash},
        voting::VoteInfo,
    };
    use monad_crypto::secp256k1::{KeyPair, SecpSignature};
    use monad_state::{
        convert::interface::{deserialize_event, serialize_event},
        ConsensusEvent, MonadEvent,
    };
    use monad_testutil::signing::get_key;
    use monad_types::BlockId;
    use monad_types::{Hash, Round};

    #[test]
    fn test_consensus_timeout_event() {
        let event = MonadEvent::ConsensusEvent(ConsensusEvent::<
            SecpSignature,
            MultiSig<SecpSignature>,
        >::Timeout(PacemakerTimerExpire {}));

        let buf = serialize_event(&event);
        let rx_event = deserialize_event(&buf);

        assert_eq!(event, rx_event.unwrap());
    }

    #[test]
    fn test_consensus_message_event() {
        let keypair: KeyPair = get_key(0);
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
        let votemsg: ConsensusMessage<SecpSignature, MultiSig<SecpSignature>> =
            ConsensusMessage::Vote(VoteMessage {
                vote_info: vi,
                ledger_commit_info: lci,
            });
        let votemsg_hash = Sha256Hash::hash_object(&votemsg);
        let sig = keypair.sign(votemsg_hash.as_ref());

        let unverified_votemsg = Unverified::new(votemsg, sig);

        let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
            sender: keypair.pubkey(),
            unverified_message: unverified_votemsg,
        });

        let buf = serialize_event(&event);
        let rx_event = deserialize_event(&buf);

        assert_eq!(event, rx_event.unwrap());
    }
}
