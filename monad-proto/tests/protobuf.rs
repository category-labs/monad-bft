#[cfg(test)]
mod test {
    use monad_consensus::{
        types::{ledger::LedgerCommitInfo, message::VoteMessage, voting::VoteInfo},
        validation::{
            hashing::{Hasher, Sha256Hash},
            signing::{Unverified, ValidatorMember},
        },
    };
    use monad_crypto::secp256k1::KeyPair;
    use monad_proto::types::message::{
        deserialize_unverified_vote_message, serialize_verified_vote_message,
    };
    use monad_types::{BlockId, NodeId, Round};
    use monad_validator::validator::Validator;

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
}
