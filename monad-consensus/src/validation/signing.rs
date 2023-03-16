use crate::types::signature::ConsensusSignature;
use crate::*;

pub trait Signable {
    type Output;

    fn signed_object(self, author: NodeId, signature: ConsensusSignature) -> Self::Output;
}

pub trait Hashable<'a> {
    type DataIter: Iterator<Item = &'a [u8]>;

    fn msg_parts(&self) -> Self::DataIter;
}

pub struct Signed<M> {
    pub obj: M,
    pub author: NodeId,
    pub author_signature: ConsensusSignature,
}

#[cfg(test)]
mod tests {
    use crate::types::ledger::LedgerCommitInfo;
    use crate::types::message::VoteMessage;
    use crate::types::signature::ConsensusSignature;
    use crate::NodeId;
    use monad_crypto::secp256k1::KeyPair;

    use super::Hashable;
    use super::Signable;

    use sha2::Digest;

    struct Signer;
    impl Signer {
        fn hash_object<'a, T: Hashable<'a>>(o: T) -> [u8; 32] {
            let mut hasher = sha2::Sha256::new();

            for f in o.msg_parts() {
                hasher.update(f);
            }
            hasher.finalize().into()
        }

        fn sign_object<'a, T: Signable>(o: T, msg: &[u8], key: KeyPair) -> <T as Signable>::Output {
            let sig = key.sign(msg);

            let id = NodeId(0);
            o.signed_object(id, ConsensusSignature(sig))
        }
    }

    #[test]
    fn test_hash() {
        let lci = LedgerCommitInfo {
            commit_state_hash: Some(Default::default()),
            vote_info_hash: Default::default(),
        };

        let vm = VoteMessage {
            vote_info: Default::default(),
            ledger_commit_info: lci,
        };

        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();

        let expected_vote_info_hash = vm.ledger_commit_info.vote_info_hash.clone();

        let msg = Signer::hash_object(&vm);
        let svm = Signer::sign_object(vm, &msg, keypair);

        assert_eq!(svm.author, NodeId(0));
        assert_eq!(
            svm.obj.ledger_commit_info.vote_info_hash,
            expected_vote_info_hash
        );
    }
}
