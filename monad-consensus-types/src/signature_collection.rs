use monad_types::{Hash, NodeId};

use crate::{
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    validation::{Hashable, Hasher},
    voting::ValidatorMapping,
};

pub type SignatureCollectionKeyPairType<SCT> =
    <<SCT as SignatureCollection>::SignatureType as CertificateSignature>::KeyPairType;
pub type SignatureCollectionPubKeyType<SCT> =
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::PubKeyType;

#[derive(Debug, PartialEq, Eq)]
pub enum SignatureCollectionError<S> {
    NodeIdNotInMapping(Vec<(NodeId, S)>),
    // only possible for non-deterministic signature
    ConflictingSignatures((NodeId, S, S)),
    // verification error
    InvalidSignatures(Vec<S>),
}

impl<S: CertificateSignature> std::fmt::Display for SignatureCollectionError<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignatureCollectionError::NodeIdNotInMapping(v) => {
                write!(f, "NodeId not in validator mapping: {v:?}")
            }
            SignatureCollectionError::ConflictingSignatures((node_id, s1, s2)) => {
                write!(
                    f,
                    "Conflicting signatures from {node_id:?}\ns1: {s1:?}\ns2: {s2:?}"
                )
            }
            SignatureCollectionError::InvalidSignatures(sig) => {
                write!(f, "Invalid signature ({sig:?})")
            }
        }
    }
}

impl<S: CertificateSignature> std::error::Error for SignatureCollectionError<S> {}

pub trait SignatureCollection: Clone + Hashable + Send + Sync + std::fmt::Debug + 'static {
    type SignatureType: CertificateSignature;

    fn new(
        sigs: Vec<(NodeId, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::SignatureType>>;

    // hash of all the signatures
    fn get_hash<H: Hasher>(&self) -> Hash;

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Vec<NodeId>, SignatureCollectionError<Self::SignatureType>>;

    // TODO: deprecate this function: only used by tests
    fn num_signatures(&self) -> usize;
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_testutil::signing::get_key;
    use monad_types::{Hash, NodeId};

    use crate::{
        certificate_signature::CertificateSignature,
        signature_collection::{SignatureCollection, SignatureCollectionError},
        test_utils::{get_certificate_key, setup_sigcol_test},
        voting::ValidatorMapping,
    };

    macro_rules! test_all_signature_collection {
        ($test_name:ident, $test_code:block) => {
            mod $test_name {
                use monad_crypto::{secp256k1::SecpSignature, NopSignature};

                use super::*;
                use crate::multi_sig::*;

                fn invoke<T: SignatureCollection>() {
                    $test_code
                }

                #[test]
                fn multi_sig_secp() {
                    invoke::<MultiSig<SecpSignature>>();
                }

                #[test]
                fn multi_sig_nop() {
                    invoke::<MultiSig<NopSignature>>();
                }
            }
        };
    }

    test_all_signature_collection!(test_num_signatures, {
        let validator_mapping = ValidatorMapping::new(std::iter::empty());
        let hash = Hash([0_u8; 32]);
        let sigs = T::new(Vec::new(), &validator_mapping, hash.as_ref()).unwrap();
        assert_eq!(sigs.num_signatures(), 0);
    });

    test_all_signature_collection!(test_creation, {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(5);

        let msg_hash = Hash([129_u8; 32]);
        let mut sigs = Vec::new();

        for (node_id, certkey) in voting_keys.iter() {
            let sig = <T::SignatureType as CertificateSignature>::sign(msg_hash.as_ref(), certkey);
            sigs.push((*node_id, sig));
        }

        assert!(T::new(sigs, &valmap, msg_hash.as_ref()).is_ok());
    });

    test_all_signature_collection!(test_node_id_not_in_validator_mapping, {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(5);

        let non_validator_node_id = NodeId(get_key(100).pubkey());
        let non_validator_cert_key = get_certificate_key::<T>(100);

        assert!(!valmap.map.contains_key(&non_validator_node_id));

        let msg_hash = Hash([129_u8; 32]);
        let mut sigs = Vec::new();

        for (node_id, certkey) in voting_keys.iter() {
            let sig = <T::SignatureType as CertificateSignature>::sign(msg_hash.as_ref(), certkey);
            sigs.push((*node_id, sig));
        }

        let non_validator_sig = <T::SignatureType as CertificateSignature>::sign(
            msg_hash.as_ref(),
            &non_validator_cert_key,
        );
        sigs.push((non_validator_node_id, non_validator_sig));

        assert_eq!(
            T::new(sigs, &valmap, msg_hash.as_ref()).unwrap_err(),
            SignatureCollectionError::NodeIdNotInMapping(vec![(
                non_validator_node_id,
                non_validator_sig
            )])
        );
    });

    test_all_signature_collection!(test_duplicate_sig, {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(5);

        let msg_hash = Hash([129_u8; 32]);
        let mut sigs = Vec::new();

        for (node_id, certkey) in voting_keys.iter() {
            let sig = <T::SignatureType as CertificateSignature>::sign(msg_hash.as_ref(), certkey);
            sigs.push((*node_id, sig));
        }

        // duplicate the last signature
        sigs.push(*sigs.last().unwrap());
        assert_eq!(sigs[sigs.len() - 1], sigs[sigs.len() - 2]);
        let sigcol = T::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        // duplicate signature is removed
        assert_eq!(sigcol.num_signatures(), voting_keys.len());
    });

    test_all_signature_collection!(test_verify, {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(5);

        let msg_hash = Hash([129_u8; 32]);
        let mut sigs = Vec::new();

        for (node_id, certkey) in voting_keys.iter() {
            let sig = <T::SignatureType as CertificateSignature>::sign(msg_hash.as_ref(), certkey);
            sigs.push((*node_id, sig));
        }

        let sigcol = T::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        let signers = sigcol.verify(&valmap, msg_hash.as_ref()).unwrap();

        let signers_set = signers.iter().collect::<HashSet<_>>();
        let expected_set = valmap.map.keys().collect::<HashSet<_>>();
        assert_eq!(signers_set, expected_set);
    });

    // invalid verification goes in specific impls
}
