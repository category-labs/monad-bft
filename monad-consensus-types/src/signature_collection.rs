use std::collections::HashSet;

use monad_crypto::hasher::{Hash, Hashable, Hasher};
use monad_types::NodeId;

use crate::{
    certificate_signature::{CertificateKeyPair, CertificateSignature},
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
    InvalidSignaturesCreate(Vec<(NodeId, S)>),
    InvalidSignaturesVerify(Vec<S>),
    DeserializeError(String),
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
            SignatureCollectionError::InvalidSignaturesCreate(sig) => {
                write!(f, "Invalid signature on create: ({sig:?})")
            }
            SignatureCollectionError::InvalidSignaturesVerify(sig) => {
                write!(f, "Invalid signature on verify: ({sig:?})")
            }
            SignatureCollectionError::DeserializeError(err) => {
                write!(f, "Deserialization error {:?}", err)
            }
        }
    }
}

impl<S: CertificateSignature> std::error::Error for SignatureCollectionError<S> {}

pub trait SignatureCollection:
    Clone + Hashable + Eq + Send + Sync + std::fmt::Debug + 'static
{
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

    /**
     * Get participants doesn't verify the validity of the certificate,
     * but retrieve any valid nodeId participated given a validator mapping.
     */
    fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> HashSet<NodeId>;
    // TODO-4: deprecate this function: only used by tests
    fn num_signatures(&self) -> usize;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(data: &[u8]) -> Result<Self, SignatureCollectionError<Self::SignatureType>>;
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_crypto::hasher::Hash;
    use monad_testutil::signing::get_key;
    use monad_types::NodeId;

    use crate::{
        certificate_signature::CertificateSignature,
        signature_collection::{SignatureCollection, SignatureCollectionError},
        test_utils::{get_certificate_key, get_sigs, setup_sigcol_test},
    };

    macro_rules! test_all_signature_collection {
        ($test_name:ident, $test_code:expr) => {
            mod $test_name {
                use monad_crypto::{secp256k1::SecpSignature, NopSignature};
                use test_case::test_case;

                use super::*;
                use crate::{bls::BlsSignatureCollection, multi_sig::*};

                fn invoke<T: SignatureCollection>(num_keys: u32) {
                    $test_code(num_keys)
                }

                #[test_case(1; "1 sig")]
                #[test_case(5; "5 sigs")]
                #[test_case(100; "100 sigs")]
                fn multi_sig_secp(num_keys: u32) {
                    invoke::<MultiSig<SecpSignature>>(num_keys);
                }

                #[test_case(1; "1 sig")]
                #[test_case(5; "5 sigs")]
                #[test_case(100; "100 sigs")]
                fn multi_sig_nop(num_keys: u32) {
                    invoke::<MultiSig<NopSignature>>(num_keys);
                }

                #[test_case(1; "1 sig")]
                #[test_case(5; "5 sigs")]
                #[test_case(100; "100 sigs")]
                fn bls(num_keys: u32) {
                    invoke::<BlsSignatureCollection>(num_keys);
                }
            }
        };
    }

    test_all_signature_collection!(test_creation, |num_keys| {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter());

        let sigcol = T::new(sigs, &valmap, msg_hash.as_ref());
        assert!(sigcol.is_ok());
    });

    test_all_signature_collection!(test_creation_steal_signature, |num_keys| {
        if num_keys <= 1 {
            // skip test because we can't "steal" others signature
            return;
        }
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter().skip(1));

        // the first voter steals the signature of the second voter
        let first_voter_node_id = voting_keys[0].0;
        let invalid_sig = sigs.first().unwrap().1;
        sigs.push((first_voter_node_id, invalid_sig));

        let sigcol_err = T::new(sigs, &valmap, msg_hash.as_ref());
        assert!(matches!(
            sigcol_err,
            Err(SignatureCollectionError::InvalidSignaturesCreate(_))
        ));

        match sigcol_err.unwrap_err() {
            SignatureCollectionError::InvalidSignaturesCreate(sigs) => {
                let expected_set = vec![(first_voter_node_id, invalid_sig)]
                    .into_iter()
                    .collect::<HashSet<_>>();

                let test_set = sigs.into_iter().collect::<HashSet<_>>();

                assert_eq!(expected_set, test_set);
            }
            _ => unreachable!(),
        }
    });

    test_all_signature_collection!(test_num_signatures, |num_keys| {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);
        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter());

        let sig_col = T::new(sigs, &valmap, msg_hash.as_ref()).unwrap();
        assert_eq!(sig_col.num_signatures(), num_keys as usize);
    });

    test_all_signature_collection!(test_node_id_not_in_validator_mapping, |num_keys| {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);

        let non_validator_node_id = NodeId(get_key(100).pubkey());
        let non_validator_cert_key = get_certificate_key::<T>(100);

        assert!(!valmap.map.contains_key(&non_validator_node_id));

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter());

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

    test_all_signature_collection!(test_duplicate_sig, |num_keys| {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter());

        // duplicate the last signature
        sigs.push(*sigs.last().unwrap());
        assert_eq!(sigs[sigs.len() - 1], sigs[sigs.len() - 2]);
        let sigcol = T::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        // duplicate signature is removed
        assert_eq!(sigcol.num_signatures(), voting_keys.len());
    });

    test_all_signature_collection!(test_verify, |num_keys| {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter());
        let sigcol = T::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        let signers = sigcol.verify(&valmap, msg_hash.as_ref()).unwrap();

        let signers_set = signers.iter().collect::<HashSet<_>>();
        let expected_set = valmap.map.keys().collect::<HashSet<_>>();
        assert_eq!(signers_set, expected_set);
    });

    test_all_signature_collection!(test_get_participants, |num_keys| {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter());
        let sigcol = T::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        let signers = sigcol.get_participants(&valmap, msg_hash.as_ref());

        let expected_set = valmap.map.into_keys().collect::<HashSet<_>>();
        assert_eq!(signers, expected_set);
    });

    test_all_signature_collection!(test_serialization_roundtrip, |num_keys| {
        let (voting_keys, valmap) = setup_sigcol_test::<T>(num_keys);

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs::<T>(msg_hash.as_ref(), voting_keys.iter());
        let sigcol = T::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        let sigcol_bytes = sigcol.serialize();
        assert_eq!(sigcol, T::deserialize(&sigcol_bytes).unwrap());
    });

    test_all_signature_collection!(test_invalid_deserialization, |_num_keys| {
        let bytes = [127; 27];
        let sig_col_err = T::deserialize(&bytes).unwrap_err();

        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::DeserializeError(_)
        ));
    });

    // invalid verification goes in specific impls
}
