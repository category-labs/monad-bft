use std::collections::{BTreeMap, HashMap, HashSet};

use log::{error, warn};
use monad_crypto::hasher::{Hash, Hashable, Hasher};
use monad_proto::proto::signing::ProtoMultiSig;
use monad_types::NodeId;
use prost::Message;

use crate::{
    certificate_signature::CertificateSignatureRecoverable,
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiSig<S> {
    pub sigs: Vec<S>,
}

impl<S: CertificateSignatureRecoverable> Default for MultiSig<S> {
    fn default() -> Self {
        Self { sigs: Vec::new() }
    }
}

impl<S: CertificateSignatureRecoverable> Hashable for MultiSig<S> {
    fn hash(&self, state: &mut impl Hasher) {
        for s in self.sigs.iter() {
            Hashable::hash(s, state);
        }
    }
}

impl<S: CertificateSignatureRecoverable> SignatureCollection for MultiSig<S> {
    type SignatureType = S;

    fn new(
        sigs: Vec<(NodeId, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::SignatureType>> {
        let mut sig_map = BTreeMap::new();
        // software bug: signature collector should check the node_id is in validator set
        let mut external_sigs = Vec::new();
        // "slashable" behavior
        let mut invalid_sigs = Vec::new();

        for (node_id, sig) in sigs.into_iter() {
            // check if node_id is in validator_mapping
            if let Some(pubkey) = validator_mapping.map.get(&node_id) {
                if let Ok(pubkey_recovered) = sig.recover_pubkey(msg) {
                    if pubkey_recovered != *pubkey {
                        warn!(
                            "Pubkey mismatch NodeId={:?} sig={:?} expected={:?} recovered={:?}",
                            node_id, sig, *pubkey, pubkey_recovered,
                        );
                        invalid_sigs.push((node_id, sig));
                        continue;
                    }

                    if sig.verify(msg, pubkey).is_err() {
                        warn!("Invalid sig NodeId={:?} sig={:?} ", node_id, sig);
                        invalid_sigs.push((node_id, sig));
                        continue;
                    }

                    let sig_entry = sig_map.entry(node_id).or_insert(sig);
                    if *sig_entry != sig {
                        // unreachable if signature algo is deterministic
                        // no two valid signatures can be created with the same keypair
                        error!(
                            "Conflicting signatures NodeId={:?} sig1={:?} sig2={:?}",
                            node_id, *sig_entry, sig
                        );
                        return Err(SignatureCollectionError::ConflictingSignatures((
                            node_id, *sig_entry, sig,
                        )));
                    }
                } else {
                    warn!(
                        "Failed to recover pubkey NodeId={:?} sig={:?}",
                        node_id, sig
                    );
                    invalid_sigs.push((node_id, sig));
                };
            } else {
                external_sigs.push((node_id, sig));
            }
        }

        if !external_sigs.is_empty() {
            return Err(SignatureCollectionError::NodeIdNotInMapping(external_sigs));
        }

        if !invalid_sigs.is_empty() {
            return Err(SignatureCollectionError::InvalidSignaturesCreate(
                invalid_sigs,
            ));
        }

        Ok(MultiSig {
            sigs: sig_map.into_values().collect::<Vec<_>>(),
        })
    }

    fn get_hash<H: Hasher>(&self) -> Hash {
        H::hash_object(self)
    }

    fn num_signatures(&self) -> usize {
        self.sigs.len()
    }

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Vec<NodeId>, SignatureCollectionError<Self::SignatureType>> {
        let mut node_ids = Vec::new();
        let mut verified_unvalidated_sigs = HashMap::new();
        let mut invalid_sigs = Vec::new();

        for sig in self.sigs.iter() {
            if let Ok(pubkey) = sig.recover_pubkey(msg) {
                if sig.verify(msg, &pubkey).is_err() {
                    warn!("Invalid sig sig={:?}", sig);
                    invalid_sigs.push(sig);
                    continue;
                }
                verified_unvalidated_sigs.insert(pubkey, *sig);
            } else {
                warn!("Failed to recover pubkey sig={:?}", sig);
                invalid_sigs.push(sig);
            }
        }

        // validate sigs: filter verified_unvalidated_sigs by validator node_id
        for (node_id, pk) in validator_mapping.map.iter() {
            if verified_unvalidated_sigs.contains_key(pk) {
                node_ids.push(*node_id);
                verified_unvalidated_sigs.remove(pk);
            }
        }

        // verified_sigs contains valid but non-validator signatures
        invalid_sigs.extend(verified_unvalidated_sigs.values());

        if !invalid_sigs.is_empty() {
            return Err(SignatureCollectionError::InvalidSignaturesVerify(
                verified_unvalidated_sigs.into_values().collect::<Vec<_>>(),
            ));
        }
        Ok(node_ids)
    }

    fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> HashSet<NodeId> {
        let mut node_ids = HashSet::new();
        let mut pub_keys = HashSet::new();

        for sig in self.sigs.iter() {
            if let Ok(pubkey) = sig.recover_pubkey(msg) {
                pub_keys.insert(pubkey);
            }
        }

        for (node_id, pk) in validator_mapping.map.iter() {
            if pub_keys.contains(pk) {
                node_ids.insert(*node_id);
            }
        }

        node_ids
    }

    fn serialize(&self) -> Vec<u8> {
        let proto: ProtoMultiSig = self.into();
        proto.encode_to_vec()
    }

    fn deserialize(data: &[u8]) -> Result<Self, SignatureCollectionError<Self::SignatureType>> {
        let multisig = ProtoMultiSig::decode(data)
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))?;
        multisig
            .try_into()
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_crypto::{hasher::Hash, secp256k1::SecpSignature};
    use monad_testutil::signing::get_key;
    use monad_types::NodeId;
    use rand::{seq::SliceRandom, SeedableRng};
    use rand_chacha::ChaChaRng;
    use test_case::test_case;

    use super::MultiSig;
    use crate::{
        certificate_signature::CertificateSignature,
        signature_collection::{
            SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
        },
        test_utils::setup_sigcol_test,
    };

    type SignatureCollectionType = MultiSig<SecpSignature>;

    fn get_sigs<'a>(
        msg: &[u8],
        iter: impl Iterator<
            Item = &'a (
                NodeId,
                SignatureCollectionKeyPairType<SignatureCollectionType>,
            ),
        >,
    ) -> Vec<(
        NodeId,
        <SignatureCollectionType as SignatureCollection>::SignatureType,
    )> {
        crate::test_utils::get_sigs::<SignatureCollectionType>(msg, iter)
    }

    #[test_case(7, 5, 1; "seed 7, 1/5 invalid")]
    #[test_case(7, 32, 1; "seed 7, 1/32 invalid")]
    #[test_case(7, 32, 5; "seed 7, 5/32 invalid")]
    #[test_case(8, 32, 5; "seed 8, 5/32 invalid")]
    #[test_case(7, 32, 32; "seed 7, 32/32 invalid")]
    #[test_case(8, 32, 32; "seed 8, 32/32 invalid")]
    fn test_creation_invalid_signature(seed: u64, num_keys: u32, num_invalid: u32) {
        assert!(num_invalid <= num_keys);
        let (cert_keys, valmap) = setup_sigcol_test::<SignatureCollectionType>(num_keys);

        let msg_hash = Hash([129_u8; 32]);
        let invalid_msg_hash = Hash([229_u8; 32]);

        let invalid_sigs = get_sigs(
            invalid_msg_hash.as_ref(),
            cert_keys.iter().take(num_invalid as usize),
        );

        let mut sigs = get_sigs(
            msg_hash.as_ref(),
            cert_keys.iter().skip(num_invalid as usize),
        );
        sigs.extend(invalid_sigs.clone());

        sigs.shuffle(&mut ChaChaRng::seed_from_u64(seed));

        let sigcol_err =
            SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap_err();

        assert!(matches!(
            sigcol_err,
            SignatureCollectionError::InvalidSignaturesCreate(_)
        ));

        match sigcol_err {
            SignatureCollectionError::InvalidSignaturesCreate(sigs) => {
                let expected_set = invalid_sigs.into_iter().collect::<HashSet<_>>();
                let test_set = sigs.into_iter().collect::<HashSet<_>>();

                assert_eq!(expected_set, test_set);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_verify_invalid_signature() {
        let (cert_keys, valmap) = setup_sigcol_test::<SignatureCollectionType>(5);

        let msg_hash = Hash([129_u8; 32]);
        let invalid_msg_hash = Hash([229_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), cert_keys.iter());

        let mut sig_col = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap();

        // replace last one with an invalid signature
        let _ = sig_col.sigs.pop().unwrap();
        let invalid_sig = << SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(invalid_msg_hash.as_ref(), &cert_keys[0].1);
        sig_col.sigs.push(invalid_sig);

        let err = sig_col.verify(&valmap, msg_hash.as_ref()).unwrap_err();

        assert_eq!(
            err,
            SignatureCollectionError::InvalidSignaturesVerify(vec![invalid_sig])
        );
    }

    #[test]
    fn test_verify_non_validator_signer() {
        let (cert_keys, valmap) = setup_sigcol_test::<SignatureCollectionType>(5);
        let non_validator_cert_key = get_key(100);

        assert_eq!(
            valmap
                .map
                .values()
                .find(|&&pk| pk == non_validator_cert_key.pubkey()),
            None
        );

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), cert_keys.iter());

        let mut sig_col = SignatureCollectionType::new(sigs, &valmap, msg_hash.as_ref()).unwrap();
        // add a signature from a non-validator signer
        let sig = << SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg_hash.as_ref(), &non_validator_cert_key);
        sig_col.sigs.push(sig);

        assert_eq!(
            sig_col.verify(&valmap, msg_hash.as_ref()).unwrap_err(),
            SignatureCollectionError::InvalidSignaturesVerify(vec![sig])
        );
    }
}
