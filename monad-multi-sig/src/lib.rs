// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::{BTreeMap, HashMap};

use alloy_rlp::{BytesMut, Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_crypto::{
    certificate_signature::CertificateSignatureRecoverable, signing_domain::SigningDomain,
};
use monad_types::NodeId;
use monad_validator::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
        SignatureCollectionPubKeyType,
    },
    validator_mapping::ValidatorMapping,
};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

#[derive(Clone, Debug, PartialEq, Eq, RlpDecodable, RlpEncodable, Serialize, Deserialize)]
pub struct MultiSig<S> {
    pub sigs: Vec<S>,
}

impl<S: CertificateSignatureRecoverable> Default for MultiSig<S> {
    fn default() -> Self {
        Self { sigs: Vec::new() }
    }
}

impl<S: CertificateSignatureRecoverable> SignatureCollection for MultiSig<S> {
    type NodeIdPubKey = SignatureCollectionPubKeyType<Self>;
    type SignatureType = S;

    fn new<SD: SigningDomain>(
        sigs: impl IntoIterator<Item = (NodeId<Self::NodeIdPubKey>, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>> {
        let mut sig_map = BTreeMap::new();
        // software bug: signature collector should check the node_id is in validator set
        let mut external_sigs = Vec::new();
        // "slashable" behavior
        let mut invalid_sigs = Vec::new();

        for (node_id, sig) in sigs {
            // check if node_id is in validator_mapping
            if let Some(pubkey) = validator_mapping.map.get(&node_id) {
                if let Ok(pubkey_recovered) = sig.recover_pubkey::<SD>(msg) {
                    if pubkey_recovered != *pubkey {
                        warn!(
                            "Pubkey mismatch NodeId={:?} sig={:?} expected={:?} recovered={:?}",
                            node_id, sig, *pubkey, pubkey_recovered,
                        );
                        invalid_sigs.push((node_id, sig));
                        continue;
                    }

                    if sig.verify::<SD>(msg, pubkey).is_err() {
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

    fn num_signatures(&self) -> usize {
        self.sigs.len()
    }

    fn verify<SD: SigningDomain>(
        &self,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> Result<
        Vec<NodeId<Self::NodeIdPubKey>>,
        SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>,
    > {
        let mut node_ids = Vec::new();
        let mut verified_unvalidated_sigs = HashMap::new();
        let mut invalid_sigs = Vec::new();

        for sig in self.sigs.iter() {
            if let Ok(pubkey) = sig.recover_pubkey::<SD>(msg) {
                if sig.verify::<SD>(msg, &pubkey).is_err() {
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
            return Err(SignatureCollectionError::InvalidSignaturesVerify);
        }
        Ok(node_ids)
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        buf.to_vec()
    }

    fn deserialize(
        data: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>> {
        Self::decode(&mut data.as_ref())
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::Hash,
        signing_domain, NopSignature,
    };
    use monad_testutil::{
        signing::{get_certificate_key, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::NodeId;
    use monad_validator::{
        signature_collection::{
            SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
        },
        validator_set::ValidatorSetFactory,
    };
    use rand::{seq::SliceRandom, SeedableRng};
    use rand_chacha::ChaChaRng;
    use test_case::test_case;

    use super::MultiSig;

    type SigningDomainType = signing_domain::Vote;
    type SignatureType = NopSignature;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;
    type SignatureCollectionType = MultiSig<SignatureType>;

    fn get_sigs<'a>(
        msg: &[u8],
        iter: impl Iterator<
            Item = &'a (
                NodeId<PubKeyType>,
                SignatureCollectionKeyPairType<SignatureCollectionType>,
            ),
        >,
    ) -> Vec<(
        NodeId<PubKeyType>,
        <SignatureCollectionType as SignatureCollection>::SignatureType,
    )> {
        let mut sigs = Vec::new();
        for (node_id, key) in iter {
            let sig =
                <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<SigningDomainType>(msg, key);
            sigs.push((*node_id, sig));
        }
        sigs
    }

    /// SUCCESSFUL SIGNATURE_COLLECTION TESTS

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_creation(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        let sigcol =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref());
        assert!(sigcol.is_ok());
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_creation_steal_signature(num_keys: u32) {
        if num_keys <= 1 {
            // skip test because we can't "steal" others signature
            return;
        }
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter().skip(1));

        // the first voter steals the signature of the second voter
        let first_voter_node_id = voting_keys[0].0;
        let invalid_sig = sigs.first().unwrap().1;
        sigs.push((first_voter_node_id, invalid_sig));

        let sigcol_err =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref());
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
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_num_signatures(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();
        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        let sig_col =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap();
        assert_eq!(sig_col.num_signatures(), num_keys as usize);
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_node_id_not_in_validator_mapping(num_keys: u32) {
        use monad_crypto::certificate_signature::CertificateKeyPair;
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let non_validator_node_id = NodeId::new(get_key::<SignatureType>(100).pubkey());
        let non_validator_cert_key = get_certificate_key::<SignatureCollectionType>(100);

        assert!(!valmap.map.contains_key(&non_validator_node_id));

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        let non_validator_sig =
            <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<SigningDomainType>(
                msg_hash.as_ref(),
                &non_validator_cert_key,
            );
        sigs.push((non_validator_node_id, non_validator_sig));

        assert_eq!(
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap_err(),
            SignatureCollectionError::NodeIdNotInMapping(vec![(
                non_validator_node_id,
                non_validator_sig
            )])
        );
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_duplicate_sig(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let mut sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());

        // duplicate the last signature
        sigs.push(*sigs.last().unwrap());
        assert_eq!(sigs[sigs.len() - 1], sigs[sigs.len() - 2]);
        let sigcol =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap();

        // duplicate signature is removed
        assert_eq!(sigcol.num_signatures(), voting_keys.len());
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_verify(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());
        let sigcol =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap();

        let signers = sigcol
            .verify::<SigningDomainType>(&valmap, msg_hash.as_ref())
            .unwrap();

        let signers_set = signers.iter().collect::<HashSet<_>>();
        let expected_set = valmap.map.keys().collect::<HashSet<_>>();
        assert_eq!(signers_set, expected_set);
    }

    #[test_case(1; "1 sig")]
    #[test_case(5; "5 sigs")]
    #[test_case(100; "100 sigs")]
    fn test_serialization_roundtrip(num_keys: u32) {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());
        let sigcol =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap();

        let sigcol_bytes = sigcol.serialize();
        assert_eq!(
            sigcol,
            SignatureCollectionType::deserialize(&sigcol_bytes).unwrap()
        );
    }

    #[test]
    fn test_invalid_deserialization() {
        let bytes = [127; 27];
        let sig_col_err = SignatureCollectionType::deserialize(&bytes).unwrap_err();

        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::DeserializeError(_)
        ));
    }

    // INVALID TESTS
    #[test_case(7, 5, 1; "seed 7, 1/5 invalid")]
    #[test_case(7, 32, 1; "seed 7, 1/32 invalid")]
    #[test_case(7, 32, 5; "seed 7, 5/32 invalid")]
    #[test_case(8, 32, 5; "seed 8, 5/32 invalid")]
    #[test_case(7, 32, 32; "seed 7, 32/32 invalid")]
    #[test_case(8, 32, 32; "seed 8, 32/32 invalid")]
    fn test_creation_invalid_signature(seed: u64, num_keys: u32, num_invalid: u32) {
        assert!(num_invalid <= num_keys);
        let (keys, cert_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_keys, ValidatorSetFactory::default());
        let cert_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(cert_keys)
            .collect();

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
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap_err();

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
        let (keys, cert_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(5, ValidatorSetFactory::default());
        let cert_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(cert_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);
        let invalid_msg_hash = Hash([229_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), cert_keys.iter());

        let mut sig_col =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap();

        // replace last one with an invalid signature
        let _ = sig_col.sigs.pop().unwrap();
        let invalid_sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<SigningDomainType>(invalid_msg_hash.as_ref(), &cert_keys[0].1);
        sig_col.sigs.push(invalid_sig);

        let err = sig_col
            .verify::<SigningDomainType>(&valmap, msg_hash.as_ref())
            .unwrap_err();

        assert_eq!(err, SignatureCollectionError::InvalidSignaturesVerify);
    }

    #[test]
    fn test_verify_non_validator_signer() {
        let (keys, cert_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(5, ValidatorSetFactory::default());
        let cert_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(cert_keys)
            .collect();
        let non_validator_cert_key = get_key::<SignatureType>(100);

        assert_eq!(
            valmap
                .map
                .values()
                .find(|&&pk| pk == non_validator_cert_key.pubkey()),
            None
        );

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), cert_keys.iter());

        let mut sig_col =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg_hash.as_ref())
                .unwrap();
        // add a signature from a non-validator signer
        let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<SigningDomainType>(msg_hash.as_ref(), &non_validator_cert_key);
        sig_col.sigs.push(sig);

        assert_eq!(
            sig_col
                .verify::<SigningDomainType>(&valmap, msg_hash.as_ref())
                .unwrap_err(),
            SignatureCollectionError::InvalidSignaturesVerify
        );
    }
}
