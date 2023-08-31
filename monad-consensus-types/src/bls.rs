use std::collections::{HashMap, VecDeque};

use bitvec::prelude::*;
use monad_crypto::bls12_381::{
    BlsAggregatePubKey, BlsAggregateSignature, BlsKeyPair, BlsSignature,
};
use monad_proto::proto::signing::ProtoBlsSignatureCollection;
use monad_types::{Hash, NodeId};
use prost::Message;

use crate::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    validation::{Hashable, Hasher},
    voting::ValidatorMapping,
};

#[derive(Debug)]
struct AggregationTree {
    nodes: Vec<BlsSignatureCollection>,
}

impl AggregationTree {
    fn new(
        sigs: &Vec<(NodeId, BlsSignature)>,
        validator_mapping: &ValidatorMapping<BlsKeyPair>,
        validator_index: &HashMap<NodeId, usize>,
    ) -> Self {
        // build the binary heap represented as vector
        if sigs.is_empty() {
            return Self {
                nodes: vec![BlsSignatureCollection::with_capacity(
                    validator_mapping.map.len(),
                )],
            };
        }

        let n = sigs.len();
        let total_nodes = n * 2 - 1;
        let mut nodes =
            vec![BlsSignatureCollection::with_capacity(validator_mapping.map.len()); total_nodes];

        // copy all the signature as leaves
        for (i, (node_id, sig)) in sigs.iter().enumerate() {
            let cert = nodes.get_mut(n + i - 1).expect("node in range");
            cert.signers.set(
                *validator_index.get(node_id).expect("validator index"),
                true,
            );
            cert.sig.add_assign(sig).expect("bls add assign");
        }

        // build the tree bottom-up. A node is the aggregation of its two children
        if n > 1 {
            for i in (0..n - 1).rev() {
                let left = &nodes[2 * i + 1];
                let right = &nodes[2 * i + 2];

                *nodes.get_mut(i).unwrap() = merge_nodes(left, right);
            }
        }

        Self { nodes }
    }

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<BlsKeyPair>,
        msg: &[u8],
    ) -> Result<BlsSignatureCollection, Vec<BlsSignature>> {
        // verify the certificate, if invalid, search the binary tree for invalid sig
        let mut unverified_certs = VecDeque::new();
        unverified_certs.push_back(0_usize);
        let mut invalid_sig = Vec::new();

        while !unverified_certs.is_empty() {
            let cert_idx = unverified_certs.pop_front().unwrap();
            let cert = &self.nodes[cert_idx];
            match cert.verify(validator_mapping, msg) {
                Ok(_) => {}
                Err(_) => {
                    if 2 * cert_idx + 1 >= self.nodes.len() {
                        invalid_sig.push(cert.sig.as_signature());
                    } else {
                        unverified_certs.push_back(cert_idx * 2 + 1);
                        unverified_certs.push_back(cert_idx * 2 + 2);
                    }
                }
            }
        }

        if invalid_sig.is_empty() {
            Ok(self.nodes[0].clone())
        } else {
            Err(invalid_sig)
        }
    }
}

fn merge_nodes(n1: &BlsSignatureCollection, n2: &BlsSignatureCollection) -> BlsSignatureCollection {
    assert_eq!(n1.signers.len(), n2.signers.len());

    let signers = n1.signers.clone() | n2.signers.clone();
    let mut sig = n1.sig;
    sig.add_assign_aggregate(&n2.sig);
    let cert = BlsSignatureCollection { signers, sig };
    // the signer sets should be disjoint
    assert_eq!(
        n1.num_signatures() + n2.num_signatures(),
        cert.num_signatures()
    );

    cert
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlsSignatureCollection {
    pub signers: BitVec<u8, Msb0>,
    pub sig: BlsAggregateSignature,
}

impl Hashable for BlsSignatureCollection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bitvec_slice = self.signers.as_raw_slice();
        state.update(bitvec_slice);
        self.sig.hash(state);
    }
}

impl BlsSignatureCollection {
    fn with_capacity(n: usize) -> Self {
        let signers = bitvec![u8, Msb0; 0; n];
        let sig = BlsAggregateSignature::infinity();
        Self { signers, sig }
    }
}

impl SignatureCollection for BlsSignatureCollection {
    type SignatureType = BlsSignature;

    fn new(
        sigs: Vec<(NodeId, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::SignatureType>> {
        let mut sigs_map = HashMap::new();
        let mut non_validator = Vec::new();
        let mut validator_index = HashMap::new();

        for (i, node_id) in validator_mapping.map.keys().enumerate() {
            validator_index.insert(*node_id, i);
        }
        for (node_id, sig) in sigs.iter() {
            if !validator_mapping.map.contains_key(node_id) {
                non_validator.push((*node_id, *sig));
            }
            let entry = sigs_map.entry(*node_id).or_insert(*sig);
            if *entry != *sig {
                return Err(SignatureCollectionError::ConflictingSignatures((
                    *node_id, *entry, *sig,
                )));
            }
        }

        if !non_validator.is_empty() {
            return Err(SignatureCollectionError::NodeIdNotInMapping(non_validator));
        }

        // use a binary tree to store intermediate aggregation result
        let tree = AggregationTree::new(
            &sigs_map.into_iter().collect::<Vec<_>>(),
            validator_mapping,
            &validator_index,
        );

        tree.verify(validator_mapping, msg)
            .map_err(SignatureCollectionError::InvalidSignatures)
    }

    fn get_hash<H: Hasher>(&self) -> Hash {
        H::hash_object(self)
    }

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Vec<NodeId>, SignatureCollectionError<Self::SignatureType>> {
        assert_eq!(self.signers.len(), validator_mapping.map.len());

        let mut aggpk = BlsAggregatePubKey::infinity();
        let mut signers = Vec::new();
        for (bit, (node_id, pubkey)) in self.signers.iter().zip(validator_mapping.map.iter()) {
            if *bit {
                aggpk.add_assign(pubkey).expect("pubkey aggregation");
                signers.push(*node_id);
            }
        }

        // return empty signers for empty signature collection
        if signers.is_empty() && self.sig == BlsAggregateSignature::infinity() {
            return Ok(signers);
        }

        if self.sig.fast_verify(msg, &aggpk).is_err() {
            return Err(SignatureCollectionError::InvalidSignatures(vec![self
                .sig
                .as_signature()]));
        }
        Ok(signers)
    }

    fn num_signatures(&self) -> usize {
        self.signers.count_ones()
    }

    fn serialize(&self) -> Vec<u8> {
        let proto: ProtoBlsSignatureCollection = self.into();
        proto.encode_to_vec()
    }

    fn deserialize(data: &[u8]) -> Result<Self, SignatureCollectionError<Self::SignatureType>> {
        let bls = ProtoBlsSignatureCollection::decode(data)
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))?;
        bls.try_into()
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use monad_types::NodeId;
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
    use test_case::test_case;

    use super::{merge_nodes, AggregationTree, BlsSignatureCollection};
    use crate::{
        certificate_signature::CertificateSignature,
        signature_collection::{
            SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
        },
        test_utils::setup_sigcol_test,
    };

    type SignatureCollectionType = BlsSignatureCollection;

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

    #[test]
    fn test_aggregation_tree() {
        // with 7 nodes, the tree looks like
        //                        (0123456)
        //                  /                  \
        //            (1234)                    (056)
        //          /        \                 /     \
        //     (12)          (34)          (56)       (0)
        //     /  \          /   \         /   \
        //  (1)    (2)    (3)    (4)    (5)    (6)

        let (voting_keys, valmap) = setup_sigcol_test::<SignatureCollectionType>(7);

        let mut validator_index = HashMap::new();

        for (i, node_id) in valmap.map.keys().enumerate() {
            validator_index.insert(*node_id, i);
        }

        let msg = b"hello world";
        let sigs = get_sigs(msg, voting_keys.iter());

        let tree = AggregationTree::new(&sigs, &valmap, &validator_index);

        let expected_n0 = SignatureCollectionType::new(sigs.clone(), &valmap, msg).unwrap();
        let expected_n1 =
            SignatureCollectionType::new(vec![sigs[1], sigs[2], sigs[3], sigs[4]], &valmap, msg)
                .unwrap();
        let expected_n2 =
            SignatureCollectionType::new(vec![sigs[0], sigs[5], sigs[6]], &valmap, msg).unwrap();
        let expected_n3 =
            SignatureCollectionType::new(vec![sigs[1], sigs[2]], &valmap, msg).unwrap();
        let expected_n4 =
            SignatureCollectionType::new(vec![sigs[3], sigs[4]], &valmap, msg).unwrap();
        let expected_n5 =
            SignatureCollectionType::new(vec![sigs[5], sigs[6]], &valmap, msg).unwrap();
        let expected_n6 = SignatureCollectionType::new(vec![sigs[0]], &valmap, msg).unwrap();
        let expected_n7 = SignatureCollectionType::new(vec![sigs[1]], &valmap, msg).unwrap();
        let expected_n8 = SignatureCollectionType::new(vec![sigs[2]], &valmap, msg).unwrap();
        let expected_n9 = SignatureCollectionType::new(vec![sigs[3]], &valmap, msg).unwrap();
        let expected_n10 = SignatureCollectionType::new(vec![sigs[4]], &valmap, msg).unwrap();
        let expected_n11 = SignatureCollectionType::new(vec![sigs[5]], &valmap, msg).unwrap();
        let expected_n12 = SignatureCollectionType::new(vec![sigs[6]], &valmap, msg).unwrap();

        let expected = vec![
            expected_n0,
            expected_n1,
            expected_n2,
            expected_n3,
            expected_n4,
            expected_n5,
            expected_n6,
            expected_n7,
            expected_n8,
            expected_n9,
            expected_n10,
            expected_n11,
            expected_n12,
        ];

        assert_eq!(tree.nodes, expected);
    }

    #[test_case(5,0; "5 signatures, 0-5 split")]
    #[test_case(5,1; "5 signatures, 1-4 split")]
    #[test_case(5,2; "5 signatures, 2-3 split")]
    fn test_merge_node(num_sigs: u32, first: usize) {
        assert!(num_sigs as usize >= first);
        let (voting_keys, valmap) = setup_sigcol_test::<SignatureCollectionType>(num_sigs);

        let msg = b"hello world";

        let sigs1 = get_sigs(msg, voting_keys.iter().take(first));
        let sigs2 = get_sigs(msg, voting_keys.iter().skip(first));

        let sc1 = SignatureCollectionType::new(sigs1, &valmap, msg).unwrap();
        let sc2 = SignatureCollectionType::new(sigs2, &valmap, msg).unwrap();

        let sigs_all = get_sigs(msg, voting_keys.iter());

        let sc_all = SignatureCollectionType::new(sigs_all, &valmap, msg).unwrap();

        let sc_merged = merge_nodes(&sc1, &sc2);
        assert_eq!(sc_all, sc_merged);
    }

    #[test_case(7, 1, 1; "seed 7, 1/1 invalid")]
    #[test_case(7, 2, 1; "seed 7, 1/2 invalid")]
    #[test_case(7, 3, 1; "seed 7, 1/3 invalid")]
    #[test_case(7, 6, 1; "seed 7, 1/6 invalid")]
    #[test_case(7, 6, 3; "seed 7, 3/6 invalid")]
    #[test_case(7, 6, 6; "seed 7, 6/6 invalid")]
    #[test_case(7, 32, 1; "seed 7, 1/32 invalid")]
    #[test_case(7, 32, 10; "seed 7, 10/32 invalid")]
    #[test_case(7, 32, 32; "seed 7, 32/32 invalid")]

    fn test_creation_multiple_invalid(seed: u64, num_sigs: u32, num_invalid: u32) {
        assert!(num_invalid <= num_sigs);
        let (voting_keys, valmap) = setup_sigcol_test::<SignatureCollectionType>(num_sigs);

        let msg = b"hello world";
        let wrong_msg = b"bye world";

        let invalid_sigs = get_sigs(wrong_msg, voting_keys.iter().take(num_invalid as usize));
        let mut sigs = get_sigs(msg, voting_keys.iter().skip(num_invalid as usize));
        sigs.extend(invalid_sigs.clone());

        // shuffle the signature ordering
        sigs.shuffle(&mut StdRng::seed_from_u64(seed));

        let sig_col_err = SignatureCollectionType::new(sigs, &valmap, msg).unwrap_err();
        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::InvalidSignatures(_)
        ));

        match sig_col_err {
            SignatureCollectionError::InvalidSignatures(sigs) => {
                let test_set = sigs.into_iter().collect::<HashSet<_>>();
                let invalid_set = invalid_sigs
                    .into_iter()
                    .map(|(_node_id, sig)| sig)
                    .collect::<HashSet<_>>();

                assert_eq!(test_set, invalid_set);
            }
            _ => {
                unreachable!()
            }
        };
    }

    #[test]
    fn test_conflict_signatures() {
        let (voting_keys, valmap) = setup_sigcol_test::<SignatureCollectionType>(5);

        let msg = b"hello world";
        let invalid_msg = b"bye world";

        let mut sigs = Vec::new();

        for (node_id, key) in voting_keys.iter().skip(1) {
            let sig =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg, key);
            sigs.push((*node_id, sig));
        }

        let expected_node_id = voting_keys[0].0;
        let valid_sig =  < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg, &voting_keys[0].1);
        let invalid_sig = < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(invalid_msg, &voting_keys[0].1);
        sigs.push((expected_node_id, valid_sig));
        sigs.push((expected_node_id, invalid_sig));

        let sig_col_err = SignatureCollectionType::new(sigs, &valmap, msg).unwrap_err();

        // resolves to ConflictingSignatures error because verification is deferred
        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::ConflictingSignatures(_)
        ));

        match sig_col_err {
            SignatureCollectionError::ConflictingSignatures((node_id, s1, s2)) => {
                assert_eq!(node_id, expected_node_id);
                assert!(
                    (s1 == valid_sig && s2 == invalid_sig)
                        || (s1 == invalid_sig || s2 == valid_sig)
                );
            }
            _ => {
                unreachable!()
            }
        };
    }
}
