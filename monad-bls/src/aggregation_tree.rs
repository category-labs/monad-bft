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

use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
};

use alloy_rlp::{encode_list, BytesMut, Decodable, Encodable, RlpDecodable, RlpEncodable};
use bitvec::prelude::*;
use monad_crypto::{certificate_signature::PubKey, signing_domain::SigningDomain};
use monad_types::NodeId;
use monad_validator::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    validator_mapping::ValidatorMapping,
};
use serde::Serialize;

use crate::{bls::BlsAggregatePubKey, BlsAggregateSignature, BlsKeyPair, BlsSignature};

const MAX_SIGNERS_LEN: usize = 1024 * 16;

#[derive(Debug)]
struct AggregationTree<PT: PubKey> {
    nodes: Vec<BlsSignatureCollection<PT>>,
}

impl<PT: PubKey> AggregationTree<PT> {
    fn new(
        sigs: &[(NodeId<PT>, BlsSignature)],
        validator_mapping: &ValidatorMapping<PT, BlsKeyPair>,
        validator_index: &HashMap<NodeId<PT>, usize>,
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
            cert.signers.0.set(
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

    fn verify<SD: SigningDomain>(
        &self,
        validator_mapping: &ValidatorMapping<PT, BlsKeyPair>,
        msg: &[u8],
    ) -> Result<BlsSignatureCollection<PT>, Vec<(NodeId<PT>, BlsSignature)>> {
        // verify the certificate, if invalid, search the binary tree for invalid sig
        let mut unverified_certs = VecDeque::new();
        unverified_certs.push_back(0_usize);
        let mut invalid_sig = Vec::new();

        while !unverified_certs.is_empty() {
            let cert_idx = unverified_certs.pop_front().unwrap();
            let cert = &self.nodes[cert_idx];
            match cert.verify::<SD>(validator_mapping, msg) {
                Ok(_) => {}
                Err(_) => {
                    if 2 * cert_idx + 1 >= self.nodes.len() {
                        let signer_idx = cert
                            .signers
                            .0
                            .first_one()
                            .expect("signer should be one-hot encoded");
                        let (node_id, _) = validator_mapping
                            .map
                            .iter()
                            .nth(signer_idx)
                            .expect("signer idx in range");

                        invalid_sig.push((*node_id, cert.sig.as_signature()));
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

fn merge_nodes<PT: PubKey>(
    n1: &BlsSignatureCollection<PT>,
    n2: &BlsSignatureCollection<PT>,
) -> BlsSignatureCollection<PT> {
    assert_eq!(n1.signers.0.len(), n2.signers.0.len());

    let signers = n1.signers.0.clone() | n2.signers.0.clone();
    let mut sig = n1.sig;
    sig.add_assign_aggregate(&n2.sig);
    let cert = BlsSignatureCollection {
        signers: SignerMap(signers),
        sig,
        _phantom: PhantomData,
    };
    // the signer sets should be disjoint
    assert_eq!(
        n1.num_signatures() + n2.num_signatures(),
        cert.num_signatures()
    );

    cert
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SignerMap(pub BitVec<u8, Lsb0>);

#[derive(Clone, PartialEq, Eq, RlpDecodable, RlpEncodable, Serialize)]
pub struct BlsSignatureCollection<PT: PubKey> {
    pub signers: SignerMap,

    pub sig: BlsAggregateSignature,

    pub(crate) _phantom: PhantomData<PT>,
}

impl<PT: PubKey> std::fmt::Debug for BlsSignatureCollection<PT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlsSignatureCollection")
            .field("signers", &format_args!("{:?}", &self.signers))
            .field("sig", &self.sig)
            .finish()
    }
}

impl<PT: PubKey> BlsSignatureCollection<PT> {
    fn with_capacity(n: usize) -> Self {
        let signers = SignerMap(bitvec![u8, Lsb0; 0; n]);
        let sig = BlsAggregateSignature::infinity();
        Self {
            signers,
            sig,
            _phantom: PhantomData,
        }
    }
}

impl Encodable for SignerMap {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let num_bits: u32 = self.0.len() as u32;
        let num_bytes = self.0.len().div_ceil(8);

        let mut buf = vec![0_u8; num_bytes];

        for (bit_idx_from_back, _set_bit) in
            self.0.iter().rev().enumerate().filter(|(_, bit)| **bit)
        {
            let byte_idx_from_back = bit_idx_from_back / 8;
            let bit_idx_from_back = bit_idx_from_back % 8;

            buf[num_bytes - 1 - byte_idx_from_back] |= 1 << bit_idx_from_back;
        }
        assert_eq!(buf.len(), num_bytes);

        let enc: [&dyn Encodable; 2] = [&&num_bits, &buf.as_slice()];
        encode_list::<_, dyn Encodable>(&enc, out)
    }
}

impl Decodable for SignerMap {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let num_bits: usize = <u32 as Decodable>::decode(&mut payload)? as usize;

        if num_bits > MAX_SIGNERS_LEN {
            return Err(alloy_rlp::Error::Custom("signers count exceeds limit"));
        }

        let num_bytes = num_bits.div_ceil(8);

        let decoded_bytes = alloy_rlp::Header::decode_bytes(&mut payload, false)?;
        if decoded_bytes.len() != num_bytes {
            return Err(alloy_rlp::Error::Custom("wrong number of bytes in bitvec"));
        }

        let mut bitvec = BitVec::with_capacity(num_bits);
        while bitvec.len() < num_bits {
            let byte_idx_from_back = bitvec.len() / 8;
            let bit_idx_from_back = bitvec.len() % 8;

            let byte = decoded_bytes[decoded_bytes.len() - 1 - byte_idx_from_back];
            let bit = (byte >> bit_idx_from_back) & 1 == 1;
            bitvec.push(bit);
        }
        bitvec.reverse();

        Ok(SignerMap(bitvec))
    }
}

impl<PT: PubKey> SignatureCollection for BlsSignatureCollection<PT> {
    type NodeIdPubKey = PT;
    type SignatureType = BlsSignature;

    fn new<SD: SigningDomain>(
        sigs: impl IntoIterator<Item = (NodeId<PT>, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<PT, SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<PT, Self::SignatureType>> {
        let mut sigs_map = HashMap::new();
        let mut non_validator = Vec::new();
        let mut validator_index = HashMap::new();

        for (i, node_id) in validator_mapping.map.keys().enumerate() {
            validator_index.insert(*node_id, i);
        }
        for (node_id, sig) in sigs {
            if !validator_mapping.map.contains_key(&node_id) {
                non_validator.push((node_id, sig));
            }
            let entry = sigs_map.entry(node_id).or_insert(sig);
            if *entry != sig {
                return Err(SignatureCollectionError::ConflictingSignatures((
                    node_id, *entry, sig,
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

        tree.verify::<SD>(validator_mapping, msg)
            .map_err(SignatureCollectionError::InvalidSignaturesCreate)
    }

    fn verify<SD: SigningDomain>(
        &self,
        validator_mapping: &ValidatorMapping<PT, SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Vec<NodeId<PT>>, SignatureCollectionError<PT, Self::SignatureType>> {
        if self.signers.0.len() != validator_mapping.map.len() {
            return Err(SignatureCollectionError::InvalidSignaturesVerify);
        }

        if self.signers.0.len() > MAX_SIGNERS_LEN {
            return Err(SignatureCollectionError::InvalidSignaturesVerify);
        }

        let mut aggpk = BlsAggregatePubKey::infinity();
        let mut signers = Vec::new();

        for (bit, (node_id, pubkey)) in self.signers.0.iter().zip(validator_mapping.map.iter()) {
            if *bit {
                aggpk.add_assign(pubkey).expect("pubkey aggregation");
                signers.push(*node_id);
            }
        }

        // infinity signature is invalid unless validator set is empty (signers
        // bitmap length is zero) and signers are empty
        if signers.is_empty()
            && self.signers.0.is_empty()
            && self.sig == BlsAggregateSignature::infinity()
        {
            return Ok(signers);
        }

        if self.sig.fast_verify::<SD>(msg, &aggpk).is_err() {
            return Err(SignatureCollectionError::InvalidSignaturesVerify);
        }

        Ok(signers)
    }

    fn num_signatures(&self) -> usize {
        self.signers.0.count_ones()
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        buf.to_vec()
    }

    fn deserialize(data: &[u8]) -> Result<Self, SignatureCollectionError<PT, Self::SignatureType>> {
        Self::decode(&mut data.as_ref())
            .map_err(|e| SignatureCollectionError::DeserializeError(format!("{}", e)))
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use alloy_rlp::Decodable;
    use bitvec::prelude::*;
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
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
    use test_case::test_case;

    use super::{merge_nodes, AggregationTree, BlsSignatureCollection, SignerMap};
    use crate::BlsAggregateSignature;

    type SigningDomainType = signing_domain::Vote;
    type SignatureType = NopSignature;
    type PubKey = CertificateSignaturePubKey<SignatureType>;
    type SignatureCollectionType = BlsSignatureCollection<PubKey>;

    fn get_sigs<'a>(
        msg: &[u8],
        iter: impl Iterator<
            Item = &'a (
                NodeId<PubKey>,
                SignatureCollectionKeyPairType<SignatureCollectionType>,
            ),
        >,
    ) -> Vec<(
        NodeId<PubKey>,
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

        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(7, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let mut validator_index = HashMap::new();

        for (i, node_id) in valmap.map.keys().enumerate() {
            validator_index.insert(*node_id, i);
        }

        let msg = b"hello world";
        let sigs = get_sigs(msg, voting_keys.iter());

        let tree = AggregationTree::new(&sigs, &valmap, &validator_index);

        let expected_n0 =
            SignatureCollectionType::new::<SigningDomainType>(sigs.clone(), &valmap, msg).unwrap();
        let expected_n1 = SignatureCollectionType::new::<SigningDomainType>(
            vec![sigs[1], sigs[2], sigs[3], sigs[4]],
            &valmap,
            msg,
        )
        .unwrap();
        let expected_n2 = SignatureCollectionType::new::<SigningDomainType>(
            vec![sigs[0], sigs[5], sigs[6]],
            &valmap,
            msg,
        )
        .unwrap();
        let expected_n3 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[1], sigs[2]], &valmap, msg)
                .unwrap();
        let expected_n4 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[3], sigs[4]], &valmap, msg)
                .unwrap();
        let expected_n5 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[5], sigs[6]], &valmap, msg)
                .unwrap();
        let expected_n6 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[0]], &valmap, msg).unwrap();
        let expected_n7 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[1]], &valmap, msg).unwrap();
        let expected_n8 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[2]], &valmap, msg).unwrap();
        let expected_n9 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[3]], &valmap, msg).unwrap();
        let expected_n10 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[4]], &valmap, msg).unwrap();
        let expected_n11 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[5]], &valmap, msg).unwrap();
        let expected_n12 =
            SignatureCollectionType::new::<SigningDomainType>(vec![sigs[6]], &valmap, msg).unwrap();

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

    #[test_case(5,1; "5 signatures, 1-4 split")]
    #[test_case(5,2; "5 signatures, 2-3 split")]
    #[test_case(5,3; "5 signatures, 3-2 split")]
    #[test_case(5,4; "5 signatures, 4-1 split")]
    fn test_merge_node(num_sigs: u32, first: usize) {
        assert!(num_sigs as usize >= first);
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_sigs, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg = b"hello world";

        let sigs1 = get_sigs(msg, voting_keys.iter().take(first));
        let sigs2 = get_sigs(msg, voting_keys.iter().skip(first));

        let sc1 = SignatureCollectionType::new::<SigningDomainType>(sigs1, &valmap, msg).unwrap();
        let sc2 = SignatureCollectionType::new::<SigningDomainType>(sigs2, &valmap, msg).unwrap();

        let sigs_all = get_sigs(msg, voting_keys.iter());

        let sc_all =
            SignatureCollectionType::new::<SigningDomainType>(sigs_all, &valmap, msg).unwrap();

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
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(num_sigs, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg = b"hello world";
        let wrong_msg = b"bye world";

        let invalid_sigs = get_sigs(wrong_msg, voting_keys.iter().take(num_invalid as usize));
        let mut sigs = get_sigs(msg, voting_keys.iter().skip(num_invalid as usize));
        sigs.extend(invalid_sigs.clone());

        // shuffle the signature ordering
        sigs.shuffle(&mut StdRng::seed_from_u64(seed));

        let sig_col_err =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg).unwrap_err();
        assert!(matches!(
            sig_col_err,
            SignatureCollectionError::InvalidSignaturesCreate(_)
        ));

        match sig_col_err {
            SignatureCollectionError::InvalidSignaturesCreate(sigs) => {
                let test_set = sigs.into_iter().collect::<HashSet<_>>();
                let invalid_set = invalid_sigs.into_iter().collect::<HashSet<_>>();

                assert_eq!(test_set, invalid_set);
            }
            _ => {
                unreachable!()
            }
        };
    }

    #[test]
    fn test_conflict_signatures() {
        let (keys, voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(5, ValidatorSetFactory::default());
        let voting_keys: Vec<_> = keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(voting_keys)
            .collect();

        let msg = b"hello world";
        let invalid_msg = b"bye world";

        let mut sigs = Vec::new();

        for (node_id, key) in voting_keys.iter().skip(1) {
            let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<SigningDomainType>(msg, key);
            sigs.push((*node_id, sig));
        }

        let expected_node_id = voting_keys[0].0;
        let valid_sig =  < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<SigningDomainType>(msg, &voting_keys[0].1);
        let invalid_sig = < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<SigningDomainType>(invalid_msg, &voting_keys[0].1);
        sigs.push((expected_node_id, valid_sig));
        sigs.push((expected_node_id, invalid_sig));

        let sig_col_err =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &valmap, msg).unwrap_err();

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

    // Verifying a QC with unexpected signer bitvec length should not panic
    #[test]
    fn test_verify_invalid_no_crash() {
        let (mal_keys, mal_voting_keys, _, mal_valmap) =
            create_keys_w_validators::<SignatureType, SignatureCollectionType, _>(
                5,
                ValidatorSetFactory::default(),
            );
        let voting_keys: Vec<_> = mal_keys
            .iter()
            .map(CertificateKeyPair::pubkey)
            .map(NodeId::new)
            .zip(mal_voting_keys)
            .collect();

        let msg_hash = Hash([129_u8; 32]);

        let sigs = get_sigs(msg_hash.as_ref(), voting_keys.iter());
        let sigcol =
            SignatureCollectionType::new::<SigningDomainType>(sigs, &mal_valmap, msg_hash.as_ref())
                .unwrap();

        let (_, _, _, valmap) = create_keys_w_validators::<SignatureType, SignatureCollectionType, _>(
            4,
            ValidatorSetFactory::default(),
        );

        assert_ne!(sigcol.signers.0.len(), valmap.map.len());

        assert!(matches!(
            sigcol.verify::<SigningDomainType>(&valmap, msg_hash.as_ref()),
            Err(SignatureCollectionError::InvalidSignaturesVerify)
        ));
    }

    #[test]
    fn test_signer_map_rlp() {
        let mut a = SignerMap(bitvec![u8, Lsb0; 0; 8]);
        let mut b = SignerMap(bitvec![u8, Lsb0; 0; 4]);

        a.0.set(0, true);
        a.0.set(1, false);
        a.0.set(2, true);
        a.0.set(3, false);
        a.0.set(4, false);
        a.0.set(5, true);
        a.0.set(6, false);
        a.0.set(7, true);

        b.0.set(0, true);
        b.0.set(1, false);
        b.0.set(2, true);
        b.0.set(3, false);

        let x = alloy_rlp::encode(a.clone());
        let y = alloy_rlp::encode(b.clone());

        let j = <SignerMap>::decode(&mut x.as_slice()).unwrap();
        let k = <SignerMap>::decode(&mut y.as_slice()).unwrap();
        assert_eq!(a, j);
        assert_eq!(b, k);
    }

    #[test]
    fn test_signer_map_rlp_2() {
        let mut a = SignerMap(bitvec![u8, Lsb0; 0; 9]);
        let mut b = SignerMap(bitvec![u8, Lsb0; 0; 4]);

        a.0.set(0, false);
        a.0.set(1, false);
        a.0.set(2, false);
        a.0.set(3, false);
        a.0.set(4, false);
        a.0.set(5, false);
        a.0.set(6, false);
        a.0.set(7, false);
        a.0.set(8, true);

        b.0.set(0, true);
        b.0.set(1, true);
        b.0.set(2, false);
        b.0.set(3, false);

        let x = alloy_rlp::encode(a.clone());
        let y = alloy_rlp::encode(b.clone());

        let j = <SignerMap>::decode(&mut x.as_slice()).unwrap();
        let k = <SignerMap>::decode(&mut y.as_slice()).unwrap();
        assert_eq!(a, j);
        assert_eq!(b, k);
    }

    #[test]
    fn test_signer_map_rlp_max_pass() {
        use bitvec::prelude::*;

        const NUM_BITS: usize = 1024 * 16;

        let big_map1 = SignerMap(BitVec::<u8, Lsb0>::repeat(true, NUM_BITS));
        let encoded1 = alloy_rlp::encode(big_map1.clone());
        let mut input1 = encoded1.as_slice();
        let decoded1 = <SignerMap>::decode(&mut input1).unwrap();

        assert_eq!(big_map1, decoded1);
    }

    #[test]
    fn test_signer_map_rlp_max_fail() {
        use bitvec::prelude::*;

        const NUM_BITS: usize = 1024 * 16 + 1;

        let big_map1 = SignerMap(BitVec::<u8, Lsb0>::repeat(true, NUM_BITS));
        let encoded1 = alloy_rlp::encode(big_map1);
        let mut input1 = encoded1.as_slice();
        assert!(matches!(
            <SignerMap>::decode(&mut input1),
            Err(alloy_rlp::Error::Custom(_))
        ));
    }

    #[test]
    fn test_reject_universal_signature() {
        let bitvector = BitVec::repeat(false, 5);

        let (_keys, _voting_keys, _, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(5, ValidatorSetFactory::default());

        let sigs = SignatureCollectionType {
            signers: SignerMap(bitvector),
            sig: BlsAggregateSignature::infinity(),
            _phantom: Default::default(),
        };

        assert!(sigs.verify::<SigningDomainType>(&valmap, &[]).is_err());
        assert!(sigs.verify::<SigningDomainType>(&valmap, &[0x12]).is_err());
        assert!(sigs.verify::<SigningDomainType>(&valmap, &[0x34]).is_err());
    }
}
