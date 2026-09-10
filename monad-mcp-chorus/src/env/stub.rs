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

pub use self::{proposal::*, validator::*, vote::*};

mod validator {
    use std::collections::HashMap;

    use alloy_rlp::{RlpDecodableWrapper, RlpEncodableWrapper};
    // Into implemented for testing purpose only.
    use derive_more::Into;

    use super::vote::{KeyPair, PubKey};
    use crate::spec::{self, validator::Stake as _};

    #[derive(
        Clone,
        Copy,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        Debug,
        Into,
        RlpEncodableWrapper,
        RlpDecodableWrapper,
    )]
    pub struct NodeId(u64);

    impl spec::validator::NodeId for NodeId {}

    impl NodeId {
        pub fn dummy(id: u64) -> Self {
            NodeId(id)
        }

        // allows quickly deriving the KeyPair from a NodeId for
        // testing purpose.
        pub fn keypair(&self) -> KeyPair {
            KeyPair::dummy(self.0)
        }
    }

    #[derive(
        Clone,
        Copy,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        derive_more::Add,
        derive_more::Sum,
        derive_more::From,
    )]
    pub struct Stake(u64);

    impl spec::validator::Stake for Stake {
        const ZERO: Self = Self(0);

        // 2f
        fn supermajority_threshold(&self) -> Self {
            Self(self.0 / 3 * 2 + 2 * (self.0 % 3) / 3)
        }

        // f
        fn honest_threshold(&self) -> Self {
            Self(self.0 / 3)
        }

        fn obligation(&self, total: &Self, shares: usize) -> (usize, usize) {
            let prod = self.0 as u128 * shares as u128;
            let total = total.0 as u128;
            ((prod / total) as usize, (prod % total) as usize)
        }
    }

    // invariant: valset/mapping have exactly the same key set, and
    // sorted/indices are the canonical enumeration of the
    // valset. valset is non-empty, all stakes are nonzero.
    //
    // sorted/indices are representation shared with this env's
    // SignatureCollection; they are deliberately not exposed outside
    // the env.
    pub struct ValidatorData {
        valset: HashMap<NodeId, Stake>,
        sorted: Vec<NodeId>,
        indices: HashMap<NodeId, usize>,
        mapping: HashMap<NodeId, PubKey>,
    }

    impl ValidatorData {
        pub fn new(valset: HashMap<NodeId, Stake>, mapping: HashMap<NodeId, PubKey>) -> Self {
            assert_eq!(valset.len(), mapping.len());
            assert!(!valset.is_empty());
            assert!(valset.keys().all(|node_id| mapping.contains_key(node_id)));
            assert!(valset.values().all(|stake| *stake > Stake::ZERO));

            let sorted = sort_valset(&valset);
            Self::new_unchecked(valset, mapping, sorted)
        }

        // The caller must ensure the invariants on the valset/mapping
        // are satisfied, as seen in the assertions in new() method. The
        // `sorted` must be consistent with the output of sort_valset().
        pub fn new_unchecked(
            valset: HashMap<NodeId, Stake>,
            mapping: HashMap<NodeId, PubKey>,
            sorted: Vec<NodeId>,
        ) -> Self {
            debug_assert!(sorted == sort_valset(&valset));
            let indices = sorted
                .iter()
                .enumerate()
                .map(|(i, node_id)| (*node_id, i))
                .collect();

            Self {
                valset,
                sorted,
                indices,
                mapping,
            }
        }

        pub(crate) fn indices(&self) -> &HashMap<NodeId, usize> {
            &self.indices
        }

        pub(crate) fn get_node(&self, index: usize) -> Option<&NodeId> {
            self.sorted.get(index)
        }
    }

    impl spec::validator::ValidatorData for ValidatorData {
        type NodeId = NodeId;
        type PubKey = PubKey;
        type Stake = Stake;

        fn nodes(&self) -> impl Iterator<Item = &NodeId> {
            self.sorted.iter()
        }

        fn len(&self) -> usize {
            self.sorted.len()
        }

        fn contains(&self, node_id: &NodeId) -> bool {
            self.valset.contains_key(node_id)
        }

        fn get_pubkey(&self, node_id: &NodeId) -> &PubKey {
            &self.mapping[node_id]
        }

        fn get_stake(&self, node_id: &NodeId) -> &Stake {
            &self.valset[node_id]
        }

        fn sum_stake<'a>(&self, nodes: impl IntoIterator<Item = &'a NodeId>) -> Stake {
            nodes.into_iter().map(|node_id| self.valset[node_id]).sum()
        }

        fn total_stake(&self) -> Stake {
            self.valset.values().copied().sum()
        }
    }

    pub fn sort_valset<S>(valset: &HashMap<NodeId, S>) -> Vec<NodeId> {
        let mut sorted = valset.keys().copied().collect::<Vec<_>>();
        sorted.sort();
        sorted
    }

    #[cfg(test)]
    mod tests {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            #[test]
            fn stake_exact_and_not_overflowing(n in any::<u64>()) {
                let expected = (n as u128 * 2 / 3) as u64;
                prop_assert!(Stake(n).supermajority_threshold() == Stake(expected));

                let expected = n / 3;
                prop_assert!(Stake(n).honest_threshold() == Stake(expected));
            }
        }
    }
}

mod proposal {
    use alloy_rlp::{
        Decodable, Encodable, Header, RlpDecodable, RlpDecodableWrapper, RlpEncodable,
        RlpEncodableWrapper, encode_list, list_length,
    };

    use super::NodeId;
    use crate::{
        spec,
        stub::types::{ProposalIndex, Slot},
    };

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
    pub struct MerkleHash(pub [u8; 20]);

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
    pub struct MerkleRoot(pub MerkleHash);
    impl spec::MerkleRoot for MerkleRoot {}

    // stub proposal signature, opaque to consensus
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct ProposalSignature {
        pub signer: NodeId,
        pub checksum: u64,
    }

    // the encoding scheme descriptor used by DA. opaque to consensus.
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub enum EncodingScheme {
        D25(D25),
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct D25 {
        pub msg_len: u32,
        pub unix_ts: u64,
        // the merkle tree depth
        pub depth: u8,
    }

    impl Encodable for EncodingScheme {
        fn encode(&self, out: &mut dyn bytes::BufMut) {
            match self {
                Self::D25(f0) => {
                    let fields: [&dyn Encodable; 2] = [&1u8, f0];
                    encode_list::<_, dyn Encodable>(&fields, out);
                }
            }
        }

        fn length(&self) -> usize {
            match self {
                Self::D25(f0) => {
                    let fields: [&dyn Encodable; 2] = [&1u8, f0];
                    list_length::<_, dyn Encodable>(&fields)
                }
            }
        }
    }

    impl Decodable for EncodingScheme {
        fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
            let mut payload = Header::decode_bytes(buf, true)?;
            let result = match <u8 as Decodable>::decode(&mut payload)? {
                1 => Self::D25(<D25 as Decodable>::decode(&mut payload)?),
                _ => return Err(alloy_rlp::Error::Custom("unknown EncodingScheme tag")),
            };
            if !payload.is_empty() {
                return Err(alloy_rlp::Error::UnexpectedLength);
            }
            Ok(result)
        }
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct ProposalHeader {
        pub slot: Slot,
        pub root: MerkleRoot,

        // DA-owned fields. defined here rather than in DA because we
        // don't want chorus to depend on DA.
        // todo: we may extract a monad-mcp-da-types crate and depend
        // on it from monad-mcp-chorus and monad-mcp-da.
        pub sig: ProposalSignature,
        pub scheme: EncodingScheme,
    }

    impl spec::ProposalHeader for ProposalHeader {
        type Root = MerkleRoot;

        fn slot(&self) -> u64 {
            self.slot.0
        }

        fn root(&self) -> &MerkleRoot {
            &self.root
        }
    }

    // the proposal index of a header the legitimate proposer of the
    // slot signed. Supplied by the DA env, which owns the signature
    // and scheme checks.
    type Authenticator = dyn Fn(&ProposalHeader, u64) -> Option<ProposalIndex> + Send + Sync;

    pub struct HeaderAuth {
        authenticator: Box<Authenticator>,
    }

    impl HeaderAuth {
        pub fn new<F>(authenticator: F) -> Self
        where
            F: Fn(&ProposalHeader, u64) -> Option<ProposalIndex> + Send + Sync + 'static,
        {
            Self {
                authenticator: Box::new(authenticator),
            }
        }
    }

    impl spec::proposal::HeaderAuth for HeaderAuth {
        type Header = ProposalHeader;

        fn authenticate(&self, header: &ProposalHeader, slot: u64) -> Option<ProposalIndex> {
            if header.slot.get() != slot {
                return None;
            }
            (self.authenticator)(header, slot)
        }
    }
}

mod vote {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use alloy_rlp::{
        Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
    };
    use bytes::Bytes;
    // Into implemented on these types for testing purpose only.
    use derive_more::Into;

    use super::{NodeId, Stake, ValidatorData};
    use crate::spec::{
        self, SignatureCollection as _, validator::ValidatorData as _, vote::Signature as _,
    };

    #[derive(PartialEq, Eq, Hash, Debug, Into)]
    pub struct KeyPair(u64);

    #[derive(
        Clone, Copy, PartialEq, Eq, Hash, Debug, Into, RlpEncodableWrapper, RlpDecodableWrapper,
    )]
    pub struct PubKey(u64);

    #[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct Signature {
        by: PubKey,
        data: Bytes,
        malformed: bool,
    }

    impl Signature {
        pub fn make_invalid(&mut self) {
            // Simulate an invalid signature
            self.by = PubKey(self.by.0.wrapping_add(10000));
        }

        pub fn make_malformed(&mut self) {
            // Simulate a bad point, e.g. outside the BLS subgroup
            self.malformed = true;
        }
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct SignatureCollection {
        // index -> sig to simulate the erasure of PubKey/NodeId in
        // BLS. keyed by index so duplicates cannot be represented,
        // like a bitmap.
        sigs: BTreeMap<usize, Signature>,
    }

    #[derive(RlpEncodable, RlpDecodable)]
    pub(super) struct IndexedSignature<S> {
        pub(super) index: usize,
        pub(super) signature: S,
    }

    impl Encodable for SignatureCollection {
        fn encode(&self, out: &mut dyn bytes::BufMut) {
            let pairs: Vec<_> = self
                .sigs
                .iter()
                .map(|(index, sig)| IndexedSignature {
                    index: *index,
                    signature: sig,
                })
                .collect();
            pairs.encode(out);
        }

        fn length(&self) -> usize {
            let pairs: Vec<_> = self
                .sigs
                .iter()
                .map(|(index, sig)| IndexedSignature {
                    index: *index,
                    signature: sig,
                })
                .collect();
            pairs.length()
        }
    }

    impl Decodable for SignatureCollection {
        fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
            let pairs = Vec::<IndexedSignature<Signature>>::decode(buf)?;
            let mut sigs = BTreeMap::new();
            let mut previous = None;
            for IndexedSignature { index, signature } in pairs {
                if previous.is_some_and(|prev| index <= prev) {
                    return Err(alloy_rlp::Error::Custom(
                        "signature indices must be strictly increasing",
                    ));
                }
                previous = Some(index);
                sigs.insert(index, signature);
            }
            Ok(Self { sigs })
        }
    }

    impl spec::vote::PubKey for PubKey {}

    impl KeyPair {
        pub fn dummy(id: u64) -> Self {
            KeyPair(id)
        }
    }

    impl spec::vote::KeyPair for KeyPair {
        type PubKey = PubKey;
        type Signature = Signature;

        fn pubkey(&self) -> Self::PubKey {
            PubKey(self.0)
        }

        fn sign(&self, data: &Bytes) -> Self::Signature {
            Signature {
                by: self.pubkey(),
                data: data.clone(),
                malformed: false,
            }
        }
    }

    impl spec::vote::Signature for Signature {
        type PubKey = PubKey;

        fn is_well_formed(&self) -> bool {
            !self.malformed
        }

        fn verify(&self, data: &[u8], pubkey: &Self::PubKey) -> bool {
            self.is_well_formed() && self.by == *pubkey && self.data == data
        }
    }

    impl spec::vote::SignatureCollection for SignatureCollection {
        type Signature = Signature;
        type ValidatorData = ValidatorData;

        fn aggregate(
            sig_map: &HashMap<&NodeId, &Signature>,
            validator_data: &ValidatorData,
        ) -> Self {
            debug_assert!(sig_map.values().all(|sig| sig.is_well_formed()));

            let sigs = sig_map
                .iter()
                .map(|(node_id, sig)| (validator_data.indices()[node_id], (*sig).clone()))
                .collect();
            SignatureCollection { sigs }
        }

        fn signers<'s>(&'s self, validator_data: &'s ValidatorData) -> Option<HashSet<&'s NodeId>> {
            let mut signers = HashSet::new();

            for index in self.sigs.keys() {
                let Some(node_id) = validator_data.get_node(*index) else {
                    return None; // Invalid index
                };

                signers.insert(node_id);
            }

            Some(signers)
        }

        fn verify<'s>(
            &'s self,
            data: &[u8],
            validator_data: &'s ValidatorData,
        ) -> Option<HashSet<&'s NodeId>> {
            let mut signers = HashSet::new();

            for (index, sig) in &self.sigs {
                let Some(node_id) = validator_data.get_node(*index) else {
                    return None; // Invalid index
                };

                let pubkey = validator_data.get_pubkey(node_id);

                if !sig.verify(data, pubkey) {
                    return None; // Invalid signature
                }
                signers.insert(node_id);
            }

            Some(signers)
        }
    }

    pub struct VoteAggregation<'a> {
        validator_data: &'a ValidatorData,
    }

    impl<'a> spec::vote::VoteAggregation<'a, Stake> for VoteAggregation<'a> {
        type SignatureCollection = SignatureCollection;

        fn from_validator_data(validator_data: &'a ValidatorData) -> Self {
            Self { validator_data }
        }

        fn try_aggregate(
            &mut self,
            data: &[u8],
            mut votes: HashMap<&NodeId, &Signature>,
            target_stake: Stake,
        ) -> Option<SignatureCollection> {
            // caller guarantee: every vote comes from a known
            // validator and carries a well-formed signature.
            let mut all_voters = votes.keys();
            assert!(all_voters.all(|node_id| self.validator_data.contains(*node_id)));
            assert!(votes.values().all(|sig| sig.is_well_formed()));

            let sum = |votes: &HashMap<_, _>| self.validator_data.sum_stake(votes.keys().copied());

            // just naively exclude any bad signatures. we should use
            // an aggregation tree for bls to iteratively exclude bad
            // signatures.
            votes.retain(|node_id, sig| {
                let pubkey = self.validator_data.get_pubkey(node_id);
                sig.verify(data, pubkey)
            });

            if sum(&votes) <= target_stake {
                // not enough votes to reach the target stake
                return None;
            }

            let sigcol = SignatureCollection::aggregate(&votes, self.validator_data);

            let voters = sigcol
                .signers(self.validator_data)
                .expect("signers should be valid");
            assert!(sum(&votes) > target_stake);
            assert!(sigcol.verify(data, self.validator_data) == Some(voters));

            Some(sigcol)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;

        use bytes::Bytes;

        use super::*;
        use crate::spec::vote::{KeyPair as _, VoteAggregation as _};

        // 7 nodes of stake 1 each against threshold 5: excluding one bad
        // sig leaves 6 > 5, excluding two leaves 5 == 5 which must fail.
        #[test]
        fn try_aggregate_excludes_bad_sigs() {
            let nodes: Vec<NodeId> = (0..7).map(NodeId::dummy).collect();
            let valset: HashMap<NodeId, Stake> =
                nodes.iter().map(|node| (*node, Stake::from(1))).collect();
            let mapping: HashMap<NodeId, PubKey> = nodes
                .iter()
                .map(|node| (*node, node.keypair().pubkey()))
                .collect();
            let validator_data = ValidatorData::new(valset, mapping);

            let data = Bytes::from_static(b"vote data");
            let mut sigs: Vec<Signature> = nodes
                .iter()
                .map(|node| node.keypair().sign(&data))
                .collect();

            let threshold = Stake::from(5);
            let mut vote_agg = VoteAggregation::from_validator_data(&validator_data);

            sigs[0].make_invalid();
            let votes: HashMap<&NodeId, &Signature> = nodes.iter().zip(&sigs).collect();
            let sigcol = vote_agg
                .try_aggregate(&data, votes, threshold)
                .expect("6 valid votes exceed threshold 5");
            let voters = sigcol
                .signers(&validator_data)
                .expect("signers should be valid");
            assert_eq!(voters, nodes[1..].iter().collect());

            sigs[1].make_invalid();
            let votes: HashMap<&NodeId, &Signature> = nodes.iter().zip(&sigs).collect();
            assert!(vote_agg.try_aggregate(&data, votes, threshold).is_none());
        }
    }
}

const _: () = crate::spec::assert_env::<
    NodeId,
    Stake,
    PubKey,
    KeyPair,
    Signature,
    ValidatorData,
    SignatureCollection,
    VoteAggregation<'_>,
    MerkleRoot,
    ProposalHeader,
    HeaderAuth,
>();

#[cfg(test)]
mod rlp_tests {
    use super::{vote::IndexedSignature, *};
    use crate::spec::vote::KeyPair as _;

    #[test]
    fn signature_maps_reject_duplicate_and_unordered_indices() {
        let signature = KeyPair::dummy(1).sign(&bytes::Bytes::from_static(b"test"));
        for indices in [[1usize, 1], [2, 1]] {
            let encoded = alloy_rlp::encode(vec![
                IndexedSignature {
                    index: indices[0],
                    signature: &signature,
                },
                IndexedSignature {
                    index: indices[1],
                    signature: &signature,
                },
            ]);
            assert!(alloy_rlp::decode_exact::<SignatureCollection>(&encoded).is_err());
        }
        let encoded = alloy_rlp::encode(vec![
            IndexedSignature {
                index: 1usize,
                signature: &signature,
            },
            IndexedSignature {
                index: 2usize,
                signature: &signature,
            },
        ]);
        let collection: SignatureCollection = alloy_rlp::decode_exact(&encoded).unwrap();
        assert_eq!(alloy_rlp::encode(&collection), encoded);

        let mut malformed = signature;
        malformed.make_malformed();
        assert_eq!(
            alloy_rlp::decode_exact::<Signature>(alloy_rlp::encode(&malformed)).unwrap(),
            malformed
        );
    }
}
