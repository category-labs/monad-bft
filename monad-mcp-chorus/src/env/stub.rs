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

pub(crate) use monad_mcp_common::stub::{KeyPair, NodeId, PubKey, Signature, Stake};

pub use self::{proposal::*, validator::*, vote::*};

mod validator {
    use monad_mcp_common::{
        containers::{FastHashMap, IndexVec, MAX_VALIDATORS, ValidatorIndex, ValidatorSet},
        spec::Stake as _,
    };

    use super::{NodeId, PubKey, Stake};
    use crate::spec;

    // invariant: the validators are indexed in NodeId order, the canonical
    // enumeration of the valset; indices, nodes, stakes and pubkeys cover the
    // same validators. The valset is non-empty and at most MAX_VALIDATORS
    // large, all stakes are nonzero, and total_stake is their sum.
    pub struct ValidatorData {
        indices: FastHashMap<NodeId, ValidatorIndex>,
        nodes: IndexVec<ValidatorIndex, NodeId>,
        stakes: IndexVec<ValidatorIndex, Stake>,
        pubkeys: IndexVec<ValidatorIndex, PubKey>,
        total_stake: Stake,
    }

    impl ValidatorData {
        pub fn new(
            valset: FastHashMap<NodeId, Stake>,
            mapping: FastHashMap<NodeId, PubKey>,
        ) -> Self {
            assert_eq!(valset.len(), mapping.len());
            assert!(!valset.is_empty());
            assert!(valset.len() <= MAX_VALIDATORS);
            assert!(
                valset
                    .keys_unordered()
                    .all(|node_id| mapping.contains_key(node_id))
            );
            assert!(valset.values_unordered().all(|stake| *stake > Stake::ZERO));

            let mut sorted: Vec<NodeId> = valset.keys_unordered().copied().collect();
            sorted.sort();
            let nodes: IndexVec<ValidatorIndex, NodeId> = sorted.into_iter().collect();
            let indices = nodes
                .iter_enumerated()
                .map(|(index, node_id)| (*node_id, index))
                .collect();
            let stakes: IndexVec<_, _> = nodes.iter().map(|node_id| valset[node_id]).collect();
            let pubkeys = nodes.iter().map(|node_id| mapping[node_id]).collect();
            let total_stake = stakes.iter().copied().sum();

            Self {
                indices,
                nodes,
                stakes,
                pubkeys,
                total_stake,
            }
        }

        // a wire index as one of this validator set's, if it is one
        pub(crate) fn checked_index(&self, index: usize) -> Option<ValidatorIndex> {
            let index = ValidatorIndex::new(index)?;
            self.nodes.get(index).map(|_| index)
        }
    }

    impl spec::validator::ValidatorData for ValidatorData {
        type NodeId = NodeId;
        type PubKey = PubKey;
        type Stake = Stake;

        fn nodes(&self) -> impl Iterator<Item = &NodeId> {
            self.nodes.iter()
        }

        fn len(&self) -> usize {
            self.nodes.len()
        }

        fn index_of(&self, node_id: &NodeId) -> Option<ValidatorIndex> {
            self.indices.get(node_id).copied()
        }

        fn node_id(&self, index: ValidatorIndex) -> &NodeId {
            &self.nodes[index]
        }

        fn pubkey(&self, index: ValidatorIndex) -> &PubKey {
            &self.pubkeys[index]
        }

        fn get_pubkey(&self, node_id: &NodeId) -> &PubKey {
            &self.pubkeys[self.indices[node_id]]
        }

        fn get_stake(&self, node_id: &NodeId) -> &Stake {
            &self.stakes[self.indices[node_id]]
        }

        fn sum_stake(&self, validators: &ValidatorSet) -> Stake {
            validators.iter().map(|index| self.stakes[index]).sum()
        }

        fn total_stake(&self) -> Stake {
            self.total_stake
        }
    }
}

mod proposal {
    use std::{fmt, ops::Range};

    use alloy_rlp::{
        Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
    };
    use monad_mcp_common::{Lane, Slot};

    use super::NodeId;
    use crate::spec::{self, ProposalHeader as _};

    #[derive(Clone, Copy, PartialEq, Eq, Hash, RlpEncodableWrapper, RlpDecodableWrapper)]
    pub struct MerkleHash(pub [u8; 20]);

    impl fmt::Debug for MerkleHash {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            for byte in self.0 {
                write!(f, "{byte:02x}")?;
            }
            Ok(())
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash, RlpEncodableWrapper, RlpDecodableWrapper)]
    pub struct MerkleRoot(pub MerkleHash);

    impl fmt::Debug for MerkleRoot {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "root:{:?}", self.0)
        }
    }
    impl spec::MerkleRoot for MerkleRoot {}

    #[derive(Clone, Copy, PartialEq, Eq, Hash, RlpEncodableWrapper, RlpDecodableWrapper)]
    pub struct ProposalId([u8; 32]);

    // stub proposal id layout: root(20)+[0u8;4]+H(header)
    const PID_ROOT: Range<usize> = 0..20;
    const PID_DIGEST: Range<usize> = 24..32;

    impl ProposalId {
        pub fn as_bytes(&self) -> &[u8; 32] {
            &self.0
        }
    }

    impl fmt::Debug for ProposalId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let root = hex::encode(&self.0[PID_ROOT]);
            let digest = hex::encode(&self.0[PID_DIGEST]);
            write!(f, "pid:{root}...{digest}")
        }
    }
    impl spec::ProposalId for ProposalId {}

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
        S11(S11),
        D33(D33),
    }

    #[cfg(test)]
    impl EncodingScheme {
        pub fn dummy(slot: Slot) -> Self {
            Self::D25(D25 {
                slot,
                msg_len: 1,
                unix_ts: 0,
                depth: 3,
            })
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct D25 {
        pub slot: Slot,
        pub msg_len: u32,
        pub unix_ts: u64,
        // the merkle tree depth
        pub depth: u8,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct S11 {
        pub slot: Slot,
        // the lane the author claims for the slot
        pub lane: Lane,
        // the merkle tree depth
        pub depth: u8,
        pub msg_len: u32,
        pub unix_ts: u64,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct D33 {
        pub slot: Slot,
        // the lane the author claims for the slot
        pub lane: Lane,
        // the merkle tree depth
        pub depth: u8,
        pub msg_len: u32,
        pub unix_ts: u64,
    }

    impl EncodingScheme {
        pub fn slot(&self) -> Slot {
            match self {
                Self::D25(d25) => d25.slot,
                Self::S11(s11) => s11.slot,
                Self::D33(d33) => d33.slot,
            }
        }
    }

    impl From<D25> for EncodingScheme {
        fn from(d25: D25) -> Self {
            Self::D25(d25)
        }
    }

    impl From<S11> for EncodingScheme {
        fn from(s11: S11) -> Self {
            Self::S11(s11)
        }
    }

    impl From<D33> for EncodingScheme {
        fn from(value: D33) -> Self {
            Self::D33(value)
        }
    }

    impl Encodable for EncodingScheme {
        fn encode(&self, out: &mut dyn bytes::BufMut) {
            match self {
                Self::D25(f0) => {
                    let fields: [&dyn Encodable; 2] = [&1u8, f0];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
                }
                Self::S11(f0) => {
                    let fields: [&dyn Encodable; 2] = [&2u8, f0];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
                }
                Self::D33(f0) => {
                    let fields: [&dyn Encodable; 2] = [&3u8, f0];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
                }
            }
        }

        fn length(&self) -> usize {
            match self {
                Self::D25(f0) => {
                    let fields: [&dyn Encodable; 2] = [&1u8, f0];
                    alloy_rlp::list_length::<_, dyn Encodable>(&fields)
                }
                Self::S11(f0) => {
                    let fields: [&dyn Encodable; 2] = [&2u8, f0];
                    alloy_rlp::list_length::<_, dyn Encodable>(&fields)
                }
                Self::D33(f0) => {
                    let fields: [&dyn Encodable; 2] = [&3u8, f0];
                    alloy_rlp::list_length::<_, dyn Encodable>(&fields)
                }
            }
        }
    }

    impl Decodable for EncodingScheme {
        fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
            let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
            let result = match <u8 as Decodable>::decode(&mut payload)? {
                1 => Self::D25(<D25 as Decodable>::decode(&mut payload)?),
                2 => Self::S11(<S11 as Decodable>::decode(&mut payload)?),
                3 => Self::D33(<D33 as Decodable>::decode(&mut payload)?),
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
        pub root: MerkleRoot,
        pub scheme: EncodingScheme,
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    pub struct SignedProposalHeader {
        pub header: ProposalHeader,

        // DA-owned fields. defined here rather than in DA because we
        // don't want chorus to depend on DA.
        // todo: we may extract a monad-mcp-da-types crate and depend
        // on it from monad-mcp-chorus and monad-mcp-da.
        pub sig: ProposalSignature,
    }

    impl spec::ProposalHeader for ProposalHeader {
        type Root = MerkleRoot;
        type Pid = ProposalId;

        fn slot(&self) -> u64 {
            self.scheme.slot().0
        }

        fn root(&self) -> &MerkleRoot {
            &self.root
        }

        // a non-cryptographic stand-in over the rlp encoding; see
        // noncrypto_digest
        fn pid(&self) -> ProposalId {
            let digest = monad_mcp_common::stub::noncrypto_digest(&alloy_rlp::encode(self));
            let mut pid = [0; 32];
            pid[PID_ROOT].copy_from_slice(&self.root.0.0);
            pid[PID_DIGEST].copy_from_slice(&digest.to_le_bytes());
            ProposalId(pid)
        }
    }

    impl spec::ProposalHeader for SignedProposalHeader {
        type Root = MerkleRoot;
        type Pid = ProposalId;

        fn slot(&self) -> u64 {
            self.header.slot()
        }

        fn root(&self) -> &MerkleRoot {
            &self.header.root
        }

        fn pid(&self) -> ProposalId {
            self.header.pid()
        }
    }

    impl spec::SignedProposalHeader for SignedProposalHeader {
        type Sig = ProposalSignature;

        fn sig(&self) -> &ProposalSignature {
            &self.sig
        }
    }

    // the lane of a header the legitimate proposer of the
    // slot signed. Supplied by the DA env, which owns the signature
    // and scheme checks.
    type Authenticator = dyn Fn(&SignedProposalHeader, u64) -> Option<Lane> + Send + Sync;

    pub struct HeaderAuth {
        authenticator: Box<Authenticator>,
    }

    impl HeaderAuth {
        pub fn new<F>(authenticator: F) -> Self
        where
            F: Fn(&SignedProposalHeader, u64) -> Option<Lane> + Send + Sync + 'static,
        {
            Self {
                authenticator: Box::new(authenticator),
            }
        }
    }

    impl spec::proposal::HeaderAuth for HeaderAuth {
        type Signed = SignedProposalHeader;

        fn authenticate(&self, signed: &SignedProposalHeader, slot: u64) -> Option<Lane> {
            if signed.slot() != slot {
                return None;
            }
            (self.authenticator)(signed, slot)
        }
    }
}

mod vote {
    use std::collections::BTreeMap;

    use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
    use monad_mcp_common::{
        containers::{ValidatorIndex, ValidatorSet},
        spec::Signature as _,
    };

    use super::{Signature, Stake, ValidatorData};
    use crate::spec::{self, SignatureCollection as _, validator::ValidatorData as _};

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct SignatureCollection {
        // index -> sig to simulate the erasure of PubKey/NodeId in
        // BLS. keyed by index so duplicates cannot be represented,
        // like a bitmap. Wire indices: checked against the validator set
        // by signers and verify.
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

    impl spec::vote::SignatureCollection for SignatureCollection {
        type Signature = Signature;
        type ValidatorData = ValidatorData;

        fn aggregate<'a>(
            sigs: impl IntoIterator<Item = (ValidatorIndex, &'a Signature)>,
            _validator_data: &ValidatorData,
        ) -> Self {
            let sigs: BTreeMap<_, _> = sigs
                .into_iter()
                .map(|(index, sig)| (index.get(), sig.clone()))
                .collect();
            debug_assert!(sigs.values().all(|sig| sig.is_well_formed()));
            SignatureCollection { sigs }
        }

        fn signers(&self, validator_data: &ValidatorData) -> Option<ValidatorSet> {
            let mut signers = ValidatorSet::new();
            for index in self.sigs.keys() {
                // None for an index outside the validator set
                signers.insert(validator_data.checked_index(*index)?);
            }
            Some(signers)
        }

        fn verify(&self, data: &[u8], validator_data: &ValidatorData) -> Option<ValidatorSet> {
            let mut signers = ValidatorSet::new();
            for (index, sig) in &self.sigs {
                // None for an index outside the validator set
                let index = validator_data.checked_index(*index)?;
                if !sig.verify(data, validator_data.pubkey(index)) {
                    return None; // Invalid signature
                }
                signers.insert(index);
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

        fn try_aggregate<'v>(
            &mut self,
            data: &[u8],
            votes: impl IntoIterator<Item = (ValidatorIndex, &'v Signature)>,
            target_stake: Stake,
        ) -> Option<SignatureCollection> {
            // caller guarantee: every vote comes from a distinct known
            // validator and carries a well-formed signature.
            let mut voters = ValidatorSet::new();
            let mut valid = Vec::new();
            for (index, sig) in votes {
                assert!(index.get() < self.validator_data.len());
                assert!(voters.insert(index), "one vote per validator");
                assert!(sig.is_well_formed());

                // just naively exclude any bad signatures. we should use
                // an aggregation tree for bls to iteratively exclude bad
                // signatures.
                if sig.verify(data, self.validator_data.pubkey(index)) {
                    valid.push((index, sig));
                }
            }

            let signers: ValidatorSet = valid.iter().map(|(index, _)| *index).collect();
            if self.validator_data.sum_stake(&signers) <= target_stake {
                // not enough votes to reach the target stake
                return None;
            }

            let sigcol = SignatureCollection::aggregate(valid, self.validator_data);

            assert!(sigcol.signers(self.validator_data) == Some(signers));
            assert!(sigcol.verify(data, self.validator_data) == Some(signers));

            Some(sigcol)
        }
    }

    #[cfg(test)]
    mod tests {
        use bytes::Bytes;
        use monad_mcp_common::{containers::FastHashMap, spec::KeyPair as _};

        use super::*;
        use crate::{
            env::stub::{NodeId, PubKey},
            spec::vote::VoteAggregation as _,
        };

        // 7 nodes of stake 1 each against threshold 5: excluding one bad
        // sig leaves 6 > 5, excluding two leaves 5 == 5 which must fail.
        #[test]
        fn try_aggregate_excludes_bad_sigs() {
            let nodes: Vec<NodeId> = (0..7).map(NodeId::dummy).collect();
            let valset: FastHashMap<NodeId, Stake> =
                nodes.iter().map(|node| (*node, Stake::from(1))).collect();
            let mapping: FastHashMap<NodeId, PubKey> = nodes
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

            let index = |node: &NodeId| validator_data.index_of(node).unwrap();

            sigs[0].make_invalid();
            let votes = nodes.iter().map(index).zip(&sigs);
            let sigcol = vote_agg
                .try_aggregate(&data, votes, threshold)
                .expect("6 valid votes exceed threshold 5");
            let voters = sigcol
                .signers(&validator_data)
                .expect("signers should be valid");
            assert_eq!(voters, nodes[1..].iter().map(index).collect());

            sigs[1].make_invalid();
            let votes = nodes.iter().map(index).zip(&sigs);
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
    ProposalId,
    ProposalHeader,
    SignedProposalHeader,
    HeaderAuth,
>();

#[cfg(test)]
mod rlp_tests {
    use monad_mcp_common::spec::KeyPair as _;

    use super::{vote::IndexedSignature, *};

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
        assert!(alloy_rlp::decode_exact::<Signature>(alloy_rlp::encode(&malformed)).is_err());
    }
}

#[cfg(test)]
mod proposal_id_tests {
    use monad_mcp_common::Slot;

    use super::*;
    use crate::spec::ProposalHeader as _;

    fn header(root: u8, msg_len: u32) -> ProposalHeader {
        ProposalHeader {
            root: MerkleRoot(MerkleHash([root; 20])),
            scheme: EncodingScheme::D25(D25 {
                slot: Slot(1),
                msg_len,
                unix_ts: 0,
                depth: 3,
            }),
        }
    }

    #[test]
    fn an_id_covers_the_scheme_and_the_root_but_not_the_signature() {
        let header = header(1, 100);
        assert_eq!(header.pid(), self::header(1, 100).pid());
        assert_ne!(header.pid(), self::header(1, 99).pid());
        assert_ne!(header.pid(), self::header(2, 100).pid());

        let signed = |checksum| SignedProposalHeader {
            header: header.clone(),
            sig: ProposalSignature {
                signer: NodeId::dummy(0),
                checksum,
            },
        };
        assert_eq!(signed(0).pid(), header.pid());
        assert_eq!(signed(0).pid(), signed(1).pid());
    }
}
