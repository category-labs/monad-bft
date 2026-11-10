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
    use std::collections::HashMap;

    use monad_mcp_common::{
        containers::{IndexVec, MAX_VALIDATORS, ValidatorIndex, ValidatorSet},
        spec::Stake as _,
    };

    use super::{NodeId, PubKey, Stake};
    use crate::spec;

    // invariant: the validators are indexed in NodeId order, the canonical
    // enumeration of the valset; indices, nodes, stakes and pubkeys cover the
    // same validators. The valset is non-empty and at most MAX_VALIDATORS
    // large, all stakes are nonzero, and total_stake is their sum.
    pub struct ValidatorData {
        indices: HashMap<NodeId, ValidatorIndex>,
        nodes: IndexVec<ValidatorIndex, NodeId>,
        stakes: IndexVec<ValidatorIndex, Stake>,
        pubkeys: IndexVec<ValidatorIndex, PubKey>,
        total_stake: Stake,
    }

    impl ValidatorData {
        pub fn new(valset: HashMap<NodeId, Stake>, mapping: HashMap<NodeId, PubKey>) -> Self {
            assert_eq!(valset.len(), mapping.len());
            assert!(!valset.is_empty());
            assert!(valset.len() <= MAX_VALIDATORS);
            assert!(valset.keys().all(|node_id| mapping.contains_key(node_id)));
            assert!(valset.values().all(|stake| *stake > Stake::ZERO));

            let mut sorted: Vec<NodeId> = valset.keys().copied().collect();
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
    use monad_mcp_common::{Lane, ProposalScope, Slot};

    use super::NodeId;
    use crate::spec;

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
        D33(D33),
        // d33 without chunk data on the wire, for simulations
        #[cfg(feature = "enable_hollow")]
        D33Hollow(D33),
        #[cfg(test)]
        Trivial(ProposalScope),
    }

    #[cfg(test)]
    impl EncodingScheme {
        pub fn dummy(slot: Slot) -> Self {
            Self::Trivial(ProposalScope::new(slot, Lane::new_unchecked(0)))
        }
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
                Self::D33(d33) => d33.slot,
                #[cfg(feature = "enable_hollow")]
                Self::D33Hollow(d33) => d33.slot,
                #[cfg(test)]
                Self::Trivial(scope) => scope.slot,
            }
        }

        pub fn lane(&self) -> Lane {
            match self {
                Self::D33(d33) => d33.lane,
                #[cfg(feature = "enable_hollow")]
                Self::D33Hollow(d33) => d33.lane,
                #[cfg(test)]
                Self::Trivial(scope) => scope.lane,
            }
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
                Self::D33(f0) => {
                    let fields: [&dyn Encodable; 2] = [&1u8, f0];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
                }
                #[cfg(feature = "enable_hollow")]
                Self::D33Hollow(f0) => {
                    let fields: [&dyn Encodable; 2] = [&2u8, f0];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
                }
                #[cfg(test)]
                Self::Trivial(f0) => {
                    let fields: [&dyn Encodable; 2] = [&255u8, f0];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
                }
            }
        }

        fn length(&self) -> usize {
            match self {
                Self::D33(f0) => {
                    let fields: [&dyn Encodable; 2] = [&1u8, f0];
                    alloy_rlp::list_length::<_, dyn Encodable>(&fields)
                }
                #[cfg(feature = "enable_hollow")]
                Self::D33Hollow(f0) => {
                    let fields: [&dyn Encodable; 2] = [&2u8, f0];
                    alloy_rlp::list_length::<_, dyn Encodable>(&fields)
                }
                #[cfg(test)]
                Self::Trivial(f0) => {
                    let fields: [&dyn Encodable; 2] = [&255u8, f0];
                    alloy_rlp::list_length::<_, dyn Encodable>(&fields)
                }
            }
        }
    }

    impl Decodable for EncodingScheme {
        fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
            let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
            let result = match <u8 as Decodable>::decode(&mut payload)? {
                1 => Self::D33(<D33 as Decodable>::decode(&mut payload)?),
                #[cfg(feature = "enable_hollow")]
                2 => Self::D33Hollow(<D33 as Decodable>::decode(&mut payload)?),
                #[cfg(test)]
                255 => Self::Trivial(<ProposalScope as Decodable>::decode(&mut payload)?),
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
        // on it from monad-mcp-cadence and monad-mcp-da.
        pub sig: ProposalSignature,
    }

    impl spec::ProposalHeader for ProposalHeader {
        type Root = MerkleRoot;
        type Pid = ProposalId;

        fn scope(&self) -> ProposalScope {
            ProposalScope::new(self.scheme.slot(), self.scheme.lane())
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

        fn scope(&self) -> ProposalScope {
            self.header.scope()
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

    // whether the legitimate proposer of the header's slot and lane
    // signed it. Supplied by the DA env, which owns the signature
    // and scheme checks.
    type Authenticator = dyn Fn(&SignedProposalHeader) -> bool + Send + Sync;

    pub struct HeaderAuth {
        authenticator: Box<Authenticator>,
    }

    impl HeaderAuth {
        pub fn new<F>(authenticator: F) -> Self
        where
            F: Fn(&SignedProposalHeader) -> bool + Send + Sync + 'static,
        {
            Self {
                authenticator: Box::new(authenticator),
            }
        }
    }

    impl spec::proposal::HeaderAuth for HeaderAuth {
        type Signed = SignedProposalHeader;

        fn authenticate(&self, signed: &SignedProposalHeader) -> bool {
            (self.authenticator)(signed)
        }
    }
}

mod vote {
    use alloy_rlp::{Decodable, Encodable};
    use monad_mcp_common::{
        containers::{MAX_VALIDATORS, ValidatorIndex, ValidatorSet},
        spec::Signature as _,
    };

    use super::{Signature, Stake, ValidatorData};
    use crate::spec::{self, SignatureCollection as _, validator::ValidatorData as _};

    // the shape of a BLS collection: a signer bitmap, and the one digest
    // every signer signed in place of the aggregate. Wire indices: checked
    // against the validator set by signers and verify.
    // a signer bitmap, the digest of the signed data, and the wrapping sum
    // of the signers' shares (PubKey::share), which binds the bitmap to the
    // signatures as an aggregate signature does
    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct SignatureCollection {
        signers: ValidatorSet,
        digest: u64,
        shares: u64,
    }

    const BITMAP_LEN: usize = MAX_VALIDATORS / 8;

    impl SignatureCollection {
        // little-endian bits, ending with the last signer's byte
        fn bitmap<'a>(&self, buf: &'a mut [u8; BITMAP_LEN]) -> &'a [u8] {
            let mut len = 0;
            for index in self.signers.iter() {
                let index = index.get();
                buf[index / 8] |= 1 << (index % 8);
                len = index / 8 + 1;
            }
            &buf[..len]
        }
    }

    // the rlp list [bitmap, digest]
    impl Encodable for SignatureCollection {
        fn encode(&self, out: &mut dyn bytes::BufMut) {
            let mut buf = [0; BITMAP_LEN];
            let bitmap = self.bitmap(&mut buf);
            let fields: [&dyn Encodable; 3] = [&bitmap, &self.digest, &self.shares];
            alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
        }

        fn length(&self) -> usize {
            let mut buf = [0; BITMAP_LEN];
            let bitmap = self.bitmap(&mut buf);
            let fields: [&dyn Encodable; 3] = [&bitmap, &self.digest, &self.shares];
            alloy_rlp::list_length::<_, dyn Encodable>(&fields)
        }
    }

    impl Decodable for SignatureCollection {
        fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
            let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
            let bitmap = alloy_rlp::Header::decode_bytes(&mut payload, false)?;
            let digest = u64::decode(&mut payload)?;
            let shares = u64::decode(&mut payload)?;
            if !payload.is_empty() {
                return Err(alloy_rlp::Error::UnexpectedLength);
            }
            let signers = signers_of(bitmap)?;
            Ok(Self {
                signers,
                digest,
                shares,
            })
        }
    }

    // a bitmap has one encoding: non-empty, with no trailing zero byte
    fn signers_of(bitmap: &[u8]) -> alloy_rlp::Result<ValidatorSet> {
        let Some(last) = bitmap.last() else {
            return Err(alloy_rlp::Error::Custom("no signatures"));
        };
        if *last == 0 {
            return Err(alloy_rlp::Error::Custom(
                "signer bitmap ends in a zero byte",
            ));
        }
        if bitmap.len() > BITMAP_LEN {
            return Err(alloy_rlp::Error::Custom("signature index out of range"));
        }
        let mut signers = ValidatorSet::new();
        for (byte_index, byte) in bitmap.iter().enumerate() {
            let mut bits = *byte;
            while bits != 0 {
                let index = byte_index * 8 + bits.trailing_zeros() as usize;
                signers.insert(ValidatorIndex::new(index).expect("within the bitmap"));
                bits &= bits - 1;
            }
        }
        Ok(signers)
    }

    impl spec::vote::SignatureCollection for SignatureCollection {
        type Signature = Signature;
        type ValidatorData = ValidatorData;

        // the caller passes verified signatures over one message
        fn aggregate<'a>(
            sigs: impl IntoIterator<Item = (ValidatorIndex, &'a Signature)>,
            _validator_data: &ValidatorData,
        ) -> Self {
            let mut signers = ValidatorSet::new();
            let mut digest = None;
            let mut shares = 0u64;
            for (index, sig) in sigs {
                debug_assert!(sig.is_well_formed());
                debug_assert!(digest.is_none_or(|digest| digest == sig.digest()));
                digest = Some(sig.digest());
                signers.insert(index);
                shares = shares.wrapping_add(sig.share());
            }
            let digest = digest.expect("a collection aggregates at least one signature");
            SignatureCollection {
                signers,
                digest,
                shares,
            }
        }

        fn signers(&self, validator_data: &ValidatorData) -> Option<ValidatorSet> {
            for index in self.signers.iter() {
                // None for an index outside the validator set
                validator_data.checked_index(index.get())?;
            }
            Some(self.signers)
        }

        fn verify(&self, data: &[u8], validator_data: &ValidatorData) -> Option<ValidatorSet> {
            let digest = monad_mcp_common::stub::noncrypto_digest(data);
            if self.digest != digest {
                return None; // Invalid signature
            }
            let signers = self.signers(validator_data)?;
            // the signers named must be the ones whose signatures were
            // aggregated, under their own keys
            let mut shares = 0u64;
            for index in signers.iter() {
                shares = shares.wrapping_add(validator_data.pubkey(index).share(digest));
            }
            (shares == self.shares).then_some(signers)
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
            let digest = monad_mcp_common::stub::noncrypto_digest(data);
            let mut voters = ValidatorSet::new();
            let mut valid = Vec::new();
            for (index, sig) in votes {
                assert!(index.get() < self.validator_data.len());
                assert!(voters.insert(index), "one vote per validator");
                assert!(sig.is_well_formed());

                // just naively exclude any bad signatures. we should use
                // an aggregation tree for bls to iteratively exclude bad
                // signatures.
                if sig.verify_digest(digest, self.validator_data.pubkey(index)) {
                    valid.push((index, sig));
                }
            }

            let signers: ValidatorSet = valid.iter().map(|(index, _)| *index).collect();
            if self.validator_data.sum_stake(&signers) <= target_stake {
                // not enough votes to reach the target stake
                return None;
            }

            let sigcol = SignatureCollection::aggregate(valid, self.validator_data);

            debug_assert!(sigcol.signers(self.validator_data) == Some(signers));
            debug_assert!(sigcol.verify(data, self.validator_data) == Some(signers));

            Some(sigcol)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;

        use bytes::Bytes;
        use monad_mcp_common::spec::KeyPair as _;

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

        // a collection verifies only for the signers whose signatures it
        // aggregates, as an aggregate signature does
        #[test]
        fn a_collection_binds_its_signers() {
            let nodes: Vec<NodeId> = (0..10).map(NodeId::dummy).collect();
            let valset: HashMap<NodeId, Stake> =
                nodes.iter().map(|node| (*node, Stake::from(1))).collect();
            let mapping: HashMap<NodeId, PubKey> = nodes
                .iter()
                .map(|node| (*node, node.keypair().pubkey()))
                .collect();
            let validator_data = ValidatorData::new(valset, mapping);

            let data = Bytes::from_static(b"vote data");
            let sigs: Vec<Signature> = nodes
                .iter()
                .map(|node| node.keypair().sign(&data))
                .collect();
            let index = |i: usize| validator_data.index_of(&nodes[i]).unwrap();
            let set =
                |signers: &[usize]| signers.iter().map(|&i| index(i)).collect::<ValidatorSet>();
            let collection_of = |signers: &[(usize, &Signature)]| {
                let sigs = signers.iter().map(|&(i, sig)| (index(i), sig));
                SignatureCollection::aggregate(sigs, &validator_data)
            };

            let honest = collection_of(&[(1, &sigs[1]), (2, &sigs[2]), (3, &sigs[3])]);
            assert_eq!(honest.verify(&data, &validator_data), Some(set(&[1, 2, 3])));

            // the bitmap names a validator that did not sign, or leaves out
            // one that did
            for signers in [set(&[1, 2, 3, 4]), set(&[1, 2])] {
                let forged = SignatureCollection {
                    signers,
                    ..honest.clone()
                };
                assert_eq!(forged.signers(&validator_data), Some(signers));
                assert_eq!(forged.verify(&data, &validator_data), None);
            }

            // a signature filed under another validator's index
            let misfiled = collection_of(&[(1, &sigs[1]), (4, &sigs[2])]);
            assert_eq!(misfiled.verify(&data, &validator_data), None);

            // an invalid signature aggregated directly
            let mut invalid = sigs[3].clone();
            invalid.make_invalid();
            let with_invalid = collection_of(&[(1, &sigs[1]), (3, &invalid)]);
            assert_eq!(with_invalid.verify(&data, &validator_data), None);
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
    use std::collections::HashMap;

    use alloy_rlp::Encodable;
    use monad_mcp_common::{
        containers::{MAX_VALIDATORS, ValidatorIndex, ValidatorSet},
        spec::KeyPair as _,
    };

    use super::*;
    use crate::spec::vote::SignatureCollection as _;

    // validators 0 to 9 of stake 1
    fn validator_data() -> ValidatorData {
        let nodes: Vec<NodeId> = (0..10).map(NodeId::dummy).collect();
        let valset = nodes.iter().map(|node| (*node, Stake::from(1))).collect();
        let mapping: HashMap<NodeId, PubKey> = nodes
            .iter()
            .map(|node| (*node, node.keypair().pubkey()))
            .collect();
        ValidatorData::new(valset, mapping)
    }

    fn encoded(bitmap: &[u8], digest: u64, shares: u64) -> Vec<u8> {
        let fields: [&dyn Encodable; 3] = [&bitmap, &digest, &shares];
        let mut out = Vec::new();
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, &mut out);
        out
    }

    fn index(index: usize) -> ValidatorIndex {
        ValidatorIndex::new(index).expect("in range")
    }

    // validator i's signature over data
    fn signed(i: usize, data: &bytes::Bytes) -> Signature {
        KeyPair::dummy(i as u64).sign(data)
    }

    #[test]
    fn signer_bitmaps_have_one_encoding() {
        let data = bytes::Bytes::from_static(b"test");
        let (one, nine) = (signed(1, &data), signed(9, &data));
        let validator_data = validator_data();
        let signers = [(index(1), &one), (index(9), &nine)];
        let collection = SignatureCollection::aggregate(signers, &validator_data);
        let set = ValidatorSet::from_iter([index(1), index(9)]);
        assert_eq!(collection.verify(&data, &validator_data), Some(set));
        assert_eq!(collection.verify(b"other", &validator_data), None);
        let outside = [(index(10), &one)];
        let outside = SignatureCollection::aggregate(outside, &validator_data);
        assert_eq!(outside.signers(&validator_data), None);

        let shares = one.share().wrapping_add(nine.share());
        let bytes = alloy_rlp::encode(&collection);
        assert_eq!(bytes, encoded(&[0b10, 0b10], one.digest(), shares));
        assert_eq!(collection.length(), bytes.len());
        assert_eq!(alloy_rlp::decode_exact(&bytes), Ok(collection));

        let decode = |bitmap: &[u8]| {
            alloy_rlp::decode_exact::<SignatureCollection>(encoded(bitmap, one.digest(), shares))
        };
        assert!(decode(&[]).is_err());
        assert!(decode(&[0b10, 0]).is_err());
        assert!(decode(&[1; MAX_VALIDATORS / 8 + 1]).is_err());
        assert!(decode(&[1; MAX_VALIDATORS / 8]).is_ok());

        let mut malformed = one;
        malformed.make_malformed();
        assert!(alloy_rlp::decode_exact::<Signature>(alloy_rlp::encode(&malformed)).is_err());
    }
}

#[cfg(test)]
mod proposal_id_tests {
    use monad_mcp_common::{Lane, ProposalScope, Slot};

    use super::*;
    use crate::spec::ProposalHeader as _;

    fn header(root: u8, lane: u8) -> ProposalHeader {
        ProposalHeader {
            root: MerkleRoot(MerkleHash([root; 20])),
            scheme: EncodingScheme::Trivial(ProposalScope::new(Slot(1), Lane::new_unchecked(lane))),
        }
    }

    #[test]
    fn an_id_covers_the_scheme_and_the_root_but_not_the_signature() {
        let header = header(1, 0);
        assert_eq!(header.pid(), self::header(1, 0).pid());
        assert_ne!(header.pid(), self::header(1, 1).pid());
        assert_ne!(header.pid(), self::header(2, 0).pid());

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
