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

pub use proposal::{ChunkHeader, MerkleRoot, ProposalSignature};
pub use validator::{NodeId, Stake};
pub use vote::{KeyPair, PubKey, Signature, SignatureCollection};

pub mod validator {
    use std::{iter::Sum, ops::Add};

    // A simple, pure identifier to a node. The NodeId type
    // is not involved in any sort of crypto operation *in this module*.
    pub trait NodeId: Copy + Ord + std::hash::Hash {}

    pub trait Stake: Add<Output = Self> + Sum + From<u64> + Ord + Copy {
        const ZERO: Self;

        // floor(2/3 * self)
        fn supermajority_threshold(&self) -> Self;

        // floor(1/3 * self)
        fn honest_threshold(&self) -> Self;
    }

    pub trait ValidatorData {
        type NodeId: NodeId;
        type PubKey: super::vote::PubKey;
        type Stake: Stake;

        fn nodes(&self) -> impl Iterator<Item = &Self::NodeId>;
        fn contains(&self, node_id: &Self::NodeId) -> bool;

        // the caller must gurantee that the node_id is in the valset.
        fn get_pubkey(&self, node_id: &Self::NodeId) -> &Self::PubKey;

        // the caller must gurantee that the node_id is in the valset.
        fn get_stake(&self, node_id: &Self::NodeId) -> &Self::Stake;

        // the caller must guarantee that the nodes are all in the valset.
        fn sum_stake<'a>(&self, nodes: impl IntoIterator<Item = &'a Self::NodeId>) -> Self::Stake
        where
            Self::NodeId: 'a;

        fn total_stake(&self) -> Self::Stake;
    }
}

pub mod vote {
    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;

    // Only used to verify a Signature signed using KeyPair.
    pub trait PubKey: Clone + Eq {}

    // Used to sign a piece of data to get a Signature. Clone deliberately
    // not required - the programmer should carefully evaluate any need
    // for cloning the KeyPair for security purpose.
    pub trait KeyPair {
        type PubKey: PubKey;
        type Signature: Signature<PubKey = Self::PubKey>;
        fn pubkey(&self) -> Self::PubKey;
        fn sign(&self, data: &Bytes) -> Self::Signature;
    }

    type SigColSignature<SC> = <SC as SignatureCollection>::Signature;
    type SigColPubKey<SC> = <<SC as SignatureCollection>::Signature as Signature>::PubKey;
    type SigColNodeId<SC> = ValidatorNodeId<<SC as SignatureCollection>::ValidatorData>;
    type SigColValidatorData<SC> = <SC as SignatureCollection>::ValidatorData;
    type ValidatorNodeId<VD> = <VD as super::validator::ValidatorData>::NodeId;

    // A signature on a message by a KeyPair. Once presented the message &
    // PubKey it can verify the authenticity of the message & the
    // signer. Note: no pubkey-recovery capability assumed.
    //
    // Note: the signature type on its own doesn't imply that it's
    // verified.
    pub trait Signature: Clone + Eq {
        type PubKey: PubKey;

        // Structural validity of the signature value itself,
        // independent of any message or pubkey. For BLS this is the
        // on-curve point check.
        fn is_well_formed(&self) -> bool;

        fn verify(&self, data: &[u8], pubkey: &Self::PubKey) -> bool;
    }

    // A collection of signatures on a common message.
    //
    // Contains the information of the set of signers, assuming the
    // proper context of a validator set.
    pub trait SignatureCollection: Clone + Eq {
        type Signature: Signature;

        // The validator context for this signature collection. It may
        // carry implementation-dependent data for optimized
        // performance.
        type ValidatorData: super::validator::ValidatorData<PubKey = SigColPubKey<Self>>;

        // The caller must ensure the nodes in sig_map are present in
        // the validator data, and all signatures are well-formed
        // (see Signature::is_well_formed).
        fn aggregate(
            sig_map: &HashMap<&ValidatorNodeId<Self::ValidatorData>, &Self::Signature>,
            validator_data: &Self::ValidatorData,
        ) -> Self;

        // Returns the complete set of signers who contributed to this
        // signature collection. Depending on the implementation, it
        // may be possible to return signers even if the sigcol is
        // invalid.
        //
        // Returns None in case where signature collection is
        // malformed or inconsistent with the validator data.
        fn signers<'s>(
            &'s self,
            validator_data: &'s Self::ValidatorData,
        ) -> Option<HashSet<&'s ValidatorNodeId<Self::ValidatorData>>>;

        // invariant:
        //
        // aggregate(sig_map).verify(data) == Some(_) iff all sigs are valid.
        fn verify<'s>(
            &'s self,
            data: &[u8],
            validator_data: &'s Self::ValidatorData,
        ) -> Option<HashSet<&'s ValidatorNodeId<Self::ValidatorData>>>;
    }

    pub trait VoteAggregation<'a, Stake>
    where
        Stake: super::Stake,
        SigColValidatorData<Self::SignatureCollection>:
            super::validator::ValidatorData<Stake = Stake>,
    {
        type SignatureCollection: SignatureCollection;

        fn from_validator_data(
            validator_data: &'a SigColValidatorData<Self::SignatureCollection>,
        ) -> Self;

        // The caller must guarantee that:
        //
        // - all votes's NodeId are present in validator data
        // - all signatures are well-formed
        // - summing all voter's stake does not overflow
        //
        // Returns None if no aggregation of valid combination of
        // votes reaches target_stake.
        //
        // Invariant:
        //
        // let mut vote_agg = Self::from_validator_data(validator_data);
        // if let Some(sigcol) = vote_agg.try_aggregate(data, votes, threshold) {
        //     // signers must be extractable regarding to the same validator data
        //     let signers = sigcol.signers(validator_data).unwrap();
        //
        //     // returned `sigcol` must be valid and consistent with its `signers`
        //     assert!(Some(signers) == sigcol.verify(data, validator_data));
        //
        //     // `signers` are all in `votes`
        //     assert!(signers.all(|signer| votes.contains_key(signer)));
        //
        //     // the selected `signers` have combined stake strictly greater than `threshold`
        //     assert!(signers.map(stake).sum() > threshold);
        // }
        //
        // Simplified signature:
        //
        //     fn try_aggregate(
        //         &mut self,
        //         data: &[u8],
        //         votes: HashMap<&NodeId, &Signature>,
        //         target_stake: Stake,
        //     ) -> Option<SignatureCollection>;
        //
        fn try_aggregate(
            &mut self,
            data: &[u8],
            votes: HashMap<
                &SigColNodeId<Self::SignatureCollection>,
                &SigColSignature<Self::SignatureCollection>,
            >,
            target_stake: Stake,
        ) -> Option<Self::SignatureCollection>;
    }
}

pub mod proposal {
    // A commitment to a proposal's payload.
    pub trait MerkleRoot: Copy + Eq + std::hash::Hash + std::fmt::Debug {}

    // not the same as vote signature. at least ProposalSignature is not
    // supposed to be aggregatable.
    pub trait ProposalSignature: Clone + Eq + std::hash::Hash + std::fmt::Debug {}

    // The DA chunk header, opaque to consensus except for validation
    // against the proposal commitment and signature.
    pub trait ChunkHeader: Clone + Eq + std::hash::Hash + std::fmt::Debug {
        type Root: MerkleRoot;
        type Sig: ProposalSignature;
        fn validate(&self, root: &Self::Root, sig: &Self::Sig) -> bool;
    }
}

// Statically checks a full env against the spec
pub const fn assert_env<
    'a,
    NodeId,
    Stake,
    PubKey,
    KeyPair,
    Signature,
    ValidatorData,
    SignatureCollection,
    VoteAggregation,
    MerkleRoot,
    ProposalSignature,
    ChunkHeader,
>()
where
    NodeId: validator::NodeId,
    Stake: validator::Stake,
    PubKey: vote::PubKey,
    KeyPair: vote::KeyPair<PubKey = PubKey, Signature = Signature>,
    Signature: vote::Signature<PubKey = PubKey>,
    ValidatorData: validator::ValidatorData<NodeId = NodeId, PubKey = PubKey, Stake = Stake>,
    SignatureCollection:
        vote::SignatureCollection<Signature = Signature, ValidatorData = ValidatorData>,
    VoteAggregation: vote::VoteAggregation<'a, Stake, SignatureCollection = SignatureCollection>,
    MerkleRoot: proposal::MerkleRoot,
    ProposalSignature: proposal::ProposalSignature,
    ChunkHeader: proposal::ChunkHeader<Root = MerkleRoot, Sig = ProposalSignature>,
{
}
