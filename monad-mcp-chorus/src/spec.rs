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
    use std::ops::Add;

    // A simple, pure identifier to a node. The NodeId type
    // is not involved in any sort of crypto operation *in this module*.
    pub trait NodeId: Copy + Eq + std::hash::Hash {}

    pub trait Stake: Default + Add<Output = Self> + Ord + Copy {
        // floor(2/3 * self)
        fn supermajority_threshold(&self) -> Self;

        // floor(1/3 * self)
        fn majority_threshold(&self) -> Self;
    }
}

pub mod vote {
    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;

    use super::validator::NodeId;

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

    // A signature on a message by a KeyPair. Once presented the message &
    // PubKey it can verify the authenticity of the message & the
    // signer. Note: no pubkey-recovery capability assumed.
    pub trait Signature: Clone + Eq {
        type PubKey: PubKey;
        fn verify(&self, data: &[u8], pubkey: Self::PubKey) -> bool;
    }

    // A collection of signatures on a common message.
    pub trait SignatureCollection: Clone + Eq {
        type Signature: Signature;

        // Returns None if there is any issue with the signatures.
        // Q: is validator mapping necessary for aggregation? or just the signatures would be enough?
        fn aggregate<'a>(
            data: &Bytes,
            sigs: impl Iterator<Item = &'a Self::Signature>,
        ) -> Option<Self>
        where
            Self: 'static;

        // Returns None if the SignatureCollection is invalid or
        // inconsistent with the provided mapping. Otherwise return the
        // set of signers.
        fn verify<N>(
            &self,
            data: &[u8],
            // this allows passing &validator_data.mapping directly without cloning.
            mapping: &HashMap<N, <Self::Signature as Signature>::PubKey>,
        ) -> Option<HashSet<N>>
        where
            N: NodeId;
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
    NodeId,
    Stake,
    PubKey,
    KeyPair,
    Signature,
    SignatureCollection,
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
    SignatureCollection: vote::SignatureCollection<Signature = Signature>,
    MerkleRoot: proposal::MerkleRoot,
    ProposalSignature: proposal::ProposalSignature,
    ChunkHeader: proposal::ChunkHeader<Root = MerkleRoot, Sig = ProposalSignature>,
{
}
