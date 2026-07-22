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
    // Into implemented for testing purpose only.
    use derive_more::Into;

    use super::vote::KeyPair;
    use crate::spec;

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Into)]
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
        Default,
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
        // 2f
        fn supermajority_threshold(&self) -> Self {
            // handle overflow
            Self((self.0 * 2) / 3)
        }

        // f
        fn majority_threshold(&self) -> Self {
            Self(self.0 / 3)
        }
    }
}

mod proposal {
    use crate::spec;

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub struct MerkleRoot(pub u64);

    impl spec::proposal::MerkleRoot for MerkleRoot {}

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct ProposalSignature;

    impl spec::proposal::ProposalSignature for ProposalSignature {}

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct OpaqueChunkHeader;

    impl spec::proposal::ChunkHeader for OpaqueChunkHeader {
        type Root = MerkleRoot;
        type Sig = ProposalSignature;

        fn validate(&self, _root: &MerkleRoot, _sig: &ProposalSignature) -> bool {
            // stubbed to always return true for testing purpose
            true
        }
    }
}

mod vote {
    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;
    // Into implemented on these types for testing purpose only.
    use derive_more::Into;

    use crate::spec::{self, vote::Signature as _};

    #[derive(PartialEq, Eq, Hash, Debug, Into)]
    pub struct KeyPair(u64);

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Into)]
    pub struct PubKey(u64);

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct Signature {
        by: PubKey,
        data: Bytes,
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct SignatureCollection {
        data: Bytes,
        sigs: Vec<Signature>,
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
            }
        }
    }

    impl spec::vote::Signature for Signature {
        type PubKey = PubKey;

        fn verify(&self, data: &[u8], pubkey: Self::PubKey) -> bool {
            self.by == pubkey && self.data == data
        }
    }

    impl spec::vote::SignatureCollection for SignatureCollection {
        type Signature = Signature;

        fn aggregate<'a>(data: &Bytes, sigs: impl Iterator<Item = &'a Signature>) -> Option<Self> {
            let mut sigs_vec = Vec::new();
            for sig in sigs {
                if sig.data != data {
                    // data mismatch
                    return None;
                }
                sigs_vec.push(sig.clone());
            }

            if sigs_vec.is_empty() {
                return None;
            }

            Some(SignatureCollection {
                data: data.clone(),
                sigs: sigs_vec,
            })
        }

        fn verify<N>(&self, data: &[u8], mapping: &HashMap<N, PubKey>) -> Option<HashSet<N>>
        where
            N: spec::validator::NodeId,
        {
            if self.data != data {
                // data mismatch
                return None;
            }

            let mut signers = HashSet::new();
            for sig in &self.sigs {
                let Some((signer, pubkey)) =
                    mapping.iter().find(|(_node_id, pubkey)| sig.by == **pubkey)
                else {
                    // signer not found
                    return None;
                };

                if !sig.verify(data, *pubkey) {
                    // signer's signature is invalid
                    return None;
                }

                if signers.contains(signer) {
                    // duplicate signature from the same signer
                    return None;
                }

                signers.insert(*signer);
            }
            Some(signers)
        }
    }
}

const _: () = crate::spec::assert_env::<
    NodeId,
    Stake,
    PubKey,
    KeyPair,
    Signature,
    SignatureCollection,
    MerkleRoot,
    ProposalSignature,
    OpaqueChunkHeader,
>();
