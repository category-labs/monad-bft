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

use std::collections::{HashMap, HashSet};

use bytes::Bytes;

// A simple, pure identifier to a node. The NodeId type
// is not involved in any sort of crypto operation *in this module*.
pub trait NodeId: Copy + Eq + std::hash::Hash {}

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
    fn aggregate<'a>(data: &Bytes, sigs: impl Iterator<Item = &'a Self::Signature>) -> Option<Self>
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

pub(crate) mod test_helper {
    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;
    // Into implemented on these types for testing purpose only.
    use derive_more::Into;

    use super::Signature as _;

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Into)]
    pub struct NodeId(u64);

    impl super::NodeId for NodeId {}

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

    impl NodeId {
        pub fn dummy(id: u64) -> Self {
            NodeId(id)
        }

        // allows quickly deriving the KeyPair from a NodeId for
        // testing purpose.
        pub fn keypair(&self) -> KeyPair {
            KeyPair(self.0)
        }
    }

    impl super::PubKey for PubKey {}

    impl KeyPair {
        pub fn dummy(id: u64) -> Self {
            KeyPair(id)
        }
    }

    impl super::KeyPair for KeyPair {
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

    impl super::Signature for Signature {
        type PubKey = PubKey;

        fn verify(&self, data: &[u8], pubkey: Self::PubKey) -> bool {
            self.by == pubkey && self.data == data
        }
    }

    impl super::SignatureCollection for SignatureCollection {
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
                // reject empty signature collection
                // Q: is this necessary?
                return None;
            }

            Some(SignatureCollection {
                data: data.clone(),
                sigs: sigs_vec,
            })
        }

        fn verify<N>(&self, data: &[u8], mapping: &HashMap<N, PubKey>) -> Option<HashSet<N>>
        where
            N: super::NodeId,
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
