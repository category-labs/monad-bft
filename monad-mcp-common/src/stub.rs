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

pub use self::{validator::*, vote::*};

/// A fast, non-cryptographic 64-bit digest for the stub env: rapidhash v3,
/// whose output is fixed by its reference algorithm, so it is the same on
/// every platform and across crate versions (pinned by a test). It stands in
/// for the hash behind a signature and a proposal id. The stub provides no
/// security (keys derive from public u64s), so neither collision nor preimage
/// resistance is needed here; a production pid or signature needs a
/// cryptographic hash.
pub fn noncrypto_digest(data: &[u8]) -> u64 {
    rapidhash::v3::rapidhash_v3(data)
}

#[cfg(test)]
#[test]
fn the_noncrypto_digest_is_pinned() {
    assert_eq!(noncrypto_digest(b""), PINNED_EMPTY_DIGEST);
}

#[cfg(test)]
const PINNED_EMPTY_DIGEST: u64 = 232177599295442350;

mod validator {
    use alloy_rlp::{RlpDecodableWrapper, RlpEncodableWrapper};
    // Into implemented for testing purpose only.
    use derive_more::Into;

    use super::vote::KeyPair;
    use crate::spec;

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
        Default,
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

        fn obligation(&self, total: &Self, shares: usize) -> (usize, Self) {
            let prod = self.0 as u128 * shares as u128;
            let total = total.0 as u128;
            ((prod / total) as usize, Self((prod % total) as u64))
        }

        fn amount(&self) -> u64 {
            self.0
        }
    }

    #[cfg(test)]
    mod tests {
        use proptest::prelude::*;

        use super::*;
        use crate::spec::Stake as _;

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

mod vote {
    use alloy_rlp::{Decodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
    use bytes::Bytes;
    // Into implemented on these types for testing purpose only.
    use derive_more::Into;

    use crate::spec::{self, vote::Signature as _};

    #[derive(Clone, PartialEq, Eq, Hash, Debug, Into)]
    pub struct KeyPair(u64);

    #[derive(
        Clone, Copy, PartialEq, Eq, Hash, Debug, Into, RlpEncodableWrapper, RlpDecodableWrapper,
    )]
    pub struct PubKey(u64);

    #[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable)]
    pub struct Signature {
        by: PubKey,
        digest: u64,
        malformed: bool,
    }

    impl Decodable for Signature {
        fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
            let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
            let signature = Self {
                by: PubKey::decode(&mut payload)?,
                digest: u64::decode(&mut payload)?,
                malformed: bool::decode(&mut payload)?,
            };
            if !payload.is_empty() {
                return Err(alloy_rlp::Error::UnexpectedLength);
            }
            if !signature.is_well_formed() {
                return Err(alloy_rlp::Error::Custom("malformed signature"));
            }
            Ok(signature)
        }
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
                digest: super::noncrypto_digest(data),
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
            self.is_well_formed()
                && self.by == *pubkey
                && self.digest == super::noncrypto_digest(data)
        }
    }
}
