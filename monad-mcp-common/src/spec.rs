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

pub use validator::{NodeId, Stake};
pub use vote::{KeyPair, PubKey, Signature};

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

        // Proportional claim on shares discrete shares, as the
        // quotient and remainder of shares * (self / total). The
        // caller must guarantee that total is non-zero.
        fn obligation(&self, total: &Self, shares: usize) -> (usize, usize);

        // the raw integer stake amount, as consumed by stake-weighted
        // proposer scheduling
        fn amount(&self) -> u64;
    }
}

pub mod vote {
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
}
