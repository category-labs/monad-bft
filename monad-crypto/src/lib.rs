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

use std::fmt::{Debug, Display};

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use serde::{Deserialize, Serialize};

pub mod certificate_signature;
pub mod hasher;
pub mod signing_domain;

#[derive(Clone)]
pub struct NopKeyPair {
    pubkey: NopPubKey,
}

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    RlpDecodableWrapper,
    RlpEncodableWrapper,
    Serialize,
    Deserialize,
)]
pub struct NopPubKey([u8; 32]);

impl Debug for NopPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NopPubKey({})", self)
    }
}

impl Display for NopPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}

/// NopSignature is an implementation of CertificateSignature that's not cryptographically secure
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable, Serialize, Deserialize,
)]
pub struct NopSignature {
    pub pubkey: NopPubKey,
    pub id: u64,
}
