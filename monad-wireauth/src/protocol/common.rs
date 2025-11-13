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

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, LE, U32};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub const CIPHER_TAG_SIZE: usize = 16;
pub const MAC_TAG_SIZE: usize = 16;
pub const PUBLIC_KEY_SIZE: usize = monad_secp::COMPRESSED_PUBLIC_KEY_SIZE;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionIndex(u32);

impl SessionIndex {
    pub const MIN: SessionIndex = SessionIndex(0);
    pub const MAX: SessionIndex = SessionIndex(u32::MAX);

    pub fn new(value: u32) -> Self {
        SessionIndex(value)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn increment(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

impl From<u32> for SessionIndex {
    fn from(value: u32) -> Self {
        SessionIndex(value)
    }
}

impl From<U32<LE>> for SessionIndex {
    fn from(value: U32<LE>) -> Self {
        SessionIndex(value.get())
    }
}

impl std::fmt::Display for SessionIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for SessionIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop, Debug, PartialEq)]
pub struct CipherKey([u8; 16]);

impl From<&HashOutput> for CipherKey {
    fn from(hash: &HashOutput) -> Self {
        let mut key = [0u8; 16];
        key.copy_from_slice(&hash.0[..16]);
        CipherKey(key)
    }
}

impl AsRef<[u8]> for CipherKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8; 16]> for CipherKey {
    fn as_ref(&self) -> &[u8; 16] {
        &self.0
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Debug)]
pub struct CipherNonce(pub [u8; 16]);

impl From<u64> for CipherNonce {
    fn from(value: u64) -> Self {
        let mut nonce = [0u8; 16];
        nonce[..8].copy_from_slice(&value.to_le_bytes());
        CipherNonce(nonce)
    }
}

impl AsRef<[u8]> for CipherNonce {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8; 16]> for CipherNonce {
    fn as_ref(&self) -> &[u8; 16] {
        &self.0
    }
}

#[derive(Clone, Debug, Zeroize)]
pub struct HashOutput(pub [u8; 32]);

impl AsRef<[u8]> for HashOutput {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8; 32]> for HashOutput {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Eq)]
pub struct MacTag(pub [u8; 16]);

impl AsRef<[u8]> for MacTag {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8; 16]> for MacTag {
    fn as_ref(&self) -> &[u8; 16] {
        &self.0
    }
}

impl PartialEq<[u8; 16]> for MacTag {
    fn eq(&self, other: &[u8; 16]) -> bool {
        self.0 == *other
    }
}

impl PartialEq for MacTag {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl From<MacTag> for [u8; 16] {
    fn from(tag: MacTag) -> Self {
        tag.0
    }
}

impl From<[u8; 16]> for MacTag {
    fn from(bytes: [u8; 16]) -> Self {
        MacTag(bytes)
    }
}

impl From<HashOutput> for MacTag {
    fn from(hash: HashOutput) -> Self {
        let mut tag = [0u8; 16];
        tag.copy_from_slice(&hash.0[..16]);
        MacTag(tag)
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SharedSecret(pub [u8; 32]);

impl AsRef<[u8]> for SharedSecret {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8; 32]> for SharedSecret {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for SharedSecret {
    fn from(bytes: [u8; 32]) -> Self {
        SharedSecret(bytes)
    }
}

pub struct TransportKeys {
    pub send_key: CipherKey,
    pub recv_key: CipherKey,
}
