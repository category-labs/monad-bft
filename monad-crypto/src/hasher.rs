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

use std::{fmt::Debug, ops::Deref};

use alloy_rlp::{RlpDecodableWrapper, RlpEncodableWrapper};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Digest;
use zerocopy::IntoBytes;

/// A 32-byte/256-bit hash
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct Hash(
    #[serde(serialize_with = "serialize_hash")]
    #[serde(deserialize_with = "deserialize_hash")]
    pub [u8; 32],
);

impl Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", self)
    }
}

impl Deref for Hash {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x")?;
        for b in self.0 {
            write!(f, "{:02x?}", b)?;
        }
        Ok(())
    }
}

fn serialize_hash<S>(hash: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let hex_str = "0x".to_string() + &hex::encode(hash);
    serializer.serialize_str(&hex_str)
}

fn deserialize_hash<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;

    let Some(hex_str) = buf.strip_prefix("0x") else {
        return Err(<D::Error as serde::de::Error>::custom("Missing hex prefix"));
    };

    let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

    bytes.try_into().map_err(|e: Vec<u8>| {
        <D::Error as serde::de::Error>::custom(format!(
            "Invalid hash len: {:?} data: {:?}",
            e.len(),
            e
        ))
    })
}

pub trait Hasher: Sized {
    fn new() -> Self;
    fn update(&mut self, data: impl AsRef<[u8]>);
    fn hash(self) -> Hash;
}

/// The global hasher type
pub type HasherType = Blake3Hash;

pub struct Sha256Hash(sha2::Sha256);
impl Hasher for Sha256Hash {
    fn new() -> Self {
        Self(sha2::Sha256::new())
    }
    fn update(&mut self, data: impl AsRef<[u8]>) {
        self.0.update(data);
    }
    fn hash(self) -> Hash {
        Hash(self.0.finalize().into())
    }
}

pub struct Blake3Hash(blake3::Hasher);
impl Hasher for Blake3Hash {
    fn new() -> Self {
        Self(blake3::Hasher::new())
    }
    fn update(&mut self, data: impl AsRef<[u8]>) {
        self.0.update(data.as_ref());
    }
    fn hash(self) -> Hash {
        Hash(self.0.finalize().into())
    }
}
