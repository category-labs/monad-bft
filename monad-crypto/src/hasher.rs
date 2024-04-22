use std::{fmt::Debug, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Digest;
use zerocopy::AsBytes;

/// A 32-byte/256-bit hash
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct Hash(
    #[serde(serialize_with = "serialize_hash")]
    #[serde(deserialize_with = "deserialize_hash")]
    pub [u8; 32],
);

impl Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({:?})", self.0)
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
    let bytes = if let Some(("", hex_str)) = buf.split_once("0x") {
        hex::decode(hex_str.to_owned()).map_err(<D::Error as serde::de::Error>::custom)?
    } else {
        return Err(<D::Error as serde::de::Error>::custom("Missing hex prefix"));
    };

    bytes.try_into().map_err(|e: Vec<u8>| {
        <D::Error as serde::de::Error>::custom(format!(
            "Invalid hash len: {:?} data: {:?}",
            e.len(),
            e
        ))
    })
}

pub trait Hashable {
    fn hash(&self, state: &mut impl Hasher);
}

pub trait Hasher: Sized {
    fn new() -> Self;
    fn update(&mut self, data: impl AsRef<[u8]>);
    fn hash(self) -> Hash;

    fn hash_object(obj: &impl Hashable) -> Hash {
        let mut hasher = Self::new();
        obj.hash(&mut hasher);
        hasher.hash()
    }
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
