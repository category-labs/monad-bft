use std::{fmt::Debug, ops::Deref};

use sha2::Digest;
use zerocopy::AsBytes;

/// A 32-byte/256-bit hash
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Hash(pub [u8; 32]);

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
