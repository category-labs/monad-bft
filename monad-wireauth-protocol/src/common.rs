use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, LE, U32};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::errors::CryptoError;

pub const CIPHER_TAG_SIZE: usize = 16;
pub const MAC_TAG_SIZE: usize = 16;
pub const PUBLIC_KEY_SIZE: usize = 33;
pub const HASH_OUTPUT_SIZE: usize = 32;

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

#[repr(transparent)]
#[derive(
    Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
pub struct SerializedPublicKey([u8; 33]);

impl SerializedPublicKey {
    pub fn as_bytes(&self) -> &[u8; 33] {
        &self.0
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8; 33] {
        &mut self.0
    }
}

impl From<&PublicKey> for SerializedPublicKey {
    fn from(public_key: &PublicKey) -> Self {
        SerializedPublicKey(public_key.0.serialize())
    }
}

impl From<PublicKey> for SerializedPublicKey {
    fn from(public_key: PublicKey) -> Self {
        SerializedPublicKey(public_key.0.serialize())
    }
}

impl From<[u8; 33]> for SerializedPublicKey {
    fn from(bytes: [u8; 33]) -> Self {
        SerializedPublicKey(bytes)
    }
}

impl std::fmt::Debug for SerializedPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

impl std::fmt::Display for SerializedPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct PublicKey(secp256k1::PublicKey);

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0.serialize();
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        )
    }
}

impl From<secp256k1::PublicKey> for PublicKey {
    fn from(key: secp256k1::PublicKey) -> Self {
        PublicKey(key)
    }
}

impl From<PublicKey> for [u8; 33] {
    fn from(public_key: PublicKey) -> Self {
        public_key.0.serialize()
    }
}

impl From<&PublicKey> for [u8; 33] {
    fn from(public_key: &PublicKey) -> Self {
        public_key.0.serialize()
    }
}

impl TryFrom<[u8; 33]> for PublicKey {
    type Error = CryptoError;

    fn try_from(bytes: [u8; 33]) -> Result<Self, Self::Error> {
        let public_key = secp256k1::PublicKey::from_slice(&bytes)?;
        Ok(PublicKey(public_key))
    }
}

impl TryFrom<&[u8; 33]> for PublicKey {
    type Error = CryptoError;

    fn try_from(bytes: &[u8; 33]) -> Result<Self, Self::Error> {
        let public_key = secp256k1::PublicKey::from_slice(bytes)?;
        Ok(PublicKey(public_key))
    }
}

impl TryFrom<SerializedPublicKey> for PublicKey {
    type Error = CryptoError;

    fn try_from(key: SerializedPublicKey) -> Result<Self, Self::Error> {
        let public_key = secp256k1::PublicKey::from_slice(key.as_bytes())?;
        Ok(PublicKey(public_key))
    }
}

impl TryFrom<&SerializedPublicKey> for PublicKey {
    type Error = CryptoError;

    fn try_from(key: &SerializedPublicKey) -> Result<Self, Self::Error> {
        let public_key = secp256k1::PublicKey::from_slice(key.as_bytes())?;
        Ok(PublicKey(public_key))
    }
}

impl PublicKey {
    pub(crate) fn inner(&self) -> &secp256k1::PublicKey {
        &self.0
    }
}

#[derive(Clone)]
pub struct PrivateKey(secp256k1::SecretKey);

impl Drop for PrivateKey {
    fn drop(&mut self) {
        self.0.non_secure_erase();
    }
}

impl PrivateKey {
    pub(crate) fn inner(&self) -> &secp256k1::SecretKey {
        &self.0
    }

    pub(crate) fn from_inner(key: secp256k1::SecretKey) -> Self {
        PrivateKey(key)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError> {
        let array: [u8; 32] = bytes
            .try_into()
            .map_err(|_| secp256k1::Error::InvalidSecretKey)?;
        let secret = secp256k1::SecretKey::from_byte_array(array)?;
        Ok(PrivateKey(secret))
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
