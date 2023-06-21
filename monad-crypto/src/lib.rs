use std::fmt::Debug;
use std::hash::Hash;

#[cfg(feature = "proto")]
pub mod convert;

pub mod nop;
pub mod secp256k1;

use monad_traits::{Deserializable, Serializable};

pub trait Compressable: Sized {
    type UncompressError: std::error::Error + Send + Sync;
    fn compress(&self) -> Vec<u8>;
    fn uncompress(msg: &[u8]) -> Result<Self, Self::UncompressError>;
}

pub trait Signature:
    Serializable + Deserializable + Copy + Clone + Debug + Eq + Hash + Send + Sync + 'static
{
    type Error: std::error::Error + Send + Sync;
    type KeyPair: KeyPair<PubKey = Self::PubKey>;
    type PubKey: PubKey;

    fn sign(msg: &[u8], keypair: &Self::KeyPair) -> Self;

    fn verify(&self, msg: &[u8], pubkey: &Self::PubKey) -> Result<(), Self::Error>;

    fn recover_pubkey(&self, msg: &[u8]) -> Result<Self::PubKey, Self::Error>;
}

pub trait PubKey:
    Serializable + Deserializable + Copy + Clone + Debug + Eq + Hash + Ord + Send + Sync + 'static
{
}

pub trait KeyPair: Sized + 'static {
    type Signature: Signature;
    type PubKey: PubKey;
    type Error;

    fn from_bytes(secret: impl AsMut<[u8]>) -> Result<Self, Self::Error>;

    fn sign(&self, msg: &[u8]) -> Self::Signature;

    fn pubkey(&self) -> Self::PubKey;
}
