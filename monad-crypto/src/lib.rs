use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::secp256k1::{Error, KeyPair, PubKey};

#[cfg(feature = "proto")]
pub mod convert;

pub mod bls12_381;
pub mod secp256k1;

pub trait Signature: Copy + Clone + Eq + Hash + Send + Sync + std::fmt::Debug + 'static {
    fn sign(msg: &[u8], keypair: &KeyPair) -> Self;

    fn verify(&self, msg: &[u8], pubkey: &PubKey) -> Result<(), Error>;

    fn recover_pubkey(&self, msg: &[u8]) -> Result<PubKey, Error>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(signature: &[u8]) -> Result<Self, Error>;
}

pub trait GenericKeyPair: Sized + 'static {
    type SignatureType: GenericSignature;
    type Error: std::error::Error;

    fn sign(&self, msg: &[u8]) -> Self::SignatureType;
    fn from_bytes(secret: impl AsMut<[u8]>) -> Result<Self, Self::Error>;
    fn pubkey(&self) -> <Self::SignatureType as GenericSignature>::PubKeyType;
}

pub trait GenericSignature:
    Copy + Clone + Eq + Hash + Send + Sync + std::fmt::Debug + 'static
{
    type KeyPairType: GenericKeyPair; // do not restrict the associated type bc multiple signatures can be created from the same keypair
    type PubKeyType;
    type Error: std::error::Error;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self;

    fn verify(&self, msg: &[u8], pubkey: &Self::PubKeyType) -> Result<(), Self::Error>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error>;
}

// This implementation won't sign or verify anything, but its still required to return a PubKey
// It's Hash must also be unique (Signature's Hash is used as a MonadMessage ID) for some period
// of time (the executor message window size?)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NopSignature {
    pubkey: PubKey,
    id: u64,
}

impl GenericSignature for NopSignature {
    type KeyPairType = KeyPair;
    type PubKeyType = PubKey;
    type Error = Error;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(msg);

        NopSignature {
            pubkey: keypair.pubkey(),
            id: hasher.finish(),
        }
    }

    fn verify(&self, _msg: &[u8], _pubkey: &Self::PubKeyType) -> Result<(), Self::Error> {
        Ok(())
    }

    fn serialize(&self) -> Vec<u8> {
        <Self as Signature>::serialize(self)
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error> {
        <Self as Signature>::deserialize(signature)
    }
}

impl Signature for NopSignature {
    fn sign(msg: &[u8], keypair: &KeyPair) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(msg);

        NopSignature {
            pubkey: keypair.pubkey(),
            id: hasher.finish(),
        }
    }

    fn verify(&self, _msg: &[u8], _pubkey: &PubKey) -> Result<(), Error> {
        Ok(())
    }

    fn recover_pubkey(&self, _msg: &[u8]) -> Result<PubKey, Error> {
        Ok(self.pubkey)
    }

    fn serialize(&self) -> Vec<u8> {
        self.id
            .to_le_bytes()
            .into_iter()
            .chain(self.pubkey.bytes().into_iter())
            .collect()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Error> {
        let id = u64::from_le_bytes(signature[..8].try_into().unwrap());
        let pubkey = PubKey::from_slice(&signature[8..])?;
        Ok(Self { pubkey, id })
    }
}
