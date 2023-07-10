use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

// This implementation won't sign or verify anything, but its still required to return a PubKey
// It's Hash must also be unique (Signature's Hash is used as a MonadMessage ID) for some period
// of time (the executor message window size?)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NopSignature {
    pubkey: PubKey,
    id: u32,
}

impl Signature for NopSignature {
    fn sign(msg: &[u8], keypair: &KeyPair) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(msg);
        NopSignature {
            pubkey: keypair.pubkey(),
            id: hasher.finish() as u32,
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
        let id = u32::from_le_bytes(signature[..4].try_into().unwrap());
        let pubkey = PubKey::from_slice(&signature[4..])?;
        Ok(Self { pubkey, id })
    }
}
