use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use monad_crypto::{
    secp256k1::{Error as SecpError, KeyPair as SecpKeyPair, PubKey as SecpPubKey, SecpSignature},
    NopSignature,
};

pub trait MessageSignature:
    Copy + Clone + Eq + std::hash::Hash + Send + Sync + std::fmt::Debug + 'static
{
    fn sign(msg: &[u8], keypair: &SecpKeyPair) -> Self;

    fn verify(&self, msg: &[u8], pubkey: &SecpPubKey) -> Result<(), SecpError>;

    fn recover_pubkey(&self, msg: &[u8]) -> Result<SecpPubKey, SecpError>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(signature: &[u8]) -> Result<Self, SecpError>;
}

impl MessageSignature for SecpSignature {
    fn sign(msg: &[u8], keypair: &SecpKeyPair) -> Self {
        keypair.sign(msg)
    }

    fn verify(&self, msg: &[u8], pubkey: &SecpPubKey) -> Result<(), SecpError> {
        pubkey.verify(msg, self)
    }

    fn recover_pubkey(&self, msg: &[u8]) -> Result<SecpPubKey, SecpError> {
        self.recover_pubkey(msg)
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize().to_vec()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, SecpError> {
        Self::deserialize(signature)
    }
}

impl MessageSignature for NopSignature {
    fn sign(msg: &[u8], keypair: &SecpKeyPair) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(msg);

        NopSignature {
            pubkey: keypair.pubkey(),
            id: hasher.finish(),
        }
    }

    fn verify(&self, _msg: &[u8], _pubkey: &SecpPubKey) -> Result<(), SecpError> {
        Ok(())
    }

    fn recover_pubkey(&self, _msg: &[u8]) -> Result<SecpPubKey, SecpError> {
        Ok(self.pubkey)
    }

    fn serialize(&self) -> Vec<u8> {
        self.id
            .to_le_bytes()
            .into_iter()
            .chain(self.pubkey.bytes())
            .collect()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, SecpError> {
        let id = u64::from_le_bytes(signature[..8].try_into().unwrap());
        let pubkey = SecpPubKey::from_slice(&signature[8..])?;
        Ok(Self { pubkey, id })
    }
}
