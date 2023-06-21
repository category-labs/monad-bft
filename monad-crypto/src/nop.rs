use crate::secp256k1::{SecpError, SecpKeyPair, SecpPubKey};
use crate::{KeyPair, Signature};
use monad_traits::{Deserializable, Serializable};
use rand::Rng;

// This implementation won't sign or verify anything, but its still required to return a PubKey
// It's Hash must also be unique (Signature's Hash is used as a MonadMessage ID) for some period
// of time (the executor message window size?)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NopSignature {
    pubkey: SecpPubKey,
    id: u32,
}

impl Signature for NopSignature {
    type KeyPair = SecpKeyPair;
    type PubKey = SecpPubKey;
    type Error = SecpError;

    fn sign(_msg: &[u8], keypair: &SecpKeyPair) -> Self {
        let mut rng = rand::thread_rng();

        NopSignature {
            pubkey: keypair.pubkey(),
            id: rng.gen::<u32>(),
        }
    }

    fn verify(&self, _msg: &[u8], _pubkey: &SecpPubKey) -> Result<(), Self::Error> {
        Ok(())
    }

    fn recover_pubkey(&self, _msg: &[u8]) -> Result<SecpPubKey, Self::Error> {
        Ok(self.pubkey)
    }
}

impl Serializable for NopSignature {
    fn serialize(&self) -> Vec<u8> {
        self.id
            .to_le_bytes()
            .into_iter()
            .chain(self.pubkey.bytes().into_iter())
            .collect()
    }
}

impl Deserializable for NopSignature {
    type ReadError = SecpError;

    fn deserialize(signature: &[u8]) -> Result<Self, Self::ReadError> {
        let id = u32::from_le_bytes(signature[..4].try_into().unwrap());
        let pubkey = SecpPubKey::from_slice(&signature[4..])?;
        Ok(Self { pubkey, id })
    }
}
