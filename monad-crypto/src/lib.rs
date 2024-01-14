use hasher::{Hashable, Hasher};

#[cfg(feature = "rustls")]
pub mod rustls;

pub mod bls12_381;
pub mod convert;
pub mod hasher;
pub mod secp256k1;

pub mod certificate_signature;

// This implementation won't sign or verify anything, but its still required to
// return a PubKey It's Hash must also be unique (Signature's Hash is used as a
// MonadMessage ID) for some period of time (the executor message window size?)
pub struct NopKeyPair {
    pubkey: NopPubKey,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NopPubKey([u8; 32]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NopSignature {
    pub pubkey: NopPubKey,
    pub id: u64,
}

impl Hashable for NopSignature {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.pubkey.0);
        state.update(self.id.to_le_bytes());
    }
}
