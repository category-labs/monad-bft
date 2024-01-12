use hasher::{Hashable, Hasher};

use crate::secp256k1::PubKey;

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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NopSignature {
    pub pubkey: PubKey,
    pub id: u64,
}

impl Hashable for NopSignature {
    fn hash(&self, state: &mut impl Hasher) {
        let slice = unsafe { std::mem::transmute::<Self, [u8; 72]>(*self) };
        state.update(slice)
    }
}
