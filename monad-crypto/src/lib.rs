use std::fmt::Debug;

use hasher::{Hashable, Hasher};

#[cfg(feature = "rustls")]
pub mod rustls;

pub mod convert;
pub mod hasher;

pub mod certificate_signature;

pub struct NopKeyPair {
    pubkey: NopPubKey,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NopPubKey([u8; 32]);

impl Debug for NopPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            write!(
                f,
                "{:>02x}{:>02x}..{:>02x}{:>02x}",
                self.0[0], self.0[1], self.0[30], self.0[31]
            )
        } else {
            write!(f, "NopPubKey({:?})", self.0)
        }
    }
}

/// NopSignature is an implementation of CertificateSignature that's not cryptographically secure
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
