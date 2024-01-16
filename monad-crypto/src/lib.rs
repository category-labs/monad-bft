use hasher::{Hashable, Hasher};

#[cfg(feature = "rustls")]
pub mod rustls;

pub mod convert;
pub mod hasher;

pub mod certificate_signature;

pub struct NopKeyPair {
    pubkey: NopPubKey,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NopPubKey([u8; 32]);

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
