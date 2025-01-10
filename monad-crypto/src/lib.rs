use std::fmt::{Debug, Display};

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};

pub mod convert;
pub mod hasher;

pub mod certificate_signature;

pub struct NopKeyPair {
    pubkey: NopPubKey,
}

#[derive(
    Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, RlpDecodableWrapper, RlpEncodableWrapper,
)]
pub struct NopPubKey([u8; 32]);

impl Debug for NopPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NopPubKey({})", self)
    }
}

impl Display for NopPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}

/// NopSignature is an implementation of CertificateSignature that's not cryptographically secure
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable)]
pub struct NopSignature {
    pub pubkey: NopPubKey,
    pub id: u64,
}
