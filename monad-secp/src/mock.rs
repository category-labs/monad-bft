// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    fmt::{Debug, Display},
    mem::size_of,
};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable, PubKey,
    },
    signing_domain::SigningDomain,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

// This module implements a MockSecp signature scheme for testing purposes with the following properties:
//
// - The keypair, public key, and signature has exactly the same size as real secp256k1 types
// - The keypair can be deterministically generated from a u64 seed for easy testing
// - The signature contains a primitive checksum algorithm resistant to bit-flip
// - The signature supports public key recovery

const MAGIC_BYTE: u8 = 0xCD;

const _: () = assert!(
    size_of::<MockSecpKeyPair>() == size_of::<crate::secp::KeyPair>()
        && size_of::<MockSecpPubKey>() == size_of::<crate::secp::PubKey>()
        && size_of::<MockSecpSignature>() == size_of::<crate::secp::SecpSignature>(),
);

#[serde_as]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    RlpEncodable,
    RlpDecodable,
    Serialize,
    Deserialize,
)]
pub struct MockSecpSignature(#[serde_as(as = "[_; 65]")] [u8; 65]);

impl Display for MockSecpSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[62], self.0[63]
        )
    }
}

impl Debug for MockSecpSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockSecpSignature({})", self)
    }
}

#[serde_as]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    RlpEncodable,
    RlpDecodable,
    Serialize,
    Deserialize,
)]
pub struct MockSecpPubKey(#[serde_as(as = "[_; 64]")] [u8; 64]);

impl Display for MockSecpPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[62], self.0[63]
        )
    }
}

impl Debug for MockSecpPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockSecpPubKey({})", self)
    }
}

#[serde_as]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    RlpEncodable,
    RlpDecodable,
    Serialize,
    Deserialize,
)]
pub struct MockSecpKeyPair(#[serde_as(as = "[_; 96]")] [u8; 96]);

impl Debug for MockSecpKeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(seed) = self.to_seed() {
            write!(f, "MockSecpKeyPair(seed={})", seed)
        } else {
            write!(
                f,
                "MockSecpKeyPair({:>02x}{:>02x}..{:>02x}{:>02x})",
                self.0[0], self.0[1], self.0[94], self.0[95]
            )
        }
    }
}

impl MockSecpKeyPair {
    pub fn from_seed(n: u64) -> Self {
        let mut buf = [0u8; 96];
        buf[..8].copy_from_slice(&n.to_le_bytes());
        buf[8..].fill(MAGIC_BYTE);
        MockSecpKeyPair(buf)
    }

    pub fn to_seed(&self) -> Option<u64> {
        if self.0[8..] == [MAGIC_BYTE; 88] {
            let mut seed_bytes = [0u8; 8];
            seed_bytes.copy_from_slice(&self.0[..8]);
            Some(u64::from_le_bytes(seed_bytes))
        } else {
            None
        }
    }
}

impl CertificateSignature for MockSecpSignature {
    type KeyPairType = MockSecpKeyPair;
    type Error = String;

    fn sign<SD: SigningDomain>(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        let mut buf = [0u8; 65];
        let pubkey = keypair.pubkey();
        let csum = checksum(msg);
        buf[0] = csum;
        buf[1..].copy_from_slice(&pubkey.0);
        MockSecpSignature(buf)
    }

    fn verify<SD: SigningDomain>(
        &self,
        msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error> {
        let csum = checksum(msg);
        if self.0[0] == csum && self.0[1..] == pubkey.0[0..] {
            Ok(())
        } else {
            Err("Invalid signature".to_string())
        }
    }

    fn validate(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn serialize(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error> {
        if signature.len() != 65 {
            return Err("Invalid signature length".to_string());
        }
        let mut buf = [0u8; 65];
        buf.copy_from_slice(&signature[..65]);
        Ok(MockSecpSignature(buf))
    }
}

impl CertificateSignatureRecoverable for MockSecpSignature {
    fn recover_pubkey<SD: SigningDomain>(
        &self,
        msg: &[u8],
    ) -> Result<CertificateSignaturePubKey<Self>, <Self as CertificateSignature>::Error> {
        let csum = checksum(msg);
        if self.0[0] == csum {
            let mut buf = [0u8; 64];
            buf.copy_from_slice(&self.0[1..]);
            Ok(MockSecpPubKey(buf))
        } else {
            Err("Invalid signature for message".to_string())
        }
    }
}

impl PubKey for MockSecpPubKey {
    type Error = String;

    fn from_bytes(pubkey: &[u8]) -> Result<Self, Self::Error> {
        if pubkey.len() != 64 {
            return Err("Invalid pubkey length".to_string());
        }
        let mut buf = [0u8; 64];
        buf.copy_from_slice(&pubkey[..64]);
        Ok(MockSecpPubKey(buf))
    }

    fn bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl CertificateKeyPair for MockSecpKeyPair {
    type PubKeyType = MockSecpPubKey;
    type Error = String;

    fn from_bytes(secret: &mut [u8]) -> Result<Self, Self::Error> {
        if secret.len() != 32 {
            return Err("Invalid keypair length".to_string());
        }

        let mut buf = [0u8; 96];
        buf[..32].copy_from_slice(secret);
        buf[32..64].copy_from_slice(secret);
        buf[64..].copy_from_slice(secret);
        Ok(MockSecpKeyPair(buf))
    }

    fn pubkey(&self) -> Self::PubKeyType {
        let mut buf = [0u8; 64];
        buf.copy_from_slice(&self.0[..64]);
        MockSecpPubKey(buf)
    }
}

fn checksum(data: &[u8]) -> u8 {
    data.iter().fold(0u8, |acc, x| acc ^ x) ^ (data.len() % 256) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    type SigningDomain = monad_crypto::signing_domain::ConsensusMessage;

    #[test]
    fn test_keypair_seed_roundtrip() {
        let seed = 1234567890_u64;
        let keypair = MockSecpKeyPair::from_seed(seed);
        assert_eq!(keypair.to_seed(), Some(seed));
    }
    #[test]
    fn test_sign_and_verify() {
        let keypair = MockSecpKeyPair::from_seed(42);
        let pubkey = keypair.pubkey();
        let msg = b"correct message";

        let signature = MockSecpSignature::sign::<SigningDomain>(msg, &keypair);
        let verification_result = signature.verify::<SigningDomain>(msg, &pubkey);

        assert!(verification_result.is_ok());

        let wrong_msg = b"wrong message";
        let verification_result = signature.verify::<SigningDomain>(wrong_msg, &pubkey);
        assert!(verification_result.is_err());

        let wrong_keypair = MockSecpKeyPair::from_seed(43);
        let wrong_pubkey = wrong_keypair.pubkey();
        let verification_result = signature.verify::<SigningDomain>(msg, &wrong_pubkey);
        assert!(verification_result.is_err());
    }

    #[test]
    fn test_recover_pubkey() {
        let keypair = MockSecpKeyPair::from_seed(42);
        let expected_pubkey = keypair.pubkey();
        let msg = b"correct message";

        let signature = MockSecpSignature::sign::<SigningDomain>(msg, &keypair);
        let recovered_pubkey_result = signature.recover_pubkey::<SigningDomain>(msg);

        assert!(recovered_pubkey_result.is_ok(),);
        assert_eq!(recovered_pubkey_result.unwrap(), expected_pubkey,);

        let wrong_msg = b"wrong message";
        let recovery_result = signature.recover_pubkey::<SigningDomain>(wrong_msg);
        assert!(recovery_result.is_err())
    }
}
