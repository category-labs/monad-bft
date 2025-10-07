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

mod recoverable_address;
mod secp;

use alloy_primitives::Address;
use alloy_rlp::{Decodable, Encodable};
use monad_crypto::{
    certificate_signature::{
        self, CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    signing_domain::SigningDomain,
};
pub use monad_eth_types::ExtractEthAddress;
pub use recoverable_address::RecoverableAddress;
pub use secp::{Error, KeyPair, PubKey, SecpSignature};
use serde::{Deserialize, Serialize};

impl ExtractEthAddress for PubKey {
    fn get_eth_address(&self) -> Address {
        Address::from_raw_public_key(&self.bytes()[1..])
    }
}

impl std::fmt::Display for PubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.bytes_compressed();
        write!(
            f,
            "{:>02x}{:>02x}{:>02x}{:>02x}..{:>02x}{:>02x}{:>02x}{:>02x}",
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[bytes.len() - 4],
            bytes[bytes.len() - 3],
            bytes[bytes.len() - 2],
            bytes[bytes.len() - 1]
        )
    }
}

impl certificate_signature::PubKey for PubKey {
    type Error = Error;

    fn from_bytes(pubkey: &[u8]) -> Result<Self, Self::Error> {
        Self::from_slice(pubkey)
    }

    fn bytes(&self) -> Vec<u8> {
        Self::bytes_compressed(self).to_vec()
    }
}

impl Encodable for PubKey {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.bytes_compressed().encode(out);
    }
}

impl Decodable for PubKey {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let b = <[u8; secp256k1::constants::PUBLIC_KEY_SIZE]>::decode(buf)?;

        match <Self as certificate_signature::PubKey>::from_bytes(&b) {
            Ok(pk) => Ok(pk),
            Err(_) => Err(alloy_rlp::Error::Custom("invalid pubkey")),
        }
    }
}

impl CertificateKeyPair for KeyPair {
    type PubKeyType = PubKey;
    type Error = Error;

    fn from_bytes(secret: &mut [u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(secret)
    }

    fn pubkey(&self) -> Self::PubKeyType {
        self.pubkey()
    }
}

impl CertificateSignature for SecpSignature {
    type KeyPairType = KeyPair;
    type Error = Error;

    fn sign<SD: SigningDomain>(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        keypair.sign::<SD>(msg)
    }

    fn verify<SD: SigningDomain>(
        &self,
        msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error> {
        pubkey.verify::<SD>(msg, self)
    }

    fn validate(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize().to_vec()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error> {
        Self::deserialize(signature)
    }
}

impl CertificateSignatureRecoverable for SecpSignature {
    fn recover_pubkey<SD: SigningDomain>(
        &self,
        msg: &[u8],
    ) -> Result<CertificateSignaturePubKey<Self>, <Self as CertificateSignature>::Error> {
        self.recover_pubkey::<SD>(msg)
    }
}

impl Serialize for SecpSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hex_str = "0x".to_string() + &hex::encode(self.serialize());
        serializer.serialize_str(&hex_str)
    }
}

impl<'de> Deserialize<'de> for SecpSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = <String as Deserialize>::deserialize(deserializer)?;

        let hex_str = match buf.strip_prefix("0x") {
            Some(hex_str) => hex_str,
            None => &buf,
        };

        let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

        Self::deserialize(bytes.as_ref()).map_err(<D::Error as serde::de::Error>::custom)
    }
}

#[cfg(test)]
mod test {
    use std::ops::AddAssign;

    // valid certificate signature tests
    use monad_crypto::{certificate_signature::CertificateSignature, signing_domain};

    use crate::SecpSignature;

    type SigningDomainType = signing_domain::ConsensusMessage;
    type SignatureType = SecpSignature;
    type KeyPairType = <SignatureType as CertificateSignature>::KeyPairType;

    #[test]
    fn test_keypair_deterministic_creation() {
        let mut s1 = [127_u8; 32];
        let mut s2 = [127_u8; 32];

        assert_eq!(s1, s2);

        let k1 = KeyPairType::from_bytes(s1.as_mut_slice()).unwrap();
        let k2 = KeyPairType::from_bytes(s2.as_mut_slice()).unwrap();

        assert_eq!(k1.pubkey(), k2.pubkey());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = SignatureType::sign::<SigningDomainType>(msg, &certkey);

        let sig_bytes = sig.serialize();
        let sig_de = SignatureType::deserialize(sig_bytes.as_ref()).unwrap();

        assert_eq!(sig, sig_de);
    }

    #[test]
    fn test_signature_verify() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = SignatureType::sign::<SigningDomainType>(msg, &certkey);

        assert!(sig
            .verify::<SigningDomainType>(msg, &certkey.pubkey())
            .is_ok());
    }

    #[test]
    fn test_recover() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = SignatureType::sign::<SigningDomainType>(msg, &certkey);

        assert_eq!(
            sig.recover_pubkey::<SigningDomainType>(msg).unwrap(),
            certkey.pubkey()
        );
    }

    // invalid certificate signature tests
    #[test]
    fn test_verify_error() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let invalid_msg = b"bye world";
        let sig = SignatureType::sign::<SigningDomainType>(msg, &certkey);

        assert!(
            SignatureType::verify::<SigningDomainType>(&sig, invalid_msg, &certkey.pubkey())
                .is_err()
        );
    }

    #[test]
    fn test_deser_error() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = SignatureType::sign::<SigningDomainType>(msg, &certkey);

        let mut sig_bytes = SignatureType::serialize(&sig);

        // the last byte is the recoveryId
        // recoveryId is 0..=3, adding 4 makes it invalid
        sig_bytes.last_mut().unwrap().add_assign(5);

        assert!(SignatureType::deserialize(&sig_bytes).is_err());
    }
}
