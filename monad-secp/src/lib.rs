mod secp;
use alloy_rlp::{Decodable, Encodable};
use monad_crypto::certificate_signature::{
    self, CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
    CertificateSignatureRecoverable,
};
pub use secp::{Error, KeyPair, PubKey, SecpSignature};

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

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        keypair.sign(msg)
    }

    fn verify(
        &self,
        msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error> {
        pubkey.verify(msg, self)
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize().to_vec()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error> {
        Self::deserialize(signature)
    }
}

impl CertificateSignatureRecoverable for SecpSignature {
    fn recover_pubkey(
        &self,
        msg: &[u8],
    ) -> Result<CertificateSignaturePubKey<Self>, <Self as CertificateSignature>::Error> {
        self.recover_pubkey(msg)
    }
}

#[cfg(test)]
mod test {
    use std::ops::AddAssign;

    // valid certificate signature tests
    use monad_crypto::certificate_signature::CertificateSignature;

    use crate::SecpSignature;

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
        let sig = SignatureType::sign(msg, &certkey);

        let sig_bytes = sig.serialize();
        let sig_de = SignatureType::deserialize(sig_bytes.as_ref()).unwrap();

        assert_eq!(sig, sig_de);
    }

    #[test]
    fn test_signature_verify() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = SignatureType::sign(msg, &certkey);

        assert!(sig.verify(msg, &certkey.pubkey()).is_ok());
    }

    #[test]
    fn test_recover() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = SignatureType::sign(msg, &certkey);

        assert_eq!(sig.recover_pubkey(msg).unwrap(), certkey.pubkey());
    }

    // invalid certificate signature tests
    #[test]
    fn test_verify_error() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let invalid_msg = b"bye world";
        let sig = SignatureType::sign(msg, &certkey);

        assert!(SignatureType::verify(&sig, invalid_msg, &certkey.pubkey()).is_err());
    }

    #[test]
    fn test_deser_error() {
        let mut s = [127_u8; 32];
        let certkey = KeyPairType::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = SignatureType::sign(msg, &certkey);

        let mut sig_bytes = SignatureType::serialize(&sig);

        // the last byte is the recoveryId
        // recoveryId is 0..=3, adding 4 makes it invalid
        sig_bytes.last_mut().unwrap().add_assign(5);

        assert!(SignatureType::deserialize(&sig_bytes).is_err());
    }
}
