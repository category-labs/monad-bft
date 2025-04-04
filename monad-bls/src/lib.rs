use alloy_rlp::{Decodable, Encodable, Header};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey, PubKey,
};

mod aggregation_tree;
pub use aggregation_tree::BlsSignatureCollection;
mod bls;
pub use bls::{BlsAggregateSignature, BlsError, BlsKeyPair, BlsPubKey, BlsSignature};

impl std::fmt::Display for BlsPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.bytes();
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            bytes[0],
            bytes[1],
            bytes[bytes.len() - 2],
            bytes[bytes.len() - 1]
        )
    }
}

impl PubKey for BlsPubKey {
    type Error = BlsError;

    fn from_bytes(pubkey: &[u8]) -> Result<Self, Self::Error> {
        Self::uncompress(pubkey)
    }

    fn bytes(&self) -> Vec<u8> {
        self.compress().to_vec()
    }
}

impl Encodable for BlsPubKey {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.compress().encode(out);
    }
}

impl Decodable for BlsPubKey {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let b = Header::decode_bytes(buf, false)?;

        match <Self as PubKey>::from_bytes(b) {
            Ok(pk) => Ok(pk),
            Err(_) => Err(alloy_rlp::Error::Custom("invalid pubkey")),
        }
    }
}

impl CertificateKeyPair for BlsKeyPair {
    type PubKeyType = BlsPubKey;
    type Error = BlsError;

    fn from_bytes(secret: &mut [u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(secret)
    }

    fn pubkey(&self) -> Self::PubKeyType {
        self.pubkey()
    }
}

impl CertificateSignature for BlsSignature {
    type KeyPairType = BlsKeyPair;
    type Error = BlsError;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        keypair.sign(msg)
    }

    fn verify(
        &self,
        msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error> {
        self.verify(msg, pubkey)
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error> {
        Self::deserialize(signature)
    }
}

#[cfg(test)]
mod test {
    // valid certificate signature tests
    use monad_crypto::certificate_signature::CertificateSignature;

    use crate::BlsSignature;

    type SignatureType = BlsSignature;
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
}
