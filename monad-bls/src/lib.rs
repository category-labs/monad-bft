use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey, PubKey,
    },
    hasher::{Hashable, Hasher},
};

mod aggregation_tree;
pub use aggregation_tree::BlsSignatureCollection;
mod bls;
pub use bls::{BlsAggregateSignature, BlsError, BlsKeyPair, BlsPubKey, BlsSignature};
mod convert;

impl Hashable for BlsAggregateSignature {
    fn hash(&self, state: &mut impl Hasher) {
        Hashable::hash(&self.as_signature(), state);
    }
}

impl PubKey for BlsPubKey {
    type Error = BlsError;

    fn from_bytes(pubkey: &[u8]) -> Result<Self, Self::Error> {
        Self::deserialize(pubkey)
    }

    fn bytes(&self) -> Vec<u8> {
        self.serialize()
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
