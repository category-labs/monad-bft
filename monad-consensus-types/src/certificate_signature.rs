use monad_crypto::{
    secp256k1::{Error as SecpError, KeyPair as SecpKeyPair, PubKey as SecpPubKey, SecpSignature},
    NopSignature,
};

use crate::{message_signature::MessageSignature, validation::Hashable};

pub trait CertificateKeyPair: Send + Sized + Sync + 'static {
    type PubKeyType: std::cmp::Eq + std::hash::Hash + Copy;
    type Error: std::error::Error + Send + Sync;

    fn from_bytes(secret: impl AsMut<[u8]>) -> Result<Self, Self::Error>;
    fn pubkey(&self) -> Self::PubKeyType;
}

pub trait CertificateSignature:
    Copy + Clone + Eq + Hashable + Send + Sync + std::fmt::Debug + 'static
{
    type KeyPairType: CertificateKeyPair;
    type Error: std::error::Error + Send + Sync;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self;
    fn verify(
        &self,
        msg: &[u8],
        pubkey: &<Self::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> Result<(), Self::Error>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error>;
}

pub trait CertificateSignatureRecoverable: CertificateSignature {
    fn recover_pubkey(
        &self,
        msg: &[u8],
    ) -> Result<
        <<Self as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
        <Self as CertificateSignature>::Error,
    >;
}

impl CertificateKeyPair for SecpKeyPair {
    type PubKeyType = SecpPubKey;
    type Error = SecpError;

    fn from_bytes(secret: impl AsMut<[u8]>) -> Result<Self, Self::Error> {
        Self::from_bytes(secret)
    }

    fn pubkey(&self) -> Self::PubKeyType {
        self.pubkey()
    }
}

impl Hashable for SecpSignature {
    fn hash<H: crate::validation::Hasher>(&self, state: &mut H) {
        let slice = unsafe { std::mem::transmute::<Self, [u8; 65]>(*self) };
        state.update(slice)
    }
}

impl CertificateSignature for SecpSignature {
    type KeyPairType = SecpKeyPair;
    type Error = SecpError;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        keypair.sign(msg)
    }

    fn verify(
        &self,
        msg: &[u8],
        pubkey: &<Self::KeyPairType as CertificateKeyPair>::PubKeyType,
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
    ) -> Result<
        <<Self as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
        <Self as CertificateSignature>::Error,
    > {
        self.recover_pubkey(msg)
    }
}

impl Hashable for NopSignature {
    fn hash<H: crate::validation::Hasher>(&self, state: &mut H) {
        let slice = unsafe { std::mem::transmute::<Self, [u8; 72]>(*self) };
        state.update(slice)
    }
}

impl CertificateSignature for NopSignature {
    type KeyPairType = SecpKeyPair;
    type Error = SecpError;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        <Self as MessageSignature>::sign(msg, keypair)
    }

    fn verify(
        &self,
        msg: &[u8],
        pubkey: &<Self::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> Result<(), Self::Error> {
        <Self as MessageSignature>::verify(self, msg, pubkey)
    }

    fn serialize(&self) -> Vec<u8> {
        <Self as MessageSignature>::serialize(self)
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error> {
        <Self as MessageSignature>::deserialize(signature)
    }
}

impl CertificateSignatureRecoverable for NopSignature {
    fn recover_pubkey(
        &self,
        msg: &[u8],
    ) -> Result<
        <<Self as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType,
        <Self as CertificateSignature>::Error,
    > {
        <Self as MessageSignature>::recover_pubkey(self, msg)
    }
}

#[cfg(test)]
mod test {
    use std::ops::AddAssign;

    use monad_crypto::secp256k1::SecpSignature;

    // valid certificate signature tests
    use crate::certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignatureRecoverable,
    };
    macro_rules! test_all_certificate_signature {
        ($test_name:ident, $test_code:block) => {
            mod $test_name {
                use monad_crypto::{secp256k1::SecpSignature, NopSignature};

                use super::*;

                fn invoke<T>()
                where
                    T: CertificateSignature + std::fmt::Debug,
                    <T::KeyPairType as CertificateKeyPair>::PubKeyType:
                        std::cmp::Eq + std::fmt::Debug,
                {
                    $test_code
                }

                #[test]
                fn secpsignature() {
                    invoke::<SecpSignature>();
                }

                #[test]
                fn nopsignature() {
                    invoke::<NopSignature>();
                }

                // TODO: add module for bls signature
            }
        };
    }

    macro_rules! test_all_certificate_signature_recoverable {
        ($test_name:ident, $test_code:block) => {
            mod $test_name {
                use monad_crypto::{secp256k1::SecpSignature, NopSignature};

                use super::*;

                fn invoke<T>()
                where
                    T: CertificateSignatureRecoverable + std::fmt::Debug,
                    <T::KeyPairType as CertificateKeyPair>::PubKeyType:
                        std::cmp::Eq + std::fmt::Debug,
                {
                    $test_code
                }

                #[test]
                fn secpsignature() {
                    invoke::<SecpSignature>();
                }

                #[test]
                fn nopsignature() {
                    invoke::<NopSignature>();
                }
            }
        };
    }

    test_all_certificate_signature!(test_keypair_deterministic_creation, {
        let mut s1 = [127_u8; 32];
        let mut s2 = [127_u8; 32];

        assert_eq!(s1, s2);

        let k1 = <T::KeyPairType as CertificateKeyPair>::from_bytes(s1.as_mut_slice()).unwrap();
        let k2 = <T::KeyPairType as CertificateKeyPair>::from_bytes(s2.as_mut_slice()).unwrap();

        assert_eq!(k1.pubkey(), k2.pubkey());
    });

    test_all_certificate_signature!(test_serialization_roundtrip, {
        let mut s = [127_u8; 32];
        let certkey = <T::KeyPairType as CertificateKeyPair>::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = T::sign(msg, &certkey);

        let sig_bytes = sig.serialize();
        let sig_de = T::deserialize(sig_bytes.as_ref()).unwrap();

        assert_eq!(sig, sig_de);
    });

    test_all_certificate_signature!(test_signature_verify, {
        let mut s = [127_u8; 32];
        let certkey = <T::KeyPairType as CertificateKeyPair>::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = T::sign(msg, &certkey);

        assert!(sig.verify(msg, &certkey.pubkey()).is_ok());
    });

    test_all_certificate_signature_recoverable!(test_recover, {
        let mut s = [127_u8; 32];
        let certkey = <T::KeyPairType as CertificateKeyPair>::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = T::sign(msg, &certkey);

        assert_eq!(sig.recover_pubkey(msg).unwrap(), certkey.pubkey());
    });

    // invalid certificate signature tests
    #[test]
    fn test_verify_error() {
        let mut s = [127_u8; 32];
        let certkey = <<SecpSignature as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let invalid_msg = b"bye world";
        let sig = <SecpSignature as CertificateSignature>::sign(msg, &certkey);

        assert!(<SecpSignature as CertificateSignature>::verify(
            &sig,
            invalid_msg,
            &certkey.pubkey()
        )
        .is_err());
    }

    #[test]
    fn test_deser_error() {
        let mut s = [127_u8; 32];
        let certkey = <<SecpSignature as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = <SecpSignature as CertificateSignature>::sign(msg, &certkey);

        let mut sig_bytes = <SecpSignature as CertificateSignature>::serialize(&sig);

        // the last byte is the recoveryId
        // recoveryId is 0..=3, adding 4 makes it invalid
        sig_bytes.last_mut().unwrap().add_assign(5);

        assert!(<SecpSignature as CertificateSignature>::deserialize(&sig_bytes).is_err());
    }
}
