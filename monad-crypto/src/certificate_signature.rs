use std::{
    collections::hash_map::DefaultHasher,
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
};

use crate::{
    bls12_381::{BlsError, BlsKeyPair, BlsPubKey, BlsSignature},
    hasher::Hashable,
    secp256k1::{Error as SecpError, KeyPair as SecpKeyPair, PubKey as SecpPubKey, SecpSignature},
    NopKeyPair, NopPubKey, NopSignature,
};

pub trait PubKey:
    Debug + Eq + Hash + Ord + PartialOrd + Copy + Send + Sync + Unpin + 'static
{
    type Error: Display + Debug + Send + Sync;
    fn from_bytes(pubkey: &[u8]) -> Result<Self, Self::Error>;
    fn bytes(&self) -> Vec<u8>;
}

pub trait CertificateKeyPair: Send + Sized + Sync + 'static {
    type PubKeyType: PubKey;
    type Error: Display + Debug + Send + Sync;

    fn from_bytes(secret: &mut [u8]) -> Result<Self, Self::Error>;
    fn pubkey(&self) -> Self::PubKeyType;
}

pub type CertificateSignaturePubKey<T> =
    <<T as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType;

pub trait CertificateSignature:
    Copy + Clone + Eq + Hashable + Send + Sync + std::fmt::Debug + std::hash::Hash + 'static
{
    type KeyPairType: CertificateKeyPair;
    type Error: Display + Debug + Send + Sync;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self;
    fn verify(
        &self,
        msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error>;
}

pub trait CertificateSignatureRecoverable: CertificateSignature {
    fn recover_pubkey(
        &self,
        msg: &[u8],
    ) -> Result<CertificateSignaturePubKey<Self>, <Self as CertificateSignature>::Error>;
}

impl PubKey for SecpPubKey {
    type Error = SecpError;

    fn from_bytes(pubkey: &[u8]) -> Result<Self, Self::Error> {
        Self::from_slice(pubkey)
    }

    fn bytes(&self) -> Vec<u8> {
        SecpPubKey::bytes(self)
    }
}

impl CertificateKeyPair for SecpKeyPair {
    type PubKeyType = SecpPubKey;
    type Error = SecpError;

    fn from_bytes(secret: &mut [u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(secret)
    }

    fn pubkey(&self) -> Self::PubKeyType {
        self.pubkey()
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

impl PubKey for NopPubKey {
    type Error = &'static str;

    fn from_bytes(pubkey: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(
            pubkey
                .try_into()
                .map_err(|_| "couldn't deserialize pubkey")?,
        ))
    }

    fn bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl CertificateKeyPair for NopKeyPair {
    type PubKeyType = NopPubKey;
    type Error = &'static str;

    fn from_bytes(secret: &mut [u8]) -> Result<Self, Self::Error> {
        Ok(Self {
            pubkey: NopPubKey::from_bytes(secret)?,
        })
    }

    fn pubkey(&self) -> Self::PubKeyType {
        self.pubkey
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

impl CertificateSignature for NopSignature {
    type KeyPairType = NopKeyPair;
    type Error = &'static str;

    fn sign(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(msg);

        NopSignature {
            pubkey: keypair.pubkey,
            id: hasher.finish(),
        }
    }

    fn verify(
        &self,
        _msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error> {
        if &self.pubkey == pubkey {
            Ok(())
        } else {
            Err("invalid pubkey")
        }
    }

    fn serialize(&self) -> Vec<u8> {
        self.id
            .to_le_bytes()
            .into_iter()
            .chain(self.pubkey.bytes())
            .collect()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error> {
        let id = u64::from_le_bytes(signature[..8].try_into().unwrap());
        let pubkey = NopPubKey::from_bytes(&signature[8..])?;
        Ok(Self { pubkey, id })
    }
}

impl CertificateSignatureRecoverable for NopSignature {
    fn recover_pubkey(
        &self,
        _msg: &[u8],
    ) -> Result<CertificateSignaturePubKey<Self>, <Self as CertificateSignature>::Error> {
        Ok(self.pubkey)
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
    use std::ops::AddAssign;

    // valid certificate signature tests
    use crate::certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    };
    use crate::{bls12_381::BlsSignature, secp256k1::SecpSignature};
    macro_rules! test_all_certificate_signature {
        ($test_name:ident, $test_code:block) => {
            mod $test_name {
                use super::*;
                use crate::{secp256k1::SecpSignature, NopSignature};

                fn invoke<T>()
                where
                    T: CertificateSignature + std::fmt::Debug,
                    CertificateSignaturePubKey<T>: std::cmp::Eq + std::fmt::Debug,
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

                #[test]
                fn blssignature() {
                    invoke::<BlsSignature>();
                }
            }
        };
    }

    macro_rules! test_all_certificate_signature_recoverable {
        ($test_name:ident, $test_code:block) => {
            mod $test_name {
                use super::*;
                use crate::{secp256k1::SecpSignature, NopSignature};

                fn invoke<T>()
                where
                    T: CertificateSignatureRecoverable + std::fmt::Debug,
                    CertificateSignaturePubKey<T>: std::cmp::Eq + std::fmt::Debug,
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
