use monad_crypto::{
    secp256k1::{Error as SecpError, KeyPair as SecpKeyPair, PubKey as SecpPubKey, SecpSignature},
    NopSignature,
};

use crate::{message_signature::MessageSignature, validation::Hashable};

pub trait CertificateKeyPair: Send + Sized + Sync + 'static {
    type PubKeyType: std::cmp::Eq + std::hash::Hash;
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
