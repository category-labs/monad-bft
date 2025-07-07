use std::{
    collections::hash_map::DefaultHasher,
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
};

use alloy_rlp::{Decodable, Encodable};

use crate::{signing_domain::SigningDomain, NopKeyPair, NopPubKey, NopSignature};

pub trait PubKey:
    Debug
    + Display
    + Eq
    + Hash
    + Ord
    + PartialOrd
    + Copy
    + Send
    + Sync
    + Unpin
    + Encodable
    + Decodable
    + 'static
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
    Copy + Clone + Eq + Debug + Hash + Send + Sync + Unpin + Encodable + Decodable + 'static
{
    type KeyPairType: CertificateKeyPair;
    type Error: Display + Debug + Send + Sync;

    fn sign<SD: SigningDomain>(msg: &[u8], keypair: &Self::KeyPairType) -> Self;
    fn verify<SD: SigningDomain>(
        &self,
        msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(signature: &[u8]) -> Result<Self, Self::Error>;
}

pub trait CertificateSignatureRecoverable: CertificateSignature {
    fn recover_pubkey<SD: SigningDomain>(
        &self,
        msg: &[u8],
    ) -> Result<CertificateSignaturePubKey<Self>, <Self as CertificateSignature>::Error>;
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

impl CertificateSignature for NopSignature {
    type KeyPairType = NopKeyPair;
    type Error = &'static str;

    fn sign<SD: SigningDomain>(msg: &[u8], keypair: &Self::KeyPairType) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(SD::PREFIX);
        hasher.write(msg);

        NopSignature {
            pubkey: keypair.pubkey,
            id: hasher.finish(),
        }
    }

    fn verify<SD: SigningDomain>(
        &self,
        msg: &[u8],
        pubkey: &CertificateSignaturePubKey<Self>,
    ) -> Result<(), Self::Error> {
        if &self.pubkey == pubkey {
            let id = {
                let mut hasher = DefaultHasher::new();
                hasher.write(SD::PREFIX);
                hasher.write(msg);
                hasher.finish()
            };
            if self.id == id {
                Ok(())
            } else {
                Err("unexpected message")
            }
        } else {
            Err("unexpected pubkey")
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
    fn recover_pubkey<SD: SigningDomain>(
        &self,
        msg: &[u8],
    ) -> Result<CertificateSignaturePubKey<Self>, <Self as CertificateSignature>::Error> {
        let id = {
            let mut hasher = DefaultHasher::new();
            hasher.write(SD::PREFIX);
            hasher.write(msg);
            hasher.finish()
        };
        if self.id == id {
            Ok(self.pubkey)
        } else {
            Err("unexpected message")
        }
    }
}

#[cfg(test)]
mod test {
    // valid certificate signature tests
    use crate::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignatureRecoverable,
        },
        signing_domain, NopSignature,
    };

    type SigningDomainType = signing_domain::Vote;
    type SignatureType = NopSignature;
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
}
