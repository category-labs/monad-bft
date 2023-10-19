use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use monad_crypto::{
    secp256k1::{Error as SecpError, KeyPair as SecpKeyPair, PubKey as SecpPubKey, SecpSignature},
    NopSignature,
};

pub trait MessageSignature:
    Copy + Clone + Eq + std::hash::Hash + Send + Sync + std::fmt::Debug + Unpin + 'static
{
    fn sign(msg: &[u8], keypair: &SecpKeyPair) -> Self;

    fn verify(&self, msg: &[u8], pubkey: &SecpPubKey) -> Result<(), SecpError>;

    fn recover_pubkey(&self, msg: &[u8]) -> Result<SecpPubKey, SecpError>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(signature: &[u8]) -> Result<Self, SecpError>;
}

impl MessageSignature for SecpSignature {
    fn sign(msg: &[u8], keypair: &SecpKeyPair) -> Self {
        keypair.sign(msg)
    }

    fn verify(&self, msg: &[u8], pubkey: &SecpPubKey) -> Result<(), SecpError> {
        pubkey.verify(msg, self)
    }

    fn recover_pubkey(&self, msg: &[u8]) -> Result<SecpPubKey, SecpError> {
        self.recover_pubkey(msg)
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize().to_vec()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, SecpError> {
        Self::deserialize(signature)
    }
}

impl MessageSignature for NopSignature {
    fn sign(msg: &[u8], keypair: &SecpKeyPair) -> Self {
        let mut hasher = DefaultHasher::new();
        hasher.write(msg);

        NopSignature {
            pubkey: keypair.pubkey(),
            id: hasher.finish(),
        }
    }

    fn verify(&self, _msg: &[u8], _pubkey: &SecpPubKey) -> Result<(), SecpError> {
        Ok(())
    }

    fn recover_pubkey(&self, _msg: &[u8]) -> Result<SecpPubKey, SecpError> {
        Ok(self.pubkey)
    }

    fn serialize(&self) -> Vec<u8> {
        self.id
            .to_le_bytes()
            .into_iter()
            .chain(self.pubkey.bytes())
            .collect()
    }

    fn deserialize(signature: &[u8]) -> Result<Self, SecpError> {
        let id = u64::from_le_bytes(signature[..8].try_into().unwrap());
        let pubkey = SecpPubKey::from_slice(&signature[8..])?;
        Ok(Self { pubkey, id })
    }
}

#[cfg(test)]
mod test {
    use std::ops::AddAssign;

    use monad_crypto::secp256k1::{KeyPair, SecpSignature};

    use super::MessageSignature;

    macro_rules! test_all_message_signature {
        ($test_name:ident, $test_code:block) => {
            mod $test_name {
                use monad_crypto::{secp256k1::SecpSignature, NopSignature};

                use super::*;

                fn invoke<T>()
                where
                    T: MessageSignature + std::fmt::Debug,
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

    test_all_message_signature!(test_serialization_roundtrip, {
        let mut s = [127_u8; 32];
        let key = KeyPair::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = T::sign(msg, &key);

        let sig_bytes = sig.serialize();
        let sig_de = T::deserialize(sig_bytes.as_ref()).unwrap();

        assert_eq!(sig, sig_de);
    });

    test_all_message_signature!(test_verify, {
        let mut s = [127_u8; 32];
        let key = KeyPair::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = T::sign(msg, &key);

        assert!(sig.verify(msg, &key.pubkey()).is_ok());
    });

    test_all_message_signature!(test_recover, {
        let mut s = [127_u8; 32];
        let key = KeyPair::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = T::sign(msg, &key);

        assert_eq!(sig.recover_pubkey(msg).unwrap(), key.pubkey());
    });

    #[test]
    fn test_verify_error() {
        let mut s = [127_u8; 32];
        let key = KeyPair::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let invalid_msg = b"bye world";
        let sig = <SecpSignature as MessageSignature>::sign(msg, &key);

        assert!(
            <SecpSignature as MessageSignature>::verify(&sig, invalid_msg, &key.pubkey()).is_err()
        );
    }

    #[test]
    fn test_deser_error() {
        let mut s = [127_u8; 32];
        let certkey = KeyPair::from_bytes(s.as_mut_slice()).unwrap();

        let msg = b"hello world";
        let sig = <SecpSignature as MessageSignature>::sign(msg, &certkey);

        let mut sig_bytes = <SecpSignature as MessageSignature>::serialize(&sig);

        // the last byte is the recoveryId
        // recoveryId is 0..=3, adding 4 makes it invalid
        sig_bytes.last_mut().unwrap().add_assign(5);

        assert!(<SecpSignature as MessageSignature>::deserialize(&sig_bytes).is_err());
    }
}
