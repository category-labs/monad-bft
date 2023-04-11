use secp256k1::Secp256k1;
use sha2::Digest;

use crate::Signature;

#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct PubKey(secp256k1::PublicKey);
pub struct KeyPair(secp256k1::KeyPair);
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SecpSignature(secp256k1::ecdsa::RecoverableSignature);

#[derive(Debug, Clone)]
pub struct Error(secp256k1::Error);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn msg_hash(msg: &[u8]) -> secp256k1::Message {
    let mut hasher = sha2::Sha256::new();
    hasher.update(msg);
    let hash = hasher.finalize();

    secp256k1::Message::from_slice(&hash).expect("32 bytes")
}

impl KeyPair {
    pub fn from_slice(keypair: &[u8]) -> Result<Self, Error> {
        secp256k1::KeyPair::from_seckey_slice(secp256k1::SECP256K1, keypair)
            .map(Self)
            .map_err(Error)
    }

    pub fn sign(&self, msg: &[u8]) -> SecpSignature {
        SecpSignature(Secp256k1::sign_ecdsa_recoverable(
            secp256k1::SECP256K1,
            &msg_hash(msg),
            &self.0.secret_key(),
        ))
    }

    pub fn pubkey(&self) -> PubKey {
        PubKey(self.0.public_key())
    }
}

impl PubKey {
    pub fn from_slice(pubkey: &[u8]) -> Result<Self, Error> {
        secp256k1::PublicKey::from_slice(pubkey)
            .map(Self)
            .map_err(Error)
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        self.0.serialize_uncompressed().to_vec()
    }

    pub fn verify(&self, msg: &[u8], signature: &SecpSignature) -> Result<(), Error> {
        Secp256k1::verify_ecdsa(
            secp256k1::SECP256K1,
            &msg_hash(msg),
            &signature.0.to_standard(),
            &self.0,
        )
        .map_err(Error)
    }
}

impl SecpSignature {
    pub fn recover_pubkey(&self, msg: &[u8]) -> Result<PubKey, Error> {
        Secp256k1::recover_ecdsa(secp256k1::SECP256K1, &msg_hash(msg), &self.0)
            .map(PubKey)
            .map_err(Error)
    }

    pub fn serialize(&self) -> [u8; 65] {
        // recid is 0..3, fit in a single byte (see secp256k1 https://docs.rs/secp256k1/0.27.0/src/secp256k1/ecdsa/recovery.rs.html#39)
        let (recid, sig) = self.0.serialize_compact();
        assert!((0..=3).contains(&recid.to_i32()));
        let mut sig_vec = sig.to_vec();
        sig_vec.push(recid.to_i32() as u8);
        sig_vec.try_into().unwrap()
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        if data.len() != 64 + 1 {
            return Err(Error(secp256k1::Error::InvalidSignature));
        }
        let sig_data = &data[..64];
        let recid = secp256k1::ecdsa::RecoveryId::from_i32(data[64] as i32).map_err(Error)?;
        Ok(SecpSignature(
            secp256k1::ecdsa::RecoverableSignature::from_compact(sig_data, recid).map_err(Error)?,
        ))
    }
}

impl Signature for SecpSignature {
    fn sign(msg: &[u8], keypair: &KeyPair) -> Self {
        keypair.sign(msg)
    }

    fn verify(&self, msg: &[u8], pubkey: &PubKey) -> Result<(), Error> {
        pubkey.verify(msg, &self)
    }

    fn recover_pubkey(&self, msg: &[u8]) -> Result<PubKey, Error> {
        self.recover_pubkey(msg)
    }

    fn serialize(&self) -> [u8; 65] {
        self.serialize()
    }
}

#[cfg(test)]
mod tests {
    use tiny_keccak::Hasher;

    use super::{KeyPair, PubKey, SecpSignature};

    #[test]
    fn test_pubkey_roundtrip() {
        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();

        let pubkey_bytes = keypair.pubkey().into_bytes();
        assert!(pubkey_bytes == PubKey::from_slice(&pubkey_bytes).unwrap().into_bytes());
    }

    #[test]
    fn test_eth_address() {
        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();

        let mut hasher = tiny_keccak::Keccak::v256();
        // pubkey() returns 65 bytes, ignore first one
        hasher.update(&keypair.pubkey().into_bytes()[1..]);
        let mut output = [0u8; 32];
        hasher.finalize(&mut output);

        let generated_eth_address = output[12..].to_vec();

        let expected_eth_address = hex::decode("ff7F1B7DbaaF35259dDa7cb42564CB7507C1D88d").unwrap();
        assert!(generated_eth_address == expected_eth_address);
    }

    #[test]
    fn test_verify() {
        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        assert!(keypair.pubkey().verify(msg, &signature).is_ok());
        assert!(keypair.pubkey().verify(b"bye world", &signature).is_err());
    }

    #[test]
    fn test_recovery() {
        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        let recovered_key = signature.recover_pubkey(msg).unwrap();

        assert!(keypair.pubkey().into_bytes() == recovered_key.into_bytes());
    }

    #[test]
    fn test_signature_serde() {
        let privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_slice(&privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        let ser = signature.serialize();
        let deser = SecpSignature::deserialize(&ser);
        assert_eq!(signature, deser.unwrap());
    }
}
