use sha2::Digest;

#[derive(PartialEq)]
pub struct PubKey(secp256k1::PublicKey);
pub struct KeyPair(secp256k1::KeyPair);
pub struct Signature(secp256k1::ecdsa::Signature);

#[derive(Debug)]
pub struct Error(secp256k1::Error);

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

    pub fn sign(&self, msg: &[u8]) -> Signature {
        Signature(self.0.secret_key().sign_ecdsa(msg_hash(msg)))
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

    pub fn verify(&self, msg: &[u8], signature: &Signature) -> Result<(), Error> {
        signature.0.verify(&msg_hash(msg), &self.0).map_err(Error)
    }
}

#[cfg(test)]
mod tests {
    use tiny_keccak::Hasher;

    use super::{KeyPair, PubKey};

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
}
