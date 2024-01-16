use monad_crypto::hasher::{Hasher, HasherType};
use secp256k1::Secp256k1;
use zeroize::Zeroize;

/// secp256k1 public key
#[derive(Copy, Clone)]
pub struct PubKey(secp256k1::PublicKey);
/// secp256k1 keypair
pub struct KeyPair(secp256k1::KeyPair);
/// secp256k1 ecsda recoverable signature
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SecpSignature(secp256k1::ecdsa::RecoverableSignature);

/// wrapped secp256k1 library errors
#[derive(Debug, Clone)]
pub struct Error(secp256k1::Error);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Error {}

impl std::fmt::Debug for PubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let ser = self.bytes();
        write!(
            f,
            "{:02x}{:02x}..{:02x}{:02x}",
            ser[0], ser[1], ser[30], ser[31]
        )?;
        Ok(())
    }
}

/// The comparison is faster but might not be stable across library versions
impl std::cmp::PartialEq for PubKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_fast_unstable(&other.0)
    }
}

impl std::cmp::Eq for PubKey {}

impl std::cmp::Ord for PubKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp_fast_unstable(&other.0)
    }
}

impl std::cmp::PartialOrd for PubKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Faster to use the transmuted memory values, but might not be stable across
/// library versions
impl std::hash::Hash for PubKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let slice = unsafe { std::mem::transmute::<Self, [u8; 64]>(*self) };
        slice.hash(state)
    }
}

fn msg_hash(msg: &[u8]) -> secp256k1::Message {
    let mut hasher = HasherType::new();
    hasher.update(msg);
    let hash = hasher.hash();

    secp256k1::Message::from_slice(&hash.0).expect("32 bytes")
}

impl KeyPair {
    /// Create a keypair from a secret key slice. The secret is zero-ized after
    /// use. The secret must be 32 byytes.
    pub fn from_bytes(secret: &mut [u8]) -> Result<Self, Error> {
        let keypair = secp256k1::KeyPair::from_seckey_slice(secp256k1::SECP256K1, secret)
            .map(Self)
            .map_err(Error);
        secret.zeroize();
        keypair
    }

    /// Create a SecpSignature over Hash(msg)
    pub fn sign(&self, msg: &[u8]) -> SecpSignature {
        SecpSignature(Secp256k1::sign_ecdsa_recoverable(
            secp256k1::SECP256K1,
            &msg_hash(msg),
            &self.0.secret_key(),
        ))
    }

    /// Get the pubkey
    pub fn pubkey(&self) -> PubKey {
        PubKey(self.0.public_key())
    }
}

impl PubKey {
    /// Deserialize public key from bytes
    /// Can be compressed OR uncompressed pubkey
    pub fn from_slice(pubkey: &[u8]) -> Result<Self, Error> {
        secp256k1::PublicKey::from_slice(pubkey)
            .map(Self)
            .map_err(Error)
    }

    /// Serialize public key
    pub fn bytes(&self) -> Vec<u8> {
        self.0.serialize_uncompressed().to_vec()
    }

    pub fn bytes_compressed(&self) -> Vec<u8> {
        self.0.serialize().to_vec()
    }

    /// Verify that the message is correctly signed
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
    /// Recover the pubkey from signature given the message
    pub fn recover_pubkey(&self, msg: &[u8]) -> Result<PubKey, Error> {
        Secp256k1::recover_ecdsa(secp256k1::SECP256K1, &msg_hash(msg), &self.0)
            .map(PubKey)
            .map_err(Error)
    }

    /// Serialize the signature. The signature itself is 64 bytes. An extra byte
    /// is used to store the RecoveryId to recover the pubkey
    pub fn serialize(&self) -> [u8; 65] {
        // recid is 0..3, fit in a single byte (see secp256k1 https://docs.rs/secp256k1/0.27.0/src/secp256k1/ecdsa/recovery.rs.html#39)
        let (recid, sig) = self.0.serialize_compact();
        assert!((0..=3).contains(&recid.to_i32()));
        let mut sig_vec = sig.to_vec();
        sig_vec.push(recid.to_i32() as u8);
        sig_vec.try_into().unwrap()
    }

    /// Deserialize the signature
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

#[cfg(test)]
mod tests {
    use tiny_keccak::Hasher;

    use super::{KeyPair, PubKey, SecpSignature};

    #[test]
    fn test_pubkey_roundtrip() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let pubkey_bytes = keypair.pubkey().bytes();
        assert_eq!(
            pubkey_bytes,
            PubKey::from_slice(&pubkey_bytes).unwrap().bytes()
        );
        let pubkey_compressed_bytes = keypair.pubkey().bytes_compressed();
        assert_eq!(
            pubkey_bytes,
            PubKey::from_slice(&pubkey_compressed_bytes)
                .unwrap()
                .bytes()
        );
    }

    #[test]
    fn test_eth_address() {
        let mut privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let mut hasher = tiny_keccak::Keccak::v256();
        // pubkey() returns 65 bytes, ignore first one
        hasher.update(&keypair.pubkey().bytes()[1..]);
        let mut output = [0u8; 32];
        hasher.finalize(&mut output);

        let generated_eth_address = output[12..].to_vec();

        let expected_eth_address = hex::decode("ff7F1B7DbaaF35259dDa7cb42564CB7507C1D88d").unwrap();
        assert_eq!(generated_eth_address, expected_eth_address);
    }

    #[test]
    fn test_verify() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        assert!(keypair.pubkey().verify(msg, &signature).is_ok());
        assert!(keypair.pubkey().verify(b"bye world", &signature).is_err());
    }

    #[test]
    fn test_recovery() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        let recovered_key = signature.recover_pubkey(msg).unwrap();

        assert!(keypair.pubkey().bytes() == recovered_key.bytes());
    }

    #[test]
    fn test_signature_serde() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign(msg);

        let ser = signature.serialize();
        let deser = SecpSignature::deserialize(&ser);
        assert_eq!(signature, deser.unwrap());
    }
}
