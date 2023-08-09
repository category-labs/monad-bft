use secp256k1::Secp256k1;
use sha2::Digest;
use zeroize::Zeroize;

#[derive(Copy, Clone)]
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

impl std::hash::Hash for PubKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let slice = unsafe { std::mem::transmute::<Self, [u8; 64]>(*self) };
        slice.hash(state)
    }
}

fn msg_hash(msg: &[u8]) -> secp256k1::Message {
    let mut hasher = sha2::Sha256::new();
    hasher.update(msg);
    let hash = hasher.finalize();

    secp256k1::Message::from_slice(&hash).expect("32 bytes")
}

impl KeyPair {
    pub fn from_bytes(mut secret: impl AsMut<[u8]>) -> Result<Self, Error> {
        let secret = secret.as_mut();
        let keypair = secp256k1::KeyPair::from_seckey_slice(secp256k1::SECP256K1, secret)
            .map(Self)
            .map_err(Error);
        secret.zeroize();
        keypair
    }

    #[cfg(feature = "libp2p-identity")]
    /// Special implementation for creating both a (monad_crypto::KeyPair, lib2p::KeyPair)
    /// TODO Once we've unified those, hopefully we can deprecate this
    pub fn libp2p_from_bytes(
        mut secret: impl AsMut<[u8]>,
    ) -> Result<(Self, libp2p_identity::secp256k1::Keypair), Error> {
        let secret = secret.as_mut();
        let monad_keypair = {
            let secret = &*secret; // explicitly use immutable reference
            secp256k1::KeyPair::from_seckey_slice(secp256k1::SECP256K1, secret)
                .map(Self)
                .map_err(Error)?
        };
        let libp2p_keypair = libp2p_identity::secp256k1::SecretKey::from_bytes(secret)
            .expect("monad_keypair parse succeeded, libp2p parse shouldn't fail")
            .into();

        Ok((monad_keypair, libp2p_keypair))
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

    pub fn bytes(&self) -> Vec<u8> {
        self.0.serialize_uncompressed().to_vec()
    }

    fn bytes_compressed(&self) -> Vec<u8> {
        self.0.serialize().to_vec()
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

#[cfg(feature = "libp2p-identity")]
#[derive(Debug)]
pub enum PeerIdError {
    Multihash(multihash::Error),
    Decoding(libp2p_identity::DecodingError),
    UnsupportedSignature,
}
#[cfg(feature = "libp2p-identity")]
impl From<multihash::Error> for PeerIdError {
    fn from(err: multihash::Error) -> Self {
        Self::Multihash(err)
    }
}
#[cfg(feature = "libp2p-identity")]
impl From<libp2p_identity::DecodingError> for PeerIdError {
    fn from(err: libp2p_identity::DecodingError) -> Self {
        Self::Decoding(err)
    }
}

#[cfg(feature = "libp2p-identity")]
impl TryFrom<libp2p_identity::PeerId> for PubKey {
    type Error = PeerIdError;
    fn try_from(peer_id: libp2p_identity::PeerId) -> Result<Self, Self::Error> {
        let bytes = peer_id.to_bytes();
        let multihash = multihash::Multihash::from_bytes(&bytes)?;

        // code 0 == Identity
        if multihash.code() != 0 {
            return Err(multihash::Error::UnsupportedCode(multihash.code()).into());
        }

        let libp2p_pubkey_secp =
            libp2p_identity::PublicKey::from_protobuf_encoding(multihash.digest())?;
        let libp2p_pubkey = libp2p_pubkey_secp
            .into_secp256k1()
            .ok_or(PeerIdError::UnsupportedSignature)?;
        let pubkey = Self::from_slice(&libp2p_pubkey.encode_uncompressed()).expect(
            "monad_crypto::PubKeyfrom_slice(libp2p::PubKey::encode_uncompressed()) should not fail",
        );

        Ok(pubkey)
    }
}

#[cfg(feature = "libp2p-identity")]
impl From<&PubKey> for libp2p_identity::PeerId {
    fn from(pubkey: &PubKey) -> Self {
        let pubkey = libp2p_identity::secp256k1::PublicKey::decode(&pubkey.bytes_compressed())
            .expect("internal pubkey -> peer_id should never fail");

        #[allow(deprecated)] // can't find another way
        let pubkey = libp2p_identity::PublicKey::Secp256k1(pubkey);

        pubkey.to_peer_id()
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

    #[cfg(feature = "libp2p-identity")]
    #[test]
    // THIS MUST PASS!! don't comment this test out >:(
    fn test_pubkey_peerid_roundtrip() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let pubkey = keypair.pubkey();
        let peer_id: libp2p_identity::PeerId = (&pubkey).into();
        assert_eq!(pubkey, PubKey::try_from(peer_id).unwrap());
    }

    #[cfg(feature = "libp2p-identity")]
    #[test]
    // THIS MUST PASS!! don't comment this test out >:(
    fn test_keypair_pubkey_peerid_roundtrip() {
        let mut privkey: [u8; 32] = [127; 32];
        let (monad_keypair, libp2p_keypair) = KeyPair::libp2p_from_bytes(&mut privkey).unwrap();

        let monad_pubkey = monad_keypair.pubkey();
        let libp2p_pubkey = libp2p_identity::Keypair::from(libp2p_keypair).public();
        assert_eq!(
            monad_pubkey.bytes(),
            libp2p_pubkey
                .clone()
                .into_secp256k1()
                .unwrap()
                .encode_uncompressed()
        );

        let monad_peer_id: libp2p_identity::PeerId = (&monad_pubkey).into();
        let libp2p_peer_id: libp2p_identity::PeerId = libp2p_pubkey.into();

        assert_eq!(monad_peer_id, libp2p_peer_id);

        assert_eq!(monad_pubkey, PubKey::try_from(monad_peer_id).unwrap());
        assert_eq!(monad_pubkey, PubKey::try_from(libp2p_peer_id).unwrap());
    }
}
