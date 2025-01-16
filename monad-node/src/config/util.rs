use monad_secp::PubKey;
use serde::{de::Error, Deserialize, Deserializer, Serializer};

pub fn deserialize_secp256k1_pubkey<'de, D>(deserializer: D) -> Result<PubKey, D::Error>
where
    D: Deserializer<'de>,
{
    let mut hex_str: String = Deserialize::deserialize(deserializer)?;

    if let Some(hex_str_suffix) = hex_str.strip_prefix("0x") {
        hex_str = hex_str_suffix.to_owned();
    }

    let key: Vec<u8> = hex::decode(hex_str).map_err(D::Error::custom)?;

    PubKey::from_slice(&key).map_err(D::Error::custom)
}

pub fn serialize_secp256k1_pubkey<S: Serializer>(
    pk: &PubKey,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let hex_str = "0x".to_string() + &hex::encode(pk.bytes_compressed());
    serializer.serialize_str(&hex_str)
}
