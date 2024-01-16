use monad_bls::BlsPubKey;
use monad_secp::PubKey;
use serde::{de::Error, Deserialize, Deserializer};

pub fn deserialize_secp256k1_pubkey<'de, D>(deserializer: D) -> Result<PubKey, D::Error>
where
    D: Deserializer<'de>,
{
    let mut hex_str: String = Deserialize::deserialize(deserializer)?;

    if let Some(("", hex_str_suffix)) = hex_str.split_once("0x") {
        hex_str = hex_str_suffix.to_owned();
    }

    let key: Vec<u8> = hex::decode(hex_str).map_err(D::Error::custom)?;

    PubKey::from_slice(&key).map_err(D::Error::custom)
}

pub fn deserialize_bls12_381_pubkey<'de, D>(deserializer: D) -> Result<BlsPubKey, D::Error>
where
    D: Deserializer<'de>,
{
    let mut hex_str: String = Deserialize::deserialize(deserializer)?;

    if let Some(("", hex_str_suffix)) = hex_str.split_once("0x") {
        hex_str = hex_str_suffix.to_owned();
    }

    let key: Vec<u8> = hex::decode(hex_str).map_err(D::Error::custom)?;

    BlsPubKey::uncompress(&key).map_err(D::Error::custom)
}
