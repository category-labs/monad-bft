use hex;
use serde::{de::Error, Deserialize, Deserializer};

pub fn deserialize_bytes_from_hex_string<'d, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'d>,
{
    let hex_string: &str = Deserialize::deserialize(deserializer)?;
    hex::decode(hex_string).map_err(D::Error::custom)
}
