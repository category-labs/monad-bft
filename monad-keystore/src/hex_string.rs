use hex;
use serde::{de::Error, Deserialize, Deserializer, Serializer};

pub fn serialize_bytes_to_hex_string<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let hex_string = hex::encode(bytes);
    serializer.serialize_str(&hex_string)
}

pub fn deserialize_bytes_from_hex_string<'d, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'d>,
{
    let hex_string: &str = Deserialize::deserialize(deserializer)?;
    hex::decode(hex_string).map_err(D::Error::custom)
}
