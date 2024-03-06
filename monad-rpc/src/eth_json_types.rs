use std::str::FromStr;

use log::debug;
use monad_blockdb::BlockTagKey;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    hex::{decode, decode_quantity, DecodeHexError},
    jsonrpc::JsonRpcError,
};

pub type EthAddress = FixedData<20>;
pub type EthHash = FixedData<32>;
pub type EthStorageKey = FixedData<32>;

// https://ethereum.org/developers/docs/apis/json-rpc#unformatted-data-encoding
#[derive(Debug, PartialEq, Eq)]
pub struct UnformattedData(pub Vec<u8>);

impl FromStr for UnformattedData {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode(s).map(UnformattedData)
    }
}

pub fn deserialize_unformatted_data<'de, D>(deserializer: D) -> Result<UnformattedData, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    UnformattedData::from_str(&buf)
        .map_err(|e| serde::de::Error::custom(format!("UnformattedData parse failed: {e:?}")))
}

// https://ethereum.org/developers/docs/apis/json-rpc#hex-encoding
#[derive(Debug, PartialEq, Eq)]
pub struct Quantity(pub u64);

impl FromStr for Quantity {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode_quantity(s).map(Quantity)
    }
}

pub fn deserialize_quantity<'de, D>(deserializer: D) -> Result<Quantity, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    Quantity::from_str(&buf)
        .map_err(|e| serde::de::Error::custom(format!("Quantity parse failed: {e:?}")))
}

#[derive(Debug, PartialEq, Eq)]
pub struct FixedData<const N: usize>(pub [u8; N]);

impl<const N: usize> FromStr for FixedData<N> {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode(s).map(|d| match d.try_into() {
            Ok(a) => Ok(FixedData(a)),
            Err(_) => Err(DecodeHexError::InvalidLen),
        })?
    }
}

pub fn deserialize_fixed_data<'de, D, const N: usize>(
    deserializer: D,
) -> Result<FixedData<N>, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    FixedData::from_str(&buf)
        .map_err(|e| serde::de::Error::custom(format!("FixedData parse failed: {e:?}")))
}

#[derive(Debug, PartialEq, Eq)]
pub enum BlockTags {
    Number(Quantity),
    Default(BlockTagKey),
}

impl FromStr for BlockTags {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "earliest" => Ok(Self::Default(BlockTagKey::Latest)),
            "latest" => Ok(Self::Default(BlockTagKey::Latest)),
            "safe" => Ok(Self::Default(BlockTagKey::Latest)),
            "finalized" => Ok(Self::Default(BlockTagKey::Finalized)),
            "pending" => Ok(Self::Default(BlockTagKey::Latest)),
            _ => decode_quantity(s).map(|q| Self::Number(Quantity(q))),
        }
    }
}

pub fn deserialize_block_tags<'de, D>(deserializer: D) -> Result<BlockTags, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    BlockTags::from_str(&buf)
        .map_err(|e| serde::de::Error::custom(format!("BlockTags parse failed: {e:?}")))
}

pub fn serialize_result<T: Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|e| {
        debug!("blockdb serialize error {:?}", e);
        JsonRpcError::internal_error()
    })
}

#[cfg(test)]
mod tests {
    use monad_blockdb::BlockTagKey;
    use serde::Deserialize;
    use serde_json::json;

    use super::{
        deserialize_block_tags, deserialize_fixed_data, deserialize_quantity,
        deserialize_unformatted_data, BlockTags, FixedData, Quantity, UnformattedData,
    };

    #[derive(Deserialize, Debug)]
    struct OneDataParam {
        #[serde(deserialize_with = "deserialize_unformatted_data")]
        a: UnformattedData,
    }

    #[derive(Deserialize, Debug)]
    struct TwoDataParam {
        #[serde(deserialize_with = "deserialize_unformatted_data")]
        a: UnformattedData,
        #[serde(deserialize_with = "deserialize_unformatted_data")]
        b: UnformattedData,
    }

    #[derive(Deserialize, Debug)]
    struct OneQuantity {
        #[serde(deserialize_with = "deserialize_quantity")]
        a: Quantity,
    }

    #[test]
    fn test_deser_one_param() {
        let x: OneDataParam = serde_json::from_value(json!(["0x0f00"])).unwrap();
        assert_eq!(x.a.0, vec![0x0f, 0x00]);

        match serde_json::from_value::<OneDataParam>(json!([])) {
            Ok(_) => panic!("empty list should fail to parse to one param"),
            Err(e) => e,
        };

        match serde_json::from_value::<OneDataParam>(json!(["42"])) {
            Ok(_) => panic!("missing 0x prefix for UnformattedData should fail"),
            Err(e) => e,
        };

        match serde_json::from_value::<OneDataParam>(json!(["0x42", "0x43"])) {
            Ok(_) => panic!("multiple params should fail for one param"),
            Err(e) => e,
        };

        match serde_json::from_value::<OneDataParam>(json!([0xff])) {
            Ok(_) => panic!("invalid type in param list should fail"),
            Err(e) => e,
        };

        match serde_json::from_value::<OneDataParam>(json!("0xff")) {
            Ok(_) => panic!("param is expected to be in a list"),
            Err(e) => e,
        };
    }

    #[test]
    fn test_deser_two_param() {
        let x: TwoDataParam = serde_json::from_value(json!(["0x0f00", "0xaf"])).unwrap();
        assert_eq!(x.a.0, vec![0x0f, 0x00]);
        assert_eq!(x.b.0, vec![0xaf]);

        match serde_json::from_value::<TwoDataParam>(json!([])) {
            Ok(_) => panic!("empty list should fail to parse to one param"),
            Err(e) => e,
        };

        match serde_json::from_value::<TwoDataParam>(json!(["42", "43"])) {
            Ok(_) => panic!("missing 0x prefix for UnformattedData should fail"),
            Err(e) => e,
        };

        match serde_json::from_value::<TwoDataParam>(json!(["0x42"])) {
            Ok(_) => panic!("fewer params should fail for two param"),
            Err(e) => e,
        };
    }

    #[test]
    fn test_deser_quantity() {
        let x: OneQuantity = serde_json::from_value(json!(["0x400"])).unwrap();
        assert_eq!(x.a.0, 1024);
    }

    #[derive(Deserialize, Debug)]
    struct OneBlockParam {
        #[serde(deserialize_with = "deserialize_block_tags")]
        a: BlockTags,
    }

    #[test]
    fn test_block_enums() {
        let x: OneBlockParam = serde_json::from_value(json!(["latest"])).unwrap();
        assert_eq!(BlockTags::Default(BlockTagKey::Latest), x.a);

        let x: OneBlockParam = serde_json::from_value(json!(["0xffacb0"])).unwrap();
        assert_eq!(BlockTags::Number(Quantity(16755888)), x.a);
    }

    #[derive(Deserialize, Debug)]
    struct OneFixedAddr {
        #[serde(deserialize_with = "deserialize_fixed_data")]
        a: FixedData<20>,
    }

    #[derive(Deserialize, Debug)]
    struct OneFixedHash {
        #[serde(deserialize_with = "deserialize_fixed_data")]
        a: FixedData<32>,
    }

    #[test]
    fn test_fixed_data() {
        let addr = json!(["0x407d73d8a49eeb85d32cf465507dd71d507100c1"]);
        let hash = json!(["0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238"]);

        let x: OneFixedAddr = serde_json::from_value(addr).unwrap();
        assert_eq!(x.a.0.len(), 20);
        assert_eq!(
            x.a.0,
            [
                0x40, 0x7d, 0x73, 0xd8, 0xa4, 0x9e, 0xeb, 0x85, 0xd3, 0x2c, 0xf4, 0x65, 0x50, 0x7d,
                0xd7, 0x1d, 0x50, 0x71, 0x00, 0xc1
            ]
        );

        match serde_json::from_value::<OneFixedAddr>(json!(["0x40"])) {
            Ok(_) => panic!("less than 20B should fail"),
            Err(e) => e,
        };

        match serde_json::from_value::<OneFixedAddr>(hash.clone()) {
            Ok(_) => panic!("more than 20B should fail"),
            Err(e) => e,
        };

        let x: OneFixedHash = serde_json::from_value(hash).unwrap();
        assert_eq!(x.a.0.len(), 32);
    }
}
