use std::str::FromStr;

use alloy_primitives::FixedBytes;
use reth_primitives::{Address, Bloom, Bytes, U256};
use reth_rpc_types::{
    Block, BlockTransactions, FeeHistory, Header, Log, Transaction, TransactionReceipt,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use tracing::debug;

use crate::{
    hex::{self, decode, decode_quantity, DecodeHexError},
    jsonrpc::JsonRpcError,
};

pub type EthAddress = FixedData<20>;
pub type EthHash = FixedData<32>;

#[derive(Debug)]
pub struct MonadU256(pub U256);

impl Serialize for MonadU256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("0x{:x}", self.0))
    }
}

impl<'de> Deserialize<'de> for MonadU256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        let u = U256::from_str(&buf)
            .map_err(|e| serde::de::Error::custom(format!("U256 parse failed: {e:?}")))?;
        Ok(Self(u))
    }
}

#[derive(Debug, Serialize)]
pub struct MonadLog(pub Log);

impl schemars::JsonSchema for MonadLog {
    fn schema_name() -> String {
        "MonadLog".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(Log::default());
        schema.schema.into()
    }
}

#[derive(Debug, Serialize)]
pub struct MonadTransaction(pub Transaction);

impl schemars::JsonSchema for MonadTransaction {
    fn schema_name() -> String {
        "MonadTransaction".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(Transaction::default());
        schema.schema.into()
    }
}

#[derive(Debug, Serialize)]
pub struct MonadTransactionReceipt(pub TransactionReceipt);

impl schemars::JsonSchema for MonadTransactionReceipt {
    fn schema_name() -> String {
        "MonadTransactionReceipt".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(TransactionReceipt::default());
        schema.schema.into()
    }
}

#[derive(Debug, Serialize)]
pub struct MonadBlock(pub Block);

impl schemars::JsonSchema for MonadBlock {
    fn schema_name() -> String {
        "MonadBlock".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(Block {
            header: Header {
                hash: Some(FixedBytes::<32>::default()),
                uncles_hash: FixedBytes::<32>::default(),
                miner: Address::default(),
                difficulty: U256::default(),
                number: None,
                gas_limit: U256::default(),
                gas_used: U256::default(),
                timestamp: U256::default(),
                excess_blob_gas: None,
                base_fee_per_gas: None,
                extra_data: Bytes::default(),
                mix_hash: None,
                nonce: None,
                blob_gas_used: None,
                parent_beacon_block_root: None,
                withdrawals_root: None,
                parent_hash: FixedBytes::<32>::default(),
                state_root: FixedBytes::<32>::default(),
                transactions_root: FixedBytes::<32>::default(),
                receipts_root: FixedBytes::<32>::default(),
                logs_bloom: Bloom::default(),
            },
            total_difficulty: None,
            withdrawals: None,
            size: None,
            other: Default::default(),
            uncles: Vec::new(),
            transactions: BlockTransactions::Full(Vec::new()),
        });
        schema.schema.into()
    }
}

impl schemars::JsonSchema for EthAddress {
    fn schema_name() -> String {
        "EthAddress".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("hex".to_owned()),
            ..Default::default()
        }
        .into()
    }
}

impl schemars::JsonSchema for MonadU256 {
    fn schema_name() -> String {
        "MonadU256".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            ..Default::default()
        }
        .into()
    }
}

#[derive(Serialize, Debug)]
pub struct MonadFeeHistory(pub FeeHistory);

impl schemars::JsonSchema for MonadFeeHistory {
    fn schema_name() -> String {
        "FeeHistory".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(FeeHistory::default());
        schema.schema.into()
    }
}

impl schemars::JsonSchema for EthHash {
    fn schema_name() -> String {
        "EthHash".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("hex".to_owned()),
            ..Default::default()
        }
        .into()
    }
}

// https://ethereum.org/developers/docs/apis/json-rpc#unformatted-data-encoding
#[derive(Debug, PartialEq, Eq)]
pub struct UnformattedData(pub Vec<u8>);

impl schemars::JsonSchema for UnformattedData {
    fn schema_name() -> String {
        "UnformattedData".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("hex".to_owned()),
            ..Default::default()
        }
        .into()
    }
}

impl FromStr for UnformattedData {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode(s).map(UnformattedData)
    }
}

impl<'de> Deserialize<'de> for UnformattedData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        UnformattedData::from_str(&buf)
            .map_err(|e| serde::de::Error::custom(format!("UnformattedData parse failed: {e:?}")))
    }
}

// https://ethereum.org/developers/docs/apis/json-rpc#hex-encoding
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Quantity(pub u64);

impl schemars::JsonSchema for Quantity {
    fn schema_name() -> String {
        "Quantity".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("hex".to_owned()),
            ..Default::default()
        }
        .into()
    }
}

impl Serialize for Quantity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("0x{:x}", self.0))
    }
}

impl<'de> Deserialize<'de> for Quantity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum QuantityOrString {
            Num(u64),
            Str(String),
        }

        match QuantityOrString::deserialize(deserializer)? {
            QuantityOrString::Num(n) => Ok(Quantity(n)),
            QuantityOrString::Str(s) => {
                if let Some(hex) = s.strip_prefix("0x") {
                    u64::from_str_radix(hex, 16)
                        .map(Quantity)
                        .map_err(serde::de::Error::custom)
                } else {
                    s.parse().map(Quantity).map_err(serde::de::Error::custom)
                }
            }
        }
    }
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

impl From<U256> for FixedData<32> {
    fn from(u: U256) -> Self {
        let bytes: [u8; 32] = u.to_be_bytes();
        FixedData(bytes)
    }
}

impl From<Address> for FixedData<20> {
    fn from(addr: Address) -> Self {
        FixedData(*addr.0)
    }
}

impl From<FixedData<32>> for monad_types::BlockId {
    fn from(value: FixedData<32>) -> Self {
        monad_types::BlockId(monad_types::Hash(value.0))
    }
}

impl<'de, const N: usize> Deserialize<'de> for FixedData<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        FixedData::from_str(&buf)
            .map_err(|e| serde::de::Error::custom(format!("FixedData parse failed: {e:?}")))
    }
}

impl Serialize for EthHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(&self.0))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, schemars::JsonSchema)]
#[serde(untagged)]
pub enum BlockTags {
    Number(Quantity),
    Latest,
}

impl Default for BlockTags {
    fn default() -> Self {
        BlockTags::Latest
    }
}

impl FromStr for BlockTags {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "earliest" => Ok(Self::Latest),
            "latest" => Ok(Self::Latest),
            "safe" => Ok(Self::Latest),
            "finalized" => Ok(Self::Latest),
            "pending" => Ok(Self::Latest),
            _ => decode_quantity(s).map(|q| Self::Number(Quantity(q))),
        }
    }
}

impl From<BlockTags> for crate::triedb::BlockTags {
    fn from(value: BlockTags) -> Self {
        match value {
            BlockTags::Number(n) => crate::triedb::BlockTags::Number(n.0),
            BlockTags::Latest => crate::triedb::BlockTags::Latest,
        }
    }
}

impl<'de> Deserialize<'de> for BlockTags {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        BlockTags::from_str(&buf)
            .map_err(|e| serde::de::Error::custom(format!("BlockTags parse failed: {e:?}")))
    }
}

pub fn serialize_result<T: Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|e| {
        debug!("blockdb serialize error {:?}", e);
        JsonRpcError::internal_error(format!("serialization error: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use reth_primitives::U256;
    use serde::Deserialize;
    use serde_json::json;

    use super::{BlockTags, FixedData, Quantity, UnformattedData};

    #[derive(Deserialize, Debug)]
    struct OneDataParam {
        a: UnformattedData,
    }

    #[derive(Deserialize, Debug)]
    struct TwoDataParam {
        a: UnformattedData,
        b: UnformattedData,
    }

    #[derive(Deserialize, Debug)]
    struct OneQuantity {
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
        a: BlockTags,
    }

    #[test]
    fn test_block_enums() {
        let x: OneBlockParam = serde_json::from_value(json!(["latest"])).unwrap();
        assert_eq!(BlockTags::Latest, x.a);

        let x: OneBlockParam = serde_json::from_value(json!(["0xffacb0"])).unwrap();
        assert_eq!(BlockTags::Number(Quantity(16755888)), x.a);
    }

    #[derive(Deserialize, Debug)]
    struct OneFixedAddr {
        a: FixedData<20>,
    }

    #[derive(Deserialize, Debug)]
    struct OneFixedHash {
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

        let u = U256::from(1024);
        let fixed_data = FixedData::from(u);
        let mut expected_bytes = [0u8; 32];
        expected_bytes[30] = 0x04;
        assert_eq!(fixed_data.0, expected_bytes);
    }
}
