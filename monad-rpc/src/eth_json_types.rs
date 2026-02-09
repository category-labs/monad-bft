// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::str::FromStr;

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, FixedBytes, LogData, U256};
use alloy_rpc_types::{Block, FeeHistory, Header, Log, Transaction, TransactionReceipt};
use monad_exec_events::BlockCommitState;
use monad_types::BlockId;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use tracing::debug;

use crate::{
    hex::{self, decode, decode_quantity, DecodeHexError},
    jsonrpc::JsonRpcError,
};

pub type EthAddress = FixedData<20>;
pub type EthHash = FixedData<32>;

#[derive(Debug, JsonSchema)]
pub struct MonadU256(#[schemars(with = "u128")] pub U256);

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

#[derive(Debug, Serialize, JsonSchema)]
pub struct MonadLog(#[schemars(schema_with = "schema_for_log")] pub Log<LogData>);

fn schema_for_log(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema_for_value!(Log::<LogData>::default())
        .schema
        .into()
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MonadTransaction(
    #[schemars(schema_with = "schema_for_transaction")] pub Transaction<TxEnvelope>,
);

fn schema_for_transaction(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let rpc_tx = r#"{"blockHash":"0x883f974b17ca7b28cb970798d1c80f4d4bb427473dc6d39b2a7fe24edc02902d","blockNumber":"0xe26e6d","hash":"0x0e07d8b53ed3d91314c80e53cf25bcde02084939395845cbb625b029d568135c","accessList":[],"transactionIndex":"0xad","type":"0x2","nonce":"0x16d","input":"0x5ae401dc00000000000000000000000000000000000000000000000000000000628ced5b000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000016000000000000000000000000000000000000000000000000000000000000000e442712a6700000000000000000000000000000000000000000000b3ff1489674e11c40000000000000000000000000000000000000000000000000000004a6ed55bbcc18000000000000000000000000000000000000000000000000000000000000000800000000000000000000000003cf412d970474804623bb4e3a42de13f9bca54360000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc20000000000000000000000003a75941763f31c930b19c041b709742b0b31ebb600000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000412210e8a00000000000000000000000000000000000000000000000000000000","r":"0x7f2153019a74025d83a73effdd91503ceecefac7e35dd933adc1901c875539aa","s":"0x334ab2f714796d13c825fddf12aad01438db3a8152b2fe3ef7827707c25ecab3","chainId":"0x1","v":"0x0","gas":"0x46a02","maxPriorityFeePerGas":"0x59682f00","from":"0x3cf412d970474804623bb4e3a42de13f9bca5436","to":"0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45","maxFeePerGas":"0x7fc1a20a8","value":"0x4a6ed55bbcc180","gasPrice":"0x50101df3a"}"#;
    let tx = serde_json::from_str::<Transaction<TxEnvelope>>(rpc_tx).unwrap();
    schemars::schema_for_value!(tx).schema.into()
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MonadTransactionReceipt(
    #[schemars(schema_with = "schema_for_transaction_receipt")] pub TransactionReceipt,
);

fn schema_for_transaction_receipt(
    _: &mut schemars::gen::SchemaGenerator,
) -> schemars::schema::Schema {
    let json_str = r#"{"transactionHash":"0x21f6554c28453a01e7276c1db2fc1695bb512b170818bfa98fa8136433100616","blockHash":"0x4acbdefb861ef4adedb135ca52865f6743451bfbfa35db78076f881a40401a5e","blockNumber":"0x129f4b9","logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000200000000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000800000000000000000000000000000000004000000000000000000800000000100000020000000000000000000080000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000010000000000000000000000000000","gasUsed":"0xbde1","contractAddress":null,"cumulativeGasUsed":"0xa42aec","transactionIndex":"0x7f","from":"0x9a53bfba35269414f3b2d20b52ca01b15932c7b2","to":"0xdac17f958d2ee523a2206206994597c13d831ec7","type":"0x2","effectiveGasPrice":"0xfb0f6e8c9","logs":[{"blockHash":"0x4acbdefb861ef4adedb135ca52865f6743451bfbfa35db78076f881a40401a5e","address":"0xdac17f958d2ee523a2206206994597c13d831ec7","logIndex":"0x118","data":"0x00000000000000000000000000000000000000000052b7d2dcc80cd2e4000000","removed":false,"topics":["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925","0x0000000000000000000000009a53bfba35269414f3b2d20b52ca01b15932c7b2","0x00000000000000000000000039e5dbb9d2fead31234d7c647d6ce77d85826f76"],"blockNumber":"0x129f4b9","transactionIndex":"0x7f","transactionHash":"0x21f6554c28453a01e7276c1db2fc1695bb512b170818bfa98fa8136433100616"}],"status":"0x1"}"#;
    let receipt: TransactionReceipt = serde_json::from_str(json_str).unwrap();
    schemars::schema_for_value!(receipt).schema.into()
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MonadBlock(
    #[schemars(schema_with = "schema_for_block")] pub Block<Transaction<TxEnvelope>, Header>,
);

fn schema_for_block(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema_for_value!(Block::<Transaction<TxEnvelope>, Header>::default())
        .schema
        .into()
}

#[derive(Serialize, Debug, JsonSchema)]
pub struct MonadFeeHistory(#[schemars(schema_with = "schema_for_fee_history")] pub FeeHistory);

fn schema_for_fee_history(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema_for_value!(FeeHistory::default())
        .schema
        .into()
}

// https://ethereum.org/developers/docs/apis/json-rpc#unformatted-data-encoding
#[derive(Debug, PartialEq, Eq, JsonSchema)]
#[schemars(with = "String")]
pub struct UnformattedData(pub Vec<u8>);

impl UnformattedData {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromStr for UnformattedData {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode(s).map(UnformattedData)
    }
}
impl From<alloy_primitives::Bytes> for UnformattedData {
    fn from(data: alloy_primitives::Bytes) -> Self {
        UnformattedData(data.to_vec())
    }
}

impl Serialize for UnformattedData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(&self.0))
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
#[derive(Copy, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[schemars(with = "String")]
pub struct Quantity(pub u64);

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

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
pub struct FixedData<const N: usize>(#[schemars(with = "String")] pub [u8; N]);

impl<const N: usize> std::fmt::Display for FixedData<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl<const N: usize> FromStr for FixedData<N> {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode(s).map(|d| match d.try_into() {
            Ok(a) => Ok(FixedData(a)),
            Err(_) => Err(DecodeHexError::InvalidLen),
        })?
    }
}

impl<const N: usize> Serialize for FixedData<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(&self.0))
    }
}

impl From<FixedBytes<32>> for FixedData<32> {
    fn from(bytes: FixedBytes<32>) -> Self {
        Self(bytes.0)
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BlockTags {
    Number(Quantity), // voted or finalized
    #[default]
    Latest, // voted
    Safe,             // voted
    Finalized,        // finalized
}

impl schemars::JsonSchema for BlockTags {
    fn schema_name() -> String {
        "BlockTags".to_string()
    }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        block_tags_schema(gen)
    }
}

fn block_tags_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    use schemars::schema::*;

    let quantity_schema = gen.subschema_for::<Quantity>();

    let string_enum_schema = SchemaObject {
        instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::String))),
        enum_values: Some(vec![
            "latest".into(),
            "safe".into(),
            "finalized".into(),
            "pending".into(),
        ]),
        ..Default::default()
    };

    Schema::Object(SchemaObject {
        subschemas: Some(Box::new(SubschemaValidation {
            any_of: Some(vec![quantity_schema, Schema::Object(string_enum_schema)]),
            ..Default::default()
        })),
        ..Default::default()
    })
}

impl FromStr for BlockTags {
    type Err = DecodeHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "earliest" => Ok(Self::Number(Quantity(0))),
            "latest" => Ok(Self::Latest),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            "pending" => Ok(Self::Latest),
            _ => decode_quantity(s).map(|q| Self::Number(Quantity(q))),
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

#[derive(Debug, Clone, PartialEq, Eq, schemars::JsonSchema)]
#[serde(untagged)]
pub enum BlockTagOrHash {
    BlockTags(BlockTags),
    Hash(EthHash),
}

// EIP-1898 allows users to pass a block tag or a block hash either as a string or as an object.
// https://eips.ethereum.org/EIPS/eip-1898
#[derive(Deserialize)]
#[serde(untagged)]
enum BlockTagOrHashHelper {
    BlockTags(BlockTags),
    Hash(EthHash),
    WithBlockTags {
        #[serde(rename = "blockNumber")]
        tags: BlockTags,
    },

    WithHash {
        #[serde(rename = "blockHash")]
        hash: EthHash,
        #[serde(default, rename = "camelCase")]
        #[allow(dead_code)]
        require_canonical: bool,
    },
}

impl<'de> Deserialize<'de> for BlockTagOrHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match BlockTagOrHashHelper::deserialize(deserializer)? {
            BlockTagOrHashHelper::BlockTags(tags) => Ok(BlockTagOrHash::BlockTags(tags)),
            BlockTagOrHashHelper::Hash(hash) => Ok(BlockTagOrHash::Hash(hash)),
            BlockTagOrHashHelper::WithBlockTags { tags } => Ok(BlockTagOrHash::BlockTags(tags)),
            BlockTagOrHashHelper::WithHash { hash, .. } => Ok(BlockTagOrHash::Hash(hash)),
        }
    }
}

impl Default for BlockTagOrHash {
    fn default() -> Self {
        BlockTagOrHash::BlockTags(BlockTags::Latest)
    }
}

#[derive(Deserialize)]
pub struct EthSubscribeRequest {
    pub kind: SubscriptionKind,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum SubscriptionKind {
    // Equivalent to Geth's "newHeads" subscription.
    // https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub#newheads
    NewHeads,
    // Equivalent to Geth's "logs" subscription.
    // https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub#logs
    Logs,
    // Subscribes to all heads with their corresponding commit state.
    MonadNewHeads,
    // Subscribes to all logs with their corresponding commit state.
    MonadLogs,
    // Subscribes to storage slot changes for specified address/slots.
    MonadStorageChanges,
}

pub const MAX_STORAGE_SLOTS_PER_SUBSCRIPTION: usize = 8;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageChangesFilter {
    pub address: alloy_primitives::Address,
    pub storage_slots: Vec<alloy_primitives::B256>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageChangeNotification {
    pub address: alloy_primitives::Address,
    pub storage_slot: alloy_primitives::B256,
    pub old_value: alloy_primitives::B256,
    pub new_value: alloy_primitives::B256,
    pub transaction_hash: alloy_primitives::B256,
    pub block_number: alloy_primitives::U64,
    pub block_hash: alloy_primitives::B256,
    pub commit_state: BlockCommitState,
}

#[derive(Deserialize)]
pub struct EthUnsubscribeRequest {
    pub id: FixedData<16>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EthSubscribeResult<'a> {
    pub subscription: FixedData<16>,
    #[serde(borrow)]
    pub result: &'a RawValue,
}

impl<'a> EthSubscribeResult<'a> {
    pub fn new(id: FixedData<16>, result: &'a RawValue) -> Self {
        Self {
            subscription: id,
            result,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubscriptionResult {
    // NewHeads and Logs are Geth results that return finalized block details.
    NewHeads(alloy_rpc_types::eth::Header),
    Logs(alloy_rpc_types::eth::Log),

    // Returns all headers with their corresponding commit state.
    MonadNewHeads(MonadNotification<alloy_rpc_types::eth::Header>),
    // Returns all logs with their corresponding commit state.
    MonadLogs(MonadNotification<alloy_rpc_types::eth::Log>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonadNotification<T> {
    pub block_id: BlockId,
    pub commit_state: BlockCommitState,
    #[serde(flatten)]
    pub data: T,
}

impl<T> MonadNotification<T> {
    pub(crate) fn map<U>(self, f: impl FnOnce(T) -> U) -> MonadNotification<U> {
        let Self {
            block_id,
            commit_state,
            data,
        } = self;

        MonadNotification {
            block_id,
            commit_state,
            data: f(data),
        }
    }
}

pub fn serialize_result<T: Serialize>(value: T) -> Result<Box<RawValue>, JsonRpcError> {
    serde_json::value::to_raw_value(&value).map_err(|e| {
        debug!("blockdb serialize error {:?}", e);
        JsonRpcError::internal_error(format!("serialization error: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
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
