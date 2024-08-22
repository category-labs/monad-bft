use crate::{
    eth_json_types::{deserialize_fixed_data, serialize_result, EthHash},
    jsonrpc::JsonRpcError,
};
use alloy_primitives::{
    aliases::{B256, U256, U64, U8},
    Address,
};
use alloy_rlp::Decodable;
use alloy_rlp::RlpDecodable;
use bytes::Bytes;
use monad_blockdb::EthTxKey;
use monad_blockdb_utils::BlockDbEnv;
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use reth_rpc_types::Transaction;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, trace};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallFrame {
    #[serde(rename = "type")]
    pub typ: U8,
    pub flags: U64,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: U64,
    pub gas_used: U64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: U8,
    pub depth: U64,
}

impl Decodable for CallFrame {
    fn decode(rlp_buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let typ: U8 = U8::decode(rlp_buf)?;
        let flags: U64 = U64::decode(rlp_buf)?;
        let from: Address = Address::decode(rlp_buf)?;

        // Decode the `to` field, handling the case where it's `None`.
        let to: Option<Address> = {
            let first_byte = rlp_buf.get(0).ok_or(alloy_rlp::Error::InputTooShort)?;
            if *first_byte == 0x80 {
                // If the first byte is 0x80, it represents an empty value (None for the Address).
                *rlp_buf = &rlp_buf[1..]; // Advance the buffer
                None
            } else {
                // Otherwise, decode it as a normal Address.
                Some(Address::decode(rlp_buf)?)
            }
        };

        let value: U256 = U256::decode(rlp_buf)?;
        let gas: U64 = U64::decode(rlp_buf)?;
        let gas_used: U64 = U64::decode(rlp_buf)?;
        let input: Bytes = Bytes::decode(rlp_buf)?;
        let output: Bytes = Bytes::decode(rlp_buf)?;
        let status: U8 = U8::decode(rlp_buf)?;
        let depth: U64 = U64::decode(rlp_buf)?;

        Ok(Self {
            typ,
            flags,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            depth,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, RlpDecodable)]
pub struct CallFrames {
    pub call_frames: Vec<CallFrame>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TracerCfg {
    pub only_top_call: bool,
}

#[derive(Deserialize, Debug, Default)]
pub struct TracerObject {
    #[serde(default)]
    tracer: Tracer,
    tracerCfg: Option<TracerCfg>,
    timeout: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub enum Tracer {
    #[default]
    #[serde(rename = "callTracer")]
    CallTracer,
    #[serde(rename = "prestateTracer")]
    PreStateTracer,
}

#[derive(Deserialize, Debug)]
struct MonadDebugTraceTransactionParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
    #[serde(default)]
    tracer: TracerObject,
}

#[allow(non_snake_case)]
pub async fn monad_debugTraceTransaction(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_debugTraceTransaction: {params:?}");

    let p: MonadDebugTraceTransactionParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let tx_hash = p.tx_hash.0;
    let key = EthTxKey(B256::new(tx_hash));
    let Some(result) = blockdb_env.get_txn(key).await else {
        return serialize_result(None::<Transaction>);
    };
    let block_key = result.block_hash;
    let block = blockdb_env
        .get_block_by_hash(block_key)
        .await
        .expect("txn was found so its block should exist");
    let block_num = block.block.number;

    match triedb_env.get_call_frame(tx_hash, block_num).await {
        TriedbResult::Null => return serialize_result(None::<CallFrame>),
        TriedbResult::CallFrame(rlp_call_frames) => {
            let mut rlp_buf = rlp_call_frames.as_slice();
            let call_frames = CallFrames::decode(&mut rlp_buf)
                .map_err(|_| JsonRpcError::custom("Rlp Decode error".to_string()))?;

            return serialize_result(call_frames);
        }
        _ => return Err(JsonRpcError::custom("Not matched".to_string())),
    };
}
