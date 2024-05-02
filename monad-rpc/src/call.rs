use std::path::Path;

use alloy_primitives::{Address, Uint, U256, U64, U8};
use log::debug;
use monad_blockdb::BlockTagKey;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{deserialize_block_tags, BlockTags},
    hex,
    jsonrpc::JsonRpcError,
};

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallRequest {
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub gas: Option<U256>,
    pub gas_price: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub max_fee_per_gas: Option<U256>,
    pub value: Option<U256>,
    #[serde(default, flatten)]
    pub input: CallInput,
    pub nonce: Option<U64>,
    pub chain_id: Option<U64>,
    pub access_list: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_blob_gas: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_versioned_hashes: Option<Vec<U256>>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub transaction_type: Option<U8>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<alloy_primitives::Bytes>,
}

#[derive(Debug, Deserialize)]
struct MonadEthCallParams {
    transaction: CallRequest,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

pub async fn monad_eth_call(
    blockdb_env: &BlockDbEnv,
    triedb_path: &Path,
    execution_ledger_path: &Path,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let params: MonadEthCallParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let Some(block_header) = blockdb_env
        .get_block_by_tag(BlockTags::Default(BlockTagKey::Latest))
        .await
    else {
        debug!("blockdb did not have latest block header");
        return Err(JsonRpcError::internal_error());
    };

    // FIXME: add support for other types of transactions
    let txn = reth_primitives::transaction::Transaction::Legacy(reth_primitives::TxLegacy {
        chain_id: Some(1),
        nonce: params
            .transaction
            .nonce
            .unwrap_or_default()
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?,
        gas_price: params
            .transaction
            .gas_price
            .unwrap_or_default()
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?,
        gas_limit: params
            .transaction
            .gas
            .unwrap_or(Uint::from(i64::MAX))
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?,
        to: reth_primitives::TransactionKind::Call(params.transaction.to.unwrap_or_default()),
        value: reth_primitives::TxValue::from(params.transaction.value.unwrap_or_default()),
        input: params.transaction.input.data.unwrap_or_default(),
    });
    let sender = params.transaction.from.unwrap_or_default();
    let block_number = block_header.block.header.number;

    match monad_cxx::eth_call(
        txn,
        block_header.block.header,
        sender,
        block_number,
        triedb_path,
        execution_ledger_path,
    ) {
        // TODO: make this match RPC spec
        Ok(output_data) => Ok(json!({
            "output_data": hex::encode(&output_data)
        })),
        Err(status_code) => Ok(json!({
            "status_code": status_code as i64,
        })),
    }
}

#[cfg(test)]
mod tests {
    // use actix_test::*;
    use actix_web::test;
    use serde_json::json;

    use crate::{jsonrpc, tests::init_server};

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call_sha256_precompile() {
        let (app, monad) = init_server().await;
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": "0x0000000000000000000000000000000000000002",
                    "data": "0x68656c6c6f" // hex for "hello"
                },
                "latest"
            ],
            "id": 1
        });

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call() {
        let (app, monad) = init_server().await;
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "gasPrice": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            },
            "latest"
            ],
            "id": 1
        });

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }
}
