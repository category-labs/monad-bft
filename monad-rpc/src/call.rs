use std::{cmp::min, path::Path};

use alloy_primitives::{Address, Uint, U256, U64, U8};
use monad_blockdb_utils::BlockDbEnv;
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use reth_primitives::Block;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::debug;

use crate::{
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
    #[serde(flatten)]
    pub gas_price_details: GasPriceDetails,
    pub value: Option<U256>,
    #[serde(flatten)]
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

impl CallRequest {
    pub fn max_fee_per_gas(&self) -> Option<U256> {
        match self.gas_price_details {
            GasPriceDetails::Legacy { gas_price } => Some(gas_price),
            GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(max_fee_per_gas),
                ..
            } => Some(max_fee_per_gas),
            _ => None,
        }
    }

    pub fn fill_gas_prices(&mut self, base_fee: U256) -> Result<(), JsonRpcError> {
        match self.gas_price_details {
            GasPriceDetails::Legacy { .. } => {}
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                let max_fee_per_gas = match max_fee_per_gas {
                    Some(max_fee_per_gas) => {
                        if max_fee_per_gas < base_fee {
                            return Err(JsonRpcError::eth_call_error(
                                "max fee less than base".to_string(),
                                None,
                            ));
                        }

                        if max_priority_fee_per_gas.is_some()
                            && max_fee_per_gas < max_priority_fee_per_gas.unwrap_or_default()
                        {
                            return Err(JsonRpcError::eth_call_error(
                                "priority fee greater than max".to_string(),
                                None,
                            ));
                        }

                        min(
                            max_fee_per_gas,
                            base_fee
                                .checked_add(max_priority_fee_per_gas.unwrap_or_default())
                                .ok_or_else(|| {
                                    JsonRpcError::eth_call_error("tip too high".to_string(), None)
                                })?,
                        )
                    }
                    None => base_fee
                        .checked_add(max_priority_fee_per_gas.unwrap_or_default())
                        .ok_or_else(|| {
                            JsonRpcError::eth_call_error("tip too high".to_string(), None)
                        })?,
                };

                self.gas_price_details = GasPriceDetails::Eip1559 {
                    max_fee_per_gas: Some(max_fee_per_gas),
                    max_priority_fee_per_gas,
                };
            }
        };
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged, rename_all_fields = "camelCase")]
pub enum GasPriceDetails {
    Legacy {
        gas_price: U256,
    },
    Eip1559 {
        max_fee_per_gas: Option<U256>,
        max_priority_fee_per_gas: Option<U256>,
    },
}

impl Default for GasPriceDetails {
    fn default() -> Self {
        GasPriceDetails::Eip1559 {
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
        }
    }
}

/// Optimistically create a typed Ethereum transaction from a CallRequest based on provided fields.
/// TODO: add support for other transaction types.
impl TryFrom<CallRequest> for reth_primitives::transaction::Transaction {
    type Error = JsonRpcError;
    fn try_from(call_request: CallRequest) -> Result<Self, JsonRpcError> {
        match call_request {
            CallRequest {
                gas_price_details: GasPriceDetails::Legacy { gas_price },
                ..
            } => {
                // Legacy
                Ok(reth_primitives::transaction::Transaction::Legacy(
                    reth_primitives::TxLegacy {
                        chain_id: call_request
                            .chain_id
                            .map(|id| id.try_into())
                            .transpose()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        nonce: call_request
                            .nonce
                            .unwrap_or_default()
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        gas_price: gas_price
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        gas_limit: call_request
                            .gas
                            .unwrap_or(Uint::from(u64::MAX))
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        to: if let Some(to) = call_request.to {
                            reth_primitives::TransactionKind::Call(to)
                        } else {
                            reth_primitives::TransactionKind::Create
                        },
                        value: reth_primitives::TxValue::from(
                            call_request.value.unwrap_or_default(),
                        ),
                        input: call_request.input.data.unwrap_or_default(),
                    },
                ))
            }
            CallRequest {
                gas_price_details:
                    GasPriceDetails::Eip1559 {
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                    },
                ..
            } => {
                // EIP-1559
                Ok(reth_primitives::transaction::Transaction::Eip1559(
                    reth_primitives::TxEip1559 {
                        chain_id: call_request
                            .chain_id
                            .unwrap_or_default()
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        nonce: call_request
                            .nonce
                            .unwrap_or_default()
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        max_fee_per_gas: max_fee_per_gas
                            .unwrap_or_default()
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        max_priority_fee_per_gas: max_priority_fee_per_gas
                            .unwrap_or_default()
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        gas_limit: call_request
                            .gas
                            .unwrap_or(Uint::from(u64::MAX))
                            .try_into()
                            .map_err(|_| JsonRpcError::invalid_params())?,
                        access_list: reth_primitives::AccessList::default(),
                        to: if let Some(to) = call_request.to {
                            reth_primitives::TransactionKind::Call(to)
                        } else {
                            // EIP-170
                            if let Some(code) = call_request.input.data.as_ref() {
                                if code.len() > 0x6000 {
                                    return Err(JsonRpcError::code_size_too_large(code.len()));
                                }
                            }

                            reth_primitives::TransactionKind::Create
                        },
                        value: reth_primitives::TxValue::from(
                            call_request.value.unwrap_or_default(),
                        ),
                        input: call_request.input.data.unwrap_or_default(),
                    },
                ))
            }
        }
    }
}

/// Subtract the effective gas price from the balance to get an accurate gas limit.
pub async fn sender_gas_allowance(
    triedb_env: &TriedbEnv,
    block: &Block,
    request: &CallRequest,
) -> Result<u64, JsonRpcError> {
    if request.from.is_some() && request.max_fee_per_gas().is_some() {
        let from = request.from.expect("sender address");
        let TriedbResult::Account(_, balance, _) = triedb_env
            .get_account(
                from.into(),
                monad_blockdb_utils::BlockTags::Number(block.number),
            )
            .await
        else {
            debug!("triedb did not have sender account {from:}");
            return Err(JsonRpcError::internal_error());
        };

        let gas_price = request.max_fee_per_gas().expect("max_fee_per_gas");
        let gas_limit = U256::from(balance)
            .checked_sub(request.value.unwrap_or_default())
            .ok_or_else(|| {
                JsonRpcError::eth_call_error(
                    "insufficient funds for gas * price + value".to_string(),
                    None,
                )
            })?
            .checked_div(gas_price)
            .ok_or_else(JsonRpcError::internal_error)?;

        Ok(min(
            gas_limit.try_into().unwrap_or(block.gas_limit),
            block.gas_limit,
        ))
    } else {
        Ok(block.gas_limit)
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallInput {
    #[serde(
        skip_serializing_if = "Option::is_none",
        alias = "data",
        alias = "input"
    )]
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
    chain_id: u64,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let mut params: MonadEthCallParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let triedb_env: TriedbEnv = TriedbEnv::new(triedb_path);

    let block_number = match params.block {
        BlockTags::Default(_) => {
            let TriedbResult::BlockNum(triedb_block_number) = triedb_env.get_latest_block().await
            else {
                debug!("triedb did not have latest block number");
                return Err(JsonRpcError::internal_error());
            };
            let Some(blockdb_block_number) = blockdb_env.get_latest_block().await else {
                debug!("blockdb did not have latest block number");
                return Err(JsonRpcError::internal_error());
            };
            min(triedb_block_number, blockdb_block_number.0)
        }
        BlockTags::Number(block_number) => block_number.0,
    };

    let Some(block_header) = blockdb_env
        .get_block_by_tag(monad_blockdb_utils::BlockTags::Number(block_number))
        .await
    else {
        debug!("blockdb did not have latest block header");
        return Err(JsonRpcError::internal_error());
    };

    params.transaction.fill_gas_prices(U256::from(
        block_header.block.base_fee_per_gas.unwrap_or_default(),
    ))?;

    let allowance: Option<u64> = if params.transaction.gas.is_none() {
        Some(sender_gas_allowance(&triedb_env, &block_header.block, &params.transaction).await?)
    } else {
        None
    };

    if allowance.is_some() {
        params.transaction.gas = allowance.map(U256::from);
    };

    if params.transaction.chain_id.is_none() {
        params.transaction.chain_id = Some(U64::from(chain_id));
    }

    let sender = params.transaction.from.unwrap_or_default();
    let txn: reth_primitives::transaction::Transaction = params.transaction.try_into()?;
    let block_number = block_header.block.header.number;
    match monad_cxx::eth_call(
        txn,
        block_header.block.header,
        sender,
        block_number,
        triedb_path,
        execution_ledger_path,
    ) {
        monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult { output_data, .. }) => {
            Ok(json!(hex::encode(&output_data)))
        }
        monad_cxx::CallResult::Failure(error) => {
            Err(JsonRpcError::eth_call_error(error.message, error.data))
        }
    }
}

#[cfg(test)]
mod tests {
    use reth_primitives::U256;
    use serde_json::json;

    use crate::{jsonrpc, tests::init_server};

    #[test]
    fn parse_call_request() {
        let payload = json!(
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "gasPrice": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            }
        );
        let result = serde_json::from_value::<super::CallRequest>(payload).expect("parse failed");
        assert!(result.input.data.is_some());
        assert!(matches!(
            result.gas_price_details,
            super::GasPriceDetails::Legacy { gas_price: _ }
        ));
        assert_eq!(
            result.max_fee_per_gas(),
            Some(U256::from_str_radix("9184e72a000", 16).unwrap())
        );

        let payload = json!(
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "maxFeePerGas": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            }
        );
        let result = serde_json::from_value::<super::CallRequest>(payload).expect("parse failed");
        assert!(matches!(
            result.gas_price_details,
            super::GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(_),
                ..
            }
        ));
        assert_eq!(
            result.max_fee_per_gas(),
            Some(U256::from_str_radix("9184e72a000", 16).unwrap())
        );
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call_sha256_precompile() {
        let (app, _monad) = init_server().await;
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

        let req = actix_web::test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call() {
        let (app, _monad) = init_server().await;
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

        let req = actix_web::test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }
}
