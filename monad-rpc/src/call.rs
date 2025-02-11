use std::cmp::min;

use alloy_consensus::{Header, SignableTransaction, TxEip1559, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, PrimitiveSignature, TxKind, Uint, U256, U64, U8};
use alloy_rpc_types::AccessList;
use monad_cxx::StateOverrideSet;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{
    BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb, TriedbPath,
};
use monad_types::{Round, SeqNum};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    block_handlers::get_block_key_from_tag,
    eth_json_types::BlockTags,
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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

impl schemars::JsonSchema for CallRequest {
    fn schema_name() -> String {
        "CallRequest".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(CallRequest {
            from: None,
            to: None,
            gas: None,
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::default()),
                max_priority_fee_per_gas: Some(U256::default())
            },
            value: None,
            input: CallInput::default(),
            nonce: None,
            chain_id: None,
            access_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        });
        schema.schema.into()
    }
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
            GasPriceDetails::Legacy { mut gas_price } => {
                if gas_price < base_fee {
                    gas_price = base_fee;
                    self.gas_price_details = GasPriceDetails::Legacy { gas_price };
                }
            }
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                let max_fee_per_gas = match max_fee_per_gas {
                    Some(mut max_fee_per_gas) => {
                        if max_fee_per_gas < base_fee {
                            max_fee_per_gas = base_fee;
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

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallInput {
    /// Transaction data
    pub input: Option<alloy_primitives::Bytes>,

    /// This is the same as `input` but is used for backwards compatibility:
    /// <https://github.com/ethereum/go-ethereum/issues/15628>
    pub data: Option<alloy_primitives::Bytes>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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
impl TryFrom<CallRequest> for TxEnvelope {
    type Error = JsonRpcError;
    fn try_from(call_request: CallRequest) -> Result<Self, JsonRpcError> {
        match call_request {
            CallRequest {
                gas_price_details: GasPriceDetails::Legacy { gas_price },
                ..
            } => {
                // Legacy

                // default signature as eth_call doesn't require it
                let signature = PrimitiveSignature::new(U256::from(0), U256::from(0), false);
                let transaction = TxLegacy {
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
                        TxKind::Call(to)
                    } else {
                        TxKind::Create
                    },
                    value: call_request.value.unwrap_or_default(),
                    input: call_request.input.input.unwrap_or_default(),
                };

                Ok(transaction.into_signed(signature).into())
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

                // default signature as eth_call doesn't require it
                let signature = PrimitiveSignature::new(U256::from(0), U256::from(0), false);
                let transaction = TxEip1559 {
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
                    access_list: AccessList::default(),
                    to: if let Some(to) = call_request.to {
                        TxKind::Call(to)
                    } else {
                        // EIP-170
                        if let Some(code) = call_request.input.data.as_ref() {
                            if code.len() > 0x6000 {
                                return Err(JsonRpcError::code_size_too_large(code.len()));
                            }
                        }

                        TxKind::Create
                    },
                    value: call_request.value.unwrap_or_default(),
                    input: call_request.input.input.unwrap_or_default(),
                };

                Ok(transaction.into_signed(signature).into())
            }
        }
    }
}

/// Populate gas limit and gas prices
pub async fn fill_gas_params<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tx: &mut CallRequest,
    header: &mut Header,
    state_overrides: &StateOverrideSet,
) -> Result<(), JsonRpcError> {
    // Geth checks that the sender can pay for gas if gas price is populated.
    // Set the base fee to zero if gas price is not populated.
    // https://github.com/ethereum/go-ethereum/pull/20783
    match tx.gas_price_details {
        GasPriceDetails::Legacy { gas_price: _ }
        | GasPriceDetails::Eip1559 {
            max_fee_per_gas: Some(_),
            ..
        } => {
            tx.fill_gas_prices(U256::from(header.base_fee_per_gas.unwrap_or_default()))?;

            if tx.gas.is_none() {
                let allowance =
                    sender_gas_allowance(triedb_env, block_key, header, tx, state_overrides)
                        .await?;
                tx.gas = Some(U256::from(allowance));
            }
        }
        _ => {
            header.base_fee_per_gas = Some(0);
            tx.fill_gas_prices(U256::ZERO)?;
            if tx.gas.is_none() {
                tx.gas = Some(U256::from(header.gas_limit));
            }
        }
    }
    Ok(())
}

/// Subtract the effective gas price from the balance to get an accurate gas limit.
pub async fn sender_gas_allowance<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    block: &Header,
    request: &CallRequest,
    state_overrides: &StateOverrideSet,
) -> Result<u64, JsonRpcError> {
    if let (Some(sender), Some(gas_price)) = (request.from, request.max_fee_per_gas()) {
        if gas_price.is_zero() {
            return Ok(block.gas_limit);
        }

        let balance = match state_overrides
            .get(&sender)
            .and_then(|override_state| override_state.balance)
        {
            Some(balance) => balance,
            None => {
                let account = triedb_env
                    .get_account(block_key, sender.into())
                    .await
                    .map_err(JsonRpcError::internal_error)?;
                U256::from(account.balance)
            }
        };

        if balance == U256::ZERO {
            return Err(JsonRpcError::insufficient_funds());
        }

        let gas_limit = balance
            .checked_sub(request.value.unwrap_or_default())
            .ok_or_else(JsonRpcError::insufficient_funds)?
            .checked_div(gas_price)
            .ok_or_else(|| JsonRpcError::internal_error("zero gas price".into()))?;

        Ok(min(
            gas_limit.try_into().unwrap_or(block.gas_limit),
            block.gas_limit,
        ))
    } else {
        Ok(block.gas_limit)
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MonadEthCallParams {
    transaction: CallRequest,
    #[serde(default)]
    block: BlockTags,
    #[schemars(skip)] // TODO: move StateOverrideSet from monad-cxx
    #[serde(default)]
    state_overrides: StateOverrideSet, // empty = no state overrides
}

/// Executes a new message call immediately without creating a transaction on the block chain.
#[tracing::instrument(level = "debug")]
#[rpc(method = "eth_call", ignore = "chain_id")]
pub async fn monad_eth_call<T: Triedb + TriedbPath>(
    triedb_env: &T,
    chain_id: u64,
    params: MonadEthCallParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_call: {params:?}");

    let mut params = params;
    params.transaction.input.input = match (
        params.transaction.input.input.take(),
        params.transaction.input.data.take(),
    ) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    let state_overrides = &params.state_overrides;

    // TODO: check duplicate address, duplicate storage key, etc.

    let block_key = get_block_key_from_tag(triedb_env, params.block);
    let mut header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => {
            return Err(JsonRpcError::internal_error(
                "error getting block header".into(),
            ))
        }
    };

    fill_gas_params(
        triedb_env,
        block_key,
        &mut params.transaction,
        &mut header.header,
        &params.state_overrides,
    )
    .await?;

    if params.transaction.chain_id.is_none() {
        params.transaction.chain_id = Some(U64::from(chain_id));
    }

    let sender = params.transaction.from.unwrap_or_default();
    let tx_chain_id = params
        .transaction
        .chain_id
        .expect("chain id must be populated")
        .to::<u64>();
    let txn: TxEnvelope = params.transaction.try_into()?;
    let (block_number, block_round) = match block_key {
        BlockKey::Finalized(FinalizedBlockKey(SeqNum(n))) => (n, None),
        BlockKey::Proposed(ProposedBlockKey(SeqNum(n), Round(r))) => (n, Some(r)),
    };
    match monad_cxx::eth_call(
        tx_chain_id,
        txn,
        header.header,
        sender,
        block_number,
        block_round,
        &triedb_env.path(),
        state_overrides,
    ) {
        monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult { output_data, .. }) => {
            Ok(hex::encode(&output_data))
        }
        monad_cxx::CallResult::Failure(error) => {
            Err(JsonRpcError::eth_call_error(error.message, error.data))
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::Header;
    use alloy_primitives::{Address, U256};
    use monad_cxx::StateOverrideSet;
    use monad_triedb_utils::{
        mock_triedb::MockTriedb,
        triedb_env::{BlockKey, FinalizedBlockKey},
    };
    use monad_types::SeqNum;
    use serde_json::json;

    use super::{fill_gas_params, CallRequest, GasPriceDetails};
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

    #[tokio::test]
    async fn test_fill_gas_params() {
        let mock_triedb = MockTriedb::default();

        // when gas price is not populated, then
        // (1) header base fee is set to zero and (2) tx gas limit is set to block gas limit
        let mut call_request = CallRequest::default();
        let mut header = Header {
            base_fee_per_gas: Some(10_000_000_000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let block_key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(header.number)));
        let state_overrides = StateOverrideSet::default();

        let result = fill_gas_params(
            &mock_triedb,
            block_key,
            &mut call_request,
            &mut header,
            &state_overrides,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(call_request.gas, Some(U256::from(300_000_000)));
        assert_eq!(header.base_fee_per_gas, Some(0));

        // when gas price is populated but sender address is not populated, then
        // (1) tx gas limit is set to block gas limit
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(1_000_000),
            },
            ..Default::default()
        };
        let mut header = Header {
            base_fee_per_gas: Some(10_000_000_000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let result = fill_gas_params(
            &mock_triedb,
            block_key,
            &mut call_request,
            &mut header,
            &state_overrides,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(call_request.gas, Some(U256::from(300_000_000)));

        // when gas price is populated and sender address is populated, then
        // (1) check whether user has sufficient balance
        let mut call_request = CallRequest {
            from: Some(Address::default()),
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(1_000_000),
            },
            ..Default::default()
        };
        let mut header = Header {
            base_fee_per_gas: Some(10_000_000_000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let result = fill_gas_params(
            &mock_triedb,
            block_key,
            &mut call_request,
            &mut header,
            &state_overrides,
        )
        .await;
        assert!(result.is_err());
    }
}
