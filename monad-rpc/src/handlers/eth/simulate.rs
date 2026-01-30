use std::sync::Arc;

use alloy_consensus::TxEnvelope;
use alloy_primitives::U256;
use monad_ethcall::{
    eth_simulate_v1, BlockOverride, EthCallExecutor, SimulateResult, StateOverrideSet,
    SuccessSimulateResult,
};
use monad_triedb_utils::triedb_env::{
    BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb, TriedbPath,
};
use monad_types::{BlockId, Hash, SeqNum};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{
    eth_json_types::BlockTagOrHash,
    handlers::eth::{
        block::get_block_key_from_tag_or_hash,
        call::{fill_gas_params, CallRequest},
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonadBlockStateCall {
    #[serde(default)]
    pub block_overrides: BlockOverride,
    #[serde(default)]
    pub state_overrides: StateOverrideSet,
    pub calls: Vec<CallRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonadSimulation {
    pub block_state_calls: Vec<MonadBlockStateCall>,
    #[serde(default)]
    pub validation: bool,
    #[serde(default)]
    pub trace_transfers: bool,
}

#[derive(Debug, Deserialize)]
pub struct MonadSimulateParams {
    pub simulation: MonadSimulation,
    pub block: BlockTagOrHash,
}

pub async fn monad_simulate_v1<T: Triedb + TriedbPath>(
    triedb_env: &T,
    eth_call_executor: Arc<EthCallExecutor>,
    chain_id: u64,
    params: MonadSimulateParams,
) -> JsonRpcResult<Box<RawValue>> {
    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block).await?;

    let senders = params
        .simulation
        .block_state_calls
        .iter()
        .map(|bsc| {
            bsc.calls
                .iter()
                .map(|call| call.from.unwrap_or_default())
                .collect()
        })
        .collect();

    let mut header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Err(JsonRpcError::block_not_found()),
    };

    let state_overrides = StateOverrideSet::new();

    let mut calls: Vec<Vec<CallRequest>> = params
        .simulation
        .block_state_calls
        .iter()
        .map(|bsc| {
            bsc.calls
                .iter()
                .map(|call|
                    call.clone()
                )
                .collect()
        })
        .collect();

    for call_list in &mut calls {
        for call in call_list {
            fill_gas_params(
                triedb_env,
                block_key,
                call,
                &mut header.header,
                &state_overrides,
                U256::MAX,
            ).await?;
        }
    }

    let calls: Vec<Vec<TxEnvelope>> = calls
        .into_iter()
        .map(|call_list| {
            call_list
                .into_iter()
                .map(|call| call.try_into().unwrap())
                .collect()
        })
        .collect();

    let (block_number, block_id) = match block_key {
        BlockKey::Finalized(FinalizedBlockKey(SeqNum(n))) => (n, None),
        BlockKey::Proposed(ProposedBlockKey(SeqNum(n), BlockId(Hash(id)))) => (n, Some(id)),
    };

    let overrides: Vec<_> = params
        .simulation
        .block_state_calls
        .iter()
        .map(|call| (&call.block_overrides, &call.state_overrides))
        .collect();

    let result = eth_simulate_v1(
        chain_id,
        &senders,
        &calls,
        header.header,
        block_number,
        block_id,
        eth_call_executor,
        &overrides,
    )
    .await;

    match result {
        SimulateResult::Success(SuccessSimulateResult { output_data, .. }) => {
            let v: serde_cbor::Value = serde_cbor::from_slice(&output_data)
                .map_err(|e| JsonRpcError::internal_error(format!("CBOR decode error: {}", e)))?;
            serde_json::value::to_raw_value(&v).map_err(|e| {
                JsonRpcError::internal_error(format!("json serialization error: {}", e))
            })
        }

        SimulateResult::Failure(error) => {
            Err(JsonRpcError::eth_call_error(error.message, error.data))
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use serde_json::from_str;

    use crate::{
        eth_json_types::{BlockTagOrHash, BlockTags},
        handlers::eth::simulate::MonadSimulateParams,
    };

    #[test]
    fn parse_simulate() {
        let raw = r#"
        [
            {
                "blockStateCalls": [
                    {
                        "blockOverrides": {
                            "baseFeePerGas": "0x9"
                        },
                        "stateOverrides": {
                            "0xc000000000000000000000000000000000000000": {
                                "balance": "0x4a817c420"
                            }
                        },
                        "calls": [
                            {
                                "from": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                                "to": "0x014d023e954bAae7F21E56ed8a5d81b12902684D",
                                "maxFeePerGas": "0xf",
                                "value": "0x1"
                            }
                        ]
                    }
                ],
                "validation": true,
                "traceTransfers": true
            },
            "latest"
        ]
        "#;

        let params: MonadSimulateParams = from_str(raw).unwrap();

        assert_eq!(params.simulation.block_state_calls.len(), 1);
        assert_eq!(
            params.simulation.block_state_calls[0]
                .block_overrides
                .base_fee_per_gas
                .unwrap(),
            U256::from(9)
        );
        assert_eq!(
            params.simulation.block_state_calls[0].state_overrides.len(),
            1
        );
        assert_eq!(params.simulation.block_state_calls[0].calls.len(), 1);
        assert!(params.simulation.validation);
        assert!(params.simulation.trace_transfers);
        assert_eq!(params.block, BlockTagOrHash::BlockTags(BlockTags::Latest));
    }

    #[test]
    fn parse_simulate_no_optionals() {
        let raw = r#"
        [
            {
                "blockStateCalls": [
                    {
                        "calls": [
                            {
                                "from": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                                "to": "0x014d023e954bAae7F21E56ed8a5d81b12902684D",
                                "maxFeePerGas": "0xf",
                                "value": "0x1"
                            }
                        ]
                    }
                ]
            },
            "latest"
        ]
        "#;

        let params: MonadSimulateParams = from_str(raw).unwrap();
        assert!(params.simulation.block_state_calls[0]
            .block_overrides
            .base_fee_per_gas
            .is_none());
        assert!(params.simulation.block_state_calls[0]
            .state_overrides
            .is_empty());
        assert!(!params.simulation.validation);
        assert!(!params.simulation.trace_transfers);
    }

    #[test]
    fn parse_simulate_block_overrides() {
        let raw = r#"
        [
            {
                "blockStateCalls": [
                    {
                        "blockOverrides": {
                            "number": "0x14",
                            "time": "0xc8",
                            "gasLimit": "0x2e631",
                            "feeRecipient": "0xc100000000000000000000000000000000000000",
                            "prevRandao": "0x0000000000000000000000000000000000000000000000000000000000001234",
                            "baseFeePerGas": "0x14",
                            "blobBaseFee": "0x15"
                        },
                        "calls": [
                            {
                                "from": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                                "to": "0x014d023e954bAae7F21E56ed8a5d81b12902684D",
                                "maxFeePerGas": "0xf",
                                "value": "0x1"
                            }
                        ]
                    }
                ]
            },
            "latest"
        ]
        "#;

        let _params: MonadSimulateParams = from_str(raw).unwrap();
    }
}
