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

use std::ops::Add;

use alloy_consensus::TxEnvelope;
use alloy_primitives::{U256, U64};
use monad_ethcall::{
    eth_simulate_v1, BlockOverride, EthCallExecutor, SimulateResult, StateOverrideSet,
    SuccessSimulateResult,
};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{Triedb, TriedbPath};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{
    data::{block_key_to_parts, get_block_key_from_tag_or_hash, DataProvider},
    handlers::{
        eth::call::{fill_gas_params, CallRequest},
        parse_ethcall_chain_id,
    },
    types::{
        eth_json::{BlockTagOrHash, BlockTags, Quantity},
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonadBlockStateCall {
    #[schemars(skip)]
    #[serde(default)]
    pub block_overrides: BlockOverride,
    #[schemars(skip)]
    #[serde(default)]
    pub state_overrides: StateOverrideSet,
    pub calls: Vec<CallRequest>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonadSimulation {
    pub block_state_calls: Vec<MonadBlockStateCall>,
    #[serde(default = "validation_default")]
    pub validation: bool,
    #[serde(default)]
    pub trace_transfers: bool,
}

// TODO(dhil): We currently don't support validation: false mode, so we make sure to default to true for now.
fn validation_default() -> bool {
    true
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MonadSimulateParams {
    pub simulation: MonadSimulation,
    pub block: BlockTagOrHash,
}

#[rpc(
    method = "eth_simulateV1",
    ignore = "chain_id,call_gas_limit,simulation_gas_limit,max_simulated_calls,max_simulated_blocks"
)]
pub async fn monad_simulate_v1<T: Triedb + TriedbPath>(
    data_provider: &DataProvider<T>,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    call_gas_limit: u64,
    simulation_gas_limit: u64,
    max_simulated_calls: usize,
    max_simulated_blocks: usize,
    params: MonadSimulateParams,
) -> JsonRpcResult<Box<RawValue>> {
    if !params.simulation.validation {
        let msg = String::from("`\"validation\": false` is not supported yet");
        return Err(JsonRpcError::custom(msg));
    }

    let total_simulated_blocks = params.simulation.block_state_calls.len();
    if total_simulated_blocks > max_simulated_blocks {
        return Err(JsonRpcError::custom(format!(
            "Too many block simulations: {}, maximum allowed is {}",
            total_simulated_blocks, max_simulated_blocks
        )));
    }

    let total_simulated_calls = params
        .simulation
        .block_state_calls
        .iter()
        .map(|bsc| bsc.calls.len())
        .sum::<usize>();
    if total_simulated_calls > max_simulated_calls {
        return Err(JsonRpcError::custom(format!(
            "Too many calls to simulate: {}, maximum allowed is {}",
            total_simulated_calls, max_simulated_calls
        )));
    }

    let block_key = get_block_key_from_tag_or_hash(&data_provider.triedb_env, params.block)
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;

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

    let Some(header) = data_provider
        .triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Err(JsonRpcError::block_not_found());
    };

    let overrides = params
        .simulation
        .block_state_calls
        .iter()
        .map(|call| (&call.block_overrides, &call.state_overrides))
        .collect::<Vec<_>>();

    let mut calls: Vec<Vec<CallRequest>> = params
        .simulation
        .block_state_calls
        .iter()
        .map(|bsc| {
            let mut calls = bsc.calls.to_vec();
            for call in calls.iter_mut() {
                // Merge `data` and `input` fields (compatible with
                // go-ethereum, see https://github.com/ethereum/go-ethereum/issues/15628).
                call.input.input = match (call.input.input.take(), call.input.data.take()) {
                    (Some(input), Some(data)) => {
                        if input != data {
                            return Err(JsonRpcError::invalid_params());
                        }
                        Some(input)
                    }
                    (None, data) | (data, None) => data,
                };
            }
            Ok(calls)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut accumulated_gas: U256 = U256::ZERO;
    for (call_list, (_, state_override)) in calls.iter_mut().zip(overrides.iter()) {
        // Inherit the base header. The execution client applies overrides to the simulation header.
        let mut header = header.header.clone();
        for call in call_list.iter_mut() {
            fill_gas_params(
                &data_provider.triedb_env,
                block_key,
                call,
                &mut header,
                state_override,
                U256::from(call_gas_limit),
            )
            .await?;
            accumulated_gas = accumulated_gas.add(U256::from(call.gas.unwrap_or_default()));
            if accumulated_gas > U256::from(simulation_gas_limit) {
                return Err(JsonRpcError::custom(format!(
                    "Gas limit for simulation exceeded: the simulation requires minimum {} gas which exceeds the maximum allowed {}",
                    accumulated_gas, simulation_gas_limit
                )));
            }
        }
    }

    let calls: Vec<Vec<TxEnvelope>> = calls
        .into_iter()
        .map(|call_list| {
            call_list
                .into_iter()
                .map(|mut tx: CallRequest| {
                    if let Some(tx_chain_id) = tx.chain_id {
                        if tx_chain_id != U64::from(chain_id) {
                            return Err(JsonRpcError::invalid_chain_id(
                                chain_id,
                                tx_chain_id.to::<u64>(),
                            ));
                        }
                    } else {
                        tx.chain_id = Some(U64::from(chain_id));
                    }
                    match tx.try_into().map_err(|_| JsonRpcError::invalid_params()) {
                        Ok(TxEnvelope::Legacy(_)) => Err(JsonRpcError::custom(String::from(
                            "Legacy transactions are unsupported at this time",
                        ))),
                        x => x,
                    }
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let (block_number, block_id) = block_key_to_parts(block_key);

    let grandparent_block_id = if block_number > 0 {
        let block_key = get_block_key_from_tag_or_hash(
            &data_provider.triedb_env,
            BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(block_number - 1))),
        )
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;
        block_key_to_parts(block_key).1
    } else {
        None
    };

    let result = eth_simulate_v1(
        parse_ethcall_chain_id(chain_id)?,
        &senders,
        &calls,
        header.header,
        block_number,
        block_id,
        grandparent_block_id,
        simulation_gas_limit,
        max_simulated_blocks,
        params.simulation.trace_transfers,
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
        handlers::eth::simulate::MonadSimulateParams,
        types::eth_json::{BlockTagOrHash, BlockTags},
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
        // TODO(dhil): We default to validation mode 'true' as we do not support the false mode yet.
        assert!(params.simulation.validation);
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
