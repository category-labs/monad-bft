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

use alloy_consensus::{Header, TxEnvelope};
use alloy_primitives::{U256, U64};
use monad_ethcall::{
    overrides::{BlockOverride, StateOverrideSet},
    MonadExecutor,
};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{BlockKey, Triedb, TriedbPath};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{
    data::{block_key_to_parts, get_block_key_from_tag_or_hash, DataProvider},
    handlers::{
        eth::call::{sender_gas_allowance, CallRequest},
        parse_ethcall_chain_id,
    },
    types::{
        eth_json::{BlockTagOrHash, BlockTags, Quantity},
        jsonrpc::{ErrorCode, JsonRpcError, JsonRpcResult},
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

// TODO(dhil): Consider whether to flip this to `false` to align with Geth.
fn validation_default() -> bool {
    true
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MonadSimulateParams {
    pub simulation: MonadSimulation,
    pub block: BlockTagOrHash,
}

async fn fill_simulation_gas_params<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    calls: &mut [CallRequest],
    header: &mut Header,
    overrides: (&BlockOverride, &StateOverrideSet),
    call_gas_limit: u64,
    validation: bool,
) -> JsonRpcResult<()> {
    let (block_override, state_overrides) = overrides;
    let base_fee = block_override.base_fee_per_gas.unwrap_or_default();
    if let Some(gas_limit) = block_override.gas_limit {
        header.gas_limit = gas_limit.to();
    }

    for call in calls {
        call.fill_gas_prices(base_fee)?;
        if call.gas.is_none() {
            let allowance = if validation {
                sender_gas_allowance(triedb_env, block_key, header, call, state_overrides).await?
            } else {
                header.gas_limit
            };
            call.gas = Some(U256::from(allowance).min(U256::from(call_gas_limit)));
        }
    }

    Ok(())
}

#[rpc(
    method = "eth_simulateV1",
    ignore = "chain_id,max_response_size,call_gas_limit,simulation_gas_limit,max_simulated_calls,max_simulated_blocks"
)]
pub async fn monad_simulate_v1<T: Triedb + TriedbPath>(
    data_provider: &DataProvider<T>,
    eth_call_executor: &MonadExecutor,
    chain_id: u64,
    max_response_size: usize,
    call_gas_limit: u64,
    simulation_gas_limit: u64,
    max_simulated_calls: usize,
    max_simulated_blocks: usize,
    params: MonadSimulateParams,
) -> JsonRpcResult<Box<RawValue>> {
    let total_simulated_blocks = params.simulation.block_state_calls.len();
    if total_simulated_blocks > max_simulated_blocks {
        return Err(JsonRpcError::with_message(
            ErrorCode::ServerError,
            format!(
                "Too many block simulations: {}, maximum allowed is {}",
                total_simulated_blocks, max_simulated_blocks
            ),
        ));
    }

    let total_simulated_calls = params
        .simulation
        .block_state_calls
        .iter()
        .map(|bsc| bsc.calls.len())
        .sum::<usize>();
    if total_simulated_calls > max_simulated_calls {
        return Err(JsonRpcError::with_message(
            ErrorCode::ServerError,
            format!(
                "Too many calls to simulate: {}, maximum allowed is {}",
                total_simulated_calls, max_simulated_calls
            ),
        ));
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
    let mut simulation_header = header.header.clone();
    for (call_list, &block_overrides) in calls.iter_mut().zip(overrides.iter()) {
        fill_simulation_gas_params(
            &data_provider.triedb_env,
            block_key,
            call_list,
            &mut simulation_header,
            block_overrides,
            call_gas_limit,
            params.simulation.validation,
        )
        .await?;
        for call in call_list.iter_mut() {
            accumulated_gas = accumulated_gas.add(U256::from(call.gas.unwrap_or_default()));
            if accumulated_gas > U256::from(simulation_gas_limit) {
                return Err(JsonRpcError::with_message(ErrorCode::ServerError, format!(
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
                    tx.try_into().map_err(|_| JsonRpcError::invalid_params())
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

    let result = eth_call_executor
        .eth_simulate_v1(
            parse_ethcall_chain_id(chain_id)?,
            &senders,
            &calls,
            header.header,
            block_number,
            block_id,
            grandparent_block_id,
            simulation_gas_limit,
            max_simulated_blocks,
            max_response_size,
            params.simulation.validation,
            params.simulation.trace_transfers,
            &overrides,
        )
        .await;

    match result {
        Ok(result) => {
            let output_data = result.output_data;
            let v: serde_cbor::Value = serde_cbor::from_slice(&output_data)
                .map_err(|e| JsonRpcError::internal_error(format!("CBOR decode error: {}", e)))?;
            serde_json::value::to_raw_value(&v).map_err(|e| {
                JsonRpcError::internal_error(format!("json serialization error: {}", e))
            })
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::Header;
    use alloy_primitives::{Address, U256, U64};
    use monad_ethcall::overrides::{BlockOverride, StateOverrideObject, StateOverrideSet};
    use monad_triedb_utils::{
        mock_triedb::MockTriedb,
        triedb_env::{BlockKey, FinalizedBlockKey},
    };
    use monad_types::SeqNum;
    use serde_json::from_str;

    use super::{fill_simulation_gas_params, MonadSimulateParams};
    use crate::{
        handlers::eth::call::{CallRequest, GasPriceDetails},
        types::{
            eth_json::{BlockTagOrHash, BlockTags},
            jsonrpc::JsonRpcError,
        },
    };

    #[tokio::test]
    async fn simulation_gas_params_base_fee() {
        let triedb = MockTriedb::default();
        let block_key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(0)));
        let parent_header = Header {
            base_fee_per_gas: Some(1000),
            gas_limit: 1000,
            ..Default::default()
        };
        let state_overrides = StateOverrideSet::default();

        for base_fee_override in [
            None,
            Some(U256::ZERO),
            Some(U256::from(10)),
            Some(U256::from(u64::MAX) + U256::from(1)),
        ] {
            let block_override = BlockOverride {
                base_fee_per_gas: base_fee_override,
                ..Default::default()
            };
            let base_fee = base_fee_override.unwrap_or_default();

            for validation in [false, true] {
                let mut header = parent_header.clone();
                let mut calls = [
                    CallRequest::default(),
                    CallRequest {
                        gas_price_details: GasPriceDetails::Eip1559 {
                            max_fee_per_gas: Some(base_fee + U256::from(10)),
                            max_priority_fee_per_gas: Some(U256::from(5)),
                        },
                        ..Default::default()
                    },
                    CallRequest {
                        gas_price_details: GasPriceDetails::Legacy {
                            gas_price: U256::ZERO,
                        },
                        ..Default::default()
                    },
                    CallRequest {
                        gas: Some(U256::from(42)),
                        ..Default::default()
                    },
                ];

                fill_simulation_gas_params(
                    &triedb,
                    block_key,
                    &mut calls,
                    &mut header,
                    (&block_override, &state_overrides),
                    100,
                    validation,
                )
                .await
                .unwrap();

                assert_eq!(calls[0].max_fee_per_gas(), Some(base_fee));
                assert_eq!(calls[1].max_fee_per_gas(), Some(base_fee + U256::from(5)));
                assert_eq!(calls[2].max_fee_per_gas(), Some(base_fee));
                assert_eq!(calls[3].max_fee_per_gas(), Some(base_fee));
                for call in &calls[..3] {
                    assert_eq!(call.gas, Some(U256::from(100)));
                }
                assert_eq!(calls[3].gas, Some(U256::from(42)));
                assert_eq!(header, parent_header);
            }
        }
    }

    #[tokio::test]
    async fn simulation_gas_params_balance_allowance() {
        let triedb = MockTriedb::default();
        let block_key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(0)));
        let block_override = BlockOverride {
            base_fee_per_gas: Some(U256::from(10)),
            gas_limit: Some(U64::from(100)),
            ..Default::default()
        };
        let value = U256::from(20);

        for gas_price_details in [
            GasPriceDetails::Legacy {
                gas_price: U256::from(1),
            },
            GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(100)),
                max_priority_fee_per_gas: Some(U256::from(5)),
            },
        ] {
            for balance in [U256::ZERO, U256::from(10), U256::from(1000)] {
                let state_overrides = StateOverrideSet::from([(
                    Address::ZERO,
                    StateOverrideObject {
                        balance: Some(balance),
                        ..Default::default()
                    },
                )]);

                for validation in [false, true] {
                    for call_gas_limit in [50, 200] {
                        let mut calls = [CallRequest {
                            from: Some(Address::ZERO),
                            value: Some(value),
                            gas_price_details: gas_price_details.clone(),
                            ..Default::default()
                        }];
                        let mut header = Header {
                            base_fee_per_gas: Some(1000),
                            gas_limit: 1000,
                            ..Default::default()
                        };

                        let result = fill_simulation_gas_params(
                            &triedb,
                            block_key,
                            &mut calls,
                            &mut header,
                            (&block_override, &state_overrides),
                            call_gas_limit,
                            validation,
                        )
                        .await;

                        if validation && balance < value {
                            assert_eq!(result, Err(JsonRpcError::insufficient_funds()));
                            continue;
                        }

                        result.unwrap();
                        let allowance = if validation {
                            (balance - value) / calls[0].max_fee_per_gas().unwrap()
                        } else {
                            U256::from(100)
                        };
                        assert_eq!(
                            calls[0].gas,
                            Some(
                                allowance
                                    .min(U256::from(100))
                                    .min(U256::from(call_gas_limit))
                            )
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn simulation_gas_params_block_isolation() {
        let triedb = MockTriedb::default();
        let block_key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(0)));
        let state_overrides = StateOverrideSet::default();
        let mut header = Header {
            base_fee_per_gas: Some(1000),
            gas_limit: 1000,
            ..Default::default()
        };

        for (base_fee, gas_limit, expected_fee, expected_gas) in [
            (Some(10), Some(100), 10, 100),
            (None, None, 0, 100),
            (Some(20), None, 20, 100),
            (None, Some(200), 0, 200),
        ] {
            let block_override = BlockOverride {
                base_fee_per_gas: base_fee.map(U256::from),
                gas_limit: gas_limit.map(U64::from),
                ..Default::default()
            };
            let mut calls = [CallRequest::default()];

            fill_simulation_gas_params(
                &triedb,
                block_key,
                &mut calls,
                &mut header,
                (&block_override, &state_overrides),
                1000,
                true,
            )
            .await
            .unwrap();

            assert_eq!(calls[0].max_fee_per_gas(), Some(U256::from(expected_fee)));
            assert_eq!(calls[0].gas, Some(U256::from(expected_gas)));
        }
    }

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
        // TODO(dhil): We default to validation mode 'true' -- reconsider defaulting to 'false'
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
