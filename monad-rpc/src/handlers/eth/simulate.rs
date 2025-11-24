use alloy_primitives::{Address, U256, U64};
use monad_ethcall::StateOverrideSet;
use serde::Deserialize;

use crate::{
    eth_json_types::{BlockTagOrHash, EthHash},
    handlers::eth::call::CallRequest,
    jsonrpc::JsonRpcResult,
};

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonadBlockOverrides {
    #[serde(default)]
    pub number: Option<U64>,
    #[serde(default)]
    pub time: Option<U64>,
    #[serde(default)]
    pub gas_limit: Option<U256>,
    #[serde(default)]
    pub fee_recipient: Option<Address>,
    #[serde(default)]
    pub prev_randao: Option<EthHash>,
    #[serde(default)]
    pub base_fee_per_gas: Option<U256>,
    #[serde(default)]
    pub blob_base_fee: Option<U256>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonadBlockStateCall {
    #[serde(default)]
    pub block_overrides: MonadBlockOverrides,
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

pub async fn monad_simulate_v1(_params: MonadSimulateParams) -> JsonRpcResult<String> {
    todo!();
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
