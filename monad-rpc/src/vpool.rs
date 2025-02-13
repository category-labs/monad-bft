use std::collections::BTreeMap;

use alloy_primitives::{Address, TxHash};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolEvictReason};
use monad_rpc_docs::rpc;
use serde::{Deserialize, Serialize};

use crate::{
    eth_json_types::{EthAddress, EthHash},
    jsonrpc::{JsonRpcError, JsonRpcResult},
    txpool::{EthTxPoolBridgeState, TxStatus},
};

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct TxPoolStatusResult {
    status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl From<TxStatus> for TxPoolStatusResult {
    fn from(value: TxStatus) -> Self {
        let (status, reason) = match value {
            TxStatus::Unknown => ("unknown", None),
            TxStatus::Pending => ("pending", None),
            TxStatus::Tracked => ("tracked", None),
            TxStatus::Dropped { reason } => (
                "dropped",
                Some(match reason {
                    EthTxPoolDropReason::NotWellFormed => "Transaction not well formed",
                    EthTxPoolDropReason::NonceTooLow => "Transaction nonce too low",
                    EthTxPoolDropReason::FeeTooLow => "Transaction fee too low",
                    EthTxPoolDropReason::InsufficientBalance => "Signer had insufficient balance",
                    EthTxPoolDropReason::PoolFull => "Transaction pool is full",
                    EthTxPoolDropReason::ExistingHigherPriority => {
                        "Another transaction has higher priority"
                    }
                    EthTxPoolDropReason::PoolNotReady => "Transaction pool is not ready",
                    EthTxPoolDropReason::Internal(_) => "Internal error",
                }),
            ),
            TxStatus::Evicted { reason } => (
                "evicted",
                Some(match reason {
                    EthTxPoolEvictReason::Expired => "Transaction expired",
                }),
            ),
            TxStatus::Replaced => ("replaced", None),
            TxStatus::Committed => ("committed", None),
        };

        Self {
            status: status.to_string(),
            reason: reason.map(ToString::to_string),
        }
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TxPoolStatusByHashParams {
    pub hash: EthHash,
}

#[rpc(method = "txpool_statusByHash")]
#[allow(non_snake_case)]
pub async fn monad_txpool_statusByHash(
    txpool_state: &EthTxPoolBridgeState,
    params: TxPoolStatusByHashParams,
) -> JsonRpcResult<TxPoolStatusResult> {
    let Some(status) = txpool_state.get_status_by_hash(&TxHash::new(params.hash.0)) else {
        return Err(JsonRpcError::custom("Unknown tx hash".to_string()));
    };

    Ok(TxPoolStatusResult::from(status))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TxPoolStatusByAddressParams {
    pub address: EthAddress,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct TxPoolStatusByAddressResult(BTreeMap<EthHash, TxPoolStatusResult>);

#[rpc(method = "txpool_statusByAddress")]
#[allow(non_snake_case)]
pub async fn monad_txpool_statusByAddress(
    txpool_state: &EthTxPoolBridgeState,
    params: TxPoolStatusByAddressParams,
) -> JsonRpcResult<TxPoolStatusByAddressResult> {
    let Some(statuses) = txpool_state.get_status_by_address(&Address::new(params.address.0)) else {
        return Err(JsonRpcError::custom("No transactions ".to_string()));
    };

    Ok(TxPoolStatusByAddressResult(
        statuses
            .into_iter()
            .map(|(hash, status)| (EthHash::from(hash), TxPoolStatusResult::from(status)))
            .collect(),
    ))
}
