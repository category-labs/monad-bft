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

use std::collections::BTreeMap;

use alloy_primitives::{Address, TxHash};
use monad_eth_txpool_types::EthTxPoolEvictReason;
use monad_rpc_docs::rpc;
use serde::{Deserialize, Serialize};

use crate::{
    txpool::{EthTxPoolBridgeClient, TxStatus},
    types::{
        eth_json::{EthAddress, EthHash},
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
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
            TxStatus::Tracked => ("tracked", None),
            TxStatus::Dropped { reason } => ("dropped", Some(reason.as_user_string())),
            TxStatus::Evicted { reason } => (
                "evicted",
                Some(match reason {
                    EthTxPoolEvictReason::Expired => "Transaction expired".to_string(),
                }),
            ),
            TxStatus::Committed => ("committed", None),
        };

        Self {
            status: status.to_string(),
            reason,
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
    txpool_bridge_client: &EthTxPoolBridgeClient,
    params: TxPoolStatusByHashParams,
) -> JsonRpcResult<TxPoolStatusResult> {
    let Some(status) = txpool_bridge_client.get_status_by_hash(&TxHash::new(params.hash.0)) else {
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
    txpool_bridge_client: &EthTxPoolBridgeClient,
    params: TxPoolStatusByAddressParams,
) -> JsonRpcResult<TxPoolStatusByAddressResult> {
    let Some(statuses) =
        txpool_bridge_client.get_status_by_address(&Address::new(params.address.0))
    else {
        return Err(JsonRpcError::custom("No transactions ".to_string()));
    };

    Ok(TxPoolStatusByAddressResult(
        statuses
            .into_iter()
            .map(|(hash, status)| (EthHash::from(hash), TxPoolStatusResult::from(status)))
            .collect(),
    ))
}
