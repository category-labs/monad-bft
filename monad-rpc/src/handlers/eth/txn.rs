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

use std::{pin::pin, time::Duration};

use alloy_consensus::Transaction as _;
use alloy_eips::Decodable2718;
use alloy_primitives::{Address, FixedBytes, TxHash};
use alloy_rpc_types::{Filter, TransactionReceipt};
use monad_exec_events::BlockCommitState;
use monad_eth_types::{namespace_for_chain_id, EthTxEnvelope, NamespaceTransactionBatch};
use monad_ethcall::EthCallExecutor;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace, warn};

use crate::{
    data::{eth_call_handler::EthCallHandlerConfig, DataProvider},
    event::{EventServerClient, EventServerEvent},
    preconfirmation::{NamespacePreconfirmation, NamespacePreconfirmationService},
    txpool::{EthTxPoolBridgeClient, TxStatus},
    types::{
        eth_json::{
            BlockTagOrHash, BlockTags, EthHash, MonadLog, MonadTransaction,
            MonadTransactionReceipt, Quantity, UnformattedData,
        },
        jsonrpc::{ChainStateResultMap, JsonRpcError, JsonRpcResult},
    },
};

pub enum FilterError {
    InvalidBlockRange,
    RangeTooLarge,
}

impl From<FilterError> for JsonRpcError {
    fn from(e: FilterError) -> Self {
        match e {
            FilterError::InvalidBlockRange => {
                JsonRpcError::filter_error("invalid block range".into())
            }
            FilterError::RangeTooLarge => {
                JsonRpcError::filter_error("block range too large".into())
            }
        }
    }
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetLogsResult(pub Vec<MonadLog>);

#[derive(Debug, Deserialize, JsonSchema)]
pub struct MonadEthGetLogsParams {
    #[schemars(schema_with = "schema_for_filter")]
    filters: Filter,
}

fn schema_for_filter(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema_for_value!(Filter::new().from_block(0).to_block(1).address(
        "0xAc4b3DacB91461209Ae9d41EC517c2B9Cb1B7DAF"
            .parse::<Address>()
            .unwrap()
    ))
    .schema
    .into()
}

#[rpc(
    method = "eth_getLogs",
    ignore = "max_response_size,max_block_range,use_eth_get_logs_index,dry_run_get_logs_index,max_finalized_block_cache_len"
)]
#[allow(non_snake_case)]
/// Returns an array of all logs matching filter with given id.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_eth_getLogs<T: Triedb>(
    data_provider: &DataProvider<T>,
    max_response_size: u32,
    max_block_range: u64,
    p: MonadEthGetLogsParams,
    use_eth_get_logs_index: bool,
    dry_run_get_logs_index: bool,
    max_finalized_block_cache_len: u64,
) -> JsonRpcResult<MonadEthGetLogsResult> {
    trace!("monad_eth_getLogs: {p:?}");

    let MonadEthGetLogsParams { filters } = p;

    let logs = data_provider
        .get_logs(
            filters,
            max_response_size,
            max_block_range,
            use_eth_get_logs_index,
            dry_run_get_logs_index,
            max_finalized_block_cache_len,
        )
        .await?;

    Ok(MonadEthGetLogsResult(logs))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthSendRawTransactionParams {
    hex_tx: UnformattedData,
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadSendRawTransactionBatchParams {
    hex_batch: UnformattedData,
}

// TODO: need to support EIP-4844 transactions
#[rpc(
    method = "eth_sendRawTransaction",
    ignore = "tx_pool,ipc,txpool_bridge_client,base_chain_id,route_chain_id,route_namespace,allow_unprotected_txs"
)]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Submits a raw transaction. For EIP-4844 transactions, the raw form must be the network form.
/// This means it includes the blobs, KZG commitments, and KZG proofs.
pub async fn monad_eth_sendRawTransaction(
    txpool_bridge_client: &EthTxPoolBridgeClient,
    params: MonadEthSendRawTransactionParams,
    base_chain_id: u64,
    route_chain_id: u64,
    route_namespace: Option<Address>,
    allow_unprotected_txs: bool,
) -> JsonRpcResult<String> {
    trace!("monad_eth_sendRawTransaction: {params:?}");

    let tx = validate_and_decode_tx(
        &params.hex_tx.0,
        base_chain_id,
        route_chain_id,
        route_namespace,
        allow_unprotected_txs,
        JsonRpcError::txn_decode_error,
    )?;

    let tx_hash = *tx.tx_hash();
    debug!(name = "sendRawTransaction", txn_hash = ?tx_hash);

    submit_to_txpool(txpool_bridge_client, tx).await?;

    Ok(tx_hash.to_string())
}

#[rpc(
    method = "monad_sendRawTransactionPreconfirmed",
    ignore = "namespace_preconfirmation_service,base_chain_id,route_chain_id,route_namespace,allow_unprotected_txs"
)]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_sendRawTransactionPreconfirmed(
    namespace_preconfirmation_service: &NamespacePreconfirmationService,
    params: MonadEthSendRawTransactionParams,
    base_chain_id: u64,
    route_chain_id: u64,
    route_namespace: Option<Address>,
    allow_unprotected_txs: bool,
) -> JsonRpcResult<NamespacePreconfirmation> {
    trace!("monad_sendRawTransactionPreconfirmed: {params:?}");

    let route_namespace = route_namespace.ok_or_else(|| {
        JsonRpcError::custom("namespace preconfirmation requires a namespace RPC route".to_string())
    })?;

    let tx = validate_and_decode_tx(
        &params.hex_tx.0,
        base_chain_id,
        route_chain_id,
        Some(route_namespace),
        allow_unprotected_txs,
        JsonRpcError::txn_decode_error,
    )?;

    namespace_preconfirmation_service
        .submit(tx, base_chain_id, route_chain_id, route_namespace)
        .await
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadGetTransactionPreconfirmationParams {
    tx_hash: EthHash,
}

#[rpc(
    method = "monad_getTransactionPreconfirmation",
    ignore = "namespace_preconfirmation_service"
)]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_getTransactionPreconfirmation(
    namespace_preconfirmation_service: &NamespacePreconfirmationService,
    params: MonadGetTransactionPreconfirmationParams,
) -> JsonRpcResult<Option<NamespacePreconfirmation>> {
    trace!("monad_getTransactionPreconfirmation: {params:?}");

    Ok(namespace_preconfirmation_service.get(&FixedBytes(params.tx_hash.0)))
}

#[rpc(
    method = "monad_sendRawTransactionPreconfirmedSync",
    ignore = "namespace_preconfirmation_service,data_provider,eth_call_handler_config,eth_call_executor,base_chain_id,route_chain_id,route_namespace,allow_unprotected_txs,eth_send_raw_transaction_sync_default_timeout_ms,eth_send_raw_transaction_sync_max_timeout_ms"
)]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_sendRawTransactionPreconfirmedSync<T: Triedb>(
    namespace_preconfirmation_service: &NamespacePreconfirmationService,
    data_provider: &DataProvider<T>,
    _eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    params: MonadEthSendRawTransactionSyncParams,
    base_chain_id: u64,
    route_chain_id: u64,
    route_namespace: Option<Address>,
    allow_unprotected_txs: bool,
    eth_send_raw_transaction_sync_default_timeout_ms: u64,
    eth_send_raw_transaction_sync_max_timeout_ms: u64,
) -> JsonRpcResult<MonadTransactionReceipt> {
    trace!("monad_sendRawTransactionPreconfirmedSync: {params:?}");

    let route_namespace = route_namespace.ok_or_else(|| {
        JsonRpcError::custom("namespace preconfirmation requires a namespace RPC route".to_string())
    })?;
    let timeout_ms = params
        .timeout_ms
        .filter(|&t| t > 0 && t <= eth_send_raw_transaction_sync_max_timeout_ms)
        .unwrap_or(eth_send_raw_transaction_sync_default_timeout_ms);

    let tx = validate_and_decode_tx(
        &params.hex_tx.0,
        base_chain_id,
        route_chain_id,
        Some(route_namespace),
        allow_unprotected_txs,
        JsonRpcError::tx_sync_unready,
    )?;

    let tx_hash = *tx.tx_hash();
    namespace_preconfirmation_service
        .submit(tx, base_chain_id, route_chain_id, route_namespace)
        .await?;

    let receipt = poll_for_preconfirmed_receipt(
        namespace_preconfirmation_service,
        data_provider,
        eth_call_executor,
        base_chain_id,
        route_namespace,
        tx_hash,
        timeout_ms,
    )
    .await?;

    Ok(MonadTransactionReceipt(receipt))
}

#[rpc(
    method = "monad_getTransactionPreconfirmationReceipt",
    ignore = "namespace_preconfirmation_service,data_provider,eth_call_handler_config,eth_call_executor,base_chain_id,route_namespace"
)]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_getTransactionPreconfirmationReceipt<T: Triedb>(
    namespace_preconfirmation_service: &NamespacePreconfirmationService,
    data_provider: &DataProvider<T>,
    _eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    base_chain_id: u64,
    route_namespace: Option<Address>,
    params: MonadGetTransactionPreconfirmationParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_getTransactionPreconfirmationReceipt: {params:?}");

    let route_namespace = route_namespace.ok_or_else(|| {
        JsonRpcError::custom("namespace preconfirmation requires a namespace RPC route".to_string())
    })?;
    let tx_hash = FixedBytes(params.tx_hash.0);
    namespace_preconfirmation_service
        .simulate_receipt_until(
            data_provider,
            eth_call_executor,
            base_chain_id,
            route_namespace,
            tx_hash,
        )
        .await
        .map(|receipt| receipt.map(MonadTransactionReceipt))
}

#[rpc(
    method = "monad_sendRawTransactionBatch",
    ignore = "txpool_bridge_client,base_chain_id,route_chain_id,route_namespace"
)]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_sendRawTransactionBatch(
    txpool_bridge_client: &EthTxPoolBridgeClient,
    params: MonadSendRawTransactionBatchParams,
    base_chain_id: u64,
    route_chain_id: u64,
    route_namespace: Option<Address>,
) -> JsonRpcResult<Vec<String>> {
    trace!("monad_sendRawTransactionBatch: {params:?}");

    let batch = validate_and_decode_batch(
        &params.hex_batch.0,
        base_chain_id,
        route_chain_id,
        route_namespace,
    )?;
    let tx_hashes = batch
        .transactions
        .iter()
        .map(|tx| tx.tx_hash().to_string())
        .collect::<Vec<_>>();

    submit_batch_to_txpool(txpool_bridge_client, batch).await?;

    Ok(tx_hashes)
}

fn validate_and_decode_tx(
    hex_tx: &[u8],
    base_chain_id: u64,
    route_chain_id: u64,
    route_namespace: Option<Address>,
    allow_unprotected_txs: bool,
    decode_error_fn: impl FnOnce() -> JsonRpcError,
) -> Result<EthTxEnvelope, JsonRpcError> {
    let tx = EthTxEnvelope::decode_2718_exact(hex_tx).map_err(|err| {
        debug!(?err, "eth txn decode failed");
        decode_error_fn()
    })?;

    // drop pre EIP-155 transactions if disallowed by the rpc (for user protection purposes)
    if !allow_unprotected_txs && tx.chain_id().is_none() {
        return Err(JsonRpcError::custom(
            "Unprotected transactions (pre-EIP155) are not allowed over RPC".to_string(),
        ));
    }

    let tx_namespace = match namespace_for_chain_id(tx.chain_id(), base_chain_id) {
        Ok(tx_namespace) => tx_namespace,
        Err(err) => {
            let tx_chain_id = match err {
                monad_eth_types::WrongChainId::InvalidNamespaceSuffix { tx_chain_id, .. } => {
                    tx_chain_id
                }
            };
            return Err(JsonRpcError::invalid_chain_id(base_chain_id, tx_chain_id));
        }
    };

    if route_namespace.is_some() && tx_namespace != route_namespace {
        return Err(JsonRpcError::invalid_chain_id(
            route_chain_id,
            tx.chain_id().unwrap_or(base_chain_id),
        ));
    }

    Ok(tx)
}

fn validate_and_decode_batch(
    raw_batch: &[u8],
    base_chain_id: u64,
    route_chain_id: u64,
    route_namespace: Option<Address>,
) -> Result<NamespaceTransactionBatch, JsonRpcError> {
    let batch = alloy_rlp::decode_exact::<NamespaceTransactionBatch>(raw_batch).map_err(|err| {
        debug!(?err, "namespace batch decode failed");
        JsonRpcError::txn_decode_error()
    })?;

    if batch.transactions.is_empty() {
        return Err(JsonRpcError::custom(
            "namespace transaction batch cannot be empty".to_string(),
        ));
    }

    if batch.recover_signer().is_err() {
        return Err(JsonRpcError::custom(
            "invalid namespace transaction batch signature".to_string(),
        ));
    }

    let mut batch_namespace = None;
    for tx in batch.transactions.iter() {
        let tx_namespace = match namespace_for_chain_id(tx.chain_id(), base_chain_id) {
            Ok(Some(namespace)) => namespace,
            Ok(None) => {
                return Err(JsonRpcError::invalid_chain_id(
                    route_chain_id,
                    tx.chain_id().unwrap_or(base_chain_id),
                ));
            }
            Err(monad_eth_types::WrongChainId::InvalidNamespaceSuffix { tx_chain_id, .. }) => {
                return Err(JsonRpcError::invalid_chain_id(base_chain_id, tx_chain_id));
            }
        };

        if route_namespace.is_some() && Some(tx_namespace) != route_namespace {
            return Err(JsonRpcError::invalid_chain_id(
                route_chain_id,
                tx.chain_id().unwrap_or(base_chain_id),
            ));
        }

        if batch_namespace.is_some_and(|namespace| namespace != tx_namespace) {
            return Err(JsonRpcError::custom(
                "namespace transaction batch contains mixed namespaces".to_string(),
            ));
        }
        batch_namespace = Some(tx_namespace);
    }

    Ok(batch)
}

async fn submit_to_txpool(
    txpool_bridge_client: &EthTxPoolBridgeClient,
    tx: EthTxEnvelope,
) -> Result<(), JsonRpcError> {
    let Some(_tx_inflight_guard) = txpool_bridge_client.acquire_tx_inflight_guard() else {
        warn!("txpool overloaded");
        return Err(JsonRpcError::overloaded());
    };

    let (tx_status_recv_send, tx_status_recv_recv) =
        tokio::sync::oneshot::channel::<tokio::sync::watch::Receiver<TxStatus>>();

    if let Err(err) = txpool_bridge_client.try_send(tx, tx_status_recv_send) {
        error!(
            ?err,
            "txpool bridge try_send error after acquiring tx_inflight_guard"
        );
        return Err(JsonRpcError::overloaded());
    }

    let mut tx_status_recv =
        match tokio::time::timeout(Duration::from_secs(1), tx_status_recv_recv).await {
            Ok(Ok(tx_status_recv)) => tx_status_recv,
            Ok(Err(_)) | Err(_) => {
                warn!("txpool bridge not responding, tx status receiver was not sent");
                return Err(JsonRpcError::overloaded());
            }
        };

    match tokio::time::timeout(Duration::from_secs(1), tx_status_recv.changed()).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            // If the tx_status_send was dropped, then the tx was evicted from RPC state
            return match tx_status_recv.borrow().to_owned() {
                TxStatus::Unknown => Err(JsonRpcError::overloaded()),
                TxStatus::Tracked
                | TxStatus::Dropped { .. }
                | TxStatus::Evicted { .. }
                | TxStatus::Committed => Err(JsonRpcError::custom(
                    "rpc no longer tracking tx".to_string(),
                )),
            };
        }
        Err(_) => {
            // If the changed future times out, RPC should still try returning whatever status it
            // currently has, even if it might be stale.
            warn!("txpool bridge not responding, tx status has not changed");
        }
    }

    let latest_tx_status = tx_status_recv.borrow_and_update().to_owned();

    match latest_tx_status {
        TxStatus::Evicted { reason: _ } => Err(JsonRpcError::custom("rejected".to_string())),
        TxStatus::Dropped { reason } => Err(JsonRpcError::custom(reason.as_user_string())),
        TxStatus::Tracked | TxStatus::Committed => Ok(()),
        TxStatus::Unknown => {
            warn!("txpool tx status last value was unknown");
            Err(JsonRpcError::overloaded())
        }
    }
}

async fn submit_batch_to_txpool(
    txpool_bridge_client: &EthTxPoolBridgeClient,
    batch: NamespaceTransactionBatch,
) -> Result<(), JsonRpcError> {
    let Some(_tx_inflight_guard) = txpool_bridge_client.acquire_tx_inflight_guard() else {
        warn!("txpool overloaded");
        return Err(JsonRpcError::overloaded());
    };

    let mut tx_status_receivers = Vec::with_capacity(batch.transactions.len());
    let tx_status_recv_sends = (0..batch.transactions.len())
        .map(|_| {
            let (tx_status_recv_send, tx_status_recv_recv) =
                tokio::sync::oneshot::channel::<tokio::sync::watch::Receiver<TxStatus>>();
            tx_status_receivers.push(tx_status_recv_recv);
            tx_status_recv_send
        })
        .collect::<Vec<_>>();

    if let Err(err) = txpool_bridge_client.try_send_batch(batch, tx_status_recv_sends) {
        error!(
            ?err,
            "txpool bridge batch try_send error after acquiring tx_inflight_guard"
        );
        return Err(JsonRpcError::overloaded());
    }

    for tx_status_recv_recv in tx_status_receivers {
        wait_for_txpool_status(tx_status_recv_recv).await?;
    }

    Ok(())
}

async fn wait_for_txpool_status(
    tx_status_recv_recv: tokio::sync::oneshot::Receiver<tokio::sync::watch::Receiver<TxStatus>>,
) -> Result<(), JsonRpcError> {
    let mut tx_status_recv =
        match tokio::time::timeout(Duration::from_secs(1), tx_status_recv_recv).await {
            Ok(Ok(tx_status_recv)) => tx_status_recv,
            Ok(Err(_)) | Err(_) => {
                warn!("txpool bridge not responding, tx status receiver was not sent");
                return Err(JsonRpcError::overloaded());
            }
        };

    match tokio::time::timeout(Duration::from_secs(1), tx_status_recv.changed()).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            return match tx_status_recv.borrow().to_owned() {
                TxStatus::Unknown => Err(JsonRpcError::overloaded()),
                TxStatus::Tracked
                | TxStatus::Dropped { .. }
                | TxStatus::Evicted { .. }
                | TxStatus::Committed => Err(JsonRpcError::custom(
                    "rpc no longer tracking tx".to_string(),
                )),
            };
        }
        Err(_) => {
            warn!("txpool bridge not responding, tx status has not changed");
        }
    }

    let latest_tx_status = tx_status_recv.borrow_and_update().to_owned();

    match latest_tx_status {
        TxStatus::Evicted { reason: _ } => Err(JsonRpcError::custom("rejected".to_string())),
        TxStatus::Dropped { reason } => Err(JsonRpcError::custom(reason.as_user_string())),
        TxStatus::Tracked | TxStatus::Committed => Ok(()),
        TxStatus::Unknown => {
            warn!("txpool tx status last value was unknown");
            Err(JsonRpcError::overloaded())
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MonadEthSendRawTransactionSyncParams {
    hex_tx: UnformattedData,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// Poll interval in milliseconds for checking receipt availability
const RECEIPT_POLL_INTERVAL_MS: u64 = 100;

async fn poll_for_preconfirmed_receipt<T: Triedb>(
    namespace_preconfirmation_service: &NamespacePreconfirmationService,
    data_provider: &DataProvider<T>,
    eth_call_executor: &EthCallExecutor,
    base_chain_id: u64,
    route_namespace: Address,
    tx_hash: TxHash,
    timeout_ms: u64,
) -> Result<TransactionReceipt, JsonRpcError> {
    let start_time = tokio::time::Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    let poll_interval = Duration::from_millis(RECEIPT_POLL_INTERVAL_MS);

    loop {
        if let Some(receipt) = namespace_preconfirmation_service
            .simulate_receipt_until(
                data_provider,
                eth_call_executor,
                base_chain_id,
                route_namespace,
                tx_hash,
            )
            .await?
        {
            return Ok(receipt);
        }

        if start_time.elapsed() >= timeout {
            return Err(JsonRpcError::tx_sync_timeout(
                tx_hash.to_string(),
                timeout_ms,
            ));
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[rpc(
    method = "eth_sendRawTransactionSync",
    ignore = "txpool_bridge_client,event_server_client,base_chain_id,route_chain_id,route_namespace,allow_unprotected_txs,eth_send_raw_transaction_sync_default_timeout_ms,eth_send_raw_transaction_sync_max_timeout_ms"
)]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_eth_sendRawTransactionSync(
    txpool_bridge_client: &EthTxPoolBridgeClient,
    event_server_client: &EventServerClient,
    params: MonadEthSendRawTransactionSyncParams,
    base_chain_id: u64,
    route_chain_id: u64,
    route_namespace: Option<Address>,
    allow_unprotected_txs: bool,
    eth_send_raw_transaction_sync_default_timeout_ms: u64,
    eth_send_raw_transaction_sync_max_timeout_ms: u64,
) -> JsonRpcResult<MonadTransactionReceipt> {
    trace!("monad_eth_sendRawTransactionSync: {params:?}");

    let timeout_ms = params
        .timeout_ms
        .filter(|&t| t > 0 && t <= eth_send_raw_transaction_sync_max_timeout_ms)
        .unwrap_or(eth_send_raw_transaction_sync_default_timeout_ms);

    let tx = validate_and_decode_tx(
        &params.hex_tx.0,
        base_chain_id,
        route_chain_id,
        route_namespace,
        allow_unprotected_txs,
        JsonRpcError::tx_sync_unready,
    )?;

    let Ok(mut event_server_subscription) = event_server_client.subscribe() else {
        return Err(JsonRpcError::overloaded());
    };

    let tx_hash = *tx.tx_hash();
    debug!(name = "sendRawTransactionSync", txn_hash = ?tx_hash);
    submit_to_txpool(txpool_bridge_client, tx).await?;

    let mut timeout = pin!(tokio::time::sleep(Duration::from_millis(timeout_ms)));

    loop {
        let result = tokio::select! {
            result = event_server_subscription.recv() => result,

            () = &mut timeout => {
                // EIP-7966: Error code 4 with tx hash in data
                return Err(JsonRpcError::tx_sync_timeout(
                    tx_hash.to_string(),
                    timeout_ms,
                ));
            }
        };

        let Ok(event) = result else {
            return Err(JsonRpcError::overloaded());
        };

        match event {
            EventServerEvent::Gap => {
                return Err(JsonRpcError::overloaded());
            }
            EventServerEvent::Block {
                commit_state,
                header: _,
                transactions,
            } => {
                if commit_state != BlockCommitState::Proposed {
                    continue;
                }

                for (tx, tx_receipt, _) in transactions.iter() {
                    if tx.value().inner.tx_hash() != &tx_hash {
                        continue;
                    }

                    return Ok(MonadTransactionReceipt(tx_receipt.value().clone()));
                }
            }
        }
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionReceiptParams {
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionReceipt")]
#[allow(non_snake_case)]
/// Returns the receipt of a transaction by transaction hash.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_eth_getTransactionReceipt<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetTransactionReceiptParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_eth_getTransactionReceipt: {params:?}");

    data_provider
        .get_transaction_receipt(&FixedBytes(params.tx_hash.0))
        .await
        .map_present_and_no_err(MonadTransactionReceipt)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByHashParams {
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionByHash")]
#[allow(non_snake_case)]
/// Returns the information about a transaction requested by transaction hash.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn monad_eth_getTransactionByHash<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetTransactionByHashParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    data_provider
        .get_transaction(&FixedBytes(params.tx_hash.0))
        .await
        .map_present_and_no_err(MonadTransaction)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockHashAndIndexParams {
    block_hash: EthHash,
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockHashAndIndex")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns information about a transaction by block hash and transaction index position.
pub async fn monad_eth_getTransactionByBlockHashAndIndex<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetTransactionByBlockHashAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    data_provider
        .get_transaction_with_block_and_index(
            BlockTagOrHash::Hash(params.block_hash),
            params.index.0,
        )
        .await
        .map_present_and_no_err(MonadTransaction)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockNumberAndIndexParams {
    block_tag: BlockTags,
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockNumberAndIndex")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns information about a transaction by block number and transaction index position.
pub async fn monad_eth_getTransactionByBlockNumberAndIndex<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetTransactionByBlockNumberAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    data_provider
        .get_transaction_with_block_and_index(
            crate::types::eth_json::BlockTagOrHash::BlockTags(params.block_tag),
            params.index.0,
        )
        .await
        .map_present_and_no_err(MonadTransaction)
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, Transaction as _, TxEip1559, TxEnvelope};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Address, FixedBytes, Signature, TxKind};
    use alloy_rlp::Encodable;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_eth_testutil::{make_namespaced_legacy_tx, make_representable_namespace};
    use monad_eth_types::{chain_id_for_namespace, EthAccount};
    use monad_event_ring::SnapshotEventRing;
    use monad_triedb_utils::mock_triedb::MockTriedb;
    use std::time::Duration;

    use super::{
        monad_eth_sendRawTransaction, monad_eth_sendRawTransactionSync,
        monad_sendRawTransactionPreconfirmed, MonadEthSendRawTransactionParams,
        MonadEthSendRawTransactionSyncParams,
    };
    use crate::{
        event::EventServer,
        preconfirmation::NamespacePreconfirmationService,
        txpool::EthTxPoolBridgeClient,
        types::{eth_json::UnformattedData, jsonrpc::JsonRpcError},
    };

    fn serialize_tx(tx: impl Encodable + Encodable2718) -> UnformattedData {
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        UnformattedData(rlp_encoded_tx)
    }

    fn make_tx(
        sender: FixedBytes<32>,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
        gas_limit: u64,
        nonce: u64,
        chain_id: u64,
    ) -> TxEnvelope {
        let transaction = TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(Address::repeat_byte(0u8)),
            value: Default::default(),
            access_list: Default::default(),
            input: vec![].into(),
        };

        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();
        transaction.into_signed(signature).into()
    }

    #[tokio::test]
    async fn eth_send_raw_transaction() {
        let mut triedb = MockTriedb::default();
        let sender = FixedBytes::<32>::from([1u8; 32]);
        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();

        triedb.set_account(
            signer.address().0.into(),
            EthAccount {
                nonce: 10,
                ..Default::default()
            },
        );

        let expected_failures = [
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 21_000, 11, 1337)), // invaid chain id
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 1_000, 11, 1)), // intrinsic gas too low
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 400_000_000_000, 11, 1)), // gas too high
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 21_000, 1, 1)), // nonce too low
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 12000, 21_000, 11, 1)), // max priority fee too high
            },
        ];

        let txpool_bridge_client = EthTxPoolBridgeClient::for_testing();
        for (idx, case) in expected_failures.into_iter().enumerate() {
            assert!(
                monad_eth_sendRawTransaction(&txpool_bridge_client, case, 1, 1, None, true,)
                    .await
                    .is_err(),
                "Expected error for case: {:?}",
                idx + 1
            );
        }
    }

    #[tokio::test]
    async fn eth_send_raw_transaction_sync() {
        let mut triedb = MockTriedb::default();
        let sender = FixedBytes::<32>::from([1u8; 32]);
        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();

        triedb.set_account(
            signer.address().0.into(),
            EthAccount {
                nonce: 10,
                ..Default::default()
            },
        );

        let snapshot_event_ring = SnapshotEventRing::new_from_zstd_bytes(
            "TEST",
            include_bytes!(
                "../../../../monad-execution/rust/crates/monad-exec-events/test/data/exec-events-emn-30b-15m/snapshot.zst"
            ),
            None,
        )
        .unwrap();

        let event_server_client = EventServer::start_for_testing(snapshot_event_ring);

        // Test the same validation failures as eth_sendRawTransaction
        // to ensure both methods have consistent validation
        let expected_failures = [
            MonadEthSendRawTransactionSyncParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 21_000, 11, 1337)), // invalid chain id
                timeout_ms: Some(2000),
            },
            MonadEthSendRawTransactionSyncParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 1_000, 11, 1)), // intrinsic gas too low
                timeout_ms: Some(2000),
            },
            MonadEthSendRawTransactionSyncParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 400_000_000_000, 11, 1)), // gas too high
                timeout_ms: Some(2000),
            },
            MonadEthSendRawTransactionSyncParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 21_000, 1, 1)), // nonce too low
                timeout_ms: Some(2000),
            },
            MonadEthSendRawTransactionSyncParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 12000, 21_000, 11, 1)), // max priority fee too high
                timeout_ms: Some(2000),
            },
        ];

        for (idx, case) in expected_failures.into_iter().enumerate() {
            assert!(
                monad_eth_sendRawTransactionSync(
                    &EthTxPoolBridgeClient::for_testing(),
                    &event_server_client,
                    case,
                    1,
                    1,
                    None,
                    true,
                    2000,
                    30000,
                )
                .await
                .is_err(),
                "Expected error for case: {:?}",
                idx + 1
            );
        }
    }

    #[tokio::test]
    async fn send_raw_transaction_preconfirmed_returns_signed_object() {
        let operator = PrivateKeySigner::from_bytes(&FixedBytes::repeat_byte(0x44)).unwrap();
        let service = NamespacePreconfirmationService::for_testing(
            operator.clone(),
            8,
            8,
            Duration::from_secs(60),
        );
        let namespace = make_representable_namespace(1);
        let route_chain_id = chain_id_for_namespace(namespace, 1337).unwrap();
        let tx = make_namespaced_legacy_tx(
            namespace,
            FixedBytes::repeat_byte(0x11),
            1_000,
            21_000,
            0,
            0,
        );
        let tx_hash = *tx.tx_hash();

        let preconfirmation = monad_sendRawTransactionPreconfirmed(
            &service,
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(tx),
            },
            1337,
            route_chain_id,
            Some(namespace),
            true,
        )
        .await
        .unwrap();

        assert_eq!(preconfirmation.tx_hash.to_string(), tx_hash.to_string());
        assert_eq!(
            preconfirmation.operator.to_string(),
            operator.address().to_string()
        );
        assert_eq!(
            service.get(&tx_hash).unwrap().preconfirmation_id,
            preconfirmation.preconfirmation_id
        );

        let signature = preconfirmation.signature.parse::<Signature>().unwrap();
        assert_eq!(
            signature
                .recover_address_from_msg(preconfirmation.message.as_bytes())
                .unwrap(),
            operator.address()
        );
    }

    #[tokio::test]
    async fn eth_send_raw_transaction_namespace_route_still_uses_txpool() {
        let namespace = make_representable_namespace(1);
        let route_chain_id = chain_id_for_namespace(namespace, 1337).unwrap();
        let tx = make_namespaced_legacy_tx(
            namespace,
            FixedBytes::repeat_byte(0x11),
            1_000,
            21_000,
            0,
            0,
        );
        let txpool_bridge_client = EthTxPoolBridgeClient::for_testing();

        let result = monad_eth_sendRawTransaction(
            &txpool_bridge_client,
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(tx),
            },
            1337,
            route_chain_id,
            Some(namespace),
            true,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().message,
            JsonRpcError::overloaded().message
        );
    }

    #[tokio::test]
    async fn preconfirmation_rejects_wrong_namespace_route() {
        let operator = PrivateKeySigner::from_bytes(&FixedBytes::repeat_byte(0x44)).unwrap();
        let service =
            NamespacePreconfirmationService::for_testing(operator, 8, 8, Duration::from_secs(60));
        let tx_namespace = make_representable_namespace(1);
        let route_namespace = make_representable_namespace(2);
        let route_chain_id = chain_id_for_namespace(route_namespace, 1337).unwrap();
        let tx = make_namespaced_legacy_tx(
            tx_namespace,
            FixedBytes::repeat_byte(0x11),
            1_000,
            21_000,
            0,
            0,
        );

        assert!(monad_sendRawTransactionPreconfirmed(
            &service,
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(tx),
            },
            1337,
            route_chain_id,
            Some(route_namespace),
            true,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn get_preconfirmation_returns_null_after_ttl() {
        let operator = PrivateKeySigner::from_bytes(&FixedBytes::repeat_byte(0x44)).unwrap();
        let service =
            NamespacePreconfirmationService::for_testing(operator, 8, 8, Duration::from_millis(1));
        let namespace = make_representable_namespace(1);
        let route_chain_id = chain_id_for_namespace(namespace, 1337).unwrap();
        let tx = make_namespaced_legacy_tx(
            namespace,
            FixedBytes::repeat_byte(0x11),
            1_000,
            21_000,
            0,
            0,
        );
        let tx_hash = *tx.tx_hash();

        monad_sendRawTransactionPreconfirmed(
            &service,
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(tx),
            },
            1337,
            route_chain_id,
            Some(namespace),
            true,
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(2)).await;

        assert!(service.get(&tx_hash).is_none());
    }

    #[tokio::test]
    async fn preconfirmation_returns_overloaded_when_queue_full() {
        let operator = PrivateKeySigner::from_bytes(&FixedBytes::repeat_byte(0x44)).unwrap();
        let service =
            NamespacePreconfirmationService::for_testing(operator, 1, 100, Duration::from_secs(60));
        let namespace = make_representable_namespace(1);
        let route_chain_id = chain_id_for_namespace(namespace, 1337).unwrap();

        for nonce in 0..2 {
            let tx = make_namespaced_legacy_tx(
                namespace,
                FixedBytes::repeat_byte(0x11),
                1_000,
                21_000,
                nonce,
                0,
            );

            let result = monad_sendRawTransactionPreconfirmed(
                &service,
                MonadEthSendRawTransactionParams {
                    hex_tx: serialize_tx(tx),
                },
                1337,
                route_chain_id,
                Some(namespace),
                true,
            )
            .await;

            if nonce == 0 {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
    }
}
