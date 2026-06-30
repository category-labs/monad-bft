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

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::{
    transaction::SignerRecoverable, Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
    Transaction as _,
};
use alloy_primitives::{keccak256, logs_bloom, Address, Bytes, TxHash, TxKind, B256, U256, U64};
use alloy_rpc_types::{Log, TransactionReceipt};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use dashmap::DashMap;
use monad_eth_types::{EthTxEnvelope, NamespaceBatchSignature, NamespaceTransactionBatch};
use monad_ethcall::{
    eth_call, CallResult, EthCallExecutor, EthCallRequest, MonadTracer, StateOverrideObject,
    StateOverrideSet, StorageOverride,
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tracing::{error, trace, warn};

use crate::{
    data::{get_latest_block_key, DataProvider},
    handlers::{decode_receipt_logs_from_call_trace, parse_ethcall_chain_id},
    txpool::{EthTxPoolBridgeClient, TxStatus},
    types::{
        eth_json::{EthAddress, EthHash, Quantity},
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
};
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb};
use monad_types::{BlockId, Hash, SeqNum};

const SIGNATURE_TYPE_EIP191: &str = "eip191";
const PRECONFIRMATION_MESSAGE_PREFIX: &str = "Monad Namespace Preconfirmation v1";

#[derive(Clone, Debug)]
pub struct NamespacePreconfirmationConfig {
    pub operator_key_path: PathBuf,
    pub max_queued_txs: usize,
    pub max_batch_txs: usize,
    pub flush_interval: Duration,
    pub preconfirmation_ttl: Duration,
}

#[derive(Debug)]
pub enum NamespacePreconfirmationInitError {
    EmptyOperatorKey,
    InvalidOperatorKey(String),
    OperatorKeyRead(std::io::Error),
}

impl Display for NamespacePreconfirmationInitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyOperatorKey => {
                f.write_str("namespace preconfirmation operator key is empty")
            }
            Self::InvalidOperatorKey(err) => {
                write!(f, "invalid namespace preconfirmation operator key: {err}")
            }
            Self::OperatorKeyRead(err) => {
                write!(
                    f,
                    "failed to read namespace preconfirmation operator key: {err}"
                )
            }
        }
    }
}

impl std::error::Error for NamespacePreconfirmationInitError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespacePreconfirmation {
    pub tx_hash: EthHash,
    pub base_chain_id: Quantity,
    pub chain_id: Quantity,
    pub namespace: EthAddress,
    pub operator: EthAddress,
    pub queued_at_unix_millis: Quantity,
    pub preconfirmation_id: EthHash,
    pub signature_type: String,
    pub message: String,
    pub signature: String,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum PreconfirmationStatus {
    Queued,
    Submitted,
    Tracked,
    Dropped,
    Evicted,
    Committed,
}

#[derive(Clone, Debug)]
struct CachedPreconfirmation {
    preconfirmation: NamespacePreconfirmation,
    expires_at: tokio::time::Instant,
    status: PreconfirmationStatus,
}

#[derive(Clone, Debug)]
struct CachedSyntheticReceipt {
    receipt: TransactionReceipt,
    expires_at: tokio::time::Instant,
}

#[derive(Clone, Debug)]
struct QueuedTx {
    tx: EthTxEnvelope,
    namespace: Address,
}

struct NamespacePreconfirmationInner {
    txpool_bridge_client: EthTxPoolBridgeClient,
    signer: PrivateKeySigner,
    operator: Address,
    max_queued_txs: usize,
    max_batch_txs: usize,
    flush_interval: Duration,
    preconfirmation_ttl: Duration,
    queue: Mutex<VecDeque<QueuedTx>>,
    active_txs: Mutex<Vec<QueuedTx>>,
    queue_notify: Notify,
    cache: DashMap<TxHash, CachedPreconfirmation>,
    synthetic_receipts: DashMap<TxHash, CachedSyntheticReceipt>,
}

#[derive(Clone)]
pub struct NamespacePreconfirmationService {
    inner: Arc<NamespacePreconfirmationInner>,
}

impl NamespacePreconfirmationService {
    pub fn start(
        config: NamespacePreconfirmationConfig,
        txpool_bridge_client: EthTxPoolBridgeClient,
    ) -> Result<Self, NamespacePreconfirmationInitError> {
        let signer = load_operator_signer(&config.operator_key_path)?;
        let service = Self::new(config, txpool_bridge_client, signer);

        let inner = Arc::clone(&service.inner);
        tokio::spawn(async move {
            run_batcher(inner).await;
        });

        Ok(service)
    }

    fn new(
        config: NamespacePreconfirmationConfig,
        txpool_bridge_client: EthTxPoolBridgeClient,
        signer: PrivateKeySigner,
    ) -> Self {
        let max_queued_txs = config.max_queued_txs.max(1);
        let max_batch_txs = config.max_batch_txs.max(1).min(max_queued_txs);
        let flush_interval = config.flush_interval.max(Duration::from_millis(1));

        Self {
            inner: Arc::new(NamespacePreconfirmationInner {
                txpool_bridge_client,
                operator: signer.address(),
                signer,
                max_queued_txs,
                max_batch_txs,
                flush_interval,
                preconfirmation_ttl: config.preconfirmation_ttl,
                queue: Mutex::new(VecDeque::with_capacity(max_queued_txs)),
                active_txs: Mutex::new(Vec::with_capacity(max_queued_txs)),
                queue_notify: Notify::new(),
                cache: DashMap::default(),
                synthetic_receipts: DashMap::default(),
            }),
        }
    }

    #[cfg(test)]
    pub fn for_testing(
        signer: PrivateKeySigner,
        max_queued_txs: usize,
        max_batch_txs: usize,
        preconfirmation_ttl: Duration,
    ) -> Self {
        Self::new(
            NamespacePreconfirmationConfig {
                operator_key_path: PathBuf::new(),
                max_queued_txs,
                max_batch_txs,
                flush_interval: Duration::from_secs(60),
                preconfirmation_ttl,
            },
            EthTxPoolBridgeClient::for_testing(),
            signer,
        )
    }

    pub fn operator(&self) -> Address {
        self.inner.operator
    }

    pub async fn submit(
        &self,
        tx: EthTxEnvelope,
        base_chain_id: u64,
        route_chain_id: u64,
        namespace: Address,
    ) -> JsonRpcResult<NamespacePreconfirmation> {
        tx.recover_signer().map_err(|err| {
            trace!(
                ?err,
                "namespace preconfirmation tx signature recovery failed"
            );
            JsonRpcError::custom("Transaction signature is invalid".to_string())
        })?;

        let tx_hash = *tx.tx_hash();
        self.remove_expired_preconfirmations();

        if let Some(cached) = self.get_cached(&tx_hash) {
            return Ok(cached);
        }

        let queued_at_unix_millis = unix_time_millis()?;
        let preconfirmation = build_preconfirmation(
            &self.inner.signer,
            self.inner.operator,
            base_chain_id,
            route_chain_id,
            namespace,
            tx_hash,
            queued_at_unix_millis,
        )?;
        let expires_at = tokio::time::Instant::now() + self.inner.preconfirmation_ttl;

        let queued_tx = QueuedTx { tx, namespace };

        let should_notify = {
            let mut queue = self.inner.queue.lock().await;

            if let Some(cached) = self.get_cached(&tx_hash) {
                return Ok(cached);
            }

            if queue.len() >= self.inner.max_queued_txs {
                return Err(JsonRpcError::overloaded());
            }

            queue.push_back(queued_tx.clone());

            queue.len() >= self.inner.max_batch_txs
        };

        self.inner.active_txs.lock().await.push(queued_tx);

        self.inner.cache.insert(
            tx_hash,
            CachedPreconfirmation {
                preconfirmation: preconfirmation.clone(),
                expires_at,
                status: PreconfirmationStatus::Queued,
            },
        );

        if should_notify {
            self.inner.queue_notify.notify_one();
        }

        Ok(preconfirmation)
    }

    pub fn get(&self, tx_hash: &TxHash) -> Option<NamespacePreconfirmation> {
        self.remove_expired_preconfirmations();
        self.get_cached(tx_hash)
    }

    pub fn get_synthetic_receipt(&self, tx_hash: &TxHash) -> Option<TransactionReceipt> {
        self.remove_expired_preconfirmations();
        let now = tokio::time::Instant::now();
        let cached = self.inner.synthetic_receipts.get(tx_hash)?;
        if cached.expires_at <= now {
            drop(cached);
            self.inner.synthetic_receipts.remove(tx_hash);
            return None;
        }

        Some(cached.receipt.clone())
    }

    pub async fn simulate_receipt_until<T: Triedb>(
        &self,
        data_provider: &DataProvider<T>,
        eth_call_executor: &EthCallExecutor,
        base_chain_id: u64,
        namespace: Address,
        tx_hash: TxHash,
    ) -> JsonRpcResult<Option<TransactionReceipt>> {
        if let Some(receipt) = self.get_synthetic_receipt(&tx_hash) {
            return Ok(Some(receipt));
        }

        let simulation = self
            .simulate_active_txs(
                data_provider,
                eth_call_executor,
                base_chain_id,
                namespace,
                Some(tx_hash),
            )
            .await?;
        Ok(simulation.target_receipt)
    }

    pub async fn simulate_state_overrides<T: Triedb>(
        &self,
        data_provider: &DataProvider<T>,
        eth_call_executor: &EthCallExecutor,
        base_chain_id: u64,
        namespace: Address,
    ) -> JsonRpcResult<StateOverrideSet> {
        self.simulate_active_txs(
            data_provider,
            eth_call_executor,
            base_chain_id,
            namespace,
            None,
        )
        .await
        .map(|simulation| simulation.state_overrides)
    }

    async fn active_txs(&self, namespace: Address) -> Vec<QueuedTx> {
        self.remove_expired_preconfirmations();
        let cache = &self.inner.cache;
        let mut active_txs = self.inner.active_txs.lock().await;
        active_txs.retain(|queued_tx| {
            cache.get(queued_tx.tx.tx_hash()).is_some_and(|cached| {
                matches!(
                    cached.status,
                    PreconfirmationStatus::Queued
                        | PreconfirmationStatus::Submitted
                        | PreconfirmationStatus::Tracked
                )
            })
        });
        active_txs
            .iter()
            .filter(|queued_tx| queued_tx.namespace == namespace)
            .cloned()
            .collect()
    }

    async fn simulate_active_txs<T: Triedb>(
        &self,
        data_provider: &DataProvider<T>,
        eth_call_executor: &EthCallExecutor,
        base_chain_id: u64,
        namespace: Address,
        target: Option<TxHash>,
    ) -> JsonRpcResult<PreconfirmationSimulation> {
        let active_txs = self.active_txs(namespace).await;
        if target.is_some_and(|target| {
            !active_txs
                .iter()
                .any(|queued_tx| *queued_tx.tx.tx_hash() == target)
        }) {
            return Ok(PreconfirmationSimulation::default());
        }

        let block_key = get_latest_block_key(&data_provider.triedb_env);
        let header = data_provider
            .triedb_env
            .get_block_header(block_key)
            .await
            .map_err(JsonRpcError::internal_error)?
            .ok_or_else(JsonRpcError::block_not_found)?
            .header;
        let (block_number, block_id) = block_key_to_parts(block_key);
        let ethcall_chain_id = parse_ethcall_chain_id(base_chain_id)?;

        let mut state_overrides = StateOverrideSet::default();
        let mut cumulative_gas_used = 0u64;
        let mut target_receipt = None;

        for queued_tx in active_txs {
            let tx_hash = *queued_tx.tx.tx_hash();
            let sender = queued_tx.tx.recover_signer().map_err(|err| {
                JsonRpcError::internal_error(format!(
                    "preconfirmed tx signer recovery failed during simulation: {err}"
                ))
            })?;

            let simulation_result = simulate_one_tx(
                eth_call_executor,
                ethcall_chain_id,
                &queued_tx.tx,
                sender,
                &header,
                block_number,
                block_id,
                &state_overrides,
            )
            .await?;

            cumulative_gas_used = cumulative_gas_used.saturating_add(simulation_result.gas_used);
            if simulation_result.success {
                apply_state_diff(&mut state_overrides, &simulation_result.state_diff)?;
            }

            let receipt = synthetic_receipt(
                &queued_tx.tx,
                sender,
                simulation_result.success,
                simulation_result.gas_used,
                cumulative_gas_used,
                simulation_result.logs,
                header.base_fee_per_gas,
            );
            self.cache_synthetic_receipt(tx_hash, receipt.clone());

            if target == Some(tx_hash) {
                target_receipt = Some(receipt);
                break;
            }
        }

        Ok(PreconfirmationSimulation {
            state_overrides,
            target_receipt,
        })
    }

    fn cache_synthetic_receipt(&self, tx_hash: TxHash, receipt: TransactionReceipt) {
        self.inner.synthetic_receipts.insert(
            tx_hash,
            CachedSyntheticReceipt {
                receipt,
                expires_at: tokio::time::Instant::now() + self.inner.preconfirmation_ttl,
            },
        );
    }

    fn get_cached(&self, tx_hash: &TxHash) -> Option<NamespacePreconfirmation> {
        let now = tokio::time::Instant::now();
        let cached = self.inner.cache.get(tx_hash)?;
        if cached.expires_at <= now {
            drop(cached);
            self.inner.cache.remove(tx_hash);
            return None;
        }

        Some(cached.preconfirmation.clone())
    }

    fn remove_expired_preconfirmations(&self) {
        let now = tokio::time::Instant::now();
        self.inner.cache.retain(|_, cached| cached.expires_at > now);
        self.inner
            .synthetic_receipts
            .retain(|_, cached| cached.expires_at > now);
    }
}

#[derive(Default)]
struct PreconfirmationSimulation {
    state_overrides: StateOverrideSet,
    target_receipt: Option<TransactionReceipt>,
}

struct SimulatedTxResult {
    success: bool,
    gas_used: u64,
    logs: Vec<Log>,
    state_diff: Value,
}

async fn simulate_one_tx(
    eth_call_executor: &EthCallExecutor,
    chain_id: monad_ethcall::ChainId,
    tx: &EthTxEnvelope,
    sender: Address,
    header: &alloy_consensus::Header,
    block_number: u64,
    block_id: Option<[u8; 32]>,
    state_overrides: &StateOverrideSet,
) -> JsonRpcResult<SimulatedTxResult> {
    let receipt_call = eth_call(
        EthCallRequest {
            chain_id,
            transaction: tx,
            block_header: header,
            sender,
            block_number,
            block_id,
            state_override_set: state_overrides,
            tracer: MonadTracer::NoopTracer,
            gas_specified: true,
        },
        eth_call_executor,
    )
    .await;

    let (success, gas_used) = match receipt_call {
        CallResult::Success(success) => (true, success.gas_used),
        CallResult::Failure(failure)
            if matches!(
                failure.error_code,
                monad_ethcall::EthCallResult::ExecutionError
                    | monad_ethcall::EthCallResult::OutOfGas
            ) =>
        {
            (false, failure.gas_used)
        }
        CallResult::Failure(failure) => {
            return Err(JsonRpcError::eth_call_error(failure.message, failure.data));
        }
        CallResult::Revert(_) => {
            return Err(JsonRpcError::internal_error(
                "unexpected trace result from receipt simulation".to_string(),
            ));
        }
    };

    if !success {
        return Ok(SimulatedTxResult {
            success,
            gas_used,
            logs: Vec::new(),
            state_diff: Value::Object(Default::default()),
        });
    }

    let diff_call = eth_call(
        EthCallRequest {
            chain_id,
            transaction: tx,
            block_header: header,
            sender,
            block_number,
            block_id,
            state_override_set: state_overrides,
            tracer: MonadTracer::StateDiffTracer,
            gas_specified: true,
        },
        eth_call_executor,
    )
    .await;

    let output_data = match diff_call {
        CallResult::Success(success) => success.output_data,
        CallResult::Failure(failure) => {
            return Err(JsonRpcError::eth_call_error(failure.message, failure.data));
        }
        CallResult::Revert(result) => result.trace,
    };
    let state_diff = serde_cbor::from_slice(&output_data)
        .map_err(|err| JsonRpcError::internal_error(format!("state diff decode error: {err}")))?;
    let trace_call = eth_call(
        EthCallRequest {
            chain_id,
            transaction: tx,
            block_header: header,
            sender,
            block_number,
            block_id,
            state_override_set: state_overrides,
            tracer: MonadTracer::CallTracer,
            gas_specified: true,
        },
        eth_call_executor,
    )
    .await;
    let trace_output_data = match trace_call {
        CallResult::Success(success) => success.output_data,
        CallResult::Failure(failure) => {
            return Err(JsonRpcError::eth_call_error(failure.message, failure.data));
        }
        CallResult::Revert(result) => result.trace,
    };
    let mut trace_output = trace_output_data.as_slice();
    let logs = decode_receipt_logs_from_call_trace(&mut trace_output)?
        .into_iter()
        .map(|inner| Log {
            inner,
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: Some(*tx.tx_hash()),
            transaction_index: None,
            log_index: None,
            removed: false,
        })
        .collect();

    Ok(SimulatedTxResult {
        success,
        gas_used,
        logs,
        state_diff,
    })
}

fn block_key_to_parts(block_key: BlockKey) -> (u64, Option<[u8; 32]>) {
    match block_key {
        BlockKey::Finalized(FinalizedBlockKey(SeqNum(n))) => (n, None),
        BlockKey::Proposed(ProposedBlockKey(SeqNum(n), BlockId(Hash(id)))) => (n, Some(id)),
    }
}

fn apply_state_diff(overrides: &mut StateOverrideSet, state_diff: &Value) -> JsonRpcResult<()> {
    let pre = state_diff.get("pre").and_then(Value::as_object);
    let post = state_diff
        .get("post")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            JsonRpcError::internal_error("state diff missing post object".to_string())
        })?;

    let mut addresses = HashSet::new();
    addresses.extend(post.keys().cloned());
    if let Some(pre) = pre {
        addresses.extend(pre.keys().cloned());
    }

    for address in addresses {
        let address_value = parse_address(&address)?;
        let post_account = post.get(&address).and_then(Value::as_object);
        let pre_account = pre
            .and_then(|pre| pre.get(&address))
            .and_then(Value::as_object);

        let mut storage_diff = HashMap::new();

        if let Some(post_storage) = post_account
            .and_then(|account| account.get("storage"))
            .and_then(Value::as_object)
        {
            for (key, value) in post_storage {
                storage_diff.insert(
                    parse_b256(key)?,
                    parse_b256(value_as_str(value, "storage value")?)?,
                );
            }
        }

        if let Some(pre_storage) = pre_account
            .and_then(|account| account.get("storage"))
            .and_then(Value::as_object)
        {
            let post_storage = post_account
                .and_then(|account| account.get("storage"))
                .and_then(Value::as_object);
            for key in pre_storage.keys() {
                if post_storage.is_none_or(|post_storage| !post_storage.contains_key(key)) {
                    storage_diff.insert(parse_b256(key)?, B256::ZERO);
                }
            }
        }

        if post_account.is_none() {
            return Err(JsonRpcError::internal_error(
                "preconfirmed simulation produced unsupported account deletion".to_string(),
            ));
        }

        let entry = overrides.entry(address_value).or_default();
        if let Some(post_account) = post_account {
            if let Some(balance) = post_account.get("balance") {
                entry.balance = Some(parse_u256(value_as_str(balance, "balance")?)?);
            }
            if let Some(nonce) = post_account.get("nonce") {
                entry.nonce = Some(parse_nonce(nonce)?);
            }
            if let Some(code) = post_account.get("code") {
                entry.code = Some(parse_bytes(value_as_str(code, "code")?)?);
            }
        }

        if !storage_diff.is_empty() {
            merge_storage_diff(entry, storage_diff);
        }
    }

    Ok(())
}

fn merge_storage_diff(
    override_object: &mut StateOverrideObject,
    storage_diff: HashMap<B256, B256>,
) {
    match &mut override_object.storage_override {
        Some(StorageOverride::StateDiff(existing)) | Some(StorageOverride::State(existing)) => {
            existing.extend(storage_diff);
        }
        None => {
            override_object.storage_override = Some(StorageOverride::StateDiff(storage_diff));
        }
    }
}

fn synthetic_receipt(
    tx: &EthTxEnvelope,
    sender: Address,
    success: bool,
    gas_used: u64,
    cumulative_gas_used: u64,
    logs: Vec<Log>,
    base_fee: Option<u64>,
) -> TransactionReceipt {
    let logs_bloom = logs_bloom(logs.iter().map(|log| &log.inner));
    let receipt = ReceiptWithBloom {
        receipt: Receipt {
            status: Eip658Value::Eip658(success),
            cumulative_gas_used,
            logs,
        },
        logs_bloom,
    };
    let inner = match tx {
        EthTxEnvelope::Legacy(_) => ReceiptEnvelope::Legacy(receipt),
        EthTxEnvelope::Eip2930(_) => ReceiptEnvelope::Eip2930(receipt),
        EthTxEnvelope::Eip1559(_) => ReceiptEnvelope::Eip1559(receipt),
        EthTxEnvelope::Eip4844(_) => ReceiptEnvelope::Eip4844(receipt),
        EthTxEnvelope::Eip7702(_) => ReceiptEnvelope::Eip7702(receipt),
    };

    TransactionReceipt {
        inner,
        transaction_hash: *tx.tx_hash(),
        transaction_index: None,
        block_hash: None,
        block_number: None,
        from: sender,
        to: tx.to(),
        contract_address: match tx.kind() {
            TxKind::Create => Some(sender.create(tx.nonce())),
            _ => None,
        },
        gas_used,
        effective_gas_price: tx.effective_gas_price(base_fee),
        blob_gas_used: None,
        blob_gas_price: None,
    }
}

fn parse_address(value: &str) -> JsonRpcResult<Address> {
    value
        .parse()
        .map_err(|err| JsonRpcError::internal_error(format!("invalid state diff address: {err}")))
}

fn parse_b256(value: &str) -> JsonRpcResult<B256> {
    value
        .parse()
        .map_err(|err| JsonRpcError::internal_error(format!("invalid state diff bytes32: {err}")))
}

fn parse_u256(value: &str) -> JsonRpcResult<U256> {
    value
        .parse()
        .map_err(|err| JsonRpcError::internal_error(format!("invalid state diff uint: {err}")))
}

fn parse_nonce(value: &Value) -> JsonRpcResult<U64> {
    if let Some(nonce) = value.as_u64() {
        return Ok(U64::from(nonce));
    }
    let nonce = parse_u256(value_as_str(value, "nonce")?)?;
    let nonce: u64 = nonce
        .try_into()
        .map_err(|_| JsonRpcError::internal_error("state diff nonce overflow".to_string()))?;
    Ok(U64::from(nonce))
}

fn parse_bytes(value: &str) -> JsonRpcResult<Bytes> {
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| JsonRpcError::internal_error("state diff code missing 0x".to_string()))?;
    hex::decode(value)
        .map(Bytes::from)
        .map_err(|err| JsonRpcError::internal_error(format!("invalid state diff code: {err}")))
}

fn value_as_str<'a>(value: &'a Value, name: &str) -> JsonRpcResult<&'a str> {
    value.as_str().ok_or_else(|| {
        JsonRpcError::internal_error(format!("state diff {name} was not encoded as string"))
    })
}

fn load_operator_signer(
    path: &PathBuf,
) -> Result<PrivateKeySigner, NamespacePreconfirmationInitError> {
    let key = std::fs::read_to_string(path)
        .map_err(NamespacePreconfirmationInitError::OperatorKeyRead)?;
    let key = key.trim();
    if key.is_empty() {
        return Err(NamespacePreconfirmationInitError::EmptyOperatorKey);
    }

    key.parse::<PrivateKeySigner>()
        .map_err(|err| NamespacePreconfirmationInitError::InvalidOperatorKey(err.to_string()))
}

fn unix_time_millis() -> JsonRpcResult<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| JsonRpcError::internal_error(format!("system clock error: {err}")))?;

    duration
        .as_millis()
        .try_into()
        .map_err(|_| JsonRpcError::internal_error("system clock overflow".to_string()))
}

fn build_preconfirmation(
    signer: &PrivateKeySigner,
    operator: Address,
    base_chain_id: u64,
    route_chain_id: u64,
    namespace: Address,
    tx_hash: TxHash,
    queued_at_unix_millis: u64,
) -> JsonRpcResult<NamespacePreconfirmation> {
    let message = preconfirmation_message(
        base_chain_id,
        route_chain_id,
        namespace,
        operator,
        tx_hash,
        queued_at_unix_millis,
    );
    let preconfirmation_id = keccak256(message.as_bytes());
    let signature = signer
        .sign_message_sync(message.as_bytes())
        .map_err(|err| {
            JsonRpcError::internal_error(format!("preconfirmation signing failed: {err}"))
        })?;

    Ok(NamespacePreconfirmation {
        tx_hash: EthHash::from(tx_hash),
        base_chain_id: Quantity(base_chain_id),
        chain_id: Quantity(route_chain_id),
        namespace: EthAddress::from(namespace),
        operator: EthAddress::from(operator),
        queued_at_unix_millis: Quantity(queued_at_unix_millis),
        preconfirmation_id: EthHash::from(preconfirmation_id),
        signature_type: SIGNATURE_TYPE_EIP191.to_string(),
        message,
        signature: signature.to_string(),
    })
}

fn preconfirmation_message(
    base_chain_id: u64,
    route_chain_id: u64,
    namespace: Address,
    operator: Address,
    tx_hash: TxHash,
    queued_at_unix_millis: u64,
) -> String {
    format!(
        "{PRECONFIRMATION_MESSAGE_PREFIX}\n\
         baseChainId: {base_chain_id}\n\
         chainId: {route_chain_id}\n\
         namespace: {namespace}\n\
         operator: {operator}\n\
         txHash: {tx_hash}\n\
         queuedAtUnixMillis: {queued_at_unix_millis}"
    )
}

async fn run_batcher(inner: Arc<NamespacePreconfirmationInner>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(inner.flush_interval) => {}
            _ = inner.queue_notify.notified() => {}
        }

        while let Some(batch) = drain_next_batch(&inner).await {
            if !submit_batch(Arc::clone(&inner), batch).await {
                break;
            }
        }
    }
}

async fn drain_next_batch(inner: &NamespacePreconfirmationInner) -> Option<Vec<QueuedTx>> {
    let mut queue = inner.queue.lock().await;
    if queue.is_empty() {
        return None;
    }

    let namespace = queue.front()?.namespace;
    let mut batch = Vec::with_capacity(inner.max_batch_txs.min(queue.len()));
    while batch.len() < inner.max_batch_txs
        && queue
            .front()
            .is_some_and(|queued_tx| queued_tx.namespace == namespace)
    {
        batch.push(queue.pop_front().expect("front exists"));
    }

    Some(batch)
}

async fn submit_batch(
    inner: Arc<NamespacePreconfirmationInner>,
    queued_txs: Vec<QueuedTx>,
) -> bool {
    let tx_hashes = queued_txs
        .iter()
        .map(|queued_tx| *queued_tx.tx.tx_hash())
        .collect::<Vec<_>>();

    let batch = match sign_namespace_batch(
        &inner.signer,
        queued_txs.iter().map(|queued_tx| queued_tx.tx.clone()),
    ) {
        Ok(batch) => batch,
        Err(err) => {
            error!(?err, "failed to sign namespace preconfirmation batch");
            mark_batch_status(&inner, &tx_hashes, PreconfirmationStatus::Dropped);
            return true;
        }
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

    if let Err(err) = inner
        .txpool_bridge_client
        .try_send_batch(batch, tx_status_recv_sends)
    {
        warn!(
            ?err,
            "namespace preconfirmation batch txpool submission failed"
        );
        requeue_batch(&inner, queued_txs).await;
        return false;
    }

    mark_batch_status(&inner, &tx_hashes, PreconfirmationStatus::Submitted);

    for (tx_hash, tx_status_recv_recv) in tx_hashes.into_iter().zip(tx_status_receivers) {
        let inner = Arc::clone(&inner);
        tokio::spawn(async move {
            track_txpool_status(inner, tx_hash, tx_status_recv_recv).await;
        });
    }

    true
}

fn sign_namespace_batch(
    signer: &PrivateKeySigner,
    txs: impl IntoIterator<Item = EthTxEnvelope>,
) -> JsonRpcResult<NamespaceTransactionBatch> {
    let mut batch = NamespaceTransactionBatch {
        transactions: txs.into_iter().collect::<Vec<_>>().into(),
        signature: NamespaceBatchSignature::default(),
    };
    let signature = signer
        .sign_hash_sync(&batch.signature_hash())
        .map_err(|err| {
            JsonRpcError::internal_error(format!("namespace batch signing failed: {err}"))
        })?;

    batch.signature = NamespaceBatchSignature {
        y_parity: signature.v() as u8,
        r: signature.r(),
        s: signature.s(),
    };

    Ok(batch)
}

async fn requeue_batch(inner: &NamespacePreconfirmationInner, queued_txs: Vec<QueuedTx>) {
    let mut queue = inner.queue.lock().await;
    for queued_tx in queued_txs.into_iter().rev() {
        queue.push_front(queued_tx);
    }
}

async fn track_txpool_status(
    inner: Arc<NamespacePreconfirmationInner>,
    tx_hash: TxHash,
    tx_status_recv_recv: tokio::sync::oneshot::Receiver<tokio::sync::watch::Receiver<TxStatus>>,
) {
    let Ok(mut tx_status_recv) = tx_status_recv_recv.await else {
        warn!(
            ?tx_hash,
            "txpool did not return namespace preconfirmation status receiver"
        );
        return;
    };

    loop {
        if tx_status_recv.changed().await.is_err() {
            return;
        }

        let status = match tx_status_recv.borrow_and_update().to_owned() {
            TxStatus::Unknown => continue,
            TxStatus::Tracked => PreconfirmationStatus::Tracked,
            TxStatus::Dropped { .. } => PreconfirmationStatus::Dropped,
            TxStatus::Evicted { .. } => PreconfirmationStatus::Evicted,
            TxStatus::Committed => PreconfirmationStatus::Committed,
        };

        update_status(&inner, tx_hash, status);

        if matches!(
            status,
            PreconfirmationStatus::Dropped
                | PreconfirmationStatus::Evicted
                | PreconfirmationStatus::Committed
        ) {
            return;
        }
    }
}

fn mark_batch_status(
    inner: &NamespacePreconfirmationInner,
    tx_hashes: &[TxHash],
    status: PreconfirmationStatus,
) {
    for tx_hash in tx_hashes {
        update_status(inner, *tx_hash, status);
    }
}

fn update_status(
    inner: &NamespacePreconfirmationInner,
    tx_hash: TxHash,
    status: PreconfirmationStatus,
) {
    if let Some(mut cached) = inner.cache.get_mut(&tx_hash) {
        cached.status = status;
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};
    use alloy_signer_local::PrivateKeySigner;

    use super::*;

    #[test]
    fn preconfirmation_signature_recovers_operator() {
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap();
        let tx_hash = B256::repeat_byte(0x22);
        let operator = signer.address();
        let preconfirmation = build_preconfirmation(
            &signer,
            operator,
            1,
            65_537,
            Address::repeat_byte(0x33),
            tx_hash,
            1_717_171_717_000,
        )
        .unwrap();

        let signature = preconfirmation
            .signature
            .parse::<alloy_primitives::Signature>()
            .unwrap();

        assert_eq!(
            signature
                .recover_address_from_msg(preconfirmation.message.as_bytes())
                .unwrap(),
            operator
        );
        assert_eq!(
            preconfirmation.preconfirmation_id,
            EthHash::from(keccak256(preconfirmation.message.as_bytes()))
        );
    }

    #[test]
    fn signed_namespace_batch_recovers_operator() {
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap();
        let batch = sign_namespace_batch(&signer, Vec::<EthTxEnvelope>::new()).unwrap();

        assert_eq!(batch.recover_signer().unwrap(), signer.address());
    }
}
