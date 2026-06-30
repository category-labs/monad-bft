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
    collections::VecDeque,
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::{transaction::SignerRecoverable, Transaction as _};
use alloy_primitives::{keccak256, Address, TxHash};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use dashmap::DashMap;
use monad_eth_types::{EthTxEnvelope, NamespaceBatchSignature, NamespaceTransactionBatch};
use schemars::JsonSchema;
use serde::Serialize;
use tokio::sync::{Mutex, Notify};
use tracing::{error, trace, warn};

use crate::{
    txpool::{EthTxPoolBridgeClient, TxStatus},
    types::{
        eth_json::{EthAddress, EthHash, Quantity},
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
};

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
                write!(f, "failed to read namespace preconfirmation operator key: {err}")
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
    queue_notify: Notify,
    cache: DashMap<TxHash, CachedPreconfirmation>,
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
                queue_notify: Notify::new(),
                cache: DashMap::default(),
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
            trace!(?err, "namespace preconfirmation tx signature recovery failed");
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

        let should_notify = {
            let mut queue = self.inner.queue.lock().await;

            if let Some(cached) = self.get_cached(&tx_hash) {
                return Ok(cached);
            }

            if queue.len() >= self.inner.max_queued_txs {
                return Err(JsonRpcError::overloaded());
            }

            queue.push_back(QueuedTx {
                tx,
                namespace,
            });

            queue.len() >= self.inner.max_batch_txs
        };

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
    }
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
    let signature = signer.sign_message_sync(message.as_bytes()).map_err(|err| {
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
        warn!(?err, "namespace preconfirmation batch txpool submission failed");
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
    let signature = signer.sign_hash_sync(&batch.signature_hash()).map_err(|err| {
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
    tx_status_recv_recv: tokio::sync::oneshot::Receiver<
        tokio::sync::watch::Receiver<TxStatus>,
    >,
) {
    let Ok(mut tx_status_recv) = tx_status_recv_recv.await else {
        warn!(?tx_hash, "txpool did not return namespace preconfirmation status receiver");
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
