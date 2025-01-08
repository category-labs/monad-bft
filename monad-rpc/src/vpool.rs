use std::collections::VecDeque;

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_primitives::Address;
use monad_rpc_docs::rpc;
use scc::{ebr::Guard, HashIndex, HashMap, TreeIndex};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::warn;

use crate::{
    block_watcher::BlockWithReceipts,
    eth_json_types::EthAddress,
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[rpc(method = "txpool_content")]
#[allow(non_snake_case)]
pub async fn monad_txpool_content() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TxPoolContentFromParams {
    pub address: EthAddress,
}

#[rpc(method = "txpool_contentFrom")]
#[allow(non_snake_case)]
pub async fn monad_txpool_contentFrom(
    tx_pool: &VirtualPool,
    params: TxPoolContentFromParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "txpool_inspect")]
#[allow(non_snake_case)]
pub async fn monad_txpool_inspect() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct TxPoolStatus {
    pub pending: usize,
    pub queued: usize,
}

#[rpc(method = "txpool_status")]
#[allow(non_snake_case)]
pub async fn monad_txpool_status(tx_pool: &VirtualPool) -> JsonRpcResult<TxPoolStatus> {
    Ok(TxPoolStatus {
        pending: tx_pool.pending_pool.len(),
        queued: tx_pool.queued_pool.len(),
    })
}

/// Virtual maintains pools of transactions that are pending and queued for inclusion in a block.
pub struct VirtualPool {
    // Holds transactions that have been broadcast to a validator
    pending_pool: SubPool,
    // Holds transactions that are valid, but have a nonce gap.
    // Transactions are promoted to pending once the gap is resolved.
    queued_pool: SubPool,
    // Cache of chain state and account nonces
    chain_cache: ChainCache,
    // Publish transactions to validator
    publisher: flume::Sender<TxEnvelope>,
}

struct SubPool {
    // Mapping of sender to a transaction tree ordered by nonce
    pool: HashMap<Address, TreeIndex<u64, Recovered<TxEnvelope>>>,
    evict: RwLock<VecDeque<Address>>,
    capacity: usize,
}

impl SubPool {
    fn new(capacity: usize) -> Self {
        Self {
            pool: HashMap::new(),
            evict: RwLock::new(VecDeque::new()),
            capacity,
        }
    }

    async fn add(&self, txn: Recovered<TxEnvelope>, overwrite: bool) {
        match self.pool.entry(txn.signer()) {
            scc::hash_map::Entry::Occupied(mut entry) => {
                let tree = entry.get();
                match tree.contains(&txn.nonce()) {
                    true if overwrite => {
                        entry.get_mut().remove(&txn.nonce());
                        entry.get_mut().insert(txn.nonce(), txn.clone());
                    }
                    false => {
                        entry.get_mut().insert(txn.nonce(), txn.clone());
                    }
                    _ => {}
                }
            }
            scc::hash_map::Entry::Vacant(entry) => {
                let tree = TreeIndex::new();
                tree.insert(txn.nonce(), txn.clone());
                entry.insert_entry(tree);

                self.evict.write().await.push_front(txn.signer());
            }
        };

        let mut lock = self.evict.write().await;
        if lock.len() > self.capacity {
            if let Some(evicted) = lock.pop_back() {
                self.pool.remove(&evicted);
            }
        }
    }

    fn clear_included(&self, txs: &[(Address, u64)]) {
        for (sender, nonce) in txs {
            if let Some(mut entry) = self.pool.get(sender) {
                entry.get_mut().remove(nonce);
                if entry.len() == 0 {
                    let _ = entry.remove();
                }
            }
        }
    }

    fn by_addr(&self, address: &Address) -> Vec<Recovered<TxEnvelope>> {
        let mut pending = Vec::new();
        self.pool.read(address, |_, map| {
            map.iter(&Guard::new())
                .for_each(|entry| pending.push(entry.1.clone()))
        });
        pending
    }

    fn get(&self, address: &Address, nonce: &u64) -> Option<Recovered<TxEnvelope>> {
        self.pool
            .get(address)?
            .get()
            .peek(nonce, &Guard::new())
            .cloned()
    }

    // Returns a list of transaction entries that are ready to be promoted because a nonce gap was resolved.
    fn filter_by_nonce_gap(&self, state: &HashIndex<Address, u64>) -> Vec<Recovered<TxEnvelope>> {
        let mut to_promote = Vec::new();
        for (sender, nonce) in state.iter(&Guard::new()) {
            if let Some(mut entry) = self.pool.get(sender) {
                let mut removed = 0;
                let mut nonce = *nonce;
                for (min_nonce, tx) in entry.iter(&Guard::new()) {
                    if min_nonce.checked_sub(nonce) == Some(1) {
                        to_promote.push(tx.clone());
                        removed += 1;
                        nonce += 1;
                    } else {
                        break;
                    }
                }

                if removed == entry.len() {
                    let _ = entry.remove();
                } else if removed > 0 {
                    entry
                        .get_mut()
                        .remove_range(0..(nonce + removed as u64 + 1));
                }
            }
        }
        to_promote
    }

    fn len(&self) -> usize {
        let mut len = 0;
        self.pool.scan(|_, v| len += v.len());
        len
    }
}

struct ChainCache {
    inner: ChainCacheInner,
}

struct ChainCacheInner {
    accounts: HashIndex<Address, u64>,
    base_fee: RwLock<u128>,
    capacity: usize,
    evict: RwLock<VecDeque<(Address, u64)>>,
}

impl ChainCache {
    fn new(capacity: usize) -> Self {
        Self {
            inner: ChainCacheInner {
                accounts: HashIndex::new(),
                base_fee: RwLock::new(1_000),
                capacity,
                evict: RwLock::new(VecDeque::new()),
            },
        }
    }

    async fn get_base_fee(&self) -> u128 {
        *self.inner.base_fee.read().await
    }

    async fn update(
        &self,
        block: BlockWithReceipts,
        next_base_fee: u128,
    ) -> HashIndex<Address, u64> {
        *self.inner.base_fee.write().await = next_base_fee;
        // Create a hashset of all senders and their nonces in the block.
        let senders = HashIndex::new();

        for txn in block.transactions.iter().rev() {
            let sender = txn.recover_signer().unwrap_or_default();
            let nonce = txn.nonce();
            senders.insert(sender, nonce);
        }

        let mut add_to_eviction_list = Vec::<(Address, u64)>::new();
        for (sender, nonce) in senders.iter(&Guard::new()) {
            match self.inner.accounts.get(sender) {
                Some(entry) => {
                    entry.update(*nonce);
                }
                None => {
                    self.inner.accounts.insert(*sender, *nonce);
                }
            }
            add_to_eviction_list.push((*sender, *nonce));
        }

        for add in add_to_eviction_list {
            self.inner.evict.write().await.push_front(add);
        }

        if self.inner.evict.read().await.len() > self.inner.capacity {
            if let Some(key) = self.inner.evict.write().await.pop_back() {
                if let Some(entry) = self.inner.accounts.get(&key.0) {
                    if &key.1 == entry.get() {
                        drop(entry);
                        self.inner.accounts.remove(&key.0);
                    }
                }
            }
        }

        senders
    }

    async fn nonce(&self, address: &Address) -> Option<u64> {
        self.inner.accounts.peek(address, &Guard::new()).cloned()
    }

    async fn len(&self) -> usize {
        self.inner.accounts.len()
    }
}

enum TxPoolType {
    Queue,
    Pending,
    Discard,
    Replace,
}

pub enum TxPoolEvent {
    AddValidTransaction {
        txn: Recovered<TxEnvelope>,
    },
    BlockUpdate {
        block: BlockWithReceipts,
        next_base_fee: u128,
    },
}

impl VirtualPool {
    pub fn new(publisher: flume::Sender<TxEnvelope>, capacity: usize) -> Self {
        Self {
            pending_pool: SubPool::new(capacity),
            queued_pool: SubPool::new(capacity),
            chain_cache: ChainCache::new(capacity),
            publisher,
        }
    }

    async fn decide_pool(&self, txn: &Recovered<TxEnvelope>) -> TxPoolType {
        let sender = txn.signer();
        let nonce = txn.nonce();
        let base_fee = txn.max_fee_per_gas();

        if base_fee < self.chain_cache.get_base_fee().await {
            return TxPoolType::Discard;
        }

        if self
            .chain_cache
            .nonce(&sender)
            .await
            .map(|cur_nonce| nonce.checked_sub(cur_nonce).map(|v| v > 1).unwrap_or(false))
            .unwrap_or(false)
        {
            // The chain cache is updated at each new block, and we can receive transactions before chain cache is updated.
            // Check recently forward transactions to see if the transaction has a nonce gap.
            let pending = self.pending_pool.by_addr(&sender);
            if let Some(entry) = pending.last() {
                if entry
                    .nonce()
                    .checked_add(1)
                    .map(|v| v == nonce)
                    .unwrap_or(false)
                {
                    return TxPoolType::Pending;
                }
            }

            return TxPoolType::Queue;
        }

        let queued = self.queued_pool.by_addr(&sender);
        if let Some(true) = queued.first().map(|tx| tx.nonce() < nonce) {
            return TxPoolType::Queue;
        }

        let pending = self.pending_pool.by_addr(&sender);
        let Some(entry) = pending.last() else {
            return TxPoolType::Pending;
        };

        let last_pending_nonce = entry.nonce();

        if last_pending_nonce + 1 == nonce {
            TxPoolType::Pending
        } else if last_pending_nonce == nonce {
            if let Some(entry) = self.pending_pool.get(&txn.signer(), &txn.nonce()) {
                // Replace a pending transaction if the fee is at least 10% higher than the current fee
                let current_gas_price = entry.max_fee_per_gas();
                let new_gas_price = txn.max_fee_per_gas();
                if new_gas_price >= current_gas_price + (current_gas_price / 10) {
                    TxPoolType::Replace
                } else {
                    TxPoolType::Discard
                }
            } else {
                TxPoolType::Discard
            }
        } else {
            TxPoolType::Pending
        }
    }

    async fn process_event(&self, event: TxPoolEvent) {
        match event {
            TxPoolEvent::AddValidTransaction { txn } => match self.decide_pool(&txn).await {
                TxPoolType::Queue => {
                    self.queued_pool.add(txn, false).await;
                }
                TxPoolType::Pending => {
                    self.pending_pool.add(txn.clone(), false).await;
                    if self.publisher.send(txn.into_tx()).is_err() {
                        warn!("issue broadcasting transaction from pending pool");
                    }
                }
                TxPoolType::Replace => {
                    self.pending_pool.add(txn.clone(), true).await;
                    if self.publisher.send(txn.into_tx()).is_err() {
                        warn!("issue broadcasting transaction from pending pool");
                    }
                }
                TxPoolType::Discard => {}
            },
            TxPoolEvent::BlockUpdate {
                block,
                next_base_fee,
            } => {
                // Remove pending transactions that were included in the block.
                // Check queued pool for any transactions ready to be pending.
                let txs = block
                    .transactions
                    .iter()
                    .filter_map(|txn| {
                        let sender = txn.recover_signer().ok()?;
                        Some((sender, txn.nonce()))
                    })
                    .collect::<Vec<_>>();

                self.pending_pool.clear_included(&txs);
                self.queued_pool.clear_included(&txs);

                let senders = self.chain_cache.update(block.clone(), next_base_fee).await;

                // Check for available promotions
                let promote_queued = self.queued_pool.filter_by_nonce_gap(&senders);

                // Add promoted transactions to the pending pool
                for promoted in promote_queued.into_iter() {
                    self.pending_pool.add(promoted.clone(), false).await;
                    if let Err(error) = self.publisher.send(promoted.into_tx()) {
                        warn!(
                            "issue broadcasting transaction from pending pool: {:?}",
                            error
                        );
                    }
                }
            }
        }
    }

    // Adds a transaction to the txpool and decides which sub-pool to add it to
    pub async fn add_transaction(&self, txn: Recovered<TxEnvelope>) {
        self.process_event(TxPoolEvent::AddValidTransaction { txn })
            .await
    }

    pub async fn new_block(&self, block: BlockWithReceipts, next_base_fee: u128) {
        self.process_event(TxPoolEvent::BlockUpdate {
            block,
            next_base_fee,
        })
        .await
    }

    /// Returns pending + queued transactions for an address
    pub fn pool_by_address(
        &self,
        address: &Address,
    ) -> (Vec<Recovered<TxEnvelope>>, Vec<Recovered<TxEnvelope>>) {
        let pending = self.pending_pool.by_addr(address);
        let queued = self.queued_pool.by_addr(address);
        (pending, queued)
    }
}
