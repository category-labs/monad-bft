use std::{
    collections::VecDeque,
    sync::{Arc, Weak},
};

use rayon::slice::ParallelSlice;
use monad_triedb_utils::triedb_env::{Triedb, TriedbEnv};
use reth_primitives::{
    revm_primitives::bitvec::store::BitStore, Address, Header, TransactionSigned, TransactionSignedEcRecovered
};
use reth_rpc_types::TransactionReceipt;
use scc::{ebr::Guard, HashIndex, HashMap, TreeIndex};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, warn};

pub mod metrics;

use metrics::VirtualPoolMetrics;

#[derive(Debug, Default, Clone)]
pub struct BlockWithReceipts {
    pub block_header: Header,
    pub transactions: Vec<TransactionSigned>,
    pub receipts: Vec<TransactionReceipt>,
}

/// Virtual maintains pools of transactions that are pending and queued for inclusion in a block.
pub struct VirtualPool {
    // Holds transactions that have been broadcast to a validator
    pub pending_pool: SubPool,
    // Holds transactions that are valid, but have a nonce gap.
    // Transactions are promoted to pending once the gap is resolved.
    pub queued_pool: SubPool,
    // Cache of chain state and account nonces
    pub chain_cache: ChainCache,
    // Publish transactions to validator
    publisher: flume::Sender<TransactionSigned>,
    metrics: Option<Arc<VirtualPoolMetrics>>,
}

pub struct SubPool {
    // Mapping of sender to a transaction tree ordered by nonce
    pub pool: HashMap<Address, TreeIndex<u64, TransactionSignedEcRecovered>>,
    eviction_handler: EvictionPolicy,
    capacity: usize,
}

impl SubPool {
    fn new(capacity: usize) -> Self {
        Self {
            pool: HashMap::new(),
            eviction_handler: EvictionPolicy {
                lru: LruList::new(),
            },
            capacity,
        }
    }

    /// Adds a transaction to the SubPool, and returns the number of transactions evicted.
    async fn add(&mut self, txn: TransactionSignedEcRecovered, overwrite: bool) {
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
            }
        };

        self.eviction_handler.mark_used(txn.signer()).await;
    }

    async fn evict(&mut self) -> usize {
        let pool_len: usize = self.len();
        let mut evicted: usize = 0;
        while (pool_len - evicted) > self.capacity {
            if self.eviction_handler.evict_lru(&mut self.pool).await {
                evicted += 1;
            }
        }

        evicted
    }

    fn clear_included(&self, txs: &[(Address, u64)]) -> usize {
        let mut removed = 0;

        // Create a map of senders, and use their highest nonce to clear included transactions.
        let senders: HashMap<Address, u64> = HashMap::new();

        for (sender, nonce) in txs.to_owned().into_iter() {
            match senders.get(&sender) {
                Some(mut prev) if *prev.get() < nonce => {
                    prev.store_value(nonce);
                }
                None => {
                    senders.insert(sender, nonce);
                }
                _ => {}
            };
        }

        let guard = Guard::new();
        senders.scan(|sender, nonce| {
            if let Some(mut entry) = self.pool.get(sender) {
                removed += entry.range(0..=*nonce, &guard).count();
                entry.remove_range(0..=*nonce);

                if entry.is_empty() {
                    let _ = entry.remove();
                    self.pool.remove(sender);
                }
            }
        });

        removed
    }

    fn by_addr(&self, address: &Address) -> Vec<TransactionSignedEcRecovered> {
        let mut pending = Vec::new();
        self.pool.read(address, |_, map| {
            map.iter(&Guard::new())
                .for_each(|entry| pending.push(entry.1.clone()))
        });
        pending
    }

    fn get(&self, address: &Address, nonce: &u64) -> Option<TransactionSignedEcRecovered> {
        self.pool
            .get(address)?
            .get()
            .peek(nonce, &Guard::new())
            .cloned()
    }

    // Returns a tuple of lists of transaction entries that are ready to be promoted and evicted because a nonce gap was resolved.
    fn filter_by_nonce_gap(
        &self,
        state: &HashMap<Address, u64>,
    ) -> (
        Vec<TransactionSignedEcRecovered>,
        Vec<TransactionSignedEcRecovered>,
    ) {
        let mut to_promote = Vec::new();
        let mut to_evict = Vec::new();
        let guard = Guard::new();
        state.scan(|sender, nonce| {
            if let Some(mut entry) = self.pool.get(sender) {
                let mut removed = 0;
                let mut nonce = *nonce;
                for (min_nonce, tx) in entry.iter(&guard) {
                    match min_nonce.checked_sub(nonce) {
                        // Nonce gap is resolved, promote the transaction.
                        Some(1) => {
                            to_promote.push(tx.clone());
                            removed += 1;
                            nonce += 1;
                        }
                        // Gap detected.
                        Some(_) => {
                            break;
                        }
                        // New nonce is greater than nonce in the pool, evict the transactions with lesser nonces.
                        None => {
                            to_evict.push(tx.clone());
                            removed += 1;
                        }
                    }
                }

                if removed == entry.len() {
                    let _ = entry.remove();
                    self.pool.remove(sender);
                } else if removed > 0 {
                    entry.get_mut().remove_range(0..=nonce);
                }
            }
        });
        (to_promote, to_evict)
    }

    fn len(&self) -> usize {
        let mut len = 0;
        self.pool.scan(|_, v| len += v.len());
        len
    }

    pub fn sender_nonces(&self) -> Vec<(Address, u64)> {        
        let mut senders = Vec::new();
        self.pool.scan(|_, v| {
            senders.push(v.iter(&Guard::new())
                .map(|(nonce, tx)| (tx.signer(), *nonce))
                .collect::<Vec<_>>());
        });

        senders.iter()
        .flatten()
        .map(|(address, nonce)| (*address, *nonce))
        .collect::<Vec<_>>()
    }
}

struct Node {
    addr: Address,
    prev: Weak<Mutex<Node>>,
    next: Option<Arc<Mutex<Node>>>,
}

struct LruList {
    head: Option<Arc<Mutex<Node>>>,
    tail: Option<Arc<Mutex<Node>>>,
    map: std::collections::HashMap<Address, Arc<Mutex<Node>>>,
}

impl LruList {
    fn new() -> Self {
        Self {
            head: None,
            tail: None,
            map: std::collections::HashMap::new(),
        }
    }

    async fn touch(&mut self, addr: Address) {
        if let Some(node_arc) = self.map.get(&addr).cloned() {
            self.remove_node(&node_arc).await;
            self.push_back_node(node_arc).await;
        } else {
            let node = Arc::new(Mutex::new(Node {
                addr,
                prev: Weak::new(),
                next: None,
            }));
            self.map.insert(addr, node.clone());
            self.push_back_node(node).await;
        }
    }

    async fn evict_front(&mut self) -> Option<Address> {
        let front_arc = self.head.take()?;

        let addr;
        let next_arc = {
            let mut front = front_arc.lock().await;
            addr = front.addr;
            front.next.take()
        };

        if let Some(next_arc) = next_arc {
            next_arc.lock().await.prev = Weak::new();
            self.head = Some(next_arc);
        } else {
            self.tail = None;
        }

        self.map.remove(&addr);
        Some(addr)
    }

    async fn push_front(&mut self, addr: Address) {
        let node: Arc<Mutex<Node>> = Arc::new(Mutex::new(Node {
            addr,
            prev: Weak::new(),
            next: None,
        }));

        if let Some(head_arc) = self.head.take() {
            {
                let mut head = head_arc.lock().await;
                let mut node_guard = node.lock().await;
                node_guard.next = Some(head_arc.clone());
                head.prev = Arc::downgrade(&node);
            }
        } else {
            self.tail = Some(node.clone());
        }

        self.head = Some(node.clone());
        self.map.insert(addr, node);
    }

    async fn remove_node(&mut self, node_arc: &Arc<Mutex<Node>>) {
        let mut node = node_arc.lock().await;
        let prev = node.prev.upgrade();
        let next = node.next.take();

        match &prev {
            Some(prev_arc) => {
                prev_arc.lock().await.next = next.clone();
            }
            None => {
                self.head = next.clone();
            }
        }

        if let Some(next_arc) = next {
            next_arc.lock().await.prev = prev.map_or(Weak::new(), |p| Arc::downgrade(&p));
        } else {
            self.tail = prev;
        }
    }

    async fn push_back_node(&mut self, node_arc: Arc<Mutex<Node>>) {
        if let Some(tail_arc) = self.tail.take() {
            tail_arc.lock().await.next = Some(node_arc.clone());
            node_arc.lock().await.prev = Arc::downgrade(&tail_arc);
            self.tail = Some(node_arc);
            if self.head.is_none() {
                self.head = Some(tail_arc);
            }
        } else {
            self.head = Some(node_arc.clone());
            self.tail = Some(node_arc);
        }
    }
}

struct EvictionPolicy {
    lru: LruList,
}

impl EvictionPolicy {
    async fn mark_used(&mut self, addr: Address) {
        self.lru.touch(addr).await;
    }

    async fn evict_lru(
        &mut self,
        pool: &mut HashMap<Address, TreeIndex<u64, TransactionSignedEcRecovered>>,
    ) -> bool {
        if let Some(addr) = self.lru.evict_front().await {
            let (is_evicted, is_empty) = if let Some(tree) = pool.get(&addr) {
                let guard = Guard::new();
                let max_nonce = tree
                    .iter(&guard)
                    .last()
                    .map(|(k, _)| *k)
                    .unwrap_or_default();
                tree.remove_range(max_nonce..=max_nonce);

                let is_empty = tree.is_empty();

                (true, is_empty)
            } else {
                (false, true)
            };

            if !is_empty {
                self.lru.push_front(addr).await;
            }

            return is_evicted;
        }
        false
    }
}

pub struct ChainCache {
    pub inner: RwLock<ChainCacheInner>,
}

pub struct ChainCacheInner {
    pub accounts: HashIndex<Address, u64>,
    base_fee: u128,
    latest_block_height: u64,
    capacity: usize,
    evict: VecDeque<(Address, u64)>,
    triedb: Option<TriedbEnv>,
}

impl ChainCache {
    fn new(triedb_env: Option<TriedbEnv>, capacity: usize) -> Self {
        Self {
            inner: RwLock::new(ChainCacheInner {
                accounts: HashIndex::new(),
                base_fee: 1_000,
                latest_block_height: 0,
                capacity,
                evict: VecDeque::new(),
                triedb: triedb_env,
            }),
        }
    }

    async fn get_base_fee(&self) -> u128 {
        self.inner.read().await.base_fee
    }

    async fn update(
        &self,
        block: BlockWithReceipts,
        recovered_senders: Vec<(Address, u64)>,
        next_base_fee: u128,
    ) -> HashMap<Address, u64> {
        let mut inner = self.inner.write().await;
        inner.base_fee = next_base_fee;
        inner.latest_block_height = block.block_header.number;
        // Create a hashset of all senders and their nonces in the block.
        let senders = HashMap::new();

        for (sender, nonce) in recovered_senders {
            senders.upsert(sender, nonce);
        }

        let mut add_to_eviction_list = Vec::<(Address, u64)>::new();
        senders.scan(|sender, nonce| {
            match inner.accounts.get(sender) {
                Some(entry) => {
                    entry.update(*nonce + 1);
                }
                None => {
                    if inner.accounts.insert(*sender, *nonce + 1).is_err() {
                        error!("attempted to insert an account that already exists in chain cache")
                    }
                }
            }
            add_to_eviction_list.push((*sender, *nonce));
        });

        for add in add_to_eviction_list {
            inner.evict.push_front(add);
        }

        if inner.evict.len() > inner.capacity {
            if let Some(key) = inner.evict.pop_back() {
                if let Some(entry) = inner.accounts.get(&key.0) {
                    if &key.1 == entry.get() {
                        drop(entry);
                        inner.accounts.remove(&key.0);
                    }
                }
            }
        }

        senders
    }

    pub async fn nonce(&self, address: &Address) -> Option<u64> {
        let inner = self.inner.read().await;
        let nonce = inner.accounts.peek(address, &Guard::new()).cloned();
        let block_height = inner.latest_block_height;

        if nonce.is_none() {
            if let Some(triedb) = &inner.triedb {
                match triedb.get_account(address.0.into(), block_height).await {
                    Ok(account) => {
                        drop(inner);
                        self.inner
                            .write()
                            .await
                            .accounts
                            .insert(*address, account.nonce);
                        self.inner
                            .write()
                            .await
                            .evict
                            .push_front((*address, account.nonce));
                        Some(account.nonce)
                    }
                    Err(e) => {
                        error!("Error fetching nonce for account {}: {:?}", address, e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            nonce
        }
    }

    async fn len(&self) -> usize {
        self.inner.read().await.accounts.len()
    }
}

enum TxPoolType {
    Queue,
    Pending,
    Discard,
    ReplacePending,
    ReplaceQueued,
}

pub enum TxPoolEvent {
    AddValidTransaction {
        txn: TransactionSignedEcRecovered,
    },
    BlockUpdate {
        block: BlockWithReceipts,
        next_base_fee: u128,
    },
}

impl VirtualPool {
    pub fn new(
        publisher: flume::Sender<TransactionSigned>,
        capacity: usize,
        metrics: Option<Arc<VirtualPoolMetrics>>,
        triedb_env: Option<TriedbEnv>,
    ) -> Self {
        Self {
            pending_pool: SubPool::new(capacity),
            queued_pool: SubPool::new(capacity),
            chain_cache: ChainCache::new(triedb_env, capacity),
            publisher,
            metrics,
        }
    }

    async fn decide_pool(&self, txn: &TransactionSignedEcRecovered) -> TxPoolType {
        let sender = txn.signer();
        let nonce = txn.nonce();
        let base_fee = txn.transaction.max_fee_per_gas();

        if base_fee < self.chain_cache.get_base_fee().await {
            return TxPoolType::Discard;
        }

        if let Some(entry) = self.pending_pool.get(&txn.signer(), &txn.nonce()) {
            // Replace a pending transaction if the fee is at least 10% higher than the current fee
            let current_gas_price = entry.transaction.max_fee_per_gas();
            let new_gas_price = txn.transaction.max_fee_per_gas();
            if new_gas_price >= current_gas_price + (current_gas_price / 10) {
                return TxPoolType::ReplacePending;
            } else {
                return TxPoolType::Discard;
            }
        }

        // Fetch the current nonce of the sender
        let cur_nonce = self.chain_cache.nonce(&sender).await;

        // If our nonce matches sender nonce, send to pending pool.
        if cur_nonce.map(|n| n == nonce).unwrap_or(false) {
            return TxPoolType::Pending;
        }

        let pending_txs = self.pending_pool.by_addr(&sender);

        // If the nonce is greater than the current nonce, check if there is a gap.
        if cur_nonce
            .map(|cur_nonce| {
                nonce
                    .checked_sub(cur_nonce)
                    .map(|v| v >= 1)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
        {
            // The chain cache is updated at each new block, and we can receive transactions before chain cache is updated.
            // Check recently forward transactions to see if the transaction has a nonce gap.
            if let Some(entry) = pending_txs.last() {
                if entry
                    .nonce()
                    .checked_add(1)
                    .map(|v| v == nonce)
                    .unwrap_or(false)
                {
                    return TxPoolType::Pending;
                }
            }

            // The transaction is already queued, check if it is a replacement transaction with a higher fee.
            if let Some(entry) = self.queued_pool.get(&txn.signer(), &txn.nonce()) {
                let current_gas_price = entry.transaction.max_fee_per_gas();
                let new_gas_price = txn.transaction.max_fee_per_gas();
                if new_gas_price >= current_gas_price + (current_gas_price / 10) {
                    return TxPoolType::ReplaceQueued;
                } else {
                    return TxPoolType::Discard;
                }
            };

            return TxPoolType::Queue;
        }

        TxPoolType::Queue
    }

    async fn process_event(&mut self, event: TxPoolEvent) {
        match event {
            TxPoolEvent::AddValidTransaction { txn } => match self.decide_pool(&txn).await {
                TxPoolType::Queue => {
                    self.queued_pool.add(txn, false).await;
                }
                TxPoolType::Pending => {
                    self.pending_pool.add(txn.clone(), false).await;
                    if self.publisher.send(txn.clone().into()).is_err() {
                        warn!("issue broadcasting transaction from pending pool");
                    }

                    // Check for available promotions
                    let tree = HashMap::new();
                    tree.insert(txn.signer(), txn.nonce()).unwrap_or_default();
                    let (promoted, _) = self.queued_pool.filter_by_nonce_gap(&tree);
                    if !promoted.is_empty() {
                        for promoted in promoted.into_iter() {
                            self.pending_pool.add(promoted.clone(), false).await;
                            if let Err(error) = self.publisher.send(promoted.into()) {
                                warn!(
                                    "issue broadcasting transaction from pending pool: {:?}",
                                    error
                                );
                            }
                        }
                    }
                }
                TxPoolType::ReplacePending => {
                    self.pending_pool.add(txn.clone(), true).await;
                    if self.publisher.send(txn.into()).is_err() {
                        warn!("issue broadcasting transaction from pending pool");
                    }
                }
                TxPoolType::ReplaceQueued => {
                    self.queued_pool.add(txn.clone(), true).await;
                }
                TxPoolType::Discard => {}
            },
            TxPoolEvent::BlockUpdate {
                block,
                next_base_fee,
            } => {
                // Remove pending transactions that were included in the block.
                // Check queued pool for any transactions ready to be pending.
                let txs = &block.transactions;
                let Some(senders) = TransactionSigned::recover_signers_unchecked(
                    txs.as_parallel_slice(),
                    txs.len(),
                ) else {
                    error!(
                        "sender in block {} has invalid signature",
                        block.block_header.number
                    );
                    return;
                };

                let recovered_senders = senders
                    .into_iter()
                    .zip(
                        block
                            .transactions
                            .iter()
                            .map(|tx| tx.nonce())
                            .collect::<Vec<u64>>(),
                    )
                    .collect::<Vec<_>>();

                self.pending_pool.clear_included(&recovered_senders);
                self.queued_pool.clear_included(&recovered_senders);

                let pending_evicted =
                    i64::try_from(self.pending_pool.evict().await).unwrap_or_default();

                let queued_evicted =
                    i64::try_from(self.queued_pool.evict().await).unwrap_or_default();

                let senders = self
                    .chain_cache
                    .update(block.clone(), recovered_senders, next_base_fee)
                    .await;

                // Check for available promotions
                let (promote_queued, _) = self.queued_pool.filter_by_nonce_gap(&senders);

                // Add promoted transactions to the pending pool
                for promoted in promote_queued.into_iter() {
                    self.pending_pool.add(promoted.clone(), false).await;
                    if let Err(error) = self.publisher.send(promoted.into()) {
                        warn!(
                            "issue broadcasting transaction from pending pool: {:?}",
                            error
                        );
                    }
                }

                self.metrics.as_ref().map(|metrics| {
                    metrics
                        .pending_evicted
                        .add(u64::try_from(pending_evicted).unwrap_or_default(), &[]);
                });

                self.metrics.as_ref().map(|metrics| {
                    metrics
                        .queued_evicted
                        .add(u64::try_from(queued_evicted).unwrap_or_default(), &[]);
                });

                self.record_subpool_metrics();
            }
        }
    }

    fn record_subpool_metrics(&self) {
        let pending_size = u64::try_from(self.pending_pool.len()).unwrap_or_default();
        self.metrics
            .as_ref()
            .map(|metrics| metrics.pending_pool.record(pending_size, &[]));

        let queued_size = u64::try_from(self.queued_pool.len()).unwrap_or_default();
        self.metrics
            .as_ref()
            .map(|metrics| metrics.queued_pool.record(queued_size, &[]));
    }

    // Adds a transaction to the txpool and decides which sub-pool to add it to
    pub async fn add_transaction(&mut self, txn: TransactionSignedEcRecovered) {
        self.process_event(TxPoolEvent::AddValidTransaction { txn })
            .await
    }

    pub async fn new_block(&mut self, block: BlockWithReceipts, next_base_fee: u128) {
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
    ) -> (
        Vec<TransactionSignedEcRecovered>,
        Vec<TransactionSignedEcRecovered>,
    ) {
        let pending = self.pending_pool.by_addr(address);
        let queued = self.queued_pool.by_addr(address);
        (pending, queued)
    }

    pub fn pending_len(&self) -> usize {
        self.pending_pool.len()
    }

    pub fn queued_len(&self) -> usize {
        self.queued_pool.len()
    }
}

#[cfg(test)]
mod tests {
    use monad_triedb_utils::triedb_env::BlockHeader;
    use monad_eth_testutil::{ACCOUNTS, make_tx};
    use reth_primitives::{revm_primitives::FixedBytes, Header, B256};

    use super::*;

    async fn initialize_chain_cache_nonces(v_pool: &mut VirtualPool) {
        for account in ACCOUNTS {
            v_pool
                .chain_cache
                .inner
                .write()
                .await
                .accounts
                .insert(account.1, 0)
                .expect("insert accounts into chain cache");
        }
    }

    #[tokio::test]
    async fn test_vpool() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let mut v_pool = VirtualPool::new(ipc_sender.clone(), 20_000, None, None);
        let (sk, addr) = ACCOUNTS[0];

        initialize_chain_cache_nonces(&mut v_pool).await;

        let txs = vec![
            make_tx(sk, 1_000, 21_000, 0, 0),
            make_tx(sk, 1_000, 21_000, 1, 0),
            make_tx(sk, 1_000, 21_000, 2, 0),
            make_tx(sk, 1_000, 21_000, 3, 0),
        ];

        for tx in txs.clone() {
            v_pool.add_transaction(tx.into_ecrecovered().unwrap()).await;
        }
        assert_eq!(v_pool.pending_pool.pool.len(), 1);
        assert_eq!(v_pool.pending_pool.len(), 4);
        assert_eq!(v_pool.pending_pool.pool.get(&addr).unwrap().len(), 4);
        assert_eq!(v_pool.queued_pool.pool.len(), 0);

        v_pool
            .new_block(
                BlockWithReceipts {
                    transactions: txs.into_iter().collect(),
                    ..Default::default()
                },
                1000,
            )
            .await;

        // After block inclusion
        assert_eq!(v_pool.queued_pool.pool.len(), 0);
        assert_eq!(v_pool.pending_pool.pool.len(), 0);
    }

    #[tokio::test]
    async fn vpool_nonce_gap() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let mut v_pool = VirtualPool::new(ipc_sender.clone(), 20_000, None, None);
        let (sk, addr) = ACCOUNTS[0];

        initialize_chain_cache_nonces(&mut v_pool).await;

        let txs = vec![
            make_tx(sk, 1_000, 21_000, 0, 0), // included
        ];

        v_pool.new_block(BlockWithReceipts::default(), 1_000).await;
        for tx in txs {
            v_pool
                .add_transaction(tx.try_into_ecrecovered().unwrap())
                .await;
        }

        // Expect to discard txs[0] and put remaining in queued pool
        assert_eq!(v_pool.pending_pool.pool.len(), 1);
        assert_eq!(v_pool.queued_pool.pool.len(), 0);
        assert_eq!(v_pool.pending_pool.pool.get(&addr).unwrap().len(), 1);

        v_pool
            .new_block(
                BlockWithReceipts {
                    block_header: Header {
                            number: 1,
                            ..Default::default()
                    },
                    transactions: vec![make_tx(sk, 2_000, 21_000, 1, 0)],
                    ..Default::default()
                },
                1_000,
            )
            .await;

        assert_eq!(v_pool.queued_pool.pool.len(), 0);
        assert_eq!(v_pool.pending_pool.pool.len(), 0);

        let txs = vec![
            make_tx(sk, 1_000, 21_000, 3, 0),
            make_tx(sk, 1_000, 21_000, 4, 0),
        ];

        for tx in txs {
            v_pool.add_transaction(tx.into_ecrecovered().unwrap()).await;
        }
        assert_eq!(v_pool.pending_pool.pool.len(), 0);
        assert_eq!(v_pool.queued_pool.pool.len(), 1);
        assert_eq!(v_pool.queued_pool.pool.get(&addr).unwrap().len(), 2);
    }

    // Test behavior with fee replacement.
    // `vpool.add_transaction` should replace a pending transaction with a higher fee.
    #[tokio::test]
    async fn vpool_fee_replace() {
        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let mut vpool = VirtualPool::new(ipc_sender.clone(), 100, None, None);

        initialize_chain_cache_nonces(&mut vpool).await;

        // Add pending transaction with base fee of 1000
        let base = make_tx(ACCOUNTS[0].0, 1_000, 21_000, 0, 0);
        vpool
            .add_transaction(base.clone().into_ecrecovered().unwrap())
            .await;
        assert_eq!(vpool.pending_pool.pool.len(), 1);
        assert_eq!(
            vpool
                .pending_pool
                .get(&ACCOUNTS[0].1, &0)
                .unwrap()
                .transaction
                .max_fee_per_gas(),
            1000
        );

        let replace = make_tx(ACCOUNTS[0].0, 2_000, 21_000, 0, 0);
        vpool
            .add_transaction(replace.clone().try_into_ecrecovered().unwrap())
            .await;
        assert_eq!(vpool.pending_pool.pool.len(), 1);
        assert_eq!(
            vpool
                .pending_pool
                .get(&ACCOUNTS[0].1, &0)
                .unwrap()
                .transaction
                .max_fee_per_gas(),
            2000
        );

        let underpriced = make_tx(ACCOUNTS[0].0, 1_000, 21_000, 0, 0);
        vpool
            .add_transaction(underpriced.clone().try_into_ecrecovered().unwrap())
            .await;
        assert_eq!(vpool.pending_pool.pool.len(), 1);
        assert_eq!(
            vpool
                .pending_pool
                .get(&ACCOUNTS[0].1, &0)
                .unwrap()
                .transaction
                .max_fee_per_gas(),
            2000
        );

        let res = ipc_receiver.recv_async().await.unwrap();
        assert_eq!(res, base.clone());

        let res = ipc_receiver.recv_async().await.unwrap();
        assert_eq!(res, replace.clone());
    }

    // Create 10_000 transactions from a single sender.
    #[tokio::test]
    async fn vpool_stress() {
        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(10_000);
        let mut vpool = VirtualPool::new(ipc_sender.clone(), 20_000, None, None);
        initialize_chain_cache_nonces(&mut vpool).await;
        let (sk, addr) = ACCOUNTS[0];
        let mut txs = Vec::new();
        for i in 0..10_000 {
            txs.push(make_tx(sk, 1_000, 21_000, i, 0));
        }

        assert_eq!(txs.len(), 10_000);
        for tx in txs.clone() {
            vpool.add_transaction(tx.into_ecrecovered().unwrap()).await;
        }
        assert_eq!(vpool.pending_pool.pool.len(), 1);
        assert_eq!(vpool.queued_pool.pool.len(), 0);
        assert_eq!(vpool.pending_pool.pool.get(&addr).unwrap().len(), 10_000);

        let timer = std::time::Instant::now();
        vpool
            .new_block(
                BlockWithReceipts {
                    transactions: txs,
                    ..Default::default()
                },
                1_000,
            )
            .await;

        for _ in 0..10_000 {
            let _ = ipc_receiver.recv_async().await;
        }

        assert_eq!(vpool.pending_pool.pool.len(), 0);
        let elapsed = timer.elapsed();
        println!("stress test took {:?}", elapsed);
    }

    // Create 10K transactions for each 100 unique senders.
    #[tokio::test]
    async fn vpool_unique_accounts_stress() {
        let mut senders = Vec::new();
        // create 100 unique senders
        for idx in 0..100u64 {
            let mut sk: Vec<u8> = Vec::with_capacity(32);
            sk.extend_from_slice(&[1u8; 24]);
            sk.extend_from_slice(&idx.to_be_bytes());
            senders.push(sk);
        }

        let mut pending_txs = Vec::new();

        for sk in senders.clone() {
            for i in 0..100 {
                pending_txs.push(make_tx(
                    FixedBytes::<32>::from_slice(&sk),
                    1_000,
                    21_000,
                    i,
                    0,
                ));
            }
        }

        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(100_000);
        let mut vpool = VirtualPool::new(ipc_sender.clone(), 100, None, None);

        for sender in pending_txs
            .clone()
            .iter()
            .map(|tx| tx.recover_signer().unwrap())
        {
            vpool
                .chain_cache
                .inner
                .write()
                .await
                .accounts
                .insert(sender, 0);
        }

        let timer = std::time::Instant::now();
        for tx in pending_txs.clone() {
            vpool.add_transaction(tx.into_ecrecovered().unwrap()).await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K transactions took {:?}", elapsed);

        assert_eq!(vpool.pending_pool.pool.len(), 100);

        // create blockw ith transactions
        vpool
            .new_block(
                BlockWithReceipts {
                    transactions: pending_txs,
                    ..Default::default()
                },
                1_000,
            )
            .await;

        // create nonce gap
        let mut queued_txs = Vec::new();
        for sk in senders.clone() {
            for i in 101..200 {
                queued_txs.push(
                    make_tx(FixedBytes::<32>::from_slice(&sk), 1_000, 21_000, i, 0)
                        .into_ecrecovered()
                        .unwrap(),
                );
            }
        }

        let timer = std::time::Instant::now();
        for tx in queued_txs {
            vpool.add_transaction(tx.clone()).await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K queued transactions took {:?}", elapsed);

        assert_eq!(vpool.pending_pool.pool.len(), 0);
        assert_eq!(vpool.queued_pool.pool.len(), 100);

        let mut fix_nonce_gap_txs = Vec::new();
        for sk in senders.clone() {
            fix_nonce_gap_txs.push(
                make_tx(FixedBytes::<32>::from_slice(&sk), 1_000, 21_000, 100, 0)
                    .into_ecrecovered()
                    .unwrap(),
            );
        }

        vpool
            .new_block(
                BlockWithReceipts {
                    block_header: Header {
                            number: 1,
                            ..Default::default()
                    },
                    transactions: fix_nonce_gap_txs
                        .into_iter()
                        .map(|tx| tx.into_signed())
                        .collect(),
                    ..Default::default()
                },
                1_000,
            )
            .await;

        assert_eq!(vpool.queued_pool.pool.len(), 0);
    }

    #[tokio::test]
    async fn vpool_eviction() {
        // Set capacity to 2, add three transactions from unique senders. Expect pool to evict first transaction.
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(10_000);
        let mut vpool = VirtualPool::new(ipc_sender.clone(), 2, None, None);

        initialize_chain_cache_nonces(&mut vpool).await;

        vpool
            .add_transaction(
                make_tx(ACCOUNTS[0].0, 1_000, 21_000, 0, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        vpool
            .add_transaction(
                make_tx(ACCOUNTS[1].0, 1_000, 21_000, 0, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        vpool
            .add_transaction(
                make_tx(ACCOUNTS[2].0, 1_000, 21_000, 0, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;

        assert_eq!(vpool.pending_pool.len(), 3);
        let evicted = vpool.pending_pool.evict().await;
        assert_eq!(evicted, 1);
        assert_eq!(vpool.pending_pool.len(), 2);
        assert_eq!(vpool.queued_pool.len(), 0);

        // Add transactions from same sender, expect max nonce to be removed first.
        vpool
            .add_transaction(
                make_tx(ACCOUNTS[1].0, 1_000, 21_000, 1, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        vpool
            .add_transaction(
                make_tx(ACCOUNTS[1].0, 1_000, 21_000, 2, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        assert_eq!(vpool.pending_pool.len(), 4);
        assert_eq!(vpool.queued_pool.len(), 0);
        let evicted = vpool.pending_pool.evict().await;
        assert_eq!(evicted, 2);
        assert_eq!(vpool.pending_pool.len(), 2);
        assert_eq!(
            vpool.pending_pool.pool.get(&ACCOUNTS[1].1).unwrap().len(),
            2
        );

        // Check that nonce '2' from sender 1 is evicted first.
        assert_eq!(
            vpool
                .pending_pool
                .pool
                .get(&ACCOUNTS[1].1)
                .unwrap()
                .iter(&Guard::new())
                .next()
                .unwrap()
                .1
                .nonce(),
            0
        );
        assert_eq!(
            vpool
                .pending_pool
                .pool
                .get(&ACCOUNTS[1].1)
                .unwrap()
                .iter(&Guard::new())
                .last()
                .unwrap()
                .1
                .nonce(),
            1
        );

        let evicted = vpool.pending_pool.evict().await;
        assert_eq!(evicted, 0);
        assert_eq!(vpool.pending_pool.len(), 2);

        // Add transaction from sender 0, expect nonce 1 from sender 1 to be evicted.
        vpool
            .add_transaction(
                make_tx(ACCOUNTS[0].0, 1_000, 21_000, 0, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        assert_eq!(vpool.pending_pool.len(), 3);
        let evicted = vpool.pending_pool.evict().await;
        assert_eq!(evicted, 1);
        assert_eq!(vpool.pending_pool.len(), 2);
        assert_eq!(
            vpool.pending_pool.pool.get(&ACCOUNTS[1].1).unwrap().len(),
            1
        );
        assert_eq!(
            vpool
                .pending_pool
                .pool
                .get(&ACCOUNTS[1].1)
                .unwrap()
                .iter(&Guard::new())
                .last()
                .unwrap()
                .1
                .nonce(),
            0
        );
    }

    #[tokio::test]
    async fn test_nonce_gap_resolution() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(10_000);
        let mut vpool = VirtualPool::new(ipc_sender.clone(), 2, None, None);
        let tx = make_tx(ACCOUNTS[0].0, 1_000, 21_000, 0, 0)
            .into_ecrecovered()
            .unwrap();
        vpool.add_transaction(tx.clone()).await;

        vpool
            .new_block(
                BlockWithReceipts {
                    block_header: Header {
                            number: 1,
                            ..Default::default()
                    },
                    transactions: vec![tx].into_iter().map(|tx| tx.into_signed()).collect(),
                    ..Default::default()
                },
                1_000,
            )
            .await;

        vpool
            .add_transaction(
                make_tx(ACCOUNTS[0].0, 1_000, 21_000, 2, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        assert_eq!(vpool.queued_pool.len(), 1);

        vpool
            .add_transaction(
                make_tx(ACCOUNTS[0].0, 1_000, 21_000, 1, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        assert_eq!(vpool.queued_pool.len(), 0);
        assert_eq!(vpool.pending_pool.len(), 2);
    }

    #[tokio::test]
    async fn vpool_clear_included() {
        /*
        Tests behavior of clearing a pool which happens when a block commits transactions.
        */

        let mut pending_pool = SubPool::new(100);
        let mut commited = Vec::new();
        for nonce in 1..=10 {
            pending_pool
                .add(
                    make_tx(ACCOUNTS[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
            commited.push((ACCOUNTS[0].1, nonce));
        }
        assert_eq!(pending_pool.len(), 10);

        // Clear 2 transactions
        let cleared = pending_pool.clear_included(&commited[..2]);
        assert_eq!(cleared, 2);
        assert_eq!(pending_pool.len(), 8);

        // Clear the rest
        let cleared = pending_pool.clear_included(&commited);
        assert_eq!(cleared, 8);
        assert_eq!(pending_pool.len(), 0);

        // Add 10 transactions, expect to clear 9 entires in pool if the transaction with nonce 9 is commited.
        for nonce in 1..=10 {
            pending_pool
                .add(
                    make_tx(ACCOUNTS[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }
        assert_eq!(pending_pool.len(), 10);
        let cleared = pending_pool.clear_included(&[commited[8]]);
        assert_eq!(cleared, 9);
        assert_eq!(pending_pool.len(), 1);
    }

    #[tokio::test]
    async fn vpool_filter_by_nonce_gap() {
        /*
        Tests behavior of nonce gap filtering.
        Create 20 transactions with a gap at 0 and 11, then assert that pool can filter correctly when both nonce gaps are resolved.
        */
        let mut queued_pool = SubPool::new(100);

        for nonce in 1..=10 {
            queued_pool
                .add(
                    make_tx(ACCOUNTS[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        for nonce in 12..=21 {
            queued_pool
                .add(
                    make_tx(ACCOUNTS[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        assert_eq!(queued_pool.len(), 20);

        // Try to promote nonce 1, should return 0 since that does not resolve a nonce gap.
        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(ACCOUNTS[0].1, 1).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 0);
        assert_eq!(evicted.len(), 0);

        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(ACCOUNTS[0].1, 0).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 10);
        assert_eq!(evicted.len(), 0);

        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(ACCOUNTS[0].1, 11).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 0);
        assert_eq!(evicted.len(), 0);

        // Add 20 transactions again, and test that lower nonces are evicted when a noncegap is resolved.
        for nonce in 1..=10 {
            queued_pool
                .add(
                    make_tx(ACCOUNTS[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        for nonce in 12..=21 {
            queued_pool
                .add(
                    make_tx(ACCOUNTS[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        // Assume block has nonce 11, the first 10 should be evicted and the remaining 10 should be promoted.
        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(ACCOUNTS[0].1, 11).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 0);
        assert_eq!(evicted.len(), 10);

        for (evicted_nonce, expected_nonce) in evicted.iter().map(|tx| tx.nonce()).zip(1..=10) {
            assert_eq!(expected_nonce, evicted_nonce);
        }
    }
}