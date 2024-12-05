use std::{collections::VecDeque, sync::Arc};

use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{Triedb, TriedbEnv};
use rayon::slice::ParallelSlice;
use reth_primitives::{Address, TransactionSigned, TransactionSignedEcRecovered};
use scc::{ebr::Guard, HashIndex, HashMap, TreeIndex};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{error, warn};

use crate::{
    block_watcher::BlockWithReceipts,
    eth_json_types::EthAddress,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    metrics::VirtualPoolMetrics,
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
    publisher: flume::Sender<TransactionSigned>,
    metrics: Option<Arc<VirtualPoolMetrics>>,
}

struct SubPool {
    // Mapping of sender to a transaction tree ordered by nonce
    pool: HashMap<Address, TreeIndex<u64, TransactionSignedEcRecovered>>,
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

    /// Adds a transaction to the SubPool, and returns the number of transactions evicted.
    async fn add(&self, txn: TransactionSignedEcRecovered, overwrite: bool) {
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
    }

    async fn evict(&self) -> usize {
        let Ok(mut lock) = self.evict.try_write() else {
            return 0;
        };
        let mut removed_count = 0;
        while lock.len() > self.capacity {
            if let Some(evicted) = lock.pop_back() {
                if let Some((_, tree)) = self.pool.remove(&evicted) {
                    removed_count += tree.len();
                }
            }
        }

        removed_count
    }

    fn clear_included(&self, txs: &[(Address, u64)]) -> usize {
        let mut removed = 0;
        for (sender, nonce) in txs {
            if let Some(mut entry) = self.pool.get(sender) {
                if entry.get_mut().remove(nonce) {
                    removed += 1;
                }
                if entry.len() == 0 {
                    let _ = entry.remove();
                    self.pool.remove(sender);
                }
            }
        }
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
        state: &HashIndex<Address, u64>,
    ) -> (
        Vec<TransactionSignedEcRecovered>,
        Vec<TransactionSignedEcRecovered>,
    ) {
        let mut to_promote = Vec::new();
        let mut to_evict = Vec::new();
        for (sender, nonce) in state.iter(&Guard::new()) {
            if let Some(mut entry) = self.pool.get(sender) {
                let mut removed = 0;
                let mut nonce = *nonce;
                for (min_nonce, tx) in entry.iter(&Guard::new()) {
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
                    entry.get_mut().remove_range(0..(removed as u64 + 1));
                }
            }
        }
        (to_promote, to_evict)
    }

    fn len(&self) -> usize {
        let mut len = 0;
        self.pool.scan(|_, v| len += v.len());
        len
    }
}

struct ChainCache {
    inner: RwLock<ChainCacheInner>,
}

struct ChainCacheInner {
    accounts: HashIndex<Address, u64>,
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
    ) -> HashIndex<Address, u64> {
        let mut inner = self.inner.write().await;
        inner.base_fee = next_base_fee;
        inner.latest_block_height = block.block_header.header.number;
        // Create a hashset of all senders and their nonces in the block.
        let senders = HashIndex::new();

        for (sender, nonce) in recovered_senders {
            senders.insert(sender, nonce);
        }

        let mut add_to_eviction_list = Vec::<(Address, u64)>::new();
        for (sender, nonce) in senders.iter(&Guard::new()) {
            match inner.accounts.get(sender) {
                Some(entry) => {
                    entry.update(*nonce);
                }
                None => {
                    inner.accounts.insert(*sender, *nonce);
                }
            }
            add_to_eviction_list.push((*sender, *nonce));
        }

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

    async fn nonce(&self, address: &Address) -> Option<u64> {
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

        // Fetch the current nonce of the sender
        let cur_nonce = self.chain_cache.nonce(&sender).await;

        if cur_nonce.map(|n| n == 0).unwrap_or(false) && nonce == 0 {
            return TxPoolType::Pending;
        }

        if cur_nonce.map(|n| n + 1 == nonce).unwrap_or(false) {
            return TxPoolType::Pending;
        }

        let pending_txs = self.pending_pool.by_addr(&sender);
        let queued_txs = self.queued_pool.by_addr(&sender);

        if cur_nonce
            .map(|cur_nonce| nonce.checked_sub(cur_nonce).map(|v| v > 1).unwrap_or(false))
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

        if let Some(true) = queued_txs.first().map(|tx| tx.nonce() < nonce) {
            return TxPoolType::Queue;
        }

        let Some(entry) = pending_txs.last() else {
            return TxPoolType::Pending;
        };

        let last_pending_nonce = entry.nonce();

        if last_pending_nonce + 1 == nonce {
            TxPoolType::Pending
        } else if last_pending_nonce == nonce {
            if let Some(entry) = self.pending_pool.get(&txn.signer(), &txn.nonce()) {
                // Replace a pending transaction if the fee is at least 10% higher than the current fee
                let current_gas_price = entry.transaction.max_fee_per_gas();
                let new_gas_price = txn.transaction.max_fee_per_gas();
                if new_gas_price >= current_gas_price + (current_gas_price / 10) {
                    TxPoolType::ReplacePending
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
                    self.metrics
                        .as_ref()
                        .map(|metrics| metrics.queued_pool.add(1, &[]));
                }
                TxPoolType::Pending => {
                    self.pending_pool.add(txn.clone(), false).await;
                    self.metrics
                        .as_ref()
                        .map(|metrics| metrics.pending_pool.add(1, &[]));
                    if self.publisher.send(txn.clone().into()).is_err() {
                        warn!("issue broadcasting transaction from pending pool");
                    }

                    // Check for available promotions
                    let tree = HashIndex::new();
                    tree.insert(txn.signer(), txn.nonce()).unwrap_or_default();
                    let (promoted, evicted) = self.queued_pool.filter_by_nonce_gap(&tree);
                    if evicted.len() > 0 {
                        let queue_evicted = i64::try_from(evicted.len()).unwrap_or_default();
                        self.metrics
                            .as_ref()
                            .map(|metrics| metrics.queued_pool.add(queue_evicted * -1, &[]));
                    }
                    if promoted.len() > 0 {
                        let queue_removed = i64::try_from(promoted.len()).unwrap_or_default();
                        self.metrics
                            .as_ref()
                            .map(|metrics| metrics.queued_pool.add(queue_removed * -1, &[]));

                        for promoted in promoted.into_iter() {
                            self.pending_pool.add(promoted.clone(), false).await;
                            self.metrics
                                .as_ref()
                                .map(|metrics| metrics.pending_pool.add(1, &[]));
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

                let pending_cleared =
                    i64::try_from(self.pending_pool.clear_included(&recovered_senders))
                        .unwrap_or_default();
                let pending_evicted =
                    i64::try_from(self.pending_pool.evict().await).unwrap_or_default();
                let queued_cleared =
                    i64::try_from(self.queued_pool.clear_included(&recovered_senders))
                        .unwrap_or_default();
                let queued_evicted =
                    i64::try_from(self.queued_pool.evict().await).unwrap_or_default();

                let senders = self
                    .chain_cache
                    .update(block.clone(), recovered_senders, next_base_fee)
                    .await;

                // Check for available promotions
                let (promote_queued, nonce_gap_evicted) =
                    self.queued_pool.filter_by_nonce_gap(&senders);

                let promoted_cleared = i64::try_from(promote_queued.len()).unwrap_or_default();
                let nonce_gap_evicted = i64::try_from(nonce_gap_evicted.len()).unwrap_or_default();

                self.metrics.as_ref().map(|metrics| {
                    metrics.pending_pool.add(
                        (pending_cleared * -1) + (pending_evicted * -1) + promoted_cleared,
                        &[],
                    )
                });

                self.metrics.as_ref().map(|metrics| {
                    metrics.queued_pool.add(
                        (promoted_cleared + queued_cleared + queued_evicted + nonce_gap_evicted)
                            * -1,
                        &[],
                    )
                });

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
            }
        }
    }

    // Adds a transaction to the txpool and decides which sub-pool to add it to
    pub async fn add_transaction(&self, txn: TransactionSignedEcRecovered) {
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
    ) -> (
        Vec<TransactionSignedEcRecovered>,
        Vec<TransactionSignedEcRecovered>,
    ) {
        let pending = self.pending_pool.by_addr(address);
        let queued = self.queued_pool.by_addr(address);
        (pending, queued)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        hash::{DefaultHasher, Hash, Hasher},
        str::FromStr,
        sync::Arc,
    };

    use alloy_primitives::FixedBytes;
    use monad_triedb_utils::triedb_env::BlockHeader;
    use reth_primitives::{hex::FromHex, sign_message, Header, TxEip1559, B256, U256};

    use super::*;

    fn accounts() -> Vec<(B256, Address)> {
        vec![
            (
                B256::from_hex("71ca04724a6d890ca96be3c2d3aa15df5e16619bec2bfe6d891065fb5f70eff5")
                    .unwrap(),
                Address::from_str("0xc29b3e29e33fe4612c946e72ffe4fcea013bf99b").unwrap(),
            ),
            (
                B256::from_hex("07cb040b0e2bdaad5bad62d9433f6a3880358005cf054260c7ddfc8d8ae169f0")
                    .unwrap(),
                Address::from_str("0xf78357155A03e155e0EdFbC3aC5f4532C95367f6").unwrap(),
            ),
            (
                B256::from_hex("ae208cc6a28de248173a7ba8385c3e1b9811160099dc55fbf8606cc974b96c72")
                    .unwrap(),
                Address::from_str("0xB6A5df2311E4D3F5376619FD05224AAFe4352aB9").unwrap(),
            ),
        ]
    }

    fn transaction(sk: B256, nonce: u64, fee: Option<u128>) -> TransactionSignedEcRecovered {
        let transaction = reth_primitives::Transaction::Eip1559(TxEip1559 {
            nonce,
            max_fee_per_gas: fee.unwrap_or(1_000),
            ..Default::default()
        });
        let signature = sign_message(sk, transaction.signature_hash()).unwrap();

        let mut hasher = DefaultHasher::new();
        transaction.hash(&mut hasher);
        let hash = U256::from(hasher.finish()).into();

        let signed_tx = TransactionSigned {
            transaction,
            signature,
            hash,
        };

        let signer = signed_tx.recover_signer().unwrap();

        TransactionSignedEcRecovered::from_signed_transaction(signed_tx, signer)
    }

    #[tokio::test]
    async fn test_txpool() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000, None, None));
        let (sk, addr) = accounts()[0];

        let txs = vec![
            transaction(sk, 1, None),
            transaction(sk, 2, None),
            transaction(sk, 3, None),
            transaction(sk, 4, None),
        ];

        for tx in txs.clone() {
            tx_pool.add_transaction(tx).await;
        }
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(tx_pool.pending_pool.len(), 4);
        assert_eq!(tx_pool.pending_pool.pool.get(&addr).unwrap().len(), 4);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.chain_cache.len().await, 0);

        tx_pool
            .new_block(
                BlockWithReceipts {
                    transactions: txs.into_iter().map(|tx| tx.into_signed()).collect(),
                    ..Default::default()
                },
                1000,
            )
            .await;

        // After block inclusion
        assert_eq!(tx_pool.chain_cache.len().await, 1);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.len(), 0);
    }

    #[tokio::test]
    async fn txpool_nonce_gap() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000, None, None));
        let (sk, addr) = accounts()[0];

        let txs = vec![
            transaction(sk, 1, None), // included
        ];

        tx_pool.new_block(BlockWithReceipts::default(), 1_000).await;
        for tx in txs {
            tx_pool.add_transaction(tx.clone()).await;
        }

        // Expect to discard txs[0] and put remaining in queued pool
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.get(&addr).unwrap().len(), 1);
        assert_eq!(tx_pool.chain_cache.len().await, 0);

        tx_pool
            .new_block(
                BlockWithReceipts {
                    block_header: BlockHeader {
                        header: Header {
                            number: 1,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    transactions: vec![transaction(sk, 1, Some(2000)).into_signed()],
                    ..Default::default()
                },
                1_000,
            )
            .await;

        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.len(), 0);

        let txs = vec![transaction(sk, 3, None), transaction(sk, 4, None)];

        for tx in txs {
            tx_pool.add_transaction(tx.clone()).await;
        }
        assert_eq!(tx_pool.pending_pool.pool.len(), 0);
        assert_eq!(tx_pool.queued_pool.pool.len(), 1);
        assert_eq!(tx_pool.queued_pool.pool.get(&addr).unwrap().len(), 2);
    }

    // Test behavior with fee replacement.
    // `tx_pool.add_transaction` should replace a pending transaction with a higher fee.
    #[tokio::test]
    async fn txpool_fee_replace() {
        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 100, None, None));

        // Add pending transaction with base fee of 1000
        let base = transaction(accounts()[0].0, 0, Some(1000));
        tx_pool.add_transaction(base.clone()).await;
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(
            tx_pool
                .pending_pool
                .get(&accounts()[0].1, &0)
                .unwrap()
                .transaction
                .max_fee_per_gas(),
            1000
        );

        let replace = transaction(accounts()[0].0, 0, Some(2000));
        tx_pool.add_transaction(replace.clone()).await;
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(
            tx_pool
                .pending_pool
                .get(&accounts()[0].1, &0)
                .unwrap()
                .transaction
                .max_fee_per_gas(),
            2000
        );

        let underpriced = transaction(accounts()[0].0, 0, Some(1000));
        tx_pool.add_transaction(underpriced.clone()).await;
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(
            tx_pool
                .pending_pool
                .get(&accounts()[0].1, &0)
                .unwrap()
                .transaction
                .max_fee_per_gas(),
            2000
        );

        for i in 0..1 {
            let res = ipc_receiver.recv_async().await.unwrap();
            match i {
                0 => {
                    assert_eq!(res, base.clone().into_signed());
                }
                1 => {
                    assert_eq!(res, replace.clone().into_signed());
                }
                _ => {
                    panic!("unexpected txn");
                }
            }
        }
    }

    // Create 10_000 transactions from a single sender.
    #[tokio::test]
    async fn txpool_stress() {
        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(10_000);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000, None, None));
        let (sk, addr) = accounts()[0];
        let mut txs = Vec::new();
        for i in 0..10_000 {
            txs.push(transaction(sk, i, None));
        }

        assert_eq!(txs.len(), 10_000);
        for tx in txs.clone() {
            tx_pool.add_transaction(tx.clone()).await;
        }
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.get(&addr).unwrap().len(), 10_000);

        let timer = std::time::Instant::now();
        tx_pool
            .new_block(
                BlockWithReceipts {
                    transactions: txs.into_iter().map(|tx| tx.into_signed()).collect(),
                    ..Default::default()
                },
                1_000,
            )
            .await;

        for _ in 0..10_000 {
            let _ = ipc_receiver.recv_async().await;
        }

        assert_eq!(tx_pool.pending_pool.pool.len(), 0);
        let elapsed = timer.elapsed();
        println!("stress test took {:?}", elapsed);
    }

    // Create 10K transactions for each 100 unique senders.
    #[tokio::test]
    async fn txpool_unique_accounts_stress() {
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
                pending_txs.push(transaction(FixedBytes::<32>::from_slice(&sk), i, None));
            }
        }

        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(100_000);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 100, None, None));

        let timer = std::time::Instant::now();
        for tx in pending_txs.clone() {
            tx_pool.add_transaction(tx.clone()).await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K transactions took {:?}", elapsed);

        assert_eq!(tx_pool.pending_pool.pool.len(), 100);

        // create blockw ith transactions
        tx_pool
            .new_block(
                BlockWithReceipts {
                    transactions: pending_txs.into_iter().map(|tx| tx.into_signed()).collect(),
                    ..Default::default()
                },
                1_000,
            )
            .await;

        // create nonce gap
        let mut queued_txs = Vec::new();
        for sk in senders.clone() {
            for i in 101..200 {
                queued_txs.push(transaction(FixedBytes::<32>::from_slice(&sk), i, None));
            }
        }

        let timer = std::time::Instant::now();
        for tx in queued_txs {
            tx_pool.add_transaction(tx.clone()).await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K queued transactions took {:?}", elapsed);

        assert_eq!(tx_pool.pending_pool.pool.len(), 0);
        assert_eq!(tx_pool.queued_pool.pool.len(), 100);

        let mut fix_nonce_gap_txs = Vec::new();
        for sk in senders.clone() {
            fix_nonce_gap_txs.push(transaction(FixedBytes::<32>::from_slice(&sk), 100, None));
        }

        tx_pool
            .new_block(
                BlockWithReceipts {
                    block_header: BlockHeader {
                        header: Header {
                            number: 1,
                            ..Default::default()
                        },
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

        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
    }

    #[tokio::test]
    async fn vpool_eviction() {
        // Set capacity to 2, add three transactions from unique senders. Expect pool to evict first transaction.
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(10_000);
        let vpool = Arc::new(VirtualPool::new(ipc_sender.clone(), 2, None, None));
        vpool
            .add_transaction(transaction(accounts()[0].0, 0, None))
            .await;
        vpool
            .add_transaction(transaction(accounts()[1].0, 0, None))
            .await;
        vpool
            .add_transaction(transaction(accounts()[2].0, 0, None))
            .await;

        assert_eq!(vpool.pending_pool.evict.try_read().unwrap().len(), 3);
        assert_eq!(vpool.pending_pool.pool.len(), 3);
        let evicted = vpool.pending_pool.evict().await;
        assert_eq!(evicted, 1);
        assert_eq!(vpool.pending_pool.len(), 2);
    }

    #[tokio::test]
    async fn test_nonce_gap_resolution() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(10_000);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 2, None, None));
        let tx = transaction(accounts()[0].0, 0, None);
        tx_pool.add_transaction(tx.clone()).await;

        tx_pool
            .new_block(
                BlockWithReceipts {
                    block_header: BlockHeader {
                        header: Header {
                            number: 1,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    transactions: vec![tx].into_iter().map(|tx| tx.into_signed()).collect(),
                    ..Default::default()
                },
                1_000,
            )
            .await;

        tx_pool
            .add_transaction(transaction(accounts()[0].0, 2, None))
            .await;
        assert_eq!(tx_pool.queued_pool.len(), 1);

        tx_pool
            .add_transaction(transaction(accounts()[0].0, 1, None))
            .await;
        assert_eq!(tx_pool.queued_pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.len(), 2);
    }

    #[tokio::test]
    async fn vpool_clear_included() {
        /*
        Tests behavior of clearing a pool which happens when a block commits transactions.
        */

        let pending_pool = SubPool::new(100);
        let mut commited = Vec::new();
        for nonce in 1..=10 {
            pending_pool
                .add(transaction(accounts()[0].0, nonce, None), false)
                .await;
            commited.push((accounts()[0].1, nonce));
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
    }

    #[tokio::test]
    async fn vpool_filter_by_nonce_gap() {
        /*
        Tests behavior of nonce gap filtering.
        Create 20 transactions with a gap at 0 and 11, then assert that pool can filter correctly when both nonce gaps are resolved.
        */
        let queued_pool = SubPool::new(100);

        for nonce in 1..=10 {
            queued_pool
                .add(transaction(accounts()[0].0, nonce, None), false)
                .await;
        }

        for nonce in 12..=21 {
            queued_pool
                .add(transaction(accounts()[0].0, nonce, None), false)
                .await;
        }

        assert_eq!(queued_pool.len(), 20);

        // Try to promote nonce 1, should return 0 since that does not resolve a nonce gap.
        let state: HashIndex<Address, u64> = HashIndex::new();
        state.insert(accounts()[0].1, 1).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 0);
        assert_eq!(evicted.len(), 0);

        let state: HashIndex<Address, u64> = HashIndex::new();
        state.insert(accounts()[0].1, 0).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 10);
        assert_eq!(evicted.len(), 0);

        let state: HashIndex<Address, u64> = HashIndex::new();
        state.insert(accounts()[0].1, 11).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 0);
        assert_eq!(evicted.len(), 0);

        // Add 20 transactions again, and test that lower nonces are evicted when a noncegap is resolved.
        for nonce in 1..=10 {
            queued_pool
                .add(transaction(accounts()[0].0, nonce, None), false)
                .await;
        }

        for nonce in 12..=21 {
            queued_pool
                .add(transaction(accounts()[0].0, nonce, None), false)
                .await;
        }

        // Assume block has nonce 11, the first 10 should be evicted and the remaining 10 should be promoted.
        let state: HashIndex<Address, u64> = HashIndex::new();
        state.insert(accounts()[0].1, 11).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 0);
        assert_eq!(evicted.len(), 10);

        for (evicted_nonce, expected_nonce) in evicted.iter().map(|tx| tx.nonce()).zip(1..=10) {
            assert_eq!(expected_nonce, evicted_nonce);
        }
    }
}
