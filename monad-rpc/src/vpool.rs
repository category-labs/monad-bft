use std::{collections::VecDeque, sync::Arc};

use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{Triedb, TriedbEnv};
use rayon::slice::ParallelSlice;
use reth_primitives::{
    revm_primitives::bitvec::store::BitStore, Address, TransactionSigned,
    TransactionSignedEcRecovered,
};
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
    ) -> HashMap<Address, u64> {
        let mut inner = self.inner.write().await;
        inner.base_fee = next_base_fee;
        inner.latest_block_height = block.block_header.header.number;
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
                    if let Err(_) = inner.accounts.insert(*sender, *nonce + 1) {
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
            dbg!("SEND TO PENDING");
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

    async fn process_event(&self, event: TxPoolEvent) {
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
                    if promoted.len() > 0 {
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
                        block.block_header.header.number
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
    use std::{collections::BTreeMap, str::FromStr, sync::Arc, u64};

    use alloy_primitives::FixedBytes;
    use alloy_rlp::Decodable;
    use bytes::Bytes;
    use itertools::Itertools;
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_testutil::{generate_block_with_txs, make_tx};
    use monad_eth_txpool::{test_utils::Pool, EthTxPool};
    use monad_eth_types::EthAddress;
    use monad_state_backend::{InMemoryBlockState, InMemoryStateInner};
    use monad_testutil::signing::MockSignatures;
    use monad_triedb_utils::triedb_env::BlockHeader;
    use monad_types::{Round, SeqNum};
    use rand::{prelude::SliceRandom, rngs::StdRng, SeedableRng};
    use reth_primitives::{hex::FromHex, Header, B256};

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

    //
    #[tokio::test]
    async fn test_vpool() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let v_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000, None, None));
        let (sk, addr) = accounts()[0];

        initialize_chain_cache_nonces(v_pool.clone()).await;

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
        let v_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000, None, None));
        let (sk, addr) = accounts()[0];

        initialize_chain_cache_nonces(v_pool.clone()).await;

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
                    block_header: BlockHeader {
                        header: Header {
                            number: 1,
                            ..Default::default()
                        },
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
    // `tx_pool.add_transaction` should replace a pending transaction with a higher fee.
    #[tokio::test]
    async fn txpool_fee_replace() {
        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(100);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 100, None, None));

        initialize_chain_cache_nonces(tx_pool.clone()).await;

        // Add pending transaction with base fee of 1000
        let base = make_tx(accounts()[0].0, 1_000, 21_000, 0, 0);
        tx_pool
            .add_transaction(base.clone().into_ecrecovered().unwrap())
            .await;
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

        let replace = make_tx(accounts()[0].0, 2_000, 21_000, 0, 0);
        tx_pool
            .add_transaction(replace.clone().try_into_ecrecovered().unwrap())
            .await;
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

        let underpriced = make_tx(accounts()[0].0, 1_000, 21_000, 0, 0);
        tx_pool
            .add_transaction(underpriced.clone().try_into_ecrecovered().unwrap())
            .await;
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
                    assert_eq!(res, base.clone());
                }
                1 => {
                    assert_eq!(res, replace.clone());
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
        initialize_chain_cache_nonces(tx_pool.clone()).await;
        let (sk, addr) = accounts()[0];
        let mut txs = Vec::new();
        for i in 0..10_000 {
            txs.push(make_tx(sk, 1_000, 21_000, i, 0));
        }

        assert_eq!(txs.len(), 10_000);
        for tx in txs.clone() {
            tx_pool
                .add_transaction(tx.into_ecrecovered().unwrap())
                .await;
        }
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.get(&addr).unwrap().len(), 10_000);

        let timer = std::time::Instant::now();
        tx_pool
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
                pending_txs.push(make_tx(
                    FixedBytes::<32>::from_slice(&sk),
                    1_000,
                    21_000,
                    i,
                    0,
                ));
            }
        }

        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(100_000);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 100, None, None));

        for sender in pending_txs
            .clone()
            .iter()
            .map(|tx| tx.recover_signer().unwrap())
        {
            tx_pool
                .chain_cache
                .inner
                .write()
                .await
                .accounts
                .insert(sender, 0);
        }

        let timer = std::time::Instant::now();
        for tx in pending_txs.clone() {
            tx_pool
                .add_transaction(tx.into_ecrecovered().unwrap())
                .await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K transactions took {:?}", elapsed);

        assert_eq!(tx_pool.pending_pool.pool.len(), 100);

        // create blockw ith transactions
        tx_pool
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
            tx_pool.add_transaction(tx.clone()).await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K queued transactions took {:?}", elapsed);

        assert_eq!(tx_pool.pending_pool.pool.len(), 0);
        assert_eq!(tx_pool.queued_pool.pool.len(), 100);

        let mut fix_nonce_gap_txs = Vec::new();
        for sk in senders.clone() {
            fix_nonce_gap_txs.push(
                make_tx(FixedBytes::<32>::from_slice(&sk), 1_000, 21_000, 100, 0)
                    .into_ecrecovered()
                    .unwrap(),
            );
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

        initialize_chain_cache_nonces(vpool.clone()).await;

        vpool
            .add_transaction(
                make_tx(accounts()[0].0, 1_000, 21_000, 0, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        vpool
            .add_transaction(
                make_tx(accounts()[1].0, 1_000, 21_000, 0, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        vpool
            .add_transaction(
                make_tx(accounts()[2].0, 1_000, 21_000, 0, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
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
        let tx = make_tx(accounts()[0].0, 1_000, 21_000, 0, 0)
            .into_ecrecovered()
            .unwrap();
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
            .add_transaction(
                make_tx(accounts()[0].0, 1_000, 21_000, 2, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
            .await;
        assert_eq!(tx_pool.queued_pool.len(), 1);

        tx_pool
            .add_transaction(
                make_tx(accounts()[0].0, 1_000, 21_000, 1, 0)
                    .into_ecrecovered()
                    .unwrap(),
            )
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
                .add(
                    make_tx(accounts()[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
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

        // Add 10 transactions, expect to clear 9 entires in pool if the transaction with nonce 9 is commited.
        for nonce in 1..=10 {
            pending_pool
                .add(
                    make_tx(accounts()[0].0, 1_000, 21_000, nonce, 0)
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
        let queued_pool = SubPool::new(100);

        for nonce in 1..=10 {
            queued_pool
                .add(
                    make_tx(accounts()[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        for nonce in 12..=21 {
            queued_pool
                .add(
                    make_tx(accounts()[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        assert_eq!(queued_pool.len(), 20);

        // Try to promote nonce 1, should return 0 since that does not resolve a nonce gap.
        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(accounts()[0].1, 1).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 0);
        assert_eq!(evicted.len(), 0);

        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(accounts()[0].1, 0).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 10);
        assert_eq!(evicted.len(), 0);

        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(accounts()[0].1, 11).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 0);
        assert_eq!(evicted.len(), 0);

        // Add 20 transactions again, and test that lower nonces are evicted when a noncegap is resolved.
        for nonce in 1..=10 {
            queued_pool
                .add(
                    make_tx(accounts()[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        for nonce in 12..=21 {
            queued_pool
                .add(
                    make_tx(accounts()[0].0, 1_000, 21_000, nonce, 0)
                        .into_ecrecovered()
                        .unwrap(),
                    false,
                )
                .await;
        }

        // Assume block has nonce 11, the first 10 should be evicted and the remaining 10 should be promoted.
        let state: HashMap<Address, u64> = HashMap::new();
        state.insert(accounts()[0].1, 11).unwrap();
        let (promoted, evicted) = queued_pool.filter_by_nonce_gap(&state);
        assert_eq!(promoted.len(), 10);
        assert_eq!(queued_pool.len(), 0);
        assert_eq!(evicted.len(), 10);

        for (evicted_nonce, expected_nonce) in evicted.iter().map(|tx| tx.nonce()).zip(1..=10) {
            assert_eq!(expected_nonce, evicted_nonce);
        }
    }

    async fn initialize_chain_cache_nonces(v_pool: Arc<VirtualPool>) {
        for account in accounts() {
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

    fn make_txpool_state_backend(nonce: u64) -> Arc<std::sync::Mutex<InMemoryStateInner>> {
        let mut nonces: BTreeMap<EthAddress, u64> = BTreeMap::new();

        for account in accounts() {
            nonces.insert(EthAddress(account.1), nonce);
        }

        InMemoryStateInner::new(
            u128::MAX,
            SeqNum(u64::MAX),
            InMemoryBlockState::genesis(nonces),
        )
    }

    /*
    Integration test that sends transactions with nonce gaps to the virtual pool and commits them using txpool.
    Creates a list of transactions, shuffle, and take a list of transactions out of the list to make a nonce gap.
    Send the first list of transactions to the virtual pool, then send the rest of the transactions.
    Check queued pool and pending pool meets expectations after transactions are committed.
    */
    #[tokio::test]
    async fn vpool_txpool_with_gaps() {
        let mut eth_block_policy = EthBlockPolicy::new(SeqNum(0), 100, 1337);

        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(100_000);
        let v_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000, None, None));

        let mut tx_pool = EthTxPool::default();
        let mut current_round = 1u64;
        let mut current_seq_num = 1u64;
        let pending_blocks = VecDeque::default();
        let mut nonce = 0;

        initialize_chain_cache_nonces(v_pool.clone()).await;

        for _ in 0..10 {
            // Create transactions, add them to vpool.
            let mut txs = Vec::new();
            assert_eq!(
                nonce,
                v_pool.chain_cache.nonce(&accounts()[0].1).await.unwrap()
            );

            // 1000 transactions per account
            for (sk, _) in accounts() {
                for i in nonce..nonce + 1000 {
                    let tx: TransactionSigned = make_tx(sk, 1_000, 21_000, i, 0);
                    txs.push(tx);
                }
            }

            let state_backend = make_txpool_state_backend(nonce);

            nonce += 1000;

            let mut rng = StdRng::from_entropy();
            txs.shuffle(&mut rng);

            // Do not send all transactions to virtual pool at once. Instead, take a few transactions and send them later.
            // First, send the first 1500 transactions to the virtual pool. Then, send the rest of the transactions.
            let rest_txs = txs.split_off(1_500);

            for (test_idx, txs) in [txs, rest_txs].iter().enumerate() {
                for tx in txs.clone() {
                    v_pool.add_transaction(tx.into_ecrecovered().unwrap()).await;
                }

                let incoming_txs: Vec<_> = ipc_receiver.try_iter().collect();
                assert_eq!(v_pool.pending_pool.len(), incoming_txs.len());

                // After we send the rest, the queued pool should be empty.
                if test_idx == 1 {
                    assert_eq!(v_pool.queued_pool.len(), 0);
                }

                let pending_senders = incoming_txs
                    .iter()
                    .map(|tx| (tx.recover_signer_unchecked().unwrap(), tx.nonce()))
                    .collect_vec();
                let incoming_txs: Vec<_> = incoming_txs
                    .iter()
                    .map(|tx| Bytes::from(tx.envelope_encoded()))
                    .collect();
                let incoming_txs_len = incoming_txs.len();

                // Add transactions to txpool
                let inserted_txs = Pool::insert_tx(
                    &mut tx_pool,
                    incoming_txs.clone(),
                    &eth_block_policy,
                    &state_backend,
                );
                assert_eq!(inserted_txs.len(), incoming_txs_len);

                // Create a block proposal
                let encoded_txns = Pool::create_proposal(
                    &mut tx_pool,
                    SeqNum(0),
                    100_000,
                    u64::MAX,
                    &eth_block_policy,
                    pending_blocks.iter().collect_vec(),
                    &state_backend,
                )
                .expect("create proposal succeeds");

                let decoded_txns =
                    Vec::<reth_primitives::TransactionSigned>::decode(&mut encoded_txns.as_ref())
                        .unwrap();
                assert_eq!(decoded_txns.len(), incoming_txs_len);

                // Create Block
                let block = generate_block_with_txs(
                    Round(current_round),
                    SeqNum(current_seq_num),
                    decoded_txns,
                );
                assert_eq!(block.validated_txns.len(), incoming_txs_len);

                current_seq_num += 1;
                current_round += 1;

                monad_consensus_types::block::BlockPolicy::<
                    MockSignatures<monad_eth_txpool::test_utils::SignatureType>,
                    monad_eth_txpool::test_utils::StateBackendType,
                >::update_committed_block(&mut eth_block_policy, &block);

                monad_consensus_types::txpool::TxPool::<
                    MockSignatures<monad_eth_txpool::test_utils::SignatureType>,
                    EthBlockPolicy,
                    monad_eth_txpool::test_utils::StateBackendType,
                >::update_committed_block(&mut tx_pool, &block);

                let block_with_receipts = BlockWithReceipts {
                    block_header: BlockHeader {
                        header: Header {
                            number: current_seq_num,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    transactions: block
                        .validated_txns
                        .iter()
                        .map(|tx| tx.clone().into())
                        .collect(),
                    ..Default::default()
                };

                let commited_senders = block_with_receipts
                    .transactions
                    .iter()
                    .map(|tx| (tx.recover_signer_unchecked().unwrap(), tx.nonce()))
                    .collect_vec();

                // Compare differences between pending_senders and commited_senders
                assert_eq!(pending_senders.len(), commited_senders.len());
                assert!(pending_senders
                    .iter()
                    .filter(|(sender, nonce)| !commited_senders.contains(&(*sender, *nonce)))
                    .collect::<Vec<_>>()
                    .is_empty());

                // There should be no pending (sender, nonces) in the queued pool.
                let mut queued_txs = Vec::new();
                v_pool.queued_pool.pool.scan(|_, v| {
                    queued_txs.push(
                        v.iter(&Guard::new())
                            .map(|(nonce, tx)| (nonce.clone(), tx.signer()))
                            .collect::<Vec<_>>(),
                    );
                });

                let queued_senders = queued_txs
                    .iter()
                    .flatten()
                    .map(|(nonce, sender)| (*sender, *nonce))
                    .collect_vec();
                assert_eq!(v_pool.pending_pool.len(), incoming_txs.len());
                assert!(pending_senders
                    .iter()
                    .filter(|(sender, nonce)| queued_senders.contains(&(*sender, *nonce)))
                    .collect::<Vec<_>>()
                    .is_empty());

                // The pending pool should be equal to the commit senders.
                assert_eq!(v_pool.pending_pool.len(), commited_senders.len());
                assert_eq!(v_pool.pending_pool.len(), block.validated_txns.len());
                let queued_pool_len = v_pool.queued_pool.len();
                v_pool.new_block(block_with_receipts, 1_000).await;

                // Pending pool must be zero after committed transactions.
                assert_eq!(v_pool.pending_pool.len(), 0);

                // Committed transactions should not change the queued pool.
                assert_eq!(v_pool.queued_pool.len(), queued_pool_len);

                // Add the remaining transactions to the pool and expect them to be confirmed.
                if test_idx == 1 {
                    assert_eq!(v_pool.pending_pool.len(), 0);
                    assert_eq!(v_pool.queued_pool.len(), 0);
                }
            }
        }
    }

    /*
    Integration test that sends transactions to the virtual pool and commits them using txpool.
    Creates a list of transactions, shuffles and sends to the virtual pool.
    Then, creates a block proposal and block, and commits the transactions.
    */
    #[tokio::test]
    async fn vpool_txpool_ordered_txs() {
        let mut eth_block_policy = EthBlockPolicy::new(SeqNum(0), 100, 1337);

        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(100_000);
        let v_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000, None, None));

        let mut tx_pool = EthTxPool::default();
        let mut current_round = 1u64;
        let mut current_seq_num = 1u64;
        let pending_blocks = VecDeque::default();
        let mut nonce = 0;

        initialize_chain_cache_nonces(v_pool.clone()).await;

        for _ in 0..10 {
            // Create transactions, add them to vpool.
            let mut txs = Vec::new();
            assert_eq!(
                nonce,
                v_pool.chain_cache.nonce(&accounts()[0].1).await.unwrap()
            );
            for (sk, _) in accounts() {
                for i in nonce..nonce + 1000 {
                    let tx: TransactionSigned = make_tx(sk, 1_000, 21_000, i, 0);
                    txs.push(tx);
                }
            }

            let mut rng = StdRng::from_entropy();
            txs.shuffle(&mut rng);

            for tx in txs.clone() {
                v_pool.add_transaction(tx.into_ecrecovered().unwrap()).await;
            }

            let state_backend = make_txpool_state_backend(nonce);

            nonce += 1000;

            // Every tranasction should be in the pending pool because there is no nonce gap.
            assert_eq!(v_pool.pending_pool.len(), txs.len());
            assert_eq!(v_pool.queued_pool.len(), 0);

            let incoming_txs: Vec<_> = ipc_receiver.try_iter().collect();

            let pending_senders = incoming_txs
                .iter()
                .map(|tx| (tx.recover_signer_unchecked().unwrap(), tx.nonce()))
                .collect_vec();
            let incoming_txs: Vec<_> = incoming_txs
                .iter()
                .map(|tx| Bytes::from(tx.envelope_encoded()))
                .collect();
            let incoming_txs_len = incoming_txs.len();

            // Add transactions to txpool
            let inserted_txs = Pool::insert_tx(
                &mut tx_pool,
                incoming_txs,
                &eth_block_policy,
                &state_backend,
            );
            assert_eq!(inserted_txs.len(), incoming_txs_len);

            // Create a block proposal
            let encoded_txns = Pool::create_proposal(
                &mut tx_pool,
                SeqNum(0),
                100_000,
                u64::MAX,
                &eth_block_policy,
                pending_blocks.iter().collect_vec(),
                &state_backend,
            )
            .expect("create proposal succeeds");

            let decoded_txns =
                Vec::<reth_primitives::TransactionSigned>::decode(&mut encoded_txns.as_ref())
                    .unwrap();
            assert_eq!(decoded_txns.len(), incoming_txs_len);

            // Create Block
            let block = generate_block_with_txs(
                Round(current_round),
                SeqNum(current_seq_num),
                decoded_txns,
            );
            assert_eq!(block.validated_txns.len(), incoming_txs_len);

            current_seq_num += 1;
            current_round += 1;

            monad_consensus_types::block::BlockPolicy::<
                MockSignatures<monad_eth_txpool::test_utils::SignatureType>,
                monad_eth_txpool::test_utils::StateBackendType,
            >::update_committed_block(&mut eth_block_policy, &block);

            monad_consensus_types::txpool::TxPool::<
                MockSignatures<monad_eth_txpool::test_utils::SignatureType>,
                EthBlockPolicy,
                monad_eth_txpool::test_utils::StateBackendType,
            >::update_committed_block(&mut tx_pool, &block);

            let block_with_receipts = BlockWithReceipts {
                block_header: BlockHeader {
                    header: Header {
                        number: current_seq_num,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                transactions: block
                    .validated_txns
                    .iter()
                    .map(|tx| tx.clone().into())
                    .collect(),
                ..Default::default()
            };

            let commited_senders = block_with_receipts
                .transactions
                .iter()
                .map(|tx| (tx.recover_signer_unchecked().unwrap(), tx.nonce()))
                .collect_vec();

            // Compare differences between pending_senders and commited_senders
            assert_eq!(pending_senders.len(), commited_senders.len());
            assert!(pending_senders
                .iter()
                .filter(|(sender, nonce)| !commited_senders.contains(&(*sender, *nonce)))
                .collect::<Vec<_>>()
                .is_empty());

            // The pending pool should be equal to the commit senders.
            assert_eq!(v_pool.pending_pool.len(), commited_senders.len());
            assert!(!commited_senders.is_empty());
            assert_eq!(v_pool.pending_pool.len(), block.validated_txns.len());
            v_pool.new_block(block_with_receipts, 1_000).await;

            // Both pools must be cleared.
            assert_eq!(v_pool.pending_pool.len(), 0);
            assert_eq!(v_pool.queued_pool.len(), 0);
        }
    }
}
