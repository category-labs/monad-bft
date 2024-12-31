use std::collections::VecDeque;

use alloy_consensus::{Transaction as _, TxEnvelope};
use alloy_primitives::Address;
use monad_eth_types::TxEnvelopeWithSigner;
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
    pub publisher: flume::Sender<TxEnvelope>,
}

struct SubPool {
    // Mapping of sender to a transaction tree ordered by nonce
    pool: HashMap<Address, TreeIndex<u64, TxEnvelopeWithSigner>>,
    evict: RwLock<VecDeque<Address>>,
    capacity: usize,
    name: &'static str,
}

impl SubPool {
    fn new(capacity: usize, name: &'static str) -> Self {
        Self {
            pool: HashMap::new(),
            evict: RwLock::new(VecDeque::new()),
            capacity,
            name,
        }
    }

    async fn add(&self, txn: TxEnvelopeWithSigner, overwrite: bool) {
        match self.pool.entry(txn.signer) {
            scc::hash_map::Entry::Occupied(mut entry) => {
                let tree = entry.get();
                match tree.contains(&txn.transaction.nonce()) {
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

                self.evict.write().await.push_front(txn.signer);
            }
        };

        let mut lock = self.evict.write().await;
        if lock.len() > self.capacity {
            if let Some(evicted) = lock.pop_back() {
                let evicted = self.pool.remove(&evicted);
                if let Some((evicted_address, evicted_tree)) = evicted {
                    tracing::warn!(
                        "evicted name={}, address={:?}, num_tx={:?}",
                        self.name,
                        evicted_address,
                        evicted_tree.len()
                    );
                }
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

    fn by_addr(&self, address: &Address) -> Vec<TxEnvelopeWithSigner> {
        let mut pending = Vec::new();
        self.pool.read(address, |_, map| {
            map.iter(&Guard::new())
                .for_each(|entry| pending.push(entry.1.clone()))
        });
        pending
    }

    fn get(&self, address: &Address, nonce: &u64) -> Option<TxEnvelopeWithSigner> {
        self.pool
            .get(address)?
            .get()
            .peek(nonce, &Guard::new())
            .cloned()
    }

    // Returns a list of transaction entries that are ready to be promoted because a nonce gap was resolved.
    fn filter_by_nonce_gap(&self, state: &HashIndex<Address, u64>) -> Vec<TxEnvelopeWithSigner> {
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
        txn: TxEnvelopeWithSigner,
    },
    BlockUpdate {
        block: BlockWithReceipts,
        next_base_fee: u128,
    },
}

impl VirtualPool {
    pub fn new(publisher: flume::Sender<TxEnvelope>, capacity: usize) -> Self {
        Self {
            pending_pool: SubPool::new(capacity, "pending"),
            queued_pool: SubPool::new(capacity, "queued"),
            chain_cache: ChainCache::new(capacity),
            publisher,
        }
    }

    async fn decide_pool(&self, txn: &TxEnvelopeWithSigner) -> TxPoolType {
        let sender = txn.signer;
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
            if let Some(entry) = self.pending_pool.get(&txn.signer, &txn.nonce()) {
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
                    if self.publisher.send(txn.transaction).is_err() {
                        warn!("issue broadcasting transaction from pending pool");
                    }
                }
                TxPoolType::Replace => {
                    self.pending_pool.add(txn.clone(), true).await;
                    if self.publisher.send(txn.transaction).is_err() {
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
                        txn.recover_signer()
                            .ok()
                            .map(|sender| (sender, txn.nonce()))
                    })
                    .collect::<Vec<_>>();

                self.pending_pool.clear_included(&txs);
                self.queued_pool.clear_included(&txs);

                let senders = self.chain_cache.update(block.clone(), next_base_fee).await;

                // Check for available promotions
                let promote_queued = self.queued_pool.filter_by_nonce_gap(&senders);

                if !promote_queued.is_empty() {
                    tracing::info!(
                        num_promoted =? promote_queued.len(),
                        "promote_queued",
                    );
                }
                // Add promoted transactions to the pending pool
                for promoted in promote_queued.into_iter() {
                    self.pending_pool.add(promoted.clone(), false).await;
                    if let Err(error) = self.publisher.send(promoted.transaction) {
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
    pub async fn add_transaction(&self, txn: TxEnvelopeWithSigner) {
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
    ) -> (Vec<TxEnvelopeWithSigner>, Vec<TxEnvelopeWithSigner>) {
        let pending = self.pending_pool.by_addr(address);
        let queued = self.queued_pool.by_addr(address);
        (pending, queued)
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use alloy_consensus::{Header, SignableTransaction, Transaction as _, TxEip1559, TxEnvelope};
    use alloy_primitives::hex;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_triedb_utils::triedb_env::BlockHeader;

    use super::*;

    fn accounts() -> Vec<(String, Address)> {
        vec![
            (
                String::from("71ca04724a6d890ca96be3c2d3aa15df5e16619bec2bfe6d891065fb5f70eff5"),
                Address::from_str("0xc29b3e29e33fe4612c946e72ffe4fcea013bf99b").unwrap(),
            ),
            (
                String::from("07cb040b0e2bdaad5bad62d9433f6a3880358005cf054260c7ddfc8d8ae169f0"),
                Address::from_str("0xf78357155A03e155e0EdFbC3aC5f4532C95367f6").unwrap(),
            ),
            (
                String::from("ae208cc6a28de248173a7ba8385c3e1b9811160099dc55fbf8606cc974b96c72"),
                Address::from_str("0xB6A5df2311E4D3F5376619FD05224AAFe4352aB9").unwrap(),
            ),
        ]
    }

    fn transaction(sk: &str, nonce: u64, fee: Option<u128>) -> TxEnvelopeWithSigner {
        let transaction = TxEip1559 {
            nonce,
            max_fee_per_gas: fee.unwrap_or(1_000),
            ..Default::default()
        };
        let signer = sk.parse::<PrivateKeySigner>().unwrap();
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();
        let signed_tx = transaction.into_signed(signature);

        TxEnvelopeWithSigner {
            signer: signer.address(),
            transaction: signed_tx.into(),
        }
    }

    #[tokio::test]
    async fn test_txpool() {
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TxEnvelope>(100);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000));
        let (sk, addr) = &accounts()[0];

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
        assert_eq!(tx_pool.pending_pool.pool.get(addr).unwrap().len(), 4);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.chain_cache.len().await, 0);

        tx_pool
            .new_block(
                BlockWithReceipts {
                    transactions: txs.into_iter().map(|tx| tx.transaction).collect(),
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
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TxEnvelope>(100);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000));
        let (sk, addr) = &accounts()[0];

        let txs = vec![
            transaction(sk, 1, None), // included
        ];

        tx_pool.new_block(BlockWithReceipts::default(), 1_000).await;
        for tx in txs {
            tx_pool.add_transaction(tx).await;
        }

        // Expect to discard txs[0] and put remaining in queued pool
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.get(addr).unwrap().len(), 1);
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
                    transactions: vec![transaction(sk, 1, Some(2000)).transaction],
                    ..Default::default()
                },
                1_000,
            )
            .await;

        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.len(), 0);

        let txs = vec![transaction(sk, 3, None), transaction(sk, 4, None)];

        for tx in txs {
            tx_pool.add_transaction(tx).await;
        }
        assert_eq!(tx_pool.pending_pool.pool.len(), 0);
        assert_eq!(tx_pool.queued_pool.pool.len(), 1);
        assert_eq!(tx_pool.queued_pool.pool.get(addr).unwrap().len(), 2);
    }

    // Test behavior with fee replacement.
    // `tx_pool.add_transaction` should replace a pending transaction with a higher fee.
    #[tokio::test]
    async fn txpool_fee_replace() {
        let (ipc_sender, ipc_receiver) = flume::bounded::<TxEnvelope>(100);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 100));

        // Add pending transaction with base fee of 1000
        let base = transaction(&accounts()[0].0, 0, Some(1000));
        tx_pool.add_transaction(base.clone()).await;
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(
            tx_pool
                .pending_pool
                .get(&accounts()[0].1, &0)
                .unwrap()
                .max_fee_per_gas(),
            1000
        );

        let replace = transaction(&accounts()[0].0, 0, Some(2000));
        tx_pool.add_transaction(replace.clone()).await;
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(
            tx_pool
                .pending_pool
                .get(&accounts()[0].1, &0)
                .unwrap()
                .max_fee_per_gas(),
            2000
        );

        let underpriced = transaction(&accounts()[0].0, 0, Some(1000));
        tx_pool.add_transaction(underpriced).await;
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(
            tx_pool
                .pending_pool
                .get(&accounts()[0].1, &0)
                .unwrap()
                .max_fee_per_gas(),
            2000
        );

        for i in 0..2 {
            let res = ipc_receiver.recv_async().await.unwrap();
            match i {
                0 => {
                    assert_eq!(res, base.transaction);
                }
                1 => {
                    assert_eq!(res, replace.transaction);
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
        let (ipc_sender, ipc_receiver) = flume::bounded::<TxEnvelope>(10_000);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 20_000));
        let (sk, addr) = &accounts()[0];
        let mut txs = Vec::new();
        for i in 0..10_000 {
            txs.push(transaction(sk, i, None));
        }

        assert_eq!(txs.len(), 10_000);
        for tx in txs.clone() {
            tx_pool.add_transaction(tx).await;
        }
        assert_eq!(tx_pool.pending_pool.pool.len(), 1);
        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
        assert_eq!(tx_pool.pending_pool.pool.get(addr).unwrap().len(), 10_000);

        let timer = std::time::Instant::now();
        tx_pool
            .new_block(
                BlockWithReceipts {
                    transactions: txs.into_iter().map(|tx| tx.transaction).collect(),
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
                pending_txs.push(transaction(&hex::encode(sk.clone()), i, None));
            }
        }

        let (ipc_sender, ipc_receiver) = flume::bounded::<TxEnvelope>(100_000);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 100));

        let timer = std::time::Instant::now();
        for tx in pending_txs.clone() {
            tx_pool.add_transaction(tx).await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K transactions took {:?}", elapsed);

        assert_eq!(tx_pool.pending_pool.pool.len(), 100);

        // create blockw ith transactions
        tx_pool
            .new_block(
                BlockWithReceipts {
                    transactions: pending_txs.into_iter().map(|tx| tx.transaction).collect(),
                    ..Default::default()
                },
                1_000,
            )
            .await;

        // create nonce gap
        let mut queued_txs = Vec::new();
        for sk in senders.clone() {
            for i in 101..200 {
                queued_txs.push(transaction(&hex::encode(sk.clone()), i, None));
            }
        }

        let timer = std::time::Instant::now();
        for tx in queued_txs {
            tx_pool.add_transaction(tx).await;
        }
        let elapsed = timer.elapsed();
        println!("adding 10K queued transactions took {:?}", elapsed);

        assert_eq!(tx_pool.pending_pool.pool.len(), 0);
        assert_eq!(tx_pool.queued_pool.pool.len(), 100);

        let mut fix_nonce_gap_txs = Vec::new();
        for sk in senders.clone() {
            fix_nonce_gap_txs.push(transaction(&hex::encode(sk), 100, None));
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
                        .map(|tx| tx.transaction)
                        .collect(),
                    ..Default::default()
                },
                1_000,
            )
            .await;

        assert_eq!(tx_pool.queued_pool.pool.len(), 0);
    }

    #[tokio::test]
    async fn txpool_eviction() {
        // Set capacity to 2, add three transactions from unique senders. Expect pool to evict first transaction.
        let (ipc_sender, _ipc_receiver) = flume::bounded::<TxEnvelope>(10_000);
        let tx_pool = Arc::new(VirtualPool::new(ipc_sender.clone(), 2));

        let tx1 = transaction(&accounts()[0].0, 0, None);
        tx_pool.add_transaction(tx1).await;

        let tx2 = transaction(&accounts()[1].0, 0, None);
        tx_pool.add_transaction(tx2).await;

        let tx3 = transaction(&accounts()[2].0, 0, None);
        tx_pool.add_transaction(tx3).await;

        assert_eq!(tx_pool.pending_pool.evict.try_read().unwrap().len(), 2);
        assert_eq!(tx_pool.pending_pool.pool.len(), 2)
    }
}
