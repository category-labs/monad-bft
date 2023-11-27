use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    time::{Duration, SystemTime},
};

use monad_eth_types::{EthFullTransactionList, EthTransactionList};
use reth_primitives::{TransactionSignedEcRecovered, TxHash};

mod config;
pub use config::PoolConfig;

mod error;
use error::PoolError;

mod item;
use item::PoolTxHash;

pub struct Pool {
    map: HashMap<TxHash, TransactionSignedEcRecovered>,
    pq: BinaryHeap<PoolTxHash>,

    ttl_duration: Duration,
    pending_duration: Duration,
}

impl Pool {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            map: HashMap::new(),
            pq: BinaryHeap::new(),

            ttl_duration: config.ttl_duration,
            pending_duration: config.pending_duration,
        }
    }

    /// Removes a Vec of transactions from the pool by hash.
    /// Hashes that do not exist are skipped.
    pub fn remove_tx_hashes(&mut self, tx_hashes: EthTransactionList) {
        for tx_hash in &tx_hashes.0 {
            self.map.remove(tx_hash);
        }

        let set = tx_hashes.0.into_iter().collect::<HashSet<TxHash>>();

        self.pq.retain(|tx| !set.contains(&tx.hash));
    }

    /// Inserts a transaction into the pool with a given priority.
    /// Only validated transactions should be inserted.
    fn insert_with_priority(
        &mut self,
        tx: TransactionSignedEcRecovered,
        timestamp: SystemTime,
        priority: i64,
    ) -> Result<(), PoolError> {
        let hash = tx.hash();

        // TODO-4: try_insert when stable
        if self.map.contains_key(&hash) {
            return Err(PoolError::DuplicateTransactionError);
        }

        assert!(self.map.insert(hash, tx).is_none());

        self.pq.push(PoolTxHash {
            hash,
            priority,
            timestamp,
        });

        Ok(())
    }

    /// Inserts a transaction into the pool.
    /// Only validated transactions should be inserted.
    pub fn insert(
        &mut self,
        tx: TransactionSignedEcRecovered,
        timestamp: SystemTime,
    ) -> Result<(), PoolError> {
        let priority = Self::determine_tx_priority(&tx);

        self.insert_with_priority(tx, timestamp, priority)
    }

    fn determine_tx_priority(_tx: &TransactionSignedEcRecovered) -> i64 {
        // TODO-2
        0
    }

    /// Returns a list of transaction hashes to be included in a block proposal.
    /// The highest priority transactions are returned, up to the block limit.
    pub fn create_proposal(
        &mut self,
        tx_limit: usize,
        pending_blocktree_txs: Vec<EthTransactionList>,
    ) -> EthTransactionList {
        let pending_blocktree_txs: HashSet<TxHash> = pending_blocktree_txs
            .into_iter()
            .flat_map(|tx| tx.0)
            .collect();

        let mut txs = Vec::new();
        let mut return_to_mempool_txs = Vec::default();

        let now = SystemTime::now();

        let tx_max_time = now
            .checked_sub(self.pending_duration)
            .expect("Maximum mempool tx SystemTime is valid");

        loop {
            if txs.len() >= tx_limit {
                break;
            }

            let Some(tx) = self.pq.pop() else {
                break;
            };

            if !self.map.contains_key(&tx.hash) {
                continue;
            }

            let Some(tx_expiration_time) = tx.timestamp.checked_add(self.ttl_duration) else {
                continue;
            };

            if tx_expiration_time < now {
                continue;
            }

            if tx.timestamp > tx_max_time {
                return_to_mempool_txs.push(tx);
                continue;
            }

            if pending_blocktree_txs.contains(&tx.hash) {
                return_to_mempool_txs.push(tx);
                continue;
            }

            txs.push(tx);
        }

        for tx in &txs {
            self.pq.push(tx.clone());
        }

        for tx in return_to_mempool_txs {
            self.pq.push(tx);
        }

        EthTransactionList(txs.into_iter().map(|tx| tx.hash).collect())
    }

    /// Returns the list of full transactions which correspond to a list of transaction hashes.
    pub fn fetch_full_txs(&mut self, txs: EthTransactionList) -> Option<EthFullTransactionList> {
        let mut full_txs = Vec::new();

        for tx in txs.0 {
            full_txs.push(self.map.get(&tx)?.clone());
        }

        Some(EthFullTransactionList(full_txs))
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use monad_eth_types::EthTransactionList;
    use monad_mempool_testutil::create_signed_eth_txs;
    use reth_primitives::TxHash;

    use super::{Pool, PoolConfig};

    #[test]
    fn test_pool() {
        const TX_BATCH_SIZE: usize = 10;

        let mut pool =
            Pool::new(PoolConfig::default().with_pending_duration(Duration::from_secs(0)));

        // Create 2 batches of transactions + 1 extra tx, with the second batch having a higher priority
        let mut txs = create_signed_eth_txs(0, (TX_BATCH_SIZE * 2 + 1) as u16);

        for (index, tx) in txs.iter().take(TX_BATCH_SIZE).enumerate() {
            pool.insert_with_priority(
                tx.clone(),
                SystemTime::now(),
                (TX_BATCH_SIZE + index) as i64,
            )
            .unwrap();
        }

        for (index, tx) in txs
            .iter()
            .skip(TX_BATCH_SIZE)
            .take(TX_BATCH_SIZE)
            .enumerate()
        {
            pool.insert_with_priority(tx.clone(), SystemTime::now(), index as i64)
                .unwrap();
        }

        pool.insert_with_priority(
            txs.get_mut(TX_BATCH_SIZE * 2).unwrap().clone(),
            SystemTime::now(),
            (TX_BATCH_SIZE * 2) as i64,
        )
        .unwrap();

        let proposal = pool.create_proposal(TX_BATCH_SIZE, vec![]);
        let expected_proposal = txs[TX_BATCH_SIZE..TX_BATCH_SIZE * 2]
            .iter()
            .map(|tx| tx.hash)
            .collect::<Vec<TxHash>>();

        assert_eq!(proposal.0.len(), TX_BATCH_SIZE);
        assert_eq!(proposal.0, expected_proposal);

        let fetched_txs = pool.fetch_full_txs(proposal.clone()).unwrap();
        assert_eq!(fetched_txs.0, txs[TX_BATCH_SIZE..TX_BATCH_SIZE * 2]);

        pool.remove_tx_hashes(proposal.clone());

        for tx in proposal.0 {
            assert!(pool.fetch_full_txs(EthTransactionList(vec![tx])).is_none());
        }

        let proposal2 = pool.create_proposal(TX_BATCH_SIZE, vec![]);
        let expected_proposal2 = txs[0..TX_BATCH_SIZE]
            .iter()
            .map(|tx| tx.hash)
            .collect::<Vec<TxHash>>();

        assert_eq!(proposal2.0.len(), TX_BATCH_SIZE);
        assert_eq!(proposal2.0, expected_proposal2);

        let fetched_txs2 = pool.fetch_full_txs(proposal2.clone()).unwrap();
        assert_eq!(fetched_txs2.0, txs[0..TX_BATCH_SIZE]);

        for tx in proposal2.0.clone() {
            assert!(pool.fetch_full_txs(EthTransactionList(vec![tx])).is_some());
        }

        // Simulate a failed proposal, doesn't get removed

        let proposal3 = pool.create_proposal(TX_BATCH_SIZE, vec![]);
        assert_eq!(proposal3.0.len(), TX_BATCH_SIZE);
        assert_eq!(proposal3.0, expected_proposal2);
        pool.remove_tx_hashes(proposal3);

        for tx in proposal2.0 {
            assert!(pool.fetch_full_txs(EthTransactionList(vec![tx])).is_none());
        }

        let proposal3 = pool.create_proposal(TX_BATCH_SIZE, vec![]);
        assert_eq!(proposal3.0.len(), 1);
        assert_eq!(proposal3.0[0], TxHash::from(txs[TX_BATCH_SIZE * 2].hash));
        pool.remove_tx_hashes(proposal3);

        let proposal4 = pool.create_proposal(TX_BATCH_SIZE, vec![]);
        assert_eq!(proposal4.0.len(), 0);
    }

    #[test]
    fn test_create_proposal_non_empty_pending_txs() {
        let mut pool =
            Pool::new(PoolConfig::default().with_pending_duration(Duration::from_secs(0)));

        let [tx1, tx2] = &create_signed_eth_txs(0, 2)[..] else {
            panic!();
        };

        pool.insert(tx1.to_owned(), SystemTime::now()).unwrap();

        assert_eq!(
            pool.create_proposal(10, vec![]),
            EthTransactionList(
                vec![tx1]
                    .into_iter()
                    .map(|tx| tx.hash)
                    .collect::<Vec<TxHash>>()
            )
        );

        assert!(pool
            .create_proposal(10, vec![EthTransactionList(vec![tx1.hash])])
            .0
            .is_empty());

        pool.insert(tx2.to_owned(), SystemTime::now()).unwrap();

        assert_eq!(
            pool.create_proposal(10, vec![]),
            EthTransactionList(
                vec![tx1, tx2]
                    .into_iter()
                    .map(|tx| tx.hash)
                    .collect::<Vec<TxHash>>()
            )
        );

        assert_eq!(
            pool.create_proposal(10, vec![EthTransactionList(vec![tx2.hash])]),
            EthTransactionList(
                vec![tx1]
                    .into_iter()
                    .map(|tx| tx.hash)
                    .collect::<Vec<TxHash>>()
            )
        );
    }

    #[test]
    fn test_create_proposal_pending_duration() {
        let mut pool = Pool::new(PoolConfig::default());

        let [tx1] = &create_signed_eth_txs(0, 1)[..] else {
            panic!();
        };

        pool.insert(tx1.to_owned(), SystemTime::now()).unwrap();

        assert_eq!(pool.create_proposal(10, vec![]), EthTransactionList(vec![]));

        std::thread::sleep(Duration::from_secs(1));

        assert_eq!(
            pool.create_proposal(10, vec![]),
            EthTransactionList(
                vec![tx1]
                    .into_iter()
                    .map(|tx| tx.hash)
                    .collect::<Vec<TxHash>>()
            )
        );
    }
}
