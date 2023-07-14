use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    mem,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ethers::{types::Bytes, utils::keccak256};
use thiserror::Error;

use monad_mempool_types::tx::PriorityTx;

#[derive(Error, Debug)]
pub enum PoolError {
    #[error("Transaction already exists in pool")]
    DuplicateTransactionError,
}

#[derive(PartialEq, Eq, Clone)]
struct PriorityTxItem {
    hash: Bytes,
    // Lower number is higher priority
    priority: i64,
    timestamp: Duration,
}

impl PartialOrd for PriorityTxItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityTxItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering because BinaryHeap is a max heap
        (other.priority, other.timestamp).cmp(&(self.priority, self.timestamp))
    }
}

pub struct PoolConfig {
    ttl_duration: Duration,
    block_tx_limit: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            ttl_duration: Duration::from_secs(120),
            block_tx_limit: 10000,
        }
    }
}

impl PoolConfig {
    pub fn new(ttl_duration: Duration, block_tx_limit: usize) -> Self {
        Self {
            ttl_duration,
            block_tx_limit,
        }
    }
}

pub struct Pool {
    map: HashMap<Bytes, Bytes>,
    pq: BinaryHeap<PriorityTxItem>,
    ttl_duration: Duration,
    block_tx_limit: usize,
}

impl Pool {
    pub fn new(config: &PoolConfig) -> Self {
        Self {
            map: HashMap::new(),
            pq: BinaryHeap::new(),
            ttl_duration: config.ttl_duration,
            block_tx_limit: config.block_tx_limit,
        }
    }

    /// Removes a Vec of transactions from the pool by hash.
    /// Hashes that do not exist are skipped.
    pub fn remove_tx_hashes(&mut self, tx_hashes: Vec<Bytes>) {
        for tx_hash in &tx_hashes {
            self.map.remove(tx_hash);
        }

        #[allow(clippy::mutable_key_type)]
        let set = tx_hashes.into_iter().collect::<HashSet<_>>();
        self.pq = self
            .pq
            .drain()
            .filter(|tx| !set.contains(&tx.hash))
            .collect()
    }

    /// Removes a Vec of transactions from the pool using the full transaction.
    /// Transactions that do not exist are skipped.
    pub fn remove_txs(&mut self, txs: Vec<Bytes>) {
        let tx_hashes = txs
            .into_iter()
            .map(|tx| keccak256(tx).into())
            .collect::<Vec<_>>();

        self.remove_tx_hashes(tx_hashes);
    }

    /// Inserts a transaction into the pool.
    /// Only validated transactions should be inserted.
    pub fn insert(&mut self, mut tx: PriorityTx) -> Result<(), PoolError> {
        let hash: Bytes = mem::take(&mut tx.hash).into();

        if self.map.contains_key(&hash) {
            return Err(PoolError::DuplicateTransactionError);
        }

        self.map.insert(hash.clone(), tx.rlpdata.into());
        self.pq.push(PriorityTxItem {
            hash,
            priority: tx.priority,
            timestamp: Pool::get_current_epoch(),
        });

        Ok(())
    }

    /// Returns a Vec of transactions to be included in a block proposal.
    /// The highest priority transactions are returned, up to the block limit.
    /// If include_full_txs is true, the full transactions are returned.
    /// Otherwise, only the hashes are returned.
    pub fn create_proposal(&mut self, include_full_txs: bool) -> Vec<Bytes> {
        let mut txs = Vec::new();

        while txs.len() < self.block_tx_limit && !self.pq.is_empty() {
            let tx = self.pq.pop().unwrap();

            if !self.map.contains_key(&tx.hash) {
                continue;
            }
            if Self::get_current_epoch() - tx.timestamp > self.ttl_duration {
                continue;
            }

            txs.push(tx);
        }

        for tx in &txs {
            self.pq.push(tx.clone());
        }

        if include_full_txs {
            txs.into_iter()
                .map(|tx| self.map.get(&tx.hash).unwrap().clone())
                .collect()
        } else {
            txs.into_iter().map(|tx| tx.hash).collect()
        }
    }

    fn get_current_epoch() -> Duration {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::{Pool, PoolConfig};

    use ethers::types::Bytes;
    use ethers::{
        signers::LocalWallet,
        types::{transaction::eip2718::TypedTransaction, Address, TransactionRequest},
    };
    use oorandom::Rand32;

    use monad_mempool_types::tx::PriorityTx;

    const LOCAL_TEST_KEY: &str = "046507669b0b9d460fe9d48bb34642d85da927c566312ea36ac96403f0789b69";

    fn create_priority_txs(seed: u64, count: u16) -> Vec<PriorityTx> {
        let wallet = LOCAL_TEST_KEY.parse::<LocalWallet>().unwrap();

        create_txs(seed, count)
            .into_iter()
            .map(|tx| {
                let signature = wallet.sign_transaction_sync(&tx).unwrap();
                PriorityTx {
                    hash: tx.hash(&signature).as_bytes().to_vec(),
                    rlpdata: tx.rlp_signed(&signature).to_vec(),
                    priority: 0,
                }
            })
            .collect()
    }

    fn create_txs(seed: u64, count: u16) -> Vec<TypedTransaction> {
        let mut rng = Rand32::new(seed);

        (0..count)
            .map(|_| {
                TransactionRequest::new()
                    .to("0xc582768697b4a6798f286a03A2A774c8743163BB"
                        .parse::<Address>()
                        .unwrap())
                    .gas(21337)
                    .gas_price(42)
                    .value(rng.rand_u32())
                    .nonce(0)
                    .into()
            })
            .collect()
    }

    #[test]
    fn test_pool() {
        const TX_BATCH_SIZE: usize = 10;

        let mut pool = Pool::new(&PoolConfig::new(
            std::time::Duration::from_secs(120),
            TX_BATCH_SIZE,
        ));

        // Create 2 batches of transactions + 1 extra tx, with the second batch having a higher priority
        let mut txs = create_priority_txs(0, (TX_BATCH_SIZE * 2 + 1) as u16);

        for tx in txs.iter_mut().take(TX_BATCH_SIZE * 2).skip(TX_BATCH_SIZE) {
            tx.priority = -1;
        }

        for tx in &txs {
            pool.insert(tx.clone()).unwrap();
        }

        let proposal = pool.create_proposal(true);
        let expected_proposal = txs[TX_BATCH_SIZE..TX_BATCH_SIZE * 2]
            .iter()
            .map(|tx| tx.rlpdata.clone().into())
            .collect::<Vec<Bytes>>();

        assert_eq!(proposal.len(), TX_BATCH_SIZE);
        assert_eq!(proposal, expected_proposal);
        pool.remove_txs(proposal);

        let proposal2 = pool.create_proposal(false);
        let expected_proposal2 = txs[0..TX_BATCH_SIZE]
            .iter()
            .map(|tx| tx.hash.clone().into())
            .collect::<Vec<Bytes>>();

        assert_eq!(proposal2.len(), TX_BATCH_SIZE);
        assert_eq!(proposal2, expected_proposal2);

        // Simulate a failed proposal, doesn't get removed

        let proposal3 = pool.create_proposal(false);
        assert_eq!(proposal3.len(), TX_BATCH_SIZE);
        assert_eq!(proposal3, expected_proposal2);
        pool.remove_tx_hashes(proposal3);

        let proposal3 = pool.create_proposal(false);
        assert_eq!(proposal3.len(), 1);
        assert_eq!(
            proposal3[0],
            Bytes::from(txs[TX_BATCH_SIZE * 2].hash.clone())
        );
        pool.remove_tx_hashes(proposal3);

        let proposal4 = pool.create_proposal(false);
        assert_eq!(proposal4.len(), 0);
    }
}
