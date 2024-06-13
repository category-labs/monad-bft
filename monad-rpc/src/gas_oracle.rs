use std::{collections::VecDeque, sync::Arc};

use futures::{Stream, StreamExt};
use monad_blockdb::BlockValue;
use reth_primitives::{Block, TransactionSigned};
use tokio::sync::Mutex;

/// Number of transactions to sample in a block
const BLOCK_TX_SAMPLE_SIZE: usize = 3;

/// Number of blocks to sample
const BLOCK_SAMPLE_SIZE: usize = 20;

/// Gas price percentile
const GAS_PRICE_PERCENTILE: f64 = 60.;

const IGNORE_PRICE: u128 = 2;

/// Number of recent blocks to cache
const CACHE_CAPACITY: usize = 100;

pub struct Cache(VecDeque<Vec<u128>>);

impl Cache {
    fn new(capacity: Option<usize>) -> Self {
        Self(VecDeque::with_capacity(capacity.unwrap_or(CACHE_CAPACITY)))
    }

    fn insert(&mut self, value: Vec<u128>) {
        if self.0.len() == self.0.capacity() {
            self.0.pop_back();
        }
        self.0.push_front(value);
    }
}

#[derive(Debug)]
pub enum GasOracleError {
    BlockSampleSizeTooLarge,
}

#[derive(Clone)]
pub struct GasOracle {
    cache: Arc<Mutex<Cache>>,
    block_sample_size: usize,
}

impl GasOracle {
    pub fn new(
        mut block_stream: impl Stream<Item = BlockValue> + Send + Unpin + 'static,
        block_sample_size: Option<usize>,
    ) -> Result<Self, GasOracleError> {
        let cache = Arc::new(Mutex::new(Cache::new(None)));

        let block_sample_size = block_sample_size.unwrap_or(BLOCK_SAMPLE_SIZE);
        if block_sample_size > CACHE_CAPACITY {
            return Err(GasOracleError::BlockSampleSizeTooLarge);
        }

        let gas_oracle = Self {
            cache,
            block_sample_size,
        };

        let cache = Arc::clone(&gas_oracle.cache);

        tokio::spawn(async move {
            while let Some(block) = block_stream.next().await {
                let prices = sample_gas_tips(block.block);
                if prices.is_some() {
                    cache.lock().await.insert(prices.unwrap());
                }
            }
        });

        Ok(gas_oracle)
    }

    pub async fn tip(&self) -> Option<u128> {
        if self.cache.lock().await.0.len() < self.block_sample_size {
            return None;
        }

        let mut prices: Vec<u128> = self
            .cache
            .lock()
            .await
            .0
            .range(0..self.block_sample_size)
            .cloned()
            .flatten()
            .collect();

        prices.sort();
        let price = prices
            .get((GAS_PRICE_PERCENTILE * prices.len() as f64) as usize)
            .cloned();
        price
    }
}

/// Returns a sampled list of effective gas tips for a block.
/// Returns `None` if base fee is missing in the block or if the block is empty.
fn sample_gas_tips(block: Block) -> Option<Vec<u128>> {
    let base_fee = match block.base_fee_per_gas {
        Some(base_fee) => base_fee,
        None => return None,
    };

    let mut transactions = block.body.into_iter().collect::<Vec<TransactionSigned>>();
    transactions.sort_by_cached_key(|tx| tx.effective_tip_per_gas(Some(base_fee)));

    let mut prices = Vec::new();
    for tx in transactions {
        let tip = tx
            .effective_tip_per_gas(Some(base_fee))
            .expect("missing base fee");
        if tip < IGNORE_PRICE {
            continue;
        }

        prices.push(tip);

        if prices.len() > BLOCK_TX_SAMPLE_SIZE {
            break;
        }
    }

    if prices.is_empty() {
        None
    } else {
        Some(prices)
    }
}

#[cfg(test)]
mod tests {
    use reth_primitives::{Header, TxEip1559};

    use super::*;

    #[test]
    fn gas_prices_for_block() {
        let block = Block {
            header: Header {
                base_fee_per_gas: Some(1000),
                ..Default::default()
            },
            body: vec![
                TransactionSigned {
                    transaction: reth_primitives::Transaction::Eip1559(TxEip1559 {
                        max_priority_fee_per_gas: 0,
                        max_fee_per_gas: 1000,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                TransactionSigned {
                    transaction: reth_primitives::Transaction::Eip1559(TxEip1559 {
                        max_priority_fee_per_gas: 100,
                        max_fee_per_gas: 1200,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                TransactionSigned {
                    transaction: reth_primitives::Transaction::Eip1559(TxEip1559 {
                        max_priority_fee_per_gas: 300,
                        max_fee_per_gas: 1200,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                TransactionSigned {
                    transaction: reth_primitives::Transaction::Eip1559(TxEip1559 {
                        max_priority_fee_per_gas: 500,
                        max_fee_per_gas: 1200,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let prices = sample_gas_tips(block);
        assert!(prices.is_some());
        let prices = prices.unwrap();
        assert_eq!(prices.len(), 3);
        assert_eq!(prices[0], 100);
        assert_eq!(prices[1], 200);
        assert_eq!(prices[2], 200);
    }

    #[tokio::test]
    async fn gas_tip() {
        let transaction = |price: u128| TransactionSigned {
            transaction: reth_primitives::Transaction::Eip1559(TxEip1559 {
                max_priority_fee_per_gas: price - 1000,
                max_fee_per_gas: price,
                ..Default::default()
            }),
            ..Default::default()
        };

        let stream = futures::stream::iter(vec![
            BlockValue {
                block: Block {
                    header: Header {
                        base_fee_per_gas: Some(1000),
                        ..Default::default()
                    },
                    body: vec![transaction(1100), transaction(1101), transaction(1102)],
                    ..Default::default()
                },
            },
            BlockValue {
                block: Block {
                    header: Header {
                        base_fee_per_gas: Some(1000),
                        ..Default::default()
                    },
                    body: vec![transaction(1103), transaction(1104), transaction(1105)],
                    ..Default::default()
                },
            },
        ]);

        let oracle = GasOracle::new(stream, Some(2)).unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let tip = oracle.tip().await.unwrap();
        assert_eq!(tip, 104);
    }

    #[test]
    fn cache_capacity() {
        let mut cache = Cache::new(Some(2));
        cache.insert(vec![1]);
        cache.insert(vec![2]);
        cache.insert(vec![3]);
        assert_eq!(cache.0.capacity(), 2);
        assert_eq!(cache.0.len(), 2);
        assert_eq!(cache.0.pop_front(), Some(vec![3]));
        assert_eq!(cache.0.pop_front(), Some(vec![2]));
    }
}
