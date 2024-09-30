use monad_blockdb_utils::BlockDB;
use reth_primitives::{Block, TransactionSigned};
use reth_rpc_types::TransactionReceipt;
use std::{collections::VecDeque, sync::Arc};

use crate::{block_handlers::block_receipts, triedb::Triedb};

/// Number of transactions to sample in a block
const BLOCK_TX_SAMPLE_SIZE: usize = 3;

/// Number of blocks to sample
const BLOCK_SAMPLE_SIZE: usize = 20;

/// Gas price percentile
const GAS_PRICE_PERCENTILE: f64 = 0.60;

const IGNORE_PRICE: u128 = 2;

/// Number of recent blocks to cache
const CACHE_CAPACITY: usize = 100;

pub struct Cache(VecDeque<ProcessedBlock>);

impl Cache {
    fn new(capacity: Option<usize>) -> Self {
        Self(VecDeque::with_capacity(capacity.unwrap_or(CACHE_CAPACITY)))
    }

    fn insert(&mut self, value: ProcessedBlock) {
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

pub struct PollingGasOracle {
    cache: Arc<std::sync::Mutex<Cache>>,
    block_sample_size: usize,
    poll_handle: tokio::task::JoinHandle<()>,
}

pub trait GasOracle: Send + Sync {
    fn tip(&self) -> Option<u64>;
    fn shutdown(&self);
}

#[derive(Clone)]
pub struct MockOracle {}

impl GasOracle for MockOracle {
    fn tip(&self) -> Option<u64> {
        Some(100)
    }

    fn shutdown(&self) {}
}

impl PollingGasOracle {
    pub fn new<B: BlockDB + Send + 'static, T: Triedb + Send + 'static + Sync>(
        blockdb_env: B,
        triedb_env: T,
        current_height: u64,
        block_sample_size: Option<usize>,
    ) -> Result<Self, GasOracleError> {
        let cache = Arc::new(std::sync::Mutex::new(Cache::new(None)));

        let block_sample_size = block_sample_size.unwrap_or(BLOCK_SAMPLE_SIZE);
        if block_sample_size > CACHE_CAPACITY {
            return Err(GasOracleError::BlockSampleSizeTooLarge);
        }

        let cache2 = cache.clone();
        let poll_handle = tokio::spawn(async move {
            let mut cur_height = current_height;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                let Some(latest) = blockdb_env.get_latest_block().await else {
                    continue;
                };

                if latest.0 > cur_height {
                    for height in cur_height..=latest.0 {
                        let Some(block) = blockdb_env
                            .get_block_by_tag(monad_blockdb_utils::BlockTags::Number(height))
                            .await
                        else {
                            continue;
                        };

                        let receipts = block_receipts(&triedb_env, &block).await.unwrap();

                        let block = process_block(block.block, receipts);
                        match block {
                            Ok(block) => {
                                if let Ok(mut cache) = cache2.lock() {
                                    cache.insert(block);
                                } else {
                                    continue;
                                }
                            }
                            Err(_) => continue,
                        }
                        cur_height = height;
                    }
                }
            }
        });

        let gas_oracle = Self {
            poll_handle,
            cache,
            block_sample_size,
        };

        Ok(gas_oracle)
    }
}

impl GasOracle for PollingGasOracle {
    fn tip(&self) -> Option<u64> {
        let Ok(cache) = self.cache.lock() else {
            return None;
        };
        if cache.0.len() < self.block_sample_size {
            return None;
        }

        let mut prices: Vec<u64> = cache
            .0
            .range(0..self.block_sample_size)
            .cloned()
            .map(|block| block.sampled_tips)
            .flatten()
            .collect();

        prices.sort();
        prices
            .get((GAS_PRICE_PERCENTILE * prices.len() as f64) as usize)
            .cloned()
    }

    fn shutdown(&self) {
        self.poll_handle.abort();
    }
}

pub enum BlockProcessingError {
    MissingBaseFee,
}

#[derive(Clone)]
pub struct ProcessedBlock {
    // Sampled list of tips
    sampled_tips: Vec<u64>,
    // Base fees for block
    base_fee: u64,
    // Gas used ratios for each transaction
    gas_used_ratios: Vec<f64>,
    // effective priority fees per gas for each transaction
    rewards: Vec<u128>,
}

fn process_block(
    block: Block,
    receipts: Vec<TransactionReceipt>,
) -> Result<ProcessedBlock, BlockProcessingError> {
    let base_fee = match block.base_fee_per_gas {
        Some(base_fee) => base_fee,
        None => return Err(BlockProcessingError::MissingBaseFee),
    };

    let mut transactions = block
        .body
        .clone()
        .into_iter()
        .collect::<Vec<TransactionSigned>>();
    transactions.sort_by_cached_key(|tx| tx.effective_tip_per_gas(Some(base_fee)));

    let mut prices = Vec::new();
    let mut rewards = Vec::new();
    let mut gas_used_ratios = Vec::new();
    for (idx, tx) in transactions.iter().enumerate() {
        let tip = tx
            .effective_tip_per_gas(Some(base_fee))
            .expect("missing base fee");
        rewards.push(tip);

        // For each receipt, calculate gas_used_ratio
        let receipt = receipts.get(idx).unwrap();
        let Some(gas_used) = receipt.gas_used else {
            todo!()
        };

        let gas_used: f64 = gas_used.try_into().unwrap();
        let gas_used_ratio = gas_used / block.gas_limit as f64;
        gas_used_ratios.push(gas_used_ratio);

        if tip < IGNORE_PRICE {
            continue;
        }

        match tip.try_into() {
            Ok(price) => prices.push(price),
            Err(_) => continue,
        }

        if prices.len() > BLOCK_TX_SAMPLE_SIZE {
            break;
        }
    }

    Ok(ProcessedBlock {
        sampled_tips: prices,
        base_fee,
        gas_used_ratios,
        rewards,
    })
}

#[cfg(test)]
mod tests {
    use alloy_rlp::Encodable;
    use monad_blockdb::{BlockNumTableKey, BlockTableKey, BlockValue, EthTxKey, EthTxValue};
    use monad_blockdb_utils::BlockTags;
    use reth_primitives::{Header, Receipt, ReceiptWithBloom, TxEip1559};
    use reth_primitives::{Signature, U256};
    use std::sync::atomic::AtomicU64;

    use crate::triedb::TriedbResult;

    use super::*;

    struct MockBlockDB {
        latest_block_num: Arc<AtomicU64>,
        blocks: std::collections::HashMap<u64, Block>,
    }

    impl MockBlockDB {
        fn new_block(&mut self, block: Block) {
            self.blocks.insert(block.number, block);
        }
    }

    impl BlockDB for MockBlockDB {
        async fn get_latest_block(&self) -> Option<BlockNumTableKey> {
            Some(BlockNumTableKey(
                self.latest_block_num
                    .load(std::sync::atomic::Ordering::SeqCst),
            ))
        }

        async fn get_txn(&self, _: EthTxKey) -> Option<EthTxValue> {
            unimplemented!()
        }

        async fn get_block_by_hash(&self, _: BlockTableKey) -> Option<BlockValue> {
            unimplemented!()
        }

        async fn get_block_by_tag(&self, tag: BlockTags) -> Option<BlockValue> {
            match tag {
                BlockTags::Number(n) => self.blocks.get(&n).cloned().map(|b| BlockValue {
                    key: BlockTableKey(monad_types::BlockId(monad_types::Hash(b.hash_slow().0))),
                    block: b,
                }),
                BlockTags::Default(_) => unimplemented!(),
            }
        }
    }

    struct MockTriedb {}

    impl Triedb for MockTriedb {
        async fn get_latest_block(&self) -> TriedbResult {
            unimplemented!()
        }

        async fn get_receipt(&self, txn_index: u64, block_id: u64) -> TriedbResult {
            let receipt = ReceiptWithBloom {
                receipt: Receipt {
                    cumulative_gas_used: 21_000,
                    ..Default::default()
                },
                ..Default::default()
            };
            let mut buf = Vec::new();
            receipt.encode(&mut buf);

            TriedbResult::Receipt(buf)
        }

        async fn get_account(&self, addr: [u8; 20], block_tag: BlockTags) -> TriedbResult {
            unimplemented!()
        }

        async fn get_storage_at(
            &self,
            addr: [u8; 20],
            at: [u8; 32],
            block_tag: BlockTags,
        ) -> TriedbResult {
            unimplemented!()
        }

        async fn get_code(&self, code_hash: [u8; 32], block_tag: BlockTags) -> TriedbResult {
            unimplemented!()
        }
    }

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
                    signature: Signature {
                        odd_y_parity: false,
                        r: U256::from_str_radix(
                            "0000000000000000000000000000000000000000000000000000000000000000",
                            16,
                        )
                        .unwrap(),
                        s: U256::from_str_radix(
                            "0000000000000000000000000000000000000000000000000000000000000000",
                            16,
                        )
                        .unwrap(),
                    },
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
            signature: Signature {
                odd_y_parity: false,
                r: U256::from_str_radix(
                    "b129895435986f95c27e02bfae5f32e83aa09465154ed216b9534164ecab1016",
                    16,
                )
                .unwrap(),
                s: U256::from_str_radix(
                    "732a1eaaaa968aeedcfdf67fe34ee6157c169e7b6f5267601ec89a62a8b836c9",
                    16,
                )
                .unwrap(),
            },
            ..Default::default()
        };

        let block_height = Arc::new(AtomicU64::new(0));

        let mut blockdb = MockBlockDB {
            latest_block_num: block_height.clone(),
            blocks: Default::default(),
        };

        blockdb.new_block(Block {
            header: Header {
                base_fee_per_gas: Some(1000),
                number: 0,
                ..Default::default()
            },
            ..Default::default()
        });

        blockdb.new_block(Block {
            header: Header {
                base_fee_per_gas: Some(1000),
                number: 1,
                ..Default::default()
            },
            body: vec![transaction(1100), transaction(1101), transaction(1102)],
            ..Default::default()
        });
        blockdb.new_block(Block {
            header: Header {
                base_fee_per_gas: Some(1000),
                number: 2,
                ..Default::default()
            },
            body: vec![transaction(1103), transaction(1104), transaction(1105)],
            ..Default::default()
        });

        let triedb = MockTriedb {};
        let oracle = PollingGasOracle::new(blockdb, triedb, 0, Some(2)).unwrap();
        block_height.fetch_add(2, std::sync::atomic::Ordering::SeqCst);

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let tip = oracle.tip().unwrap();
        assert_eq!(tip, 103);
    }
}
