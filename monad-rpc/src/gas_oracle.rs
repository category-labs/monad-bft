use std::{collections::VecDeque, sync::Arc};

use reth_primitives::{Block, TransactionSigned};
use reth_rpc_types::TransactionReceipt;
use tracing::warn;

/// Number of transactions to sample in a block
const BLOCK_TX_SAMPLE_SIZE: usize = 3;

/// Number of blocks to sample
const BLOCK_SAMPLE_SIZE: usize = 20;

/// Gas price percentile
const GAS_PRICE_PERCENTILE: f64 = 0.60;

/// Gas tips below this price in wei are ignored
// Constant defined in geth https://github.com/ethereum/go-ethereum/blob/25bc07749ce21376e1023a6e16ec173fa3fc4e43/eth/gasprice/gasprice.go#L40
const IGNORE_PRICE: u128 = 2;

/// Number of recent blocks to cache
const CACHE_CAPACITY: usize = 100;

pub struct Oracle {
    cache: Arc<std::sync::Mutex<VecDeque<ProcessedBlock>>>,
    block_sample_size: usize,
}

pub trait GasOracle: Send + Sync {
    // Adds a block to the gas oracle's cache to process tips and base fees.
    fn process_block(
        &self,
        block: Block,
        receipts: Vec<TransactionReceipt>,
    ) -> Result<(), GasOracleError>;
    // Returns the expected base fee for the next block.
    fn base_fee(&self) -> Option<u64>;
    // Returns the suggested priority tip block inclusion.
    fn tip(&self) -> Option<u64>;
}

impl Oracle {
    pub fn new(block_sample_size: Option<usize>) -> Self {
        Self {
            cache: Arc::new(std::sync::Mutex::new(VecDeque::with_capacity(
                CACHE_CAPACITY,
            ))),
            block_sample_size: block_sample_size.unwrap_or(BLOCK_SAMPLE_SIZE),
        }
    }
}

impl GasOracle for Oracle {
    fn tip(&self) -> Option<u64> {
        let Ok(cache) = self.cache.lock() else {
            return None;
        };
        if cache.len() < self.block_sample_size {
            return None;
        }

        let mut prices: Vec<u64> = cache
            .iter()
            .take(self.block_sample_size)
            .flat_map(|block| &block.sampled_tips)
            .cloned()
            .collect();

        prices.sort();
        prices
            .get((GAS_PRICE_PERCENTILE * prices.len() as f64) as usize)
            .cloned()
    }

    fn process_block(
        &self,
        block: Block,
        receipts: Vec<TransactionReceipt>,
    ) -> Result<(), GasOracleError> {
        let processed_block = process_block(block, receipts)?;

        let Ok(mut cache) = self.cache.lock() else {
            warn!("could not access gas oracle cache");
            return Err(GasOracleError::AddToCache);
        };
        cache.push_front(processed_block);

        if cache.len() > CACHE_CAPACITY {
            cache.pop_back();
        };

        Ok(())
    }

    // The base fee is currently static, and will be not dynamically adjust.
    fn base_fee(&self) -> Option<u64> {
        let block = if let Ok(cache) = self.cache.try_lock() {
            cache.front().cloned()
        } else {
            None
        };

        let block = block.as_ref()?;

        Some(block.base_fee)
    }
}

#[derive(Debug)]
pub enum GasOracleError {
    MissingBaseFee,
    AddToCache,
    TransactionReceiptMissing,
}

#[derive(Clone)]
pub struct ProcessedBlock {
    block_gas_limit: u64,
    block_gas_used: u64,
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
) -> Result<ProcessedBlock, GasOracleError> {
    let base_fee = block
        .base_fee_per_gas
        .ok_or(GasOracleError::MissingBaseFee)?;

    let mut transactions = block.body.iter().collect::<Vec<&TransactionSigned>>();
    transactions.sort_by_cached_key(|tx| tx.effective_tip_per_gas(Some(base_fee)));

    let mut prices = Vec::new();
    let mut rewards = Vec::new();
    let mut gas_used_ratios = Vec::new();
    for (idx, tx) in transactions.iter().enumerate() {
        let tip = if let Some(tip) = tx.effective_tip_per_gas(Some(base_fee)) {
            rewards.push(tip);
            tip
        } else {
            warn!("could not calculate effective tip for {:?}", tx.hash());
            continue;
        };

        // For each receipt, calculate gas_used_ratio
        let receipt = receipts
            .get(idx)
            .ok_or(GasOracleError::TransactionReceiptMissing)?;
        let Some(gas_used) = receipt.gas_used else {
            warn!(
                "receipt for {:#?} is missing gas_used",
                receipt.transaction_hash
            );
            continue;
        };

        let gas_used: f64 = gas_used.into();
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
        block_gas_limit: block.gas_limit,
        block_gas_used: block.gas_used,
        sampled_tips: prices,
        base_fee,
        gas_used_ratios,
        rewards,
    })
}

#[cfg(test)]
mod tests {
    use reth_primitives::{Header, Signature, TxEip1559, U256};

    use super::*;

    fn make_tx(price: u128) -> TransactionSigned {
        TransactionSigned {
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
        }
    }

    #[tokio::test]
    async fn oracle_gas_tip() {
        let blocks = [
            Block {
                header: Header {
                    base_fee_per_gas: Some(1000),
                    number: 0,
                    ..Default::default()
                },
                ..Default::default()
            },
            Block {
                header: Header {
                    base_fee_per_gas: Some(1000),
                    number: 1,
                    ..Default::default()
                },
                body: vec![make_tx(1100), make_tx(1101), make_tx(1102)],
                ..Default::default()
            },
            Block {
                header: Header {
                    base_fee_per_gas: Some(1000),
                    number: 2,
                    ..Default::default()
                },
                body: vec![make_tx(1103), make_tx(1104), make_tx(1105)],
                ..Default::default()
            },
        ];

        let oracle = Oracle::new(Some(2));

        for block in blocks {
            let mut receipts = Vec::new();
            for _ in block.body.iter() {
                receipts.push(TransactionReceipt {
                    gas_used: Some(U256::from(21_000)),
                    ..Default::default()
                });
            }
            oracle.process_block(block, receipts).unwrap();
        }

        let tip = oracle.tip().unwrap();
        assert_eq!(tip, 103);
    }
}
