use core::fmt::Debug;

use monad_eth_types::EthFullTransactionList;

use crate::payload::{FullTransactionList, TransactionHashList};

pub trait BlockValidator: Clone + Default {
    fn validate(&self, txs: &TransactionHashList, full_txs: &FullTransactionList) -> bool;
}

#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct MockValidator;

impl BlockValidator for MockValidator {
    fn validate(&self, _txs: &TransactionHashList, _full_txs: &FullTransactionList) -> bool {
        true
    }
}

/// Validates transactions as valid Ethereum transactions and also validates that
/// the list of transactions will create a valid Ethereum block
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct EthereumValidator {
    /// max number of txns to fetch
    max_txs: usize,
    /// limit on cumulative gas from transactions in a block
    block_gas_limit: u64,
}

impl EthereumValidator {
    pub fn new(max_txs: usize, block_gas_limit: u64) -> Self {
        Self {
            max_txs,
            block_gas_limit,
        }
    }
}

impl BlockValidator for EthereumValidator {
    fn validate(&self, _txs: &TransactionHashList, full_txs: &FullTransactionList) -> bool {
        let Ok(eth_txns) = EthFullTransactionList::rlp_decode(full_txs.bytes().clone()) else {
            return false;
        };
        // TODO-2: Eth transaction checks

        if eth_txns.0.len() > self.max_txs {
            return false;
        }

        let total_gas = eth_txns.0.iter().fold(0, |acc, tx| acc + tx.gas_limit());
        if total_gas > self.block_gas_limit {
            return false;
        }

        true
    }
}
