use std::cmp::Ordering;

use monad_eth_tx::{EthTransaction, EthTxHash};

use crate::utils::effective_tip_per_gas;

pub type VirtualTimestamp = u64;

/// Needed to have control over Ord implementation
#[derive(Debug, PartialEq)]
pub struct WrappedTransaction<'a> {
    pub inner: &'a EthTransaction,
    pub insertion_time: VirtualTimestamp,
    pub price_gas_limit_ratio: f64,
}

impl WrappedTransaction<'_> {
    pub fn effective_tip_per_gas(&self) -> u128 {
        effective_tip_per_gas(self.inner)
    }

    pub fn hash(&self) -> EthTxHash {
        self.inner.hash()
    }

    pub fn gas_limit(&self) -> u64 {
        self.inner.gas_limit()
    }

    pub fn inner(&self) -> &EthTransaction {
        self.inner
    }
}

impl Eq for WrappedTransaction<'_> {}

impl Ord for WrappedTransaction<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Unwrap safety: Okay to unwrap here so long as we guarantee price_gas_limit_ratio is not NaN
        // when inserting into the mempool. partial_cmp is guaranteed to return Some(_) if neither
        // operand is NaN
        (self.price_gas_limit_ratio, self.insertion_time)
            .partial_cmp(&(other.price_gas_limit_ratio, other.insertion_time))
            .unwrap()
    }
}

impl PartialOrd for WrappedTransaction<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
