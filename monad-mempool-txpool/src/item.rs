use std::time::SystemTime;

use reth_primitives::TxHash;

#[derive(PartialEq, Eq, Clone)]
pub struct PoolTxHash {
    /// keccak hash of the transaction
    pub hash: TxHash,
    /// Lower number is higher priority
    pub priority: i64,
    /// Time when the transaction is first submitted to a mempool
    pub timestamp: SystemTime,
    /// Eth transaction gas limit
    pub gas_limit: u64,
}

impl PartialOrd for PoolTxHash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PoolTxHash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering because BinaryHeap is a max heap
        (other.priority, other.timestamp).cmp(&(self.priority, self.timestamp))
    }
}
