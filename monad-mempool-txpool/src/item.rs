use std::time::SystemTime;

use reth_primitives::TxHash;

#[derive(PartialEq, Eq, Clone)]
pub struct PoolTxHash {
    pub hash: TxHash,
    // Lower number is higher priority
    pub priority: i64,
    pub timestamp: SystemTime,
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
