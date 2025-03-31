use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolEvictReason};

#[derive(Clone)]
pub enum TxStatus {
    // No response
    Unknown,

    // Alive
    Pending,
    Tracked,

    // Dead
    Dropped { reason: EthTxPoolDropReason },
    Evicted { reason: EthTxPoolEvictReason },
    Replaced,
    Committed,
}
