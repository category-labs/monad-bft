use std::path::PathBuf;

use monad_eth_types::EthAddress;
use monad_types::{SeqNum, GENESIS_SEQ_NUM};
use tracing::debug;

pub enum ReserveBalanceCacheResult {
    Val(SeqNum, u128), // triedb block seq_num, balance
    None,              // No balance found for the requested block_seq_num and address
    NeedSync,          // The requested block is ahead of latest triedb block seq num
}

pub trait ReserveBalanceCacheTrait {
    fn new(triedb_path: PathBuf, execution_delay: u64) -> Self;

    fn clone(&self) -> Self;

    fn get_account_balance(
        &mut self,
        consensus_block_seq_num: SeqNum,
        address: &EthAddress,
    ) -> ReserveBalanceCacheResult;
}

#[derive(Clone, Debug, Default)]
pub struct PassthruReserveBalanceCache;

impl ReserveBalanceCacheTrait for PassthruReserveBalanceCache {
    fn new(_path: PathBuf, _execution_delay: u64) -> Self {
        Self
    }

    fn clone(&self) -> Self {
        Clone::clone(self)
    }

    fn get_account_balance(
        &mut self,
        _consensus_block_seq_num: SeqNum,
        _address: &EthAddress,
    ) -> ReserveBalanceCacheResult {
        debug!("passthru cache get_reserve_balance");
        ReserveBalanceCacheResult::Val(GENESIS_SEQ_NUM, u64::MAX.into())
    }
}
