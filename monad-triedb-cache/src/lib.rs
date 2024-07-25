use std::{collections::BTreeMap, path::PathBuf};

use monad_eth_reserve_balance::{ReserveBalanceCacheResult, ReserveBalanceCacheTrait};
use monad_eth_types::EthAddress;
use monad_triedb::Handle as TriedbHandle;
use monad_types::{SeqNum, GENESIS_SEQ_NUM};
use sorted_vector_map::SortedVectorMap;
use tracing::trace;

#[derive(Clone, Debug)]
pub struct ReserveBalanceCache {
    cache: SortedVectorMap<SeqNum, BTreeMap<EthAddress, u128>>,
    handle: TriedbHandle,
    execution_delay: u64,
}

impl ReserveBalanceCacheTrait for ReserveBalanceCache {
    fn new(triedb_path: PathBuf, execution_delay: u64) -> Self {
        Self {
            cache: SortedVectorMap::new(),
            handle: TriedbHandle::try_new(triedb_path.as_path())
                .expect("triedb should exist in path"),
            execution_delay,
        }
    }

    fn clone(&self) -> Self {
        Clone::clone(self)
    }

    fn get_account_balance(
        &mut self,
        consensus_block_seq_num: SeqNum,
        address: &EthAddress,
    ) -> ReserveBalanceCacheResult {
        trace!(
            "ReserveBalance get_account_balance 1 \
                block seq num {:?}, \
                address: {:?}",
            consensus_block_seq_num,
            address
        );
        // TODO: when the cache is separated from the reserve balance this call should be moved out.
        let triedb_block_seq_num = self.compute_triedb_block_seq_num(consensus_block_seq_num);
        if triedb_block_seq_num.is_none() {
            trace!(
                "ReserveBalance get_account_balance 2 \
                    TrieDB needs sync"
            );
            return ReserveBalanceCacheResult::NeedSync;
        }

        let block_seq_num = triedb_block_seq_num.unwrap();
        if let Some(block_balances) = self.cache.get_mut(&block_seq_num) {
            if let Some(acc_balance) = block_balances.get(address) {
                return ReserveBalanceCacheResult::Val(block_seq_num, *acc_balance);
            } else if let Some(acc_balance) =
                self.handle.get_account_balance(block_seq_num.0, address)
            {
                block_balances.insert(*address, acc_balance);
                return ReserveBalanceCacheResult::Val(block_seq_num, acc_balance);
            }
        } else {
            let mut block_balances = BTreeMap::new();
            if let Some(acc_balance) = self.handle.get_account_balance(block_seq_num.0, address) {
                block_balances.insert(*address, acc_balance);
                self.cache.insert(block_seq_num, block_balances);
                if block_seq_num.0 % self.execution_delay == 0 {
                    self.purge_old_blocks();
                }
                return ReserveBalanceCacheResult::Val(block_seq_num, acc_balance);
            }
        }
        ReserveBalanceCacheResult::None
    }
}

impl ReserveBalanceCache {
    pub fn purge_old_blocks(&mut self) {
        let len = self.cache.len();
        let elems_to_keep: usize = (self.execution_delay + 1).try_into().unwrap();
        let mut divider: Option<SeqNum> = None;

        if elems_to_keep * 2 < len {
            // do not clear on each new block
            if let Some(nth_elem) = self.cache.iter().nth_back(elems_to_keep) {
                divider = Some(*nth_elem.0);
            }
        }

        if divider.is_some() {
            self.cache.split_off(&divider.unwrap());
        }
    }

    fn compute_triedb_block_seq_num(&self, block_seq_num: SeqNum) -> Option<SeqNum> {
        let mut triedb_block_seq_num = GENESIS_SEQ_NUM;
        if block_seq_num.0 >= self.execution_delay {
            triedb_block_seq_num = block_seq_num - SeqNum(self.execution_delay);

            let triedb_latest_block = self.handle.latest_block();
            if triedb_block_seq_num > SeqNum(triedb_latest_block) {
                // TODO consensus node needs to wait for the triedb blocks to catch up.
                trace!("Compute TDB block_seq_num: the latest TDB block: {:?} is less than the computed TDB block_seq_num: {:?} :\
                       will use the latest TDB block_id available",
                    triedb_latest_block,
                    triedb_block_seq_num);
                return None;
            }
        } else {
            trace!("Compute TDB block_seq_num: block_seq_num: {:?} is less than execution delay: {:?}, using genesis triedb seq num: {:?}",
                block_seq_num,
                self.execution_delay,
                triedb_block_seq_num);
        }

        Some(triedb_block_seq_num)
    }
}
