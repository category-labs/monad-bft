use std::collections::BTreeMap;

use monad_eth_reserve_balance::{
    state_backend::StateBackend, ReserveBalanceCacheResult, ReserveBalanceCacheTrait,
};
use monad_eth_types::{EthAccount, EthAddress};
use monad_types::SeqNum;
use sorted_vector_map::SortedVectorMap;
use tracing::trace;

// TODO: rename to AccountStateCache
#[derive(Debug)]
pub struct ReserveBalanceCache<SBT> {
    cache: SortedVectorMap<SeqNum, BTreeMap<EthAddress, Option<EthAccount>>>,
    state_backend: SBT,
    execution_delay: u64,
}

impl<SBT: StateBackend> ReserveBalanceCache<SBT> {
    fn seq_num_cached(&self, base_seq_num: SeqNum) -> bool {
        if let Some((highest_seq_num, _)) = self.cache.last_key_value() {
            if *highest_seq_num >= base_seq_num {
                return true;
            }
        }

        false
    }

    fn seq_num_not_cached(&self, base_seq_num: SeqNum) -> bool {
        !self.seq_num_cached(base_seq_num)
    }

    fn seq_num_not_in_state(&self, seq_num: SeqNum) -> bool {
        !self.state_backend.is_available(seq_num.0)
    }
}

impl<SBT: StateBackend> ReserveBalanceCacheTrait<SBT> for ReserveBalanceCache<SBT> {
    fn new(state_backend: SBT, execution_delay: u64) -> Self {
        Self {
            cache: SortedVectorMap::new(),
            state_backend,
            execution_delay,
        }
    }

    fn get_account(
        &mut self,
        base_seq_num: SeqNum,
        address: &EthAddress,
    ) -> ReserveBalanceCacheResult {
        trace!(
            "ReserveBalance get_account 1 \
                base seq num {:?}, \
                address: {:?}",
            base_seq_num,
            address
        );

        if self.seq_num_not_cached(base_seq_num) && self.seq_num_not_in_state(base_seq_num) {
            trace!(
                "ReserveBalance get_account 2 \
                    TrieDB needs sync"
            );
            return ReserveBalanceCacheResult::NeedSync;
        }

        // purge cache if cache is adding a new entry, and due for periodic
        // cleaning
        let purge_cache =
            self.cache.get(&base_seq_num).is_none() && base_seq_num.0 % self.execution_delay == 0;

        let result = {
            let state_cache_entry = self.cache.entry(base_seq_num).or_default();
            // cache miss: fetch from state backend
            if !state_cache_entry.contains_key(address) {
                let maybe_account = self.state_backend.get_account(base_seq_num.0, address);
                assert!(state_cache_entry.insert(*address, maybe_account).is_none());
            }

            // read account from cache
            let maybe_account = state_cache_entry.get(address).expect("cache must hit");
            match maybe_account {
                Some(account) => ReserveBalanceCacheResult::Val(account.balance, account.nonce),
                None => ReserveBalanceCacheResult::None,
            }
        };

        if purge_cache {
            self.purge_old_blocks();
        }

        result
    }
}

impl<SBT: StateBackend> ReserveBalanceCache<SBT> {
    pub fn purge_old_blocks(&mut self) {
        let len = self.cache.len();
        let elems_to_keep = (self.execution_delay + 1) as usize;

        if elems_to_keep * 2 < len {
            // do not clear on each new block
            if let Some(nth_elem) = self.cache.iter().nth_back(elems_to_keep) {
                let divider = *nth_elem.0;
                // TODO: revisit once perf implications are understood
                self.cache = self.cache.split_off(&divider);
            }
        }
    }
}
