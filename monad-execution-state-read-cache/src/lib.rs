// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
    time::Duration,
};

use itertools::Itertools;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{AccountKey, EthAccount, EthHeader, EthStorageKey, EthStorageSlot};
use monad_execution_state_read::{ExecutionStateRead, ExecutionStateReadError};
use monad_types::{BlockId, DropTimer, Epoch, SeqNum, Stake};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};
use tracing::warn;

#[derive(Debug)]
struct BlockCache {
    seq_num: SeqNum,
    accounts: BTreeMap<AccountKey, Option<EthAccount>>,
    storage: BTreeMap<(AccountKey, EthStorageKey), EthStorageSlot>,
    execution_result: Option<EthHeader>,
}

#[derive(Debug)]
pub struct ExecutionStateReadCache<ST, SCT, ESRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ESRT: ExecutionStateRead<ST, SCT>,
{
    cache: HashMap<BlockId, BlockCache>,
    state_read: ESRT,
    execution_delay: SeqNum,

    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT, ESRT> ExecutionStateReadCache<ST, SCT, ESRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ESRT: ExecutionStateRead<ST, SCT>,
{
    pub fn new(state_read: ESRT, execution_delay: SeqNum) -> Self {
        Self {
            cache: Default::default(),
            state_read,
            execution_delay,

            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, ESRT> ExecutionStateRead<ST, SCT> for ExecutionStateReadCache<ST, SCT, ESRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ESRT: ExecutionStateRead<ST, SCT>,
{
    fn get_account_statuses<'a>(
        &mut self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        account_keys: impl Iterator<Item = &'a AccountKey>,
    ) -> Result<Vec<Option<EthAccount>>, ExecutionStateReadError> {
        let account_keys = account_keys.collect_vec();
        if account_keys.is_empty() {
            return Ok(Vec::new());
        }

        // TODO consider removing this uniqueness filter... the callers we have so far already only
        // pass in a unique set of accounts
        let unique_account_keys = account_keys.iter().unique().copied();
        // find accounts that are missing from cache
        let cache_misses: Vec<_> = match self.cache.get(block_id) {
            None => unique_account_keys.collect(),
            Some(block_cache) => unique_account_keys
                .filter(|&account_key| !block_cache.accounts.contains_key(account_key))
                .collect(),
        };

        if !cache_misses.is_empty() {
            // hydrate cache with missing accounts
            let cache_misses_data = {
                let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
                    warn!(
                        ?elapsed,
                        lookups = cache_misses.len(),
                        "long get_account_statuses"
                    )
                });
                self.state_read.get_account_statuses(
                    block_id,
                    seq_num,
                    is_finalized,
                    cache_misses.iter().copied(),
                )?
            };
            self.cache
                .entry(*block_id)
                .or_insert_with(|| BlockCache {
                    seq_num: *seq_num,
                    accounts: Default::default(),
                    storage: Default::default(),
                    execution_result: None,
                })
                .accounts
                .extend(
                    cache_misses
                        .iter()
                        .map(|&&account_key| account_key)
                        .zip_eq(cache_misses_data),
                )
        }

        let block_cache = self
            .cache
            .get(block_id)
            .expect("cache must be populated... we asserted nonzero account keys at the start");

        let accounts_data = account_keys
            .iter()
            .map(|&account_key| {
                block_cache
                    .accounts
                    .get(account_key)
                    .expect("cache was hydrated")
            })
            .cloned()
            .collect();

        let last_finalized_block = self
            .raw_read_latest_finalized_block()
            .unwrap_or(SeqNum::MAX);

        self.cache
            .retain(|_, block| block.seq_num + self.execution_delay >= last_finalized_block);

        Ok(accounts_data)
    }

    fn get_execution_result(
        &mut self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, ExecutionStateReadError> {
        if let Some(block_cache) = self.cache.get(block_id) {
            if let Some(execution_result) = &block_cache.execution_result {
                return Ok(execution_result.clone());
            }
        }

        let execution_result =
            self.state_read
                .get_execution_result(block_id, seq_num, is_finalized)?;

        self.cache
            .entry(*block_id)
            .or_insert_with(|| BlockCache {
                seq_num: *seq_num,
                accounts: Default::default(),
                storage: Default::default(),
                execution_result: None,
            })
            .execution_result = Some(execution_result.clone());

        Ok(execution_result)
    }

    fn get_storage_at_by_key(
        &mut self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        account_key: AccountKey,
        storage_key: EthStorageKey,
    ) -> Result<EthStorageSlot, ExecutionStateReadError> {
        let cache_key = (account_key, storage_key);

        if let Some(block_cache) = self.cache.get(block_id) {
            if let Some(slot) = block_cache.storage.get(&cache_key) {
                return Ok(*slot);
            }
        }

        let slot = self.state_read.get_storage_at_by_key(
            block_id,
            seq_num,
            is_finalized,
            account_key,
            storage_key,
        )?;

        self.cache
            .entry(*block_id)
            .or_insert_with(|| BlockCache {
                seq_num: *seq_num,
                accounts: Default::default(),
                storage: Default::default(),
                execution_result: None,
            })
            .storage
            .insert(cache_key, slot);

        Ok(slot)
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.state_read.raw_read_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.state_read.raw_read_latest_finalized_block()
    }

    fn read_valset_at_block(
        &mut self,
        block_num: SeqNum,
        requested_epoch: Epoch,
    ) -> Vec<(SCT::NodeIdPubKey, SignatureCollectionPubKeyType<SCT>, Stake)> {
        self.state_read
            .read_valset_at_block(block_num, requested_epoch)
    }

    fn total_db_lookups(&self) -> u64 {
        self.state_read.total_db_lookups()
    }
}
