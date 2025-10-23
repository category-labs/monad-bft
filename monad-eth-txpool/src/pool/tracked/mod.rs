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

use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use alloy_primitives::Address;
use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use itertools::Itertools;
use monad_chain_config::{
    execution_revision::MonadExecutionRevision, revision::ChainRevision, ChainConfig,
};
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    nonce_usage::{NonceUsage, NonceUsageMap},
    EthBlockPolicy,
};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolInternalDropReason};
use monad_eth_types::EthExecutionProtocol;
use monad_state_backend::StateBackend;
use monad_types::{DropTimer, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::{debug, error, info, warn};

pub(super) use self::list::TrackedTxList;
use super::{
    pending::{PendingTxList, PendingTxMap},
    transaction::ValidEthTransaction,
};
use crate::EthTxPoolEventTracker;

mod list;

// To produce 5k tx blocks, we need the tracked tx map to hold at least 15k addresses so that, after
// pruning the txpool of up to 5k unique addresses in the last committed block update and up to 5k
// unique addresses in the pending blocktree, the tracked tx map will still have at least 5k other
// addresses with at least one tx each to use when creating the next block.
const MAX_ADDRESSES: usize = 16 * 1024;

// Tx batches from rpc can contain up to roughly 500 transactions. Since we don't evict based on how
// many txs are in the pool, we need to ensure that after eviction there is always space for all 500
// txs.
const SOFT_EVICT_ADDRESSES_WATERMARK: usize = MAX_ADDRESSES - 512;

/// Stores transactions using a "snapshot" system by which each address has an associated
/// account_nonce stored in the TrackedTxList which is guaranteed to be the correct
/// account_nonce for the seqnum stored in last_commit_seq_num.
#[derive(Clone, Debug)]
pub struct TrackedTxMap<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    // By using IndexMap, we can iterate through the map with Vec-like performance and are able to
    // evict expired txs through the entry API.
    txs: IndexMap<Address, TrackedTxList>,

    soft_tx_expiry: Duration,
    hard_tx_expiry: Duration,

    _phantom: PhantomData<(ST, SCT, SBT, CCT, CRT)>,
}

impl<ST, SCT, SBT, CCT, CRT> TrackedTxMap<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new(soft_tx_expiry: Duration, hard_tx_expiry: Duration) -> Self {
        Self {
            txs: IndexMap::with_capacity(MAX_ADDRESSES),

            soft_tx_expiry,
            hard_tx_expiry,

            _phantom: PhantomData,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn num_addresses(&self) -> usize {
        self.txs.len()
    }

    pub fn num_txs(&self) -> usize {
        self.txs.values().map(TrackedTxList::num_txs).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Address, &TrackedTxList)> {
        self.txs.iter()
    }

    pub fn iter_txs(&self) -> impl Iterator<Item = &ValidEthTransaction> {
        self.txs.values().flat_map(TrackedTxList::iter)
    }

    pub fn iter_mut_txs(&mut self) -> impl Iterator<Item = &mut ValidEthTransaction> {
        self.txs.values_mut().flat_map(TrackedTxList::iter_mut)
    }

    /// Produces a reference to the tx if it was inserted, producing None when the tx signer was
    /// tracked but the tx was not inserted. If the tx signer is not tracked or the tracked pool is
    /// not ready to accept txs, an error is produced with the original tx.
    pub fn try_insert_tx(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        last_commit: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        tx: ValidEthTransaction,
    ) -> Result<Option<&ValidEthTransaction>, ValidEthTransaction> {
        let Some(tx_list) = self.txs.get_mut(tx.signer_ref()) else {
            return Err(tx);
        };

        Ok(tx_list.try_insert_tx(
            event_tracker,
            tx,
            last_commit.execution_inputs.base_fee_per_gas,
            self.hard_tx_expiry,
        ))
    }

    pub fn try_promote_pending(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        last_commit: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
        max_promotable: usize,
    ) -> bool {
        let Some(insertable) = MAX_ADDRESSES.checked_sub(self.txs.len()) else {
            return false;
        };

        let insertable = insertable.min(max_promotable);

        if insertable == 0 {
            return true;
        }

        let to_insert = pending.split_off(insertable);

        if to_insert.is_empty() {
            return true;
        }

        let last_commit_seq_num = last_commit.seq_num;

        let addresses = to_insert.len();
        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, addresses, "txpool promote_pending")
        });

        let addresses = to_insert.keys().cloned().collect_vec();

        // BlockPolicy only guarantees that data is available for seqnum (N-k, N] for some execution
        // delay k. Since block_policy looks up seqnum - execution_delay, passing the last commit
        // seqnum will result in a lookup outside that range. As a fix, we add 1 so the seqnum is on
        // the edge of the range.
        let account_nonces = match block_policy.get_account_base_nonces(
            last_commit_seq_num + SeqNum(1),
            state_backend,
            &Vec::default(),
            addresses.iter(),
        ) {
            Ok(account_nonces) => account_nonces,
            Err(err) => {
                warn!(
                    ?err,
                    "failed to lookup account nonces during promote pending"
                );
                event_tracker.drop_all(
                    to_insert
                        .into_values()
                        .map(PendingTxList::into_map)
                        .flat_map(BTreeMap::into_values)
                        .map(ValidEthTransaction::into_raw),
                    EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError),
                );
                return false;
            }
        };

        for (address, pending_tx_list) in to_insert {
            let Some(account_nonce) = account_nonces.get(&address) else {
                error!("txpool address missing from state backend");

                event_tracker
                    .pending_drop_unknown(pending_tx_list.into_map().values().map(|tx| tx.hash()));

                continue;
            };

            match self.txs.entry(address) {
                IndexMapEntry::Occupied(_) => {
                    unreachable!("pending address present in tracked map")
                }
                IndexMapEntry::Vacant(v) => {
                    let Some(tracked_tx_list) = TrackedTxList::new_from_promote_pending(
                        event_tracker,
                        *account_nonce,
                        pending_tx_list,
                    ) else {
                        continue;
                    };

                    v.insert(tracked_tx_list);
                }
            }
        }

        true
    }

    pub fn update_committed_nonce_usages(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        nonce_usages: NonceUsageMap,
        pending: &mut PendingTxMap,
    ) {
        let mut insertable = MAX_ADDRESSES.saturating_sub(self.txs.len());

        for (address, nonce_usage) in nonce_usages.into_map() {
            match self.txs.entry(address) {
                IndexMapEntry::Occupied(tx_list) => {
                    TrackedTxList::update_committed_nonce_usage(event_tracker, tx_list, nonce_usage)
                }
                IndexMapEntry::Vacant(v) => match nonce_usage {
                    NonceUsage::Possible(_) => continue,
                    NonceUsage::Known(nonce) => {
                        if insertable == 0 {
                            continue;
                        }

                        let Some(pending_tx_list) = pending.remove(&address) else {
                            continue;
                        };

                        let account_nonce = nonce
                            .checked_add(1)
                            .expect("account nonce does not overflow");

                        let Some(tracked_tx_list) = TrackedTxList::new_from_promote_pending(
                            event_tracker,
                            account_nonce,
                            pending_tx_list,
                        ) else {
                            continue;
                        };

                        insertable -= 1;

                        v.insert(tracked_tx_list);
                    }
                },
            }
        }
    }

    pub fn evict_expired_txs(&mut self, event_tracker: &mut EthTxPoolEventTracker<'_>) {
        let num_txs = self.num_txs();

        let tx_expiry = if num_txs < SOFT_EVICT_ADDRESSES_WATERMARK {
            self.hard_tx_expiry
        } else {
            info!(?num_txs, "txpool hit soft evict addresses watermark");
            self.soft_tx_expiry
        };

        let mut idx = 0;

        loop {
            if idx >= self.txs.len() {
                break;
            }

            let Some(entry) = self.txs.get_index_entry(idx) else {
                break;
            };

            if TrackedTxList::evict_expired_txs(event_tracker, entry, tx_expiry) {
                continue;
            }

            idx += 1;
        }
    }

    pub fn reset(&mut self) {
        self.txs.clear();
    }

    pub fn static_validate_all_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_id: u64,
        chain_revision: &CRT,
        execution_revision: &MonadExecutionRevision,
    ) {
        self.txs.retain(|_, tx_list| {
            tx_list.static_validate_all_txs(
                event_tracker,
                chain_id,
                chain_revision,
                execution_revision,
            )
        });
    }
}
