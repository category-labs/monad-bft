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

use std::marker::PhantomData;

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
use monad_eth_block_policy::nonce_usage::NonceUsageMap;
use monad_eth_types::{EthExecutionProtocol, ExtractEthAddress};
use monad_state_backend::StateBackend;
use monad_validator::signature_collection::SignatureCollection;
use tracing::error;

use self::limits::TrackedTxLimits;
pub use self::limits::TrackedTxLimitsConfig;
pub(super) use self::list::TrackedTxList;
use super::transaction::ValidEthTransaction;
use crate::{pool::tracked::priority::PriorityMap, EthTxPoolEventTracker};

mod limits;
mod list;
mod priority;

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
    priority: PriorityMap,
    limits: TrackedTxLimits,

    _phantom: PhantomData<(ST, SCT, SBT, CCT, CRT)>,
}

impl<ST, SCT, SBT, CCT, CRT> TrackedTxMap<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new(limits_config: TrackedTxLimitsConfig) -> Self {
        let limits = TrackedTxLimits::new(limits_config);

        Self {
            txs: limits.build_txs_map(),
            priority: PriorityMap::default(),
            limits,

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

    fn update_priority(&mut self, event_tracker: &EthTxPoolEventTracker<'_>, address: Address) {
        let Some(tx_list) = self.txs.get(&address) else {
            error!(
                ?address,
                "txpool update tx list called on non-existent address"
            );
            return;
        };

        self.priority
            .update_priority(event_tracker, address, tx_list);
    }

    pub fn try_insert_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        last_commit: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        address: Address,
        txs: Vec<ValidEthTransaction>,
        account_nonce: u64,
        on_insert: &mut impl FnMut(&ValidEthTransaction),
    ) {
        let mut inserted = false;

        match self.txs.entry(address) {
            IndexMapEntry::Occupied(o) => {
                let tx_list = o.into_mut();

                for tx in txs {
                    if let Some(tx) = tx_list.try_insert_tx(
                        event_tracker,
                        &mut self.limits,
                        tx,
                        last_commit.execution_inputs.base_fee_per_gas,
                    ) {
                        on_insert(tx);
                        inserted = true;
                    }
                }
            }
            IndexMapEntry::Vacant(v) => TrackedTxList::try_new(
                v,
                event_tracker,
                &mut self.limits,
                txs,
                account_nonce,
                &mut |tx| {
                    on_insert(tx);
                    inserted = true;
                },
                last_commit.execution_inputs.base_fee_per_gas,
            ),
        }

        if !inserted {
            return;
        }

        self.update_priority(event_tracker, address);

        while self.limits.is_exceeding_limits(self.txs.len()) {
            let Some(removal_address) = self.priority.pop_eviction_address() else {
                error!("txpool cannot find eviction address but exceeding limits");
                self.reset();
                return;
            };

            match self.txs.entry(removal_address) {
                IndexMapEntry::Vacant(_) => {
                    error!("txpool failed to find removal address during insertion");
                }
                IndexMapEntry::Occupied(o) => {
                    TrackedTxList::evict_pool_full(event_tracker, &mut self.limits, o);
                }
            }
        }
    }

    pub fn update_committed_nonce_usages(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        nonce_usages: NonceUsageMap,
    ) {
        for (address, nonce_usage) in nonce_usages.into_map() {
            match self.txs.entry(address) {
                IndexMapEntry::Occupied(tx_list) => {
                    if TrackedTxList::update_committed_nonce_usage(
                        event_tracker,
                        &mut self.limits,
                        tx_list,
                        nonce_usage,
                    ) {
                        self.update_priority(event_tracker, address);
                    } else {
                        self.priority.remove(address);
                    }
                }
                IndexMapEntry::Vacant(_) => {}
            }
        }
    }

    pub fn evict_expired_txs(&mut self, event_tracker: &mut EthTxPoolEventTracker<'_>) {
        let tx_expiry = self.limits.expiry_duration_during_evict();

        let mut idx = 0;

        loop {
            if idx >= self.txs.len() {
                break;
            }

            let Some(entry) = self.txs.get_index_entry(idx) else {
                break;
            };

            let address = *entry.key();

            if TrackedTxList::evict_expired_txs(event_tracker, &mut self.limits, entry, tx_expiry) {
                self.priority.remove(address);
                continue;
            }

            self.update_priority(event_tracker, address);

            idx += 1;
        }
    }

    pub fn static_validate_all_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_id: u64,
        chain_revision: &CRT,
        execution_revision: &MonadExecutionRevision,
    ) {
        self.txs.retain(|address, tx_list| {
            let retain = tx_list.static_validate_all_txs(
                event_tracker,
                &mut self.limits,
                chain_id,
                chain_revision,
                execution_revision,
            );

            if !retain {
                self.priority.remove(*address);
            }

            retain
        });

        for address in self.txs.keys().cloned().collect_vec() {
            self.update_priority(event_tracker, address);
        }
    }

    pub fn reset(&mut self) {
        self.txs.clear();
        self.priority.reset();
        self.limits.reset();
    }
}
