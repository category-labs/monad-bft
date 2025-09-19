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

use std::{marker::PhantomData, time::Duration};

use alloy_primitives::Address;
use indexmap::{map::Entry as IndexMapEntry, IndexMap};
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
use tracing::info;

pub(super) use self::list::TrackedTxList;
use super::transaction::ValidEthTransaction;
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
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
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

    pub fn try_insert_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        last_commit: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        address: Address,
        txs: Vec<ValidEthTransaction>,
        account_nonce: u64,
        on_insert: &mut impl FnMut(&ValidEthTransaction),
    ) {
        match self.txs.entry(address) {
            IndexMapEntry::Occupied(o) => {
                let tx_list = o.into_mut();

                for tx in txs {
                    if let Some(tx) = tx_list.try_insert_tx(
                        event_tracker,
                        tx,
                        last_commit.execution_inputs.base_fee_per_gas,
                        self.hard_tx_expiry,
                    ) {
                        on_insert(tx);
                    }
                }
            }
            IndexMapEntry::Vacant(v) => {
                TrackedTxList::try_new(
                    v,
                    event_tracker,
                    txs,
                    account_nonce,
                    on_insert,
                    last_commit.execution_inputs.base_fee_per_gas,
                    self.hard_tx_expiry,
                );
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
                    TrackedTxList::update_committed_nonce_usage(event_tracker, tx_list, nonce_usage)
                }
                IndexMapEntry::Vacant(_) => {}
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
