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

use alloy_primitives::Address;
use indexmap::IndexMap;
use monad_chain_config::{execution_revision::MonadExecutionRevision, revision::ChainRevision};
use monad_eth_txpool_types::EthTxPoolDropReason;
use tracing::warn;

pub use self::list::PendingTxList;
use super::transaction::ValidEthTransaction;
use crate::EthTxPoolEventTracker;

mod list;

// These constants were set using intuition and should be changed once we have more performance
// numbers for the txpool.
const MAX_ADDRESSES: usize = 8 * 1024;
const MAX_TXS: usize = 16 * 1024;
const PROMOTE_TXS_WATERMARK: usize = MAX_TXS * 3 / 4;

/// Wrapper type to store byte-validated transactions and quickly query the total number of
/// transactions in the txs map.
#[derive(Clone, Debug, Default)]
pub struct PendingTxMap {
    txs: IndexMap<Address, PendingTxList>,
    num_txs: usize,
}

impl PendingTxMap {
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn num_addresses(&self) -> usize {
        self.txs.len()
    }

    pub fn num_txs(&self) -> usize {
        self.num_txs
    }

    pub fn is_at_promote_txs_watermark(&self) -> bool {
        self.num_txs >= PROMOTE_TXS_WATERMARK
    }

    pub fn iter_txs(&self) -> impl Iterator<Item = &ValidEthTransaction> {
        self.txs.values().flat_map(PendingTxList::iter)
    }

    pub fn iter_mut_txs(&mut self) -> impl Iterator<Item = &mut ValidEthTransaction> {
        self.txs.values_mut().flat_map(PendingTxList::iter_mut)
    }

    pub fn try_insert_tx(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        tx: ValidEthTransaction,
        last_commit_base_fee: u64,
    ) -> Option<&ValidEthTransaction> {
        if self.num_txs >= MAX_TXS {
            event_tracker.drop(tx.hash(), EthTxPoolDropReason::PoolFull);
            return None;
        }

        let num_addresses = self.txs.len();
        assert!(num_addresses <= MAX_ADDRESSES);

        match self.txs.entry(tx.signer()) {
            indexmap::map::Entry::Occupied(tx_list) => {
                let (tx, new_tx) =
                    tx_list
                        .into_mut()
                        .try_insert_tx(event_tracker, tx, last_commit_base_fee)?;

                if new_tx {
                    self.num_txs += 1;
                }

                Some(tx)
            }
            indexmap::map::Entry::Vacant(v) => {
                if num_addresses == MAX_ADDRESSES {
                    event_tracker.drop(tx.hash(), EthTxPoolDropReason::PoolFull);
                    return None;
                }

                let tx = PendingTxList::insert_entry(event_tracker, v, tx);
                self.num_txs += 1;

                Some(tx)
            }
        }
    }

    pub fn remove(&mut self, address: &Address) -> Option<PendingTxList> {
        if let Some(tx_list) = self.txs.swap_remove(address) {
            self.num_txs = self
                .num_txs
                .checked_sub(tx_list.num_txs())
                .unwrap_or_else(|| {
                    warn!("txpool pending tx map underflowed on remove");

                    0
                });

            return Some(tx_list);
        }

        None
    }

    pub fn split_off(&mut self, num_addresses: usize) -> IndexMap<Address, PendingTxList> {
        if num_addresses >= self.txs.len() {
            self.num_txs = 0;
            return std::mem::take(&mut self.txs);
        }

        let mut split = self.txs.split_off(num_addresses);
        std::mem::swap(&mut split, &mut self.txs);

        self.num_txs = self
            .num_txs
            .checked_sub(split.values().map(PendingTxList::num_txs).sum())
            .expect("num txs does not underflow");

        split
    }

    pub fn static_validate_all_txs<CRT>(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_id: u64,
        chain_revision: &CRT,
        execution_revision: &MonadExecutionRevision,
    ) where
        CRT: ChainRevision,
    {
        self.txs.retain(|_, tx_list| {
            let removed = tx_list.static_validate_all_txs(
                event_tracker,
                chain_id,
                chain_revision,
                execution_revision,
            );

            self.num_txs = self
                .num_txs
                .checked_sub(removed)
                .expect("num txs does not underflow");

            tx_list.num_txs() > 0
        });
    }
}
