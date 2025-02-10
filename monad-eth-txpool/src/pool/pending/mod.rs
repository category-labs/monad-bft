use alloy_primitives::Address;
use indexmap::IndexMap;
use monad_eth_txpool_types::EthTxPoolDropReason;

pub use self::list::PendingTxList;
use super::transaction::ValidEthTransaction;
use crate::EthTxPoolEventTracker;

mod list;

// These constants were set using intuition and should be changed once we have more performance
// numbers for the txpool.
const MAX_ADDRESSES: usize = 16 * 1024;
const MAX_TXS: usize = 64 * 1024;
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

    pub fn try_insert_tx(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        tx: ValidEthTransaction,
    ) -> Option<&ValidEthTransaction> {
        if self.num_txs >= MAX_TXS {
            event_tracker.drop(tx.hash(), EthTxPoolDropReason::PoolFull);
            return None;
        }

        let num_addresses = self.txs.len();
        assert!(num_addresses <= MAX_ADDRESSES);

        match self.txs.entry(tx.signer()) {
            indexmap::map::Entry::Occupied(tx_list) => {
                let tx = tx_list.into_mut().try_insert(event_tracker, tx)?;

                self.num_txs += 1;
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
        if let Some(tx) = self.txs.swap_remove(address) {
            self.num_txs = self
                .num_txs
                .checked_sub(1)
                .expect("num txs does not underflow");

            return Some(tx);
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
}
