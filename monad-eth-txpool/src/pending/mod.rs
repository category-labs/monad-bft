use alloy_primitives::Address;
use indexmap::IndexMap;
use monad_consensus_types::txpool::TxPoolInsertionError;

pub use self::list::PendingTxList;
use crate::transaction::ValidEthTransaction;

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

    pub fn try_add_tx(&mut self, tx: ValidEthTransaction) -> Result<(), TxPoolInsertionError> {
        if self.num_txs >= MAX_TXS {
            return Err(TxPoolInsertionError::PoolFull);
        }

        let num_addresses = self.txs.len();
        assert!(num_addresses <= MAX_ADDRESSES);

        match self.txs.entry(tx.sender()) {
            indexmap::map::Entry::Occupied(mut tx_list) => {
                if tx_list.get_mut().try_add(tx)? {
                    self.num_txs += 1;
                }
            }
            indexmap::map::Entry::Vacant(v) => {
                if num_addresses == MAX_ADDRESSES {
                    return Err(TxPoolInsertionError::PoolFull);
                }

                v.insert(PendingTxList::new(tx));
                self.num_txs += 1;
            }
        }

        Ok(())
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
