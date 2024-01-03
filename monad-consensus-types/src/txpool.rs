use std::collections::{BTreeMap, HashSet};

use monad_eth_types::{EthFullTransactionList, EthTransaction, EthTxHash};

use crate::payload::FullTransactionList;

pub trait TxPool {
    fn new() -> Self;

    fn insert_tx(&mut self, tx: EthTransaction);

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        pending_txs: Vec<FullTransactionList>,
    ) -> FullTransactionList;
}

pub struct EthTxPool(BTreeMap<EthTxHash, EthTransaction>);

impl TxPool for EthTxPool {
    fn new() -> Self {
        Self(BTreeMap::new())
    }

    fn insert_tx(&mut self, tx: EthTransaction) {
        // TODO have another datastructure to keep sorted by gas limit for proposal creation
        self.0.insert(tx.hash, tx);
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        pending_txs: Vec<FullTransactionList>,
    ) -> FullTransactionList {
        // TODO: we should enhance the pending block tree to hold tx hashses so that
        // we don't have to calculate it here on the critical path of proposal creation
        let mut pending_tx_hashes: Vec<EthTxHash> = Vec::new();
        for x in pending_txs {
            let y = EthFullTransactionList::rlp_decode(x.bytes().clone()).expect(
                "transactions in blocks must have been verified and rlp decoded \
                before being put in the pending blocktree",
            );
            pending_tx_hashes.extend(y.get_hashes());
        }

        let pending_blocktree_txs: HashSet<EthTxHash> = HashSet::from_iter(pending_tx_hashes);

        let mut txs = Vec::new();
        let mut total_gas = 0;

        for tx in self.0.values() {
            if pending_blocktree_txs.contains(&tx.hash) {
                continue;
            }

            if txs.len() == tx_limit || (total_gas + tx.gas_limit()) > gas_limit {
                break;
            }
            total_gas += tx.gas_limit();
            txs.push(tx.clone());
        }

        // TODO cascading behaviour for leftover txns
        self.0.clear();

        FullTransactionList::new(EthFullTransactionList(txs).rlp_encode())
    }
}
