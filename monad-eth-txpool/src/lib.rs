use std::collections::{BTreeMap, HashSet};

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{payload::FullTransactionList, txpool::TxPool};
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};

#[derive(Default)]
pub struct EthTxPool(BTreeMap<EthTxHash, EthTransaction>);

impl TxPool for EthTxPool {
    fn insert_tx(&mut self, tx: Bytes) {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        let eth_tx = EthTransaction::decode(&mut tx.as_ref()).unwrap();
        // TODO: sorting by gas_limit for proposal creation
        self.0.insert(eth_tx.hash(), eth_tx);
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
