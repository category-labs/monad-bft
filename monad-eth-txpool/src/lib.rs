use std::collections::{BTreeMap, HashSet};

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{
    block::Block,
    payload::FullTransactionList,
    signature_collection::SignatureCollection,
    txpool::{HashPolicyOutput, TxPool},
};
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};

#[derive(Default)]
pub struct EthTxPool(BTreeMap<EthTxHash, EthTransaction>);

pub fn transaction_hash_policy<SCT: SignatureCollection>(
    block: &Block<SCT>,
) -> Result<HashPolicyOutput, alloy_rlp::Error> {
    Ok(HashSet::from_iter(
        EthFullTransactionList::rlp_decode(block.payload.txns.bytes().clone())?.get_hashes(),
    ))
}

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
        pending_blocktree_txs: HashPolicyOutput,
    ) -> (FullTransactionList, Option<FullTransactionList>) {
        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut txs_to_propose: Vec<_> = self.0.values().collect();
        // TODO: when sorting by gas fees is implemented, txs should be grouped by accounts
        txs_to_propose.sort_by(|a, b| a.nonce().cmp(&b.nonce()));

        for tx in txs_to_propose {
            if pending_blocktree_txs.contains(&tx.hash) {
                continue;
            }

            // Validate account nonces are increasing
            //  - If the sender account exists in the deltas -> the transaction nonce =
            //    latest nonce + 1
            //  - If the sender account is not in the deltas, it means that the account
            //    doesn't have a recent transaction in the pending blocktree.
            //    Fetch the latest nonce from the DB and validate the transaction nonce

            if txs.len() == tx_limit || (total_gas + tx.gas_limit()) > gas_limit {
                break;
            }
            total_gas += tx.gas_limit();
            txs.push(tx.clone());
        }

        let proposal_num_tx = txs.len();
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();

        tracing::info!(
            proposal_num_tx,
            proposal_total_gas = total_gas,
            proposal_tx_bytes = full_tx_list.len()
        );

        // TODO cascading behaviour for leftover txns once we have an idea of how we want
        // to forward
        self.0.clear();
        let leftovers = None;

        (FullTransactionList::new(full_tx_list), leftovers)
    }
}
