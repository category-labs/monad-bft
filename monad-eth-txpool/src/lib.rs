use std::collections::{BTreeMap, HashSet};

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{payload::FullTransactionList, txpool::TxPool};
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};
use monad_eth_types::EthAddress;

#[derive(Default)]
pub struct EthTxPool {
    tx_map: BTreeMap<EthTxHash, EthTransaction>,
    // current account nonces in the last committed block
    // if an account doesn't exist in the map, it means there are no transactions
    // from the account
    // TODO: better storage
    account_nonces: BTreeMap<EthAddress, u64>,
}

impl TxPool for EthTxPool {
    fn insert_tx(&mut self, tx: Bytes) {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        let eth_tx = EthTransaction::decode(&mut tx.as_ref()).unwrap();
        // TODO: sorting by gas_limit and nonce after reserve balance validation
        // for proposal creation
        self.tx_map.insert(eth_tx.hash(), eth_tx);
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        pending_txs: Vec<FullTransactionList>,
    ) -> (FullTransactionList, Option<FullTransactionList>) {
        // TODO: we should enhance the pending block tree to hold tx hashses so that
        // we don't have to calculate it here on the critical path of proposal creation
        let mut pending_tx_hashes: Vec<EthTxHash> = Vec::new();
        let mut curr_account_nonces = self.account_nonces.clone();

        for tx_list in pending_txs {
            let eth_tx_list = EthFullTransactionList::rlp_decode(tx_list.bytes().clone()).expect(
                "transactions in blocks must have been verified and rlp decoded \
                before being put in the pending blocktree",
            );
            pending_tx_hashes.extend(eth_tx_list.get_hashes());

            // apply the account nonces of the blocks in the blocktree
            for eth_tx in eth_tx_list.0 {
                let sender = EthAddress(eth_tx.signer());
                if let Some(curr_nonce) = curr_account_nonces.get_mut(&sender) {
                    *curr_nonce += 1;
                } else {
                    curr_account_nonces.insert(sender, 0);
                }
            }
        }

        let pending_blocktree_txs: HashSet<EthTxHash> = HashSet::from_iter(pending_tx_hashes);

        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut txs_to_propose: Vec<_> = self.tx_map.values().collect();
        // TODO: when sorting by gas fees is implemented, txs should be grouped by accounts
        txs_to_propose.sort_by(|a, b| a.nonce().cmp(&b.nonce()));

        for tx in txs_to_propose {
            if pending_blocktree_txs.contains(&tx.hash) {
                continue;
            }

            let sender = EthAddress(tx.signer());
            if let Some(curr_nonce) = curr_account_nonces.get_mut(&sender) {
                // nonce should increase by 1, else skip tx
                if tx.nonce() != *curr_nonce + 1 {
                    continue;
                }
                *curr_nonce += 1;
            } else {
                // first transaction from this account should have nonce 0
                if tx.nonce() != 0 {
                    continue;
                }
                curr_account_nonces.insert(sender, 0);
            }

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
        self.tx_map.clear();
        let leftovers = None;

        (FullTransactionList::new(full_tx_list), leftovers)
    }

    fn handle_committed_txns(&mut self, committed_txns: FullTransactionList) {
        let eth_tx_list = EthFullTransactionList::rlp_decode(committed_txns.bytes().clone())
            .expect(
                "transactions in blocks must have been verified and rlp decoded \
            before being put in the pending blocktree",
            );

        // update account nonces
        for eth_tx in eth_tx_list.0 {
            let sender = EthAddress(eth_tx.signer());
            let old_nonce = self.account_nonces.entry(sender).or_default();
            *old_nonce = eth_tx.nonce().max(*old_nonce);
        }

        // TODO remove transactions from the tx_map which have a nonce lower than
        // the current account nonce
    }
}
