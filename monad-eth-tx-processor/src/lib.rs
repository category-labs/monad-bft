use std::collections::{BTreeMap, HashSet};

use alloy_rlp::Decodable;
use bytes::Bytes;
use monad_consensus_types::{payload::FullTransactionList, tx_processor::TransactionProcessor};
use monad_eth_account_states::EthAccountStates;
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};

/// Transaction processor that is used to
/// 1. Store Ethereum transactions
/// 2. Store account states (reserve balances)
/// 3. Validate transactions as Ethereum transactions
/// 4. Validate blocks as Ethereum blocks
/// 5. Create blocks for proposal
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct EthereumTransactionProcessor {
    /// stores all transactions (includes transactions that are proposed and not proposed)
    txns: BTreeMap<EthTxHash, EthTransaction>,
    /// keeps track of reserve balances and transactions from each account
    acc_states: EthAccountStates,
    /// max number of transactions to fetch for proposal
    max_txs: usize,
    /// limit on cumulative gas from transactions in a block
    block_gas_limit: u64,
}

impl EthereumTransactionProcessor {
    pub fn new(max_txs: usize, block_gas_limit: u64) -> Self {
        Self {
            txns: BTreeMap::default(),
            acc_states: EthAccountStates::default(),
            max_txs,
            block_gas_limit,
        }
    }
}

impl TransactionProcessor for EthereumTransactionProcessor {
    fn insert_tx(&mut self, tx: Bytes) {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        let eth_tx = EthTransaction::decode(&mut tx.as_ref()).unwrap();

        if self.acc_states.validate_single_txn(&eth_tx) {
            // TODO: sorting by gas_limit for proposal creation
            self.txns.insert(eth_tx.hash(), eth_tx);
        }
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

        // TODO: validate transaction nonces for each account
        for tx in self.txns.values() {
            if pending_blocktree_txs.contains(&tx.hash) {
                continue;
            }

            if txs.len() == tx_limit || (total_gas + tx.gas_limit()) > gas_limit {
                break;
            }
            total_gas += tx.gas_limit();
            txs.push(tx.clone());
        }

        // TODO cascading behaviour for leftover transactions once we have an idea of how we want
        // to forward
        self.txns.clear();
        let leftovers = None;

        (
            FullTransactionList::new(EthFullTransactionList(txs).rlp_encode()),
            leftovers,
        )
    }

    fn validate_block_txns(&mut self, full_txs: &FullTransactionList) -> bool {
        let Ok(eth_txns) = EthFullTransactionList::rlp_decode(full_txs.bytes().clone()) else {
            return false;
        };
        // TODO-2: Eth transaction checks

        if eth_txns.0.len() > self.max_txs {
            return false;
        }

        let total_gas = eth_txns.0.iter().fold(0, |acc, tx| acc + tx.gas_limit());
        if total_gas > self.block_gas_limit {
            return false;
        }

        if !self.acc_states.validate_block_txns(full_txs) {
            return false;
        }

        true
    }

    fn update_committed_txns(&mut self, txs: &FullTransactionList) {
        self.acc_states.update_committed_txns(txs);
    }

    fn remove_block_txns(&mut self, txs: &FullTransactionList) {
        self.acc_states.remove_block_txns(txs)
    }
}
