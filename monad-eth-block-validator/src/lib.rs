use monad_eth_types::EthFullTransactionList;

/// Validates transactions as valid Ethereum transactions and also validates that
/// the list of transactions will create a valid Ethereum block
#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub struct EthereumValidator {
    /// max number of txns to fetch
    max_txs: usize,
    /// limit on cumulative gas from transactions in a block
    block_gas_limit: u64,
}

impl EthereumValidator {
    pub fn new(max_txs: usize, block_gas_limit: u64) -> Self {
        // TODO: Make a DB for account nonces
        Self {
            max_txs,
            block_gas_limit,
        }
    }
}

impl BlockValidator for EthereumValidator {
    // Add arg: Nonce deltas from blocktree
    /// A Block is valid iff:
    /// - Number of txns is less thn or equal to max txs
    /// - Total gas is less than or equal to block gas limit
    /// - The transaction nonces per account is strictly sequential
    fn validate(&self, full_txs: &FullTransactionList) -> bool {
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

        // Validate nonces
        // If a block is in the blocktree, then it has been validated already
        // Get the latest nonce of each account from the deltas
        // For each transaction in the block:
        //  - If the sender account exists in the deltas -> the transaction nonce = 
        //    latest nonce + 1
        //  - If the sender account is not in the deltas, it means that the account
        //    doesn't have a recent transaction in the pending blocktree.
        //    Fetch the latest nonce from the DB and validate the transaction nonce

        // Also validate that nonces for transactions from the same account are strictly
        // increasing in the block. i.e. nonces are sorted within the block
        // OR: nonces are non-decreasing in the block

        // When an account is not there in the account deltas but is present in the DB,
        // add it to the deltas incase there are multiple transactions from the account
        // in the same block

        // Block is valid. Create deltas of account nonces to store in blocktree

        true
    }

    // Used to update the DB with the nonces of the latest committed block
    // Args:
    //  - Commited block
    fn update_account_nonces(&mut self) {
        // For each transaction in the committed block:
        //  - Get latest nonce of sender from DB
        //  - Assert that the nonce = latest nonce + 1 (sanity check)
        //  - Update latest nonce in DB to nonce
    }
}
