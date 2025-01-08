use std::cmp::Ordering;

use alloy_consensus::Transaction;
use monad_consensus_types::txpool::TxPoolInsertionError;
use monad_eth_block_policy::{
    compute_txn_max_value_to_u128, static_validate_transaction, EthBlockPolicy,
};
use monad_eth_tx::{EthTransaction, EthTxHash};
use monad_eth_types::{Balance, EthAddress, Nonce};
use tracing::trace;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidEthTransaction {
    tx: EthTransaction,
    gas_per_gas_limit: u64,
    txn_fee: u128,
}

impl ValidEthTransaction {
    pub fn validate(
        tx: EthTransaction,
        block_policy: &EthBlockPolicy,
    ) -> Result<Self, TxPoolInsertionError> {
        // TODO(andr-dev): Block base fee is hardcoded to 1000 in monad-ledger, we need to update
        // this logic once its included in the consensus proposal
        if tx.max_fee_per_gas() < 1000 {
            return Err(TxPoolInsertionError::FeeTooLow);
        }

        if static_validate_transaction(&tx, block_policy.get_chain_id()).is_err() {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        let Some(gas_per_gas_limit) = tx
            .priority_fee_or_price()
            .checked_div(tx.gas_limit().into())
            .and_then(|ratio| TryInto::<u64>::try_into(ratio).ok())
        else {
            return Err(TxPoolInsertionError::NotWellFormed);
        };

        let txn_fee = compute_txn_max_value_to_u128(&tx);

        Ok(Self {
            tx,
            gas_per_gas_limit,
            txn_fee,
        })
    }

    pub fn apply_txn_fee(
        &self,
        account_balance: &Balance,
    ) -> Result<Balance, TxPoolInsertionError> {
        let Some(new_account_balance) = account_balance.checked_sub(self.txn_fee) else {
            trace!(
                "AccountBalance insert_tx 2 \
                            do not add txn to the pool. insufficient balance: {account_balance:?} \
                            txn_fee: {fee:?} \
                            for address: {address:?}",
                fee = self.txn_fee,
                address = self.tx.signer()
            );

            return Err(TxPoolInsertionError::InsufficientBalance);
        };

        Ok(new_account_balance)
    }

    pub fn sender(&self) -> EthAddress {
        EthAddress(self.tx.signer())
    }

    pub fn nonce(&self) -> Nonce {
        self.tx.nonce()
    }

    pub fn hash(&self) -> EthTxHash {
        *self.tx.tx_hash()
    }

    pub fn gas_limit(&self) -> u64 {
        self.tx.gas_limit()
    }

    pub fn raw(&self) -> &EthTransaction {
        &self.tx
    }
}

impl PartialOrd for ValidEthTransaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValidEthTransaction {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.tx.max_fee_per_gas(),
            self.gas_per_gas_limit,
            self.tx.gas_limit(),
        )
            .cmp(&(
                other.tx.max_fee_per_gas(),
                other.gas_per_gas_limit,
                other.tx.gas_limit(),
            ))
    }
}
