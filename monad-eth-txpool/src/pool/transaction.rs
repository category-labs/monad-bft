use std::cmp::Ordering;

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_primitives::{Address, TxHash};
use alloy_rlp::Encodable;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    compute_txn_max_value_to_u128, static_validate_transaction, EthBlockPolicy,
};
use monad_eth_types::{Balance, Nonce, BASE_FEE_PER_GAS};
use tracing::trace;

use super::error::TxPoolInsertionError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidEthTransaction {
    tx: Recovered<TxEnvelope>,
    max_value: u128,
    effective_tip_per_gas: u128,
}

impl ValidEthTransaction {
    pub fn validate<ST, SCT>(
        tx: Recovered<TxEnvelope>,
        block_policy: &EthBlockPolicy<ST, SCT>,
        proposal_gas_limit: u64,
    ) -> Result<Self, TxPoolInsertionError>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        // TODO(andr-dev): Block base fee is hardcoded we need to update
        // this logic once its included in the consensus proposal
        if tx.max_fee_per_gas() < BASE_FEE_PER_GAS.into() {
            return Err(TxPoolInsertionError::FeeTooLow);
        }

        if static_validate_transaction(&tx, block_policy.get_chain_id(), proposal_gas_limit)
            .is_err()
        {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        let max_value = compute_txn_max_value_to_u128(&tx);
        let effective_tip_per_gas = tx
            .effective_tip_per_gas(BASE_FEE_PER_GAS)
            .unwrap_or_default();

        Ok(Self {
            tx,
            max_value,
            effective_tip_per_gas,
        })
    }

    pub fn apply_max_value(
        &self,
        account_balance: &Balance,
    ) -> Result<Balance, TxPoolInsertionError> {
        let Some(new_account_balance) = account_balance.checked_sub(self.max_value) else {
            trace!(
                "AccountBalance insert_tx 2 \
                            do not add txn to the pool. insufficient balance: {account_balance:?} \
                            max_value: {max_value:?} \
                            for address: {address:?}",
                max_value = self.max_value,
                address = self.tx.signer()
            );

            return Err(TxPoolInsertionError::InsufficientBalance);
        };

        Ok(new_account_balance)
    }

    pub fn sender(&self) -> Address {
        self.tx.signer()
    }

    pub fn nonce(&self) -> Nonce {
        self.tx.nonce()
    }

    pub fn hash(&self) -> TxHash {
        *self.tx.tx_hash()
    }

    pub fn gas_limit(&self) -> u64 {
        self.tx.gas_limit()
    }

    pub fn size(&self) -> u64 {
        self.tx.length() as u64
    }

    pub fn raw(&self) -> &Recovered<TxEnvelope> {
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
        // Since the base fee is currently hard-coded, we can easily and deterministically compute
        // the effective tip per gas the proposer receives for a given tx. Proposers want to
        // maximize the total effective tip in a block so we order txs based on their effective tip
        // per gas since we do not know at proposal time how much gas the tx will use. Additionally,
        // txs with higher gas limits _typically_ have higher gas usages so we use this as a
        // heuristic tie breaker when the effective tip per gas is equal.
        (self.effective_tip_per_gas, self.tx.gas_limit())
            .cmp(&(other.effective_tip_per_gas, other.tx.gas_limit()))
    }
}
