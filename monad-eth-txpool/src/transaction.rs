use std::cmp::Ordering;

use alloy_consensus::Transaction;
use alloy_primitives::TxHash;
use alloy_rlp::Encodable;
use monad_consensus_types::{
    payload::BASE_FEE_PER_GAS, signature_collection::SignatureCollection,
    txpool::TxPoolInsertionError,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    compute_txn_max_value_to_u128, static_validate_transaction, EthBlockPolicy,
};
use monad_eth_types::{Balance, EthAddress, Nonce, TxEnvelopeWithSigner};
use tracing::trace;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidEthTransaction {
    tx: TxEnvelopeWithSigner,
    gas_per_gas_limit: u64,
    txn_fee: u128,
}

impl ValidEthTransaction {
    pub fn validate<ST, SCT>(
        tx: TxEnvelopeWithSigner,
        block_policy: &EthBlockPolicy<ST, SCT>,
    ) -> Result<Self, TxPoolInsertionError>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        if tx.max_fee_per_gas() < BASE_FEE_PER_GAS.into() {
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
                            do not add txn to the pool. insufficient balance: {:?} \
                            txn_fee: {:?} \
                            for address: {:?}",
                account_balance,
                self.txn_fee,
                self.tx.signer,
            );

            return Err(TxPoolInsertionError::InsufficientBalance);
        };

        Ok(new_account_balance)
    }

    pub fn sender(&self) -> EthAddress {
        EthAddress(self.tx.signer)
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

    pub fn raw(&self) -> &TxEnvelopeWithSigner {
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
        self.tx
            .max_fee_per_gas()
            .cmp(&other.tx.max_fee_per_gas())
            .then_with(|| self.gas_per_gas_limit.cmp(&other.gas_per_gas_limit))
            .then_with(|| self.tx.gas_limit().cmp(&other.tx.gas_limit()))
    }
}
