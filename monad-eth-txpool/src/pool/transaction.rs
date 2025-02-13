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
use monad_eth_txpool_types::EthTxPoolDropReason;
use monad_eth_types::{Balance, Nonce, BASE_FEE_PER_GAS};
use monad_types::SeqNum;
use tracing::trace;

use crate::EthTxPoolEventTracker;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidEthTransaction {
    tx: Recovered<TxEnvelope>,
    owned: bool,
    forward_last_seqnum: SeqNum,
    forward_retries: usize,
    max_value: u128,
    effective_tip_per_gas: u128,
}

impl ValidEthTransaction {
    pub fn validate<ST, SCT>(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT>,
        proposal_gas_limit: u64,
        tx: Recovered<TxEnvelope>,
        owned: bool,
        last_commit_seq_num: SeqNum,
    ) -> Option<Self>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        // TODO(andr-dev): Block base fee is hardcoded we need to update
        // this logic once its included in the consensus proposal
        if tx.max_fee_per_gas() < BASE_FEE_PER_GAS.into() {
            event_tracker.drop(tx.tx_hash().to_owned(), EthTxPoolDropReason::FeeTooLow);
            return None;
        }

        if static_validate_transaction(&tx, block_policy.get_chain_id(), proposal_gas_limit)
            .is_err()
        {
            event_tracker.drop(tx.tx_hash().to_owned(), EthTxPoolDropReason::NotWellFormed);
            return None;
        }

        let max_value = compute_txn_max_value_to_u128(&tx);
        let effective_tip_per_gas = tx
            .effective_tip_per_gas(BASE_FEE_PER_GAS)
            .unwrap_or_default();

        Some(Self {
            tx,
            owned,
            forward_last_seqnum: last_commit_seq_num,
            forward_retries: 0,
            max_value,
            effective_tip_per_gas,
        })
    }

    pub fn apply_max_value(&self, account_balance: Balance) -> Option<Balance> {
        if let Some(account_balance) = account_balance.checked_sub(self.max_value) {
            return Some(account_balance);
        }

        trace!(
            "AccountBalance insert_tx 2 \
                            do not add txn to the pool. insufficient balance: {account_balance:?} \
                            max_value: {max_value:?} \
                            for address: {address:?}",
            max_value = self.max_value,
            address = self.tx.signer()
        );

        None
    }

    pub const fn signer(&self) -> Address {
        self.tx.signer()
    }

    pub const fn signer_ref(&self) -> &Address {
        self.tx.signer_ref()
    }

    pub fn nonce(&self) -> Nonce {
        self.tx.nonce()
    }

    pub fn hash(&self) -> TxHash {
        self.tx.tx_hash().to_owned()
    }

    pub fn hash_ref(&self) -> &TxHash {
        self.tx.tx_hash()
    }

    pub fn gas_limit(&self) -> u64 {
        self.tx.gas_limit()
    }

    pub fn size(&self) -> u64 {
        self.tx.length() as u64
    }

    pub const fn raw(&self) -> &Recovered<TxEnvelope> {
        &self.tx
    }

    pub fn into_raw(self) -> Recovered<TxEnvelope> {
        self.tx
    }

    pub(crate) fn is_owned(&self) -> bool {
        self.owned
    }

    pub fn get_if_forwardable<const MIN_SEQNUM_DIFF: u64, const MAX_RETRIES: usize>(
        &mut self,
        last_commit_seq_num: SeqNum,
    ) -> Option<&TxEnvelope> {
        if !self.owned {
            return None;
        }

        if self.forward_retries >= MAX_RETRIES {
            return None;
        }

        let min_forwardable_seqnum = self
            .forward_last_seqnum
            .saturating_add(SeqNum(MIN_SEQNUM_DIFF));

        if min_forwardable_seqnum > last_commit_seq_num {
            return None;
        }

        self.forward_last_seqnum = last_commit_seq_num;
        self.forward_retries += 1;

        Some(&self.tx)
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
