// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_primitives::{Address, TxHash};
use alloy_rlp::Encodable;
use monad_chain_config::{
    execution_revision::ExecutionChainParams,
    revision::{ChainParams, ChainRevision},
    ChainConfig,
};
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    compute_txn_max_value, validation::static_validate_transaction, EthBlockPolicy,
};
use monad_eth_txpool_types::{EthTxPoolDropReason, TransactionError};
use monad_eth_types::{Balance, EthExecutionProtocol, Nonce};
use monad_system_calls::validator::SystemTransactionValidator;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::SeqNum;
use monad_validator::signature_collection::SignatureCollection;
use tracing::trace;

use crate::EthTxPoolEventTracker;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidEthTransaction {
    tx: Recovered<TxEnvelope>,
    owned: bool,
    forward_last_seqnum: SeqNum,
    forward_retries: usize,
    max_value: Balance,
}

impl ValidEthTransaction {
    pub fn validate<ST, SCT, CCT, CRT>(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        last_commit: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        chain_params: &ChainParams,
        execution_params: &ExecutionChainParams,
        tx: Recovered<TxEnvelope>,
        owned: bool,
    ) -> Option<Self>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        if SystemTransactionValidator::is_system_sender(tx.signer()) {
            return None;
        }

        if SystemTransactionValidator::is_restricted_system_call(&tx) {
            return None;
        }

        // TODO(andr-dev): Adjust minimum dynamically using current base fee.
        if tx.max_fee_per_gas() < MIN_BASE_FEE.into() {
            event_tracker.drop(tx.tx_hash().to_owned(), EthTxPoolDropReason::FeeTooLow);
            return None;
        }

        let max_value = compute_txn_max_value(&tx);

        let this = Self {
            tx,
            owned,
            forward_last_seqnum: last_commit.seq_num,
            forward_retries: 0,
            max_value,
        };

        if let Err(error) = this.static_validate(block_policy, chain_params, execution_params) {
            event_tracker.drop(
                this.tx.tx_hash().to_owned(),
                EthTxPoolDropReason::NotWellFormed(error),
            );
            return None;
        }

        Some(this)
    }

    pub fn static_validate<ST, SCT, CCT, CRT>(
        &self,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        chain_params: &ChainParams,
        execution_params: &ExecutionChainParams,
    ) -> Result<(), TransactionError>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        static_validate_transaction(
            &self.tx,
            block_policy.get_chain_id(),
            chain_params.proposal_gas_limit,
            execution_params.max_code_size,
        )
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

    pub fn max_fee_per_gas(&self) -> u128 {
        self.tx.max_fee_per_gas()
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

    pub fn has_higher_priority(&self, other: &Self, _base_fee: u64) -> bool {
        self.tx.max_fee_per_gas() > other.tx.max_fee_per_gas()
            && self.tx.max_priority_fee_per_gas() >= other.tx.max_priority_fee_per_gas()
    }

    pub fn get_if_forwardable<const MIN_SEQNUM_DIFF: u64, const MAX_RETRIES: usize>(
        &mut self,
        last_commit_seq_num: SeqNum,
        last_commit_base_fee: u64,
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

        if self.tx.max_fee_per_gas() < last_commit_base_fee as u128 {
            return None;
        }

        self.forward_last_seqnum = last_commit_seq_num;
        self.forward_retries += 1;

        Some(&self.tx)
    }
}
