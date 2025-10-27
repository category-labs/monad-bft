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
use alloy_eips::eip7702::Authorization;
use alloy_primitives::{Address, TxHash};
use alloy_rlp::Encodable;
use monad_chain_config::{execution_revision::ExecutionChainParams, revision::ChainParams};
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    compute_txn_max_gas_cost, compute_txn_max_value, validation::static_validate_transaction,
};
use monad_eth_txpool_types::{EthTxPoolDropReason, TransactionError};
use monad_eth_types::EthExecutionProtocol;
use monad_system_calls::{validator::SystemTransactionValidator, SYSTEM_SENDER_ETH_ADDRESS};
use monad_tfm::base_fee::{MIN_BASE_FEE, PRE_TFM_BASE_FEE};
use monad_types::{Balance, Nonce, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::trace;

const MAX_EIP7702_AUTHORIZATION_LIST_LENGTH: usize = 4;

pub const fn max_eip2718_encoded_length(execution_params: &ExecutionChainParams) -> usize {
    2 * execution_params.max_code_size + 128 * 1024
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidEthTransaction {
    tx: Recovered<TxEnvelope>,
    owned: bool,
    forward_last_seqnum: SeqNum,
    forward_retries: usize,
    max_value: Balance,
    max_gas_cost: Balance,
    valid_recovered_authorizations: Box<[ValidEthRecoveredAuthorization]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidEthRecoveredAuthorization {
    pub authority: Address,
    pub authorization: Authorization,
}

impl ValidEthTransaction {
    pub fn validate<ST, SCT>(
        last_commit: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        chain_id: u64,
        chain_params: &ChainParams,
        execution_params: &ExecutionChainParams,
        tx: Recovered<TxEnvelope>,
        owned: bool,
    ) -> Result<Self, (Recovered<TxEnvelope>, EthTxPoolDropReason)>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        if tx.eip2718_encoded_length() > max_eip2718_encoded_length(execution_params) {
            return Err((
                tx,
                EthTxPoolDropReason::NotWellFormed(TransactionError::EncodedLengthLimitExceeded),
            ));
        }

        if SystemTransactionValidator::is_system_sender(tx.signer()) {
            return Err((tx, EthTxPoolDropReason::InvalidSignature));
        }

        if SystemTransactionValidator::is_restricted_system_call(&tx) {
            return Err((tx, EthTxPoolDropReason::InvalidSignature));
        }

        // TODO(andr-dev): Adjust minimum dynamically using current base fee.
        let min_base_fee = if execution_params.tfm_enabled {
            MIN_BASE_FEE
        } else {
            PRE_TFM_BASE_FEE
        };
        if tx.max_fee_per_gas() < min_base_fee.into() {
            return Err((tx, EthTxPoolDropReason::FeeTooLow));
        }

        let last_commit_base_fee = last_commit
            .base_fee
            .unwrap_or(monad_tfm::base_fee::PRE_TFM_BASE_FEE);
        let max_value = compute_txn_max_value(&tx, last_commit_base_fee);
        let max_gas_cost = compute_txn_max_gas_cost(&tx, last_commit_base_fee);

        if let Err(error) =
            static_validate_transaction(&tx, chain_id, chain_params, execution_params)
        {
            return Err((tx, EthTxPoolDropReason::NotWellFormed(error)));
        }

        let valid_recovered_authorizations =
            if let Some(signed_authorizations) = tx.authorization_list() {
                if signed_authorizations.len() > MAX_EIP7702_AUTHORIZATION_LIST_LENGTH {
                    return Err((
                        tx,
                        EthTxPoolDropReason::NotWellFormed(
                            TransactionError::AuthorizationListLengthLimitExceeded,
                        ),
                    ));
                }

                match signed_authorizations
                    .iter()
                    .filter_map(|signed_authorization| {
                        if signed_authorization.chain_id != 0
                            && signed_authorization.chain_id != chain_id
                        {
                            return None;
                        }

                        let Ok(authority) = signed_authorization.recover_authority() else {
                            return None;
                        };

                        // system account cannot be used to sign authorizations
                        if authority == SYSTEM_SENDER_ETH_ADDRESS {
                            return Some(Err(EthTxPoolDropReason::InvalidSignature));
                        }

                        Some(Ok(ValidEthRecoveredAuthorization {
                            authority,
                            authorization: signed_authorization.inner().clone(),
                        }))
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Err(drop_reason) => return Err((tx, drop_reason)),
                    Ok(valid_recovered_authorizations) => {
                        valid_recovered_authorizations.into_boxed_slice()
                    }
                }
            } else {
                Box::default()
            };

        Ok(Self {
            tx,
            owned,
            forward_last_seqnum: last_commit.seq_num,
            forward_retries: 0,
            max_value,
            max_gas_cost,
            valid_recovered_authorizations,
        })
    }

    pub fn static_validate(
        &self,
        chain_id: u64,
        chain_params: &ChainParams,
        execution_params: &ExecutionChainParams,
    ) -> Result<(), TransactionError> {
        static_validate_transaction(&self.tx, chain_id, chain_params, execution_params)
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

    pub fn apply_max_gas_cost(&self, balance: Balance) -> Option<Balance> {
        balance.checked_sub(self.max_gas_cost)
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

    pub fn value(&self) -> Balance {
        self.tx.value()
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

    pub fn iter_valid_recovered_authorizations(
        &self,
    ) -> impl Iterator<Item = &ValidEthRecoveredAuthorization> {
        self.valid_recovered_authorizations.iter()
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
