// Copyright (C) 2026 Category Labs, Inc.
//
// TinyVM system call definitions.
// Mirrors the pattern from staking_contract.rs.
// All backend-specific details (address, token IDs, calldata encoding) come
// from monad-state-backend — nothing is hardcoded here.

use alloy_consensus::{Transaction, TxEnvelope, TxLegacy, transaction::Recovered};
use alloy_primitives::{Bytes, TxKind, U256};
use monad_eth_types::ValidatedTx;
use monad_state_backend::{encode_register_native_token, tinyvm_entrypoint};

use crate::{sign_with_system_sender, validator::SystemTransactionError};

/// System calls for the TinyVM precompile.
#[derive(Debug, Clone)]
pub enum TinyVmContractCall {
    /// Register the native MON token at hardfork activation.
    RegisterNativeToken,
}

impl TinyVmContractCall {
    /// Check if a user transaction targets the TinyVM precompile with a
    /// restricted system-only function.
    pub fn is_restricted_tinyvm_contract_call(_txn: &Recovered<TxEnvelope>) -> bool {
        // registerToken is permissionless in the precompile, but the system
        // call ensures the native token is registered at hardfork activation.
        false
    }

    pub fn validate_system_transaction_input(
        &self,
        sys_txn: &ValidatedTx,
    ) -> Result<(), SystemTransactionError> {
        match self {
            Self::RegisterNativeToken => {
                let expected_to = tinyvm_entrypoint();
                let expected_input = Bytes::from(encode_register_native_token());
                let to = sys_txn.to();
                let input = sys_txn.input();
                if input != &expected_input {
                    return Err(SystemTransactionError::UnexpectedInput {
                        expected_input,
                        actual_input: input.clone(),
                    });
                }
                if to != Some(expected_to) {
                    return Err(SystemTransactionError::UnexpectedDestAddress {
                        expected: expected_to,
                        actual: to,
                    });
                }
                if sys_txn.value() != U256::ZERO {
                    return Err(SystemTransactionError::UnexpectedValue {
                        expected_value: U256::ZERO,
                        actual_value: sys_txn.value(),
                    });
                }
                Ok(())
            }
        }
    }

    pub fn into_signed_transaction(
        self,
        chain_id: u64,
        nonce: u64,
    ) -> TinyVmContractTransaction {
        let input = match &self {
            Self::RegisterNativeToken => Bytes::from(encode_register_native_token()),
        };

        let tx = TxLegacy {
            chain_id: Some(chain_id),
            nonce,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(tinyvm_entrypoint()),
            value: U256::ZERO,
            input,
        };

        TinyVmContractTransaction(sign_with_system_sender(tx))
    }
}

#[derive(Debug, Clone)]
pub struct TinyVmContractTransaction(Recovered<TxEnvelope>);

impl TinyVmContractTransaction {
    pub fn inner(&self) -> &Recovered<TxEnvelope> {
        &self.0
    }

    pub fn into_inner(self) -> Recovered<TxEnvelope> {
        self.0
    }
}
