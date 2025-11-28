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

use alloy_consensus::{Transaction, TxEnvelope};
use monad_chain_config::{execution_revision::ExecutionChainParams, revision::ChainParams};
use serde::{Deserialize, Serialize};

// allow for more fine grain debugging if needed
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StaticValidationError {
    InvalidChainId {
        tx_chain_id: u64,
    },
    MaxPriorityOverMaxFee {
        tx_max_priority_fee: u128,
        tx_max_fee_per_gas: u128,
    },
    InitCodeLimitExceeded {
        tx_input_len: usize,
        max_init_code_size: usize,
    },
    EncodedLengthLimitExceeded,
    GasLimitUnderFloorDataGas {
        tx_gas_limit: u64,
        floor_data_gas: u64,
    },
    GasLimitUnderIntrinsicGas {
        tx_gas_limit: u64,
        intrinsic_gas: u64,
    },
    GasLimitOverTFMGasLimit {
        tx_gas_limit: u64,
        tfm_max_gas_limit: u64,
    },
    GasLimitOverProposalGasLimit {
        tx_gas_limit: u64,
        proposal_gas_limit: u64,
    },
    InvalidSignature,
    UnsupportedTransactionType,
    AuthorizationListEmpty,
    AuthorizationListLengthLimitExceeded,
}

/// Stateless helper function to check validity of an Ethereum transaction
pub fn static_validate_transaction(
    tx: &TxEnvelope,
    chain_id: u64,
    chain_params: &ChainParams,
    execution_chain_params: &ExecutionChainParams,
) -> Result<(), StaticValidationError> {
    if tx.is_eip4844() {
        return Err(StaticValidationError::UnsupportedTransactionType);
    }

    TfmValidator::validate(tx, chain_params)?;

    // post Ethereum Homestead fork validation
    // includes EIP-155 validation
    EthHomesteadForkValidation::validate(tx, chain_id)?;

    // post Ethereum London fork validation
    // includes EIP-1559 validation
    EthLondonForkValidation::validate(tx)?;

    // post Ethereum Shanghai fork validation
    // includes EIP-3860 validation
    EthShanghaiForkValidation::validate(tx, execution_chain_params)?;

    // Ethereum Yellow paper intrinsic gas validation
    YellowPaperValidation::validate(tx)?;

    // post Ethereum Prague fork validation
    // includes EIP-7623 validation
    EthPragueForkValidation::validate(tx, execution_chain_params)?;

    Ok(())
}

pub const TFM_MAX_GAS_LIMIT: u64 = 30_000_000;
pub const EIP_7702_PER_EMPTY_ACCOUNT_COST: u64 = 25_000;

struct TfmValidator;
impl TfmValidator {
    fn validate(tx: &TxEnvelope, chain_params: &ChainParams) -> Result<(), StaticValidationError> {
        // tfm-specific gating
        if tx.gas_limit() > TFM_MAX_GAS_LIMIT {
            return Err(StaticValidationError::GasLimitOverTFMGasLimit {
                tx_gas_limit: tx.gas_limit(),
                tfm_max_gas_limit: TFM_MAX_GAS_LIMIT,
            });
        }

        if tx.gas_limit() > chain_params.proposal_gas_limit {
            return Err(StaticValidationError::GasLimitOverProposalGasLimit {
                tx_gas_limit: tx.gas_limit(),
                proposal_gas_limit: chain_params.proposal_gas_limit,
            });
        }

        Ok(())
    }
}

struct YellowPaperValidation;
impl YellowPaperValidation {
    fn validate(tx: &TxEnvelope) -> Result<(), StaticValidationError> {
        Self::intrinsic_gas_validation(tx)
    }

    fn intrinsic_gas_validation(tx: &TxEnvelope) -> Result<(), StaticValidationError> {
        // YP eq. 62 - intrinsic gas validation
        let intrinsic_gas = compute_intrinsic_gas(tx);
        if tx.gas_limit() < intrinsic_gas {
            return Err(StaticValidationError::GasLimitUnderIntrinsicGas {
                tx_gas_limit: tx.gas_limit(),
                intrinsic_gas,
            });
        }
        Ok(())
    }
}

struct EthHomesteadForkValidation;
impl EthHomesteadForkValidation {
    fn validate(tx: &TxEnvelope, chain_id: u64) -> Result<(), StaticValidationError> {
        Self::eip_2(tx)?;
        Self::eip_155(tx, chain_id)
    }

    fn eip_2(tx: &TxEnvelope) -> Result<(), StaticValidationError> {
        // verify that s is in the lower half of the curve
        if tx.signature().normalize_s().is_some() {
            return Err(StaticValidationError::InvalidSignature);
        }
        Ok(())
    }

    fn eip_155(tx: &TxEnvelope, chain_id: u64) -> Result<(), StaticValidationError> {
        // We still allow legacy transactions without chain_id specified to pass through
        if let Some(tx_chain_id) = tx.chain_id() {
            if tx_chain_id != chain_id {
                return Err(StaticValidationError::InvalidChainId { tx_chain_id });
            }
        }
        Ok(())
    }
}

struct EthLondonForkValidation;
impl EthLondonForkValidation {
    fn validate(tx: &TxEnvelope) -> Result<(), StaticValidationError> {
        Self::eip_1559(tx)
    }

    fn eip_1559(tx: &TxEnvelope) -> Result<(), StaticValidationError> {
        if let Some(tx_max_priority_fee) = tx.max_priority_fee_per_gas() {
            if tx_max_priority_fee > tx.max_fee_per_gas() {
                return Err(StaticValidationError::MaxPriorityOverMaxFee {
                    tx_max_priority_fee,
                    tx_max_fee_per_gas: tx.max_fee_per_gas(),
                });
            }
        }
        Ok(())
    }
}

struct EthShanghaiForkValidation;
impl EthShanghaiForkValidation {
    fn validate(
        tx: &TxEnvelope,
        execution_chain_params: &ExecutionChainParams,
    ) -> Result<(), StaticValidationError> {
        Self::eip_3860(tx, execution_chain_params)
    }

    fn eip_3860(
        tx: &TxEnvelope,
        execution_chain_params: &ExecutionChainParams,
    ) -> Result<(), StaticValidationError> {
        // max init_code is (2 * max_code_size)
        let max_init_code_size: usize = 2 * execution_chain_params.max_code_size;
        if tx.kind().is_create() && tx.input().len() > max_init_code_size {
            return Err(StaticValidationError::InitCodeLimitExceeded {
                tx_input_len: tx.input().len(),
                max_init_code_size,
            });
        }
        Ok(())
    }
}

struct EthPragueForkValidation;
impl EthPragueForkValidation {
    fn validate(
        tx: &TxEnvelope,
        execution_chain_params: &ExecutionChainParams,
    ) -> Result<(), StaticValidationError> {
        if execution_chain_params.prague_enabled {
            Self::eip_7623(tx)?;
        }
        Self::eip_7702(tx, execution_chain_params)?;

        Ok(())
    }

    fn eip_7623(tx: &TxEnvelope) -> Result<(), StaticValidationError> {
        let floor_data_gas = compute_floor_data_gas(tx);
        if tx.gas_limit() < floor_data_gas {
            return Err(StaticValidationError::GasLimitUnderFloorDataGas {
                tx_gas_limit: tx.gas_limit(),
                floor_data_gas,
            });
        }
        Ok(())
    }

    fn eip_7702(
        tx: &TxEnvelope,
        execution_chain_params: &ExecutionChainParams,
    ) -> Result<(), StaticValidationError> {
        if !tx.is_eip7702() {
            return Ok(());
        }

        if !execution_chain_params.prague_enabled {
            return Err(StaticValidationError::UnsupportedTransactionType);
        }

        match tx.authorization_list() {
            Some(auth_list) => {
                if auth_list.is_empty() {
                    return Err(StaticValidationError::AuthorizationListEmpty);
                }
            }
            None => return Err(StaticValidationError::AuthorizationListEmpty),
        }

        Ok(())
    }
}

fn compute_intrinsic_gas(tx: &TxEnvelope) -> u64 {
    // base stipend
    let mut intrinsic_gas = 21000;

    // YP, Eqn. 60, first summation
    // 4 gas for each zero byte and 16 gas for each non zero byte
    let zero_data_len = tx.input().iter().filter(|v| **v == 0).count() as u64;
    let non_zero_data_len = tx.input().len() as u64 - zero_data_len;
    intrinsic_gas += zero_data_len * 4;
    // EIP-2028: Transaction data gas cost reduction (was originally 64 for non zero byte)
    intrinsic_gas += non_zero_data_len * 16;

    if tx.kind().is_create() {
        // adds 32000 to intrinsic gas if transaction is contract creation
        intrinsic_gas += 32000;
        // EIP-3860: Limit and meter initcode
        // Init code stipend for bytecode analysis
        intrinsic_gas += (tx.input().len() as u64).div_ceil(32) * 2;
    }

    // EIP-2930
    let access_list = tx
        .access_list()
        .map(|list| list.0.as_slice())
        .unwrap_or(&[]);
    let accessed_slots: usize = access_list.iter().map(|item| item.storage_keys.len()).sum();
    // each address in access list costs 2400 gas
    intrinsic_gas += access_list.len() as u64 * 2400;
    // each storage key in access list costs 1900 gas
    intrinsic_gas += accessed_slots as u64 * 1900;

    if tx.is_eip7702() {
        if let Some(auth_list) = tx.authorization_list() {
            intrinsic_gas = intrinsic_gas.saturating_add(
                EIP_7702_PER_EMPTY_ACCOUNT_COST.saturating_mul(auth_list.len() as u64),
            );
        }
    }
    intrinsic_gas
}

fn compute_floor_data_gas(tx: &TxEnvelope) -> u64 {
    // EIP-7623
    let zero_data_len = tx.input().iter().filter(|v| **v == 0).count() as u64;
    let non_zero_data_len = tx.input().len() as u64 - zero_data_len;
    21_000 + (zero_data_len * 10 + non_zero_data_len * 40)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use alloy_consensus::{SignableTransaction, TxEip1559, TxLegacy};
    use alloy_primitives::{Address, Bytes, FixedBytes, Signature, TxKind, B256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_chain_config::{
        execution_revision::MonadExecutionRevision, revision::MockChainRevision, ChainConfig,
        MockChainConfig,
    };
    use monad_eth_testutil::{
        make_eip7702_tx, make_signed_authorization, secret_to_eth_address, S1, S2,
    };

    use super::*;

    const BASE_FEE: u64 = 100_000_000_000;

    fn chain_params_with_gas_limit(proposal_gas_limit: u64) -> ChainParams {
        ChainParams {
            tx_limit: MockChainRevision::DEFAULT.chain_params.tx_limit,
            proposal_gas_limit,
            proposal_byte_limit: MockChainRevision::DEFAULT.chain_params.proposal_byte_limit,
            vote_pace: MockChainRevision::DEFAULT.chain_params.vote_pace,
            max_reserve_balance: MockChainRevision::DEFAULT.chain_params.max_reserve_balance,
        }
    }

    fn sign_tx(signature_hash: &FixedBytes<32>) -> Signature {
        let secret_key = B256::repeat_byte(0xAu8).to_string();
        let signer = &secret_key.parse::<PrivateKeySigner>().unwrap();
        signer.sign_hash_sync(signature_hash).unwrap()
    }

    #[test]
    fn test_static_validate_transaction() {
        let address = Address(FixedBytes([0x11; 20]));

        let chain_id: u64 = MockChainConfig::DEFAULT.chain_id();

        // tx exceeds tfm gas limit
        let tx_exceeds_length_limit = TxLegacy {
            chain_id: None,
            nonce: 0,
            to: TxKind::Call(address),
            gas_price: 1000,
            gas_limit: TFM_MAX_GAS_LIMIT + 1,
            ..Default::default()
        };
        let signature = sign_tx(&tx_exceeds_length_limit.signature_hash());
        let txn = tx_exceeds_length_limit.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            &chain_params_with_gas_limit(TFM_MAX_GAS_LIMIT + 2),
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(
            result,
            Err(StaticValidationError::GasLimitOverTFMGasLimit { .. })
        ));

        // transaction with gas limit higher than block gas limit
        let tx_gas_limit_too_high = TxEip1559 {
            chain_id,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: TFM_MAX_GAS_LIMIT - 1,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_high.signature_hash());
        let txn = tx_gas_limit_too_high.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            &chain_params_with_gas_limit(TFM_MAX_GAS_LIMIT - 2),
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(
            result,
            Err(StaticValidationError::GasLimitOverProposalGasLimit { .. })
        ));

        // pre EIP-155 transaction with no chain id is allowed
        let tx_no_chain_id = TxLegacy {
            chain_id: None,
            nonce: 0,
            to: TxKind::Call(address),
            gas_price: 1000,
            gas_limit: 1_000_000,
            ..Default::default()
        };
        let signature = sign_tx(&tx_no_chain_id.signature_hash());
        let txn = tx_no_chain_id.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            MockChainRevision::DEFAULT.chain_params,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(result, Ok(())));

        // transaction with incorrect chain id
        let tx_invalid_chain_id = TxEip1559 {
            chain_id: chain_id - 1,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            ..Default::default()
        };
        let signature = sign_tx(&tx_invalid_chain_id.signature_hash());
        let txn = tx_invalid_chain_id.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            MockChainRevision::DEFAULT.chain_params,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(
            result,
            Err(StaticValidationError::InvalidChainId { .. })
        ));

        // contract deployment transaction with input data larger than 2 * max_code_size (initcode limit)
        let input = vec![
            0;
            2 * MonadExecutionRevision::LATEST
                .execution_chain_params()
                .max_code_size
                + 1
        ];
        let tx_over_initcode_limit = TxEip1559 {
            chain_id,
            nonce: 0,
            to: TxKind::Create,
            max_fee_per_gas: 10000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            input: input.into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_over_initcode_limit.signature_hash());
        let txn = tx_over_initcode_limit.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            MockChainRevision::DEFAULT.chain_params,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(
            result,
            Err(StaticValidationError::InitCodeLimitExceeded { .. })
        ));

        // transaction with larger max priority fee than max fee per gas
        let tx_priority_fee_too_high = TxEip1559 {
            chain_id,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10000,
            gas_limit: 1_000_000,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_priority_fee_too_high.signature_hash());
        let txn = tx_priority_fee_too_high.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            MockChainRevision::DEFAULT.chain_params,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(
            result,
            Err(StaticValidationError::MaxPriorityOverMaxFee { .. })
        ));

        // transaction with gas limit lower than intrinsic gas
        let tx_gas_limit_too_low = TxEip1559 {
            chain_id,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 20_000,
            input: vec![].into(),
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_low.signature_hash());
        let txn = tx_gas_limit_too_low.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            MockChainRevision::DEFAULT.chain_params,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(
            result,
            Err(StaticValidationError::GasLimitUnderIntrinsicGas { .. })
        ));

        // transaction with gas limit lower than floor data gas
        // floor data gas is 21000 + (zero byte * 10) + (non-zero byte * 40)
        let tx_gas_limit_too_low_data = TxEip1559 {
            chain_id,
            nonce: 0,
            to: TxKind::Call(address),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 30_000,
            input: vec![0xaa; 226].into(), // 21000 + (226 * 40) > 30000
            ..Default::default()
        };
        let signature = sign_tx(&tx_gas_limit_too_low_data.signature_hash());
        let txn = tx_gas_limit_too_low_data.into_signed(signature);

        let result = static_validate_transaction(
            &txn.into(),
            chain_id,
            MockChainRevision::DEFAULT.chain_params,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert!(matches!(
            result,
            Err(StaticValidationError::GasLimitUnderFloorDataGas { .. })
        ));
    }

    #[test]
    fn test_compute_floor_data_gas() {
        const CHAIN_ID: u64 = 1337;
        let tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Call(Address(FixedBytes([0x11; 20]))),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            // input data with 3 zero byte and 4 non-zero byte
            input: Bytes::from_str("0x12003456000078").unwrap(),
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let tx = tx.into_signed(signature);

        let result = compute_floor_data_gas(&tx.into());
        assert_eq!(result, 21000 + (3 * 10) + (4 * 40));
    }

    #[test]
    fn test_compute_intrinsic_gas() {
        const CHAIN_ID: u64 = 1337;
        let tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 0,
            to: TxKind::Create,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit: 1_000_000,
            input: Bytes::from_str("0x6040608081523462000414").unwrap(),
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        let tx = tx.into_signed(signature);

        let result = compute_intrinsic_gas(&tx.into());
        assert_eq!(result, 53166);
    }

    #[test]
    fn test_compute_intrinsic_gas_eip7702() {
        let tx_1_auth = make_eip7702_tx(
            S1,
            BASE_FEE as u128,
            0,
            100_000,
            0,
            vec![make_signed_authorization(S2, secret_to_eth_address(S1), 0)],
            0,
        );

        let result_1_auth = compute_intrinsic_gas(&tx_1_auth);
        assert_eq!(result_1_auth, 46000);

        let tx_2_auth = make_eip7702_tx(
            S1,
            BASE_FEE as u128,
            0,
            100_000,
            0,
            vec![
                make_signed_authorization(S2, secret_to_eth_address(S1), 0),
                make_signed_authorization(S2, secret_to_eth_address(S1), 0),
            ],
            0,
        );

        let result_2_auth = compute_intrinsic_gas(&tx_2_auth);
        assert_eq!(result_2_auth, 71000);
    }
}
