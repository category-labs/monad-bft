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
    YellowPaperValidation::validate(tx, execution_chain_params)?;

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
    fn validate(
        tx: &TxEnvelope,
        execution_chain_params: &ExecutionChainParams,
    ) -> Result<(), StaticValidationError> {
        Self::intrinsic_gas_validation(tx, execution_chain_params)
    }

    fn intrinsic_gas_validation(
        tx: &TxEnvelope,
        execution_chain_params: &ExecutionChainParams,
    ) -> Result<(), StaticValidationError> {
        // YP eq. 62 - intrinsic gas validation
        let intrinsic_gas = compute_intrinsic_gas(tx, execution_chain_params);
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
            Self::eip_7623(tx, execution_chain_params)?;
        }
        Self::eip_7702(tx, execution_chain_params)?;

        Ok(())
    }

    fn eip_7623(
        tx: &TxEnvelope,
        execution_chain_params: &ExecutionChainParams,
    ) -> Result<(), StaticValidationError> {
        let floor_data_gas = compute_floor_data_gas(tx, execution_chain_params);
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

fn compute_intrinsic_gas(tx: &TxEnvelope, execution_chain_params: &ExecutionChainParams) -> u64 {
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

    if execution_chain_params.amsterdam_enabled {
        intrinsic_gas = intrinsic_gas.saturating_add(compute_access_list_data_gas(tx));
    }
    intrinsic_gas
}

fn compute_floor_data_gas(tx: &TxEnvelope, execution_chain_params: &ExecutionChainParams) -> u64 {
    // EIP-7623
    let zero_data_len = tx.input().iter().filter(|v| **v == 0).count() as u64;
    let non_zero_data_len = tx.input().len() as u64 - zero_data_len;
    let mut floor_data_gas = 21_000 + (zero_data_len * 10 + non_zero_data_len * 40);

    if execution_chain_params.amsterdam_enabled {
        floor_data_gas = floor_data_gas.saturating_add(compute_access_list_data_gas(tx));
    }
    floor_data_gas
}

// EIP-7981: access-list data cost
fn compute_access_list_data_gas(tx: &TxEnvelope) -> u64 {
    let access_list = tx
        .access_list()
        .map(|list| list.0.as_slice())
        .unwrap_or(&[]);
    let access_list_bytes = access_list.iter().fold(0u64, |bytes, item| {
        bytes
            .saturating_add(20)
            .saturating_add((item.storage_keys.len() as u64).saturating_mul(32))
    });
    access_list_bytes.saturating_mul(40)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use alloy_consensus::{SignableTransaction, TxEip1559, TxLegacy};
    use alloy_eips::eip2930::{AccessList, AccessListItem};
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

        let result = compute_floor_data_gas(
            &tx.into(),
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
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

        let result = compute_intrinsic_gas(
            &tx.into(),
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
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

        let result_1_auth = compute_intrinsic_gas(
            &tx_1_auth,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
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

        let result_2_auth = compute_intrinsic_gas(
            &tx_2_auth,
            MonadExecutionRevision::LATEST.execution_chain_params(),
        );
        assert_eq!(result_2_auth, 71000);
    }

    fn make_access_list_tx(
        gas_limit: u64,
        storage_keys_per_address: &[usize],
        input: Bytes,
    ) -> TxEnvelope {
        let access_list = AccessList(
            storage_keys_per_address
                .iter()
                .enumerate()
                .map(|(i, num_keys)| AccessListItem {
                    address: Address(FixedBytes([i as u8 + 1; 20])),
                    storage_keys: (0..*num_keys).map(|k| B256::repeat_byte(k as u8)).collect(),
                })
                .collect(),
        );
        let tx = TxEip1559 {
            chain_id: MockChainConfig::DEFAULT.chain_id(),
            nonce: 0,
            to: TxKind::Call(Address(FixedBytes([0x11; 20]))),
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 10,
            gas_limit,
            access_list,
            input,
            ..Default::default()
        };
        let signature = sign_tx(&tx.signature_hash());
        tx.into_signed(signature).into()
    }

    fn validate_with_revision(
        tx: &TxEnvelope,
        revision: MonadExecutionRevision,
    ) -> Result<(), StaticValidationError> {
        static_validate_transaction(
            tx,
            MockChainConfig::DEFAULT.chain_id(),
            MockChainRevision::DEFAULT.chain_params,
            revision.execution_chain_params(),
        )
    }

    #[test]
    fn test_eip7981_access_list_intrinsic_gas_boundary() {
        // 21000 + 2400 (EIP-2930 address)
        const PRE_AMSTERDAM_INTRINSIC_GAS: u64 = 23_400;
        // + 20 address bytes * 40 (EIP-7981)
        const AMSTERDAM_INTRINSIC_GAS: u64 = 24_200;

        let tx = make_access_list_tx(PRE_AMSTERDAM_INTRINSIC_GAS, &[0], Bytes::new());
        assert_eq!(
            validate_with_revision(&tx, MonadExecutionRevision::V_FOUR),
            Ok(())
        );
        assert_eq!(
            validate_with_revision(&tx, MonadExecutionRevision::V_NEXT),
            Err(StaticValidationError::GasLimitUnderIntrinsicGas {
                tx_gas_limit: PRE_AMSTERDAM_INTRINSIC_GAS,
                intrinsic_gas: AMSTERDAM_INTRINSIC_GAS,
            })
        );

        let tx = make_access_list_tx(AMSTERDAM_INTRINSIC_GAS - 1, &[0], Bytes::new());
        assert!(matches!(
            validate_with_revision(&tx, MonadExecutionRevision::V_NEXT),
            Err(StaticValidationError::GasLimitUnderIntrinsicGas { .. })
        ));

        let tx = make_access_list_tx(AMSTERDAM_INTRINSIC_GAS, &[0], Bytes::new());
        assert_eq!(
            validate_with_revision(&tx, MonadExecutionRevision::V_NEXT),
            Ok(())
        );
    }

    #[test]
    fn test_eip7981_access_list_floor_data_gas_boundary() {
        const PRE_AMSTERDAM_FLOOR_DATA_GAS: u64 = 61_000;
        // 1000 non-zero bytes and one address with no keys:
        // floor = 21000 + 1000 * 40 + 20 * 40 = 61_800
        const AMSTERDAM_FLOOR_DATA_GAS: u64 = 61_800;
        let input: Bytes = vec![0xaa; 1000].into();

        let tx = make_access_list_tx(PRE_AMSTERDAM_FLOOR_DATA_GAS, &[0], input.clone());
        assert_eq!(
            validate_with_revision(&tx, MonadExecutionRevision::V_FOUR),
            Ok(())
        );
        assert_eq!(
            validate_with_revision(&tx, MonadExecutionRevision::V_NEXT),
            Err(StaticValidationError::GasLimitUnderFloorDataGas {
                tx_gas_limit: PRE_AMSTERDAM_FLOOR_DATA_GAS,
                floor_data_gas: AMSTERDAM_FLOOR_DATA_GAS,
            })
        );

        let tx = make_access_list_tx(AMSTERDAM_FLOOR_DATA_GAS, &[0], input);
        assert_eq!(
            validate_with_revision(&tx, MonadExecutionRevision::V_NEXT),
            Ok(())
        );
    }

    #[test]
    fn test_compute_access_list_data_gas() {
        let pre_amsterdam = MonadExecutionRevision::V_FOUR.execution_chain_params();
        let amsterdam = MonadExecutionRevision::V_NEXT.execution_chain_params();

        let tx = make_access_list_tx(1_000_000, &[], Bytes::new());
        assert_eq!(compute_access_list_data_gas(&tx), 0);

        // 2 addresses, 3 storage keys: (2 * 20 + 3 * 32) * 40
        let tx = make_access_list_tx(1_000_000, &[1, 2], Bytes::new());
        assert_eq!(compute_access_list_data_gas(&tx), 5_440);
        assert_eq!(
            compute_intrinsic_gas(&tx, pre_amsterdam),
            21_000 + 2 * 2_400 + 3 * 1_900
        );
        assert_eq!(
            compute_intrinsic_gas(&tx, amsterdam),
            21_000 + 2 * 2_400 + 3 * 1_900 + 5_440
        );
        assert_eq!(compute_floor_data_gas(&tx, pre_amsterdam), 21_000);
        assert_eq!(compute_floor_data_gas(&tx, amsterdam), 21_000 + 5_440);

        // legacy transactions have no access list
        let legacy = TxLegacy {
            chain_id: None,
            nonce: 0,
            to: TxKind::Call(Address(FixedBytes([0x11; 20]))),
            gas_price: 1000,
            gas_limit: 21_000,
            ..Default::default()
        };
        let signature = sign_tx(&legacy.signature_hash());
        let legacy: TxEnvelope = legacy.into_signed(signature).into();
        assert_eq!(compute_intrinsic_gas(&legacy, amsterdam), 21_000);
    }
}
