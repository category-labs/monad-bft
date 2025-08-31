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

use alloy_consensus::{Transaction, TxEnvelope, TxLegacy, transaction::Recovered};
use alloy_primitives::{Address, Bytes, FixedBytes, TxKind, U256, hex};
use monad_types::Epoch;

use crate::{sign_with_system_sender, validator::SystemTransactionError};

pub(crate) enum StakingContractCall {
    Reward {
        block_author_address: Address,
        block_reward: U256,
    },
    Snapshot,
    EpochChange {
        new_epoch: Epoch,
    },
}

impl StakingContractCall {
    pub const STAKING_CONTRACT_ADDRESS: Address =
        Address::new(hex!("0x0000000000000000000000000000000000001000"));

    // System transactions related to staking
    pub const REWARD_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x00000064"));
    pub const SNAPSHOT_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x00000065"));
    pub const EPOCH_CHANGE_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x00000066"));

    pub const MON: u64 = 1_000_000_000_000_000_000;
    pub const BLOCK_REWARD_MON: u64 = 1;

    pub fn is_restricted_staking_contract_call(txn: &Recovered<TxEnvelope>) -> bool {
        if txn.to() == Some(Self::STAKING_CONTRACT_ADDRESS) {
            let input = txn.input();
            return input.starts_with(Self::REWARD_FUNCTION_SELECTOR.as_slice())
                || input.starts_with(Self::SNAPSHOT_FUNCTION_SELECTOR.as_slice())
                || input.starts_with(Self::EPOCH_CHANGE_FUNCTION_SELECTOR.as_slice());
        }

        false
    }

    pub fn into_signed_transaction(self, chain_id: u64, nonce: u64) -> StakingContractTransaction {
        let mut transaction = TxLegacy {
            chain_id: Some(chain_id),
            nonce,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(Self::STAKING_CONTRACT_ADDRESS),
            value: U256::ZERO,
            input: Bytes::new(),
        };

        match self {
            StakingContractCall::Reward {
                block_author_address,
                block_reward,
            } => {
                let mut input = [0_u8; 24];
                input[0..4].copy_from_slice(Self::REWARD_FUNCTION_SELECTOR.as_slice());
                input[4..24].copy_from_slice(block_author_address.as_slice());
                transaction.input = input.into();
                transaction.value = block_reward;

                StakingContractTransaction::Reward(sign_with_system_sender(transaction))
            }
            StakingContractCall::Snapshot => {
                transaction.input = Self::SNAPSHOT_FUNCTION_SELECTOR.into();

                StakingContractTransaction::Snapshot(sign_with_system_sender(transaction))
            }
            StakingContractCall::EpochChange { new_epoch } => {
                let mut input = [0_u8; 12];
                input[0..4].copy_from_slice(Self::EPOCH_CHANGE_FUNCTION_SELECTOR.as_slice());
                input[4..12].copy_from_slice(&new_epoch.0.to_be_bytes());
                transaction.input = input.into();

                StakingContractTransaction::EpochChange(sign_with_system_sender(transaction))
            }
        }
    }

    pub fn validate_system_transaction_input(
        self,
        sys_txn: Recovered<TxEnvelope>,
    ) -> Result<StakingContractTransaction, SystemTransactionError> {
        let to = sys_txn.to();
        let input = sys_txn.input();
        let value = sys_txn.value();

        match self {
            Self::Reward {
                block_author_address,
                block_reward,
            } => {
                if to != Some(Self::STAKING_CONTRACT_ADDRESS) {
                    return Err(SystemTransactionError::UnexpectedDestAddress);
                }
                if input.len() != Self::REWARD_FUNCTION_SELECTOR.len() + block_author_address.len()
                {
                    return Err(SystemTransactionError::UnexpectedInput);
                }
                if input[0..4] != Self::REWARD_FUNCTION_SELECTOR {
                    return Err(SystemTransactionError::UnexpectedInput);
                }
                if input[4..24] != block_author_address {
                    return Err(SystemTransactionError::UnexpectedInput);
                }
                if value != block_reward {
                    return Err(SystemTransactionError::UnexpectedValue);
                }

                Ok(StakingContractTransaction::Reward(sys_txn))
            }
            Self::Snapshot => {
                if to != Some(Self::STAKING_CONTRACT_ADDRESS) {
                    return Err(SystemTransactionError::UnexpectedDestAddress);
                }
                if input != Self::SNAPSHOT_FUNCTION_SELECTOR.as_slice() {
                    return Err(SystemTransactionError::UnexpectedInput);
                }
                if value != U256::ZERO {
                    return Err(SystemTransactionError::UnexpectedValue);
                }

                Ok(StakingContractTransaction::Snapshot(sys_txn))
            }
            Self::EpochChange { new_epoch } => {
                if to != Some(Self::STAKING_CONTRACT_ADDRESS) {
                    return Err(SystemTransactionError::UnexpectedDestAddress);
                }
                let expected_epoch_input = new_epoch.0.to_be_bytes();
                if input.len()
                    != Self::EPOCH_CHANGE_FUNCTION_SELECTOR.len() + expected_epoch_input.len()
                {
                    return Err(SystemTransactionError::UnexpectedInput);
                }
                if input[0..4] != Self::EPOCH_CHANGE_FUNCTION_SELECTOR {
                    return Err(SystemTransactionError::UnexpectedInput);
                }
                if input[4..12] != expected_epoch_input {
                    return Err(SystemTransactionError::UnexpectedInput);
                }
                if value != U256::ZERO {
                    return Err(SystemTransactionError::UnexpectedValue);
                }

                Ok(StakingContractTransaction::EpochChange(sys_txn))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum StakingContractTransaction {
    Reward(Recovered<TxEnvelope>),
    Snapshot(Recovered<TxEnvelope>),
    EpochChange(Recovered<TxEnvelope>),
}

impl StakingContractTransaction {
    pub fn into_inner(self) -> Recovered<TxEnvelope> {
        match self {
            Self::Reward(txn) => txn,
            Self::Snapshot(txn) => txn,
            Self::EpochChange(txn) => txn,
        }
    }

    pub fn inner(&self) -> &Recovered<TxEnvelope> {
        match self {
            Self::Reward(txn) => txn,
            Self::Snapshot(txn) => txn,
            Self::EpochChange(txn) => txn,
        }
    }
}
