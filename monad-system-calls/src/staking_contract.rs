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
use alloy_sol_types::SolValue;
use monad_eth_types::ValidatedTx;
use monad_types::Epoch;

use crate::{sign_with_system_sender, validator::SystemTransactionError};

#[derive(Debug)]
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
    // First 4 bytes of keccak("syscallReward(address)")
    pub const REWARD_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x791bdcf3"));
    // First 4 bytes of keccak("syscallSnapshot()")
    pub const SNAPSHOT_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x157eeb21"));
    // First 4 bytes of keccak("syscallOnEpochChange(uint64)")
    pub const EPOCH_CHANGE_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x1d4e9f02"));

    pub const MON: u64 = 1_000_000_000_000_000_000;

    pub fn is_restricted_staking_contract_call(txn: &Recovered<TxEnvelope>) -> bool {
        if txn.to() == Some(Self::STAKING_CONTRACT_ADDRESS) {
            let input = txn.input();
            return input.starts_with(Self::REWARD_FUNCTION_SELECTOR.as_slice())
                || input.starts_with(Self::SNAPSHOT_FUNCTION_SELECTOR.as_slice())
                || input.starts_with(Self::EPOCH_CHANGE_FUNCTION_SELECTOR.as_slice());
        }

        false
    }

    fn get_transaction_value(&self) -> U256 {
        match self {
            StakingContractCall::Reward {
                block_author_address: _,
                block_reward,
            } => *block_reward,
            StakingContractCall::Snapshot => U256::ZERO,
            StakingContractCall::EpochChange { new_epoch: _ } => U256::ZERO,
        }
    }

    fn get_transaction_input(&self) -> Bytes {
        match self {
            StakingContractCall::Reward {
                block_author_address,
                block_reward: _,
            } => [
                Self::REWARD_FUNCTION_SELECTOR.as_slice(),
                block_author_address.abi_encode().as_slice(),
            ]
            .concat()
            .into(),
            StakingContractCall::Snapshot => Self::SNAPSHOT_FUNCTION_SELECTOR.into(),
            StakingContractCall::EpochChange { new_epoch } => [
                Self::EPOCH_CHANGE_FUNCTION_SELECTOR.as_slice(),
                new_epoch.0.abi_encode().as_slice(),
            ]
            .concat()
            .into(),
        }
    }

    pub fn as_legacy_transaction(&self, chain_id: u64, nonce: u64) -> TxLegacy {
        TxLegacy {
            chain_id: Some(chain_id),
            nonce,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(Self::STAKING_CONTRACT_ADDRESS),
            value: self.get_transaction_value(),
            input: self.get_transaction_input(),
        }
    }

    pub fn into_signed_transaction(self, chain_id: u64, nonce: u64) -> StakingContractTransaction {
        match self {
            StakingContractCall::Reward {
                block_author_address: _,
                block_reward: _,
            } => StakingContractTransaction::Reward(sign_with_system_sender(
                self.as_legacy_transaction(chain_id, nonce),
            )),
            StakingContractCall::Snapshot => StakingContractTransaction::Snapshot(
                sign_with_system_sender(self.as_legacy_transaction(chain_id, nonce)),
            ),
            StakingContractCall::EpochChange { new_epoch: _ } => {
                StakingContractTransaction::EpochChange(sign_with_system_sender(
                    self.as_legacy_transaction(chain_id, nonce),
                ))
            }
        }
    }

    pub fn validate_system_transaction_input(
        &self,
        system_txn: &ValidatedTx,
    ) -> Result<(), SystemTransactionError> {
        let to = system_txn.to();
        if to != Some(Self::STAKING_CONTRACT_ADDRESS) {
            return Err(SystemTransactionError::UnexpectedDestAddress {
                expected: Self::STAKING_CONTRACT_ADDRESS,
                actual: to,
            });
        }

        let input = system_txn.input();
        let value = system_txn.value();

        match self {
            Self::Reward {
                block_author_address: _,
                block_reward,
            } => {
                let expected_input = self.get_transaction_input();
                if input != &expected_input {
                    return Err(SystemTransactionError::UnexpectedInput {
                        expected_input,
                        actual_input: input.clone(),
                    });
                }

                if value != *block_reward {
                    return Err(SystemTransactionError::UnexpectedValue {
                        expected_value: *block_reward,
                        actual_value: value,
                    });
                }
            }
            Self::Snapshot => {
                let expected_input = self.get_transaction_input();
                if input != &expected_input {
                    return Err(SystemTransactionError::UnexpectedInput {
                        expected_input,
                        actual_input: input.clone(),
                    });
                }

                if value != U256::ZERO {
                    return Err(SystemTransactionError::UnexpectedValue {
                        expected_value: U256::ZERO,
                        actual_value: value,
                    });
                }
            }
            Self::EpochChange { new_epoch: _ } => {
                let expected_input = self.get_transaction_input();
                if input != &expected_input {
                    return Err(SystemTransactionError::UnexpectedInput {
                        expected_input,
                        actual_input: input.clone(),
                    });
                }

                if value != U256::ZERO {
                    return Err(SystemTransactionError::UnexpectedValue {
                        expected_value: U256::ZERO,
                        actual_value: value,
                    });
                }
            }
        }

        Ok(())
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
