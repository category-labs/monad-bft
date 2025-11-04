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

//! This library is used to generate and validate expected system calls
//! for a block and generate transactions for them from the system sender.
//! To generate system calls for a block, `generate_system_calls()` should
//! be used which can then be converted into SystemTransaction(s) and
//! added to the block.

use alloy_consensus::{
    SignableTransaction, Transaction, TxEnvelope, TxLegacy, transaction::Recovered,
};
use alloy_primitives::{Address, B256, U256, hex};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_chain_config::{ChainConfig, revision::ChainRevision};
use monad_eth_types::ValidatedTx;
use monad_types::{Epoch, GENESIS_SEQ_NUM, SeqNum};
use staking_contract::{StakingContractCall, StakingContractTransaction};
use validator::SystemTransactionError;

pub mod staking_contract;
pub mod validator;

// Private key used to sign system transactions
const SYSTEM_SENDER_PRIV_KEY: B256 = B256::new(hex!(
    "b0358e6d701a955d9926676f227e40172763296b317ff554e49cdf2c2c35f8a7"
));
pub const SYSTEM_SENDER_ETH_ADDRESS: Address =
    Address::new(hex!("0x6f49a8F621353f12378d0046E7d7e4b9B249DC9e"));

fn sign_with_system_sender(transaction: TxLegacy) -> Recovered<TxEnvelope> {
    let signer = PrivateKeySigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    let signed = transaction.into_signed(signature);

    Recovered::new_unchecked(TxEnvelope::Legacy(signed), SYSTEM_SENDER_ETH_ADDRESS)
}

#[derive(Debug)]
enum SystemCall {
    StakingContractCall(StakingContractCall),
}

impl SystemCall {
    // Used to validate inputs of user transactions in RPC and TxPool
    pub fn is_restricted_system_call(txn: &Recovered<TxEnvelope>) -> bool {
        StakingContractCall::is_restricted_staking_contract_call(txn)
    }

    // Used to validate inputs of the expected system transactions
    pub fn validate_system_transaction_input(
        &self,
        sys_txn: &ValidatedTx,
    ) -> Result<(), SystemTransactionError> {
        match self {
            Self::StakingContractCall(staking_sys_call) => {
                staking_sys_call.validate_system_transaction_input(sys_txn)
            }
        }
    }

    fn into_signed_transaction(self, chain_id: u64, nonce: u64) -> SystemTransaction {
        match self {
            Self::StakingContractCall(staking_sys_call) => {
                SystemTransaction::StakingContractTransaction(
                    staking_sys_call.into_signed_transaction(chain_id, nonce),
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum SystemTransaction {
    StakingContractTransaction(StakingContractTransaction),
}

impl SystemTransaction {
    pub fn signer(&self) -> Address {
        let signer = match &self {
            Self::StakingContractTransaction(staking_transaction) => {
                staking_transaction.inner().signer()
            }
        };
        assert_eq!(signer, SYSTEM_SENDER_ETH_ADDRESS);

        signer
    }
    pub fn nonce(&self) -> u64 {
        match &self {
            Self::StakingContractTransaction(staking_transaction) => {
                staking_transaction.inner().nonce()
            }
        }
    }

    pub fn length(&self) -> usize {
        match &self {
            Self::StakingContractTransaction(staking_transaction) => {
                staking_transaction.inner().length()
            }
        }
    }
}

impl From<SystemTransaction> for Recovered<TxEnvelope> {
    fn from(sys_txn: SystemTransaction) -> Self {
        match sys_txn {
            SystemTransaction::StakingContractTransaction(staking_txn) => staking_txn.into_inner(),
        }
    }
}

fn generate_system_calls<CCT, CRT>(
    proposed_seq_num: SeqNum,
    proposed_epoch: Epoch,
    parent_block_epoch: Epoch,
    block_author_address: Address,
    chain_config: &CCT,
) -> Vec<SystemCall>
where
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    let epoch_length = chain_config.get_epoch_length();
    let staking_activation = chain_config.get_staking_activation();

    let mut system_calls = Vec::new();

    // If staking activates in Epoch N, generate snapshot transactions
    // starting at the boundary of Epoch N-1
    let generate_snapshot_txn = proposed_seq_num.is_epoch_end(epoch_length)
        && proposed_seq_num.get_locked_epoch(epoch_length) >= staking_activation;
    if generate_snapshot_txn {
        system_calls.push(SystemCall::StakingContractCall(
            StakingContractCall::Snapshot,
        ));
    }

    // If staking activates in Epoch N, generate epoch change transactions
    // starting at Epoch N-1
    // Special case: If staking starts at Epoch 2, generate epoch change
    // as the first transaction in Epoch 1
    let is_first_epoch_block =
        parent_block_epoch != proposed_epoch && proposed_epoch >= staking_activation - Epoch(1);
    let is_genesis_block =
        proposed_seq_num == GENESIS_SEQ_NUM + SeqNum(1) && staking_activation == Epoch(2);
    let generate_epoch_change_txn = is_first_epoch_block || is_genesis_block;
    if generate_epoch_change_txn {
        system_calls.push(SystemCall::StakingContractCall(
            StakingContractCall::EpochChange {
                new_epoch: proposed_epoch,
            },
        ));
    }

    // If staking activates in Epoch N, generate reward transactions
    // starting at the first block in Epoch N
    let generate_reward_txn = proposed_epoch >= staking_activation;
    if generate_reward_txn {
        let block_reward_mon = chain_config.get_block_reward_mon(proposed_epoch);
        let block_reward = U256::from(StakingContractCall::MON) * U256::from(block_reward_mon);

        system_calls.push(SystemCall::StakingContractCall(
            StakingContractCall::Reward {
                block_author_address,
                block_reward,
            },
        ));
    }

    system_calls
}

#[derive(Clone, Debug)]
pub struct SystemTransactionGenerator;

impl SystemTransactionGenerator {
    // Used by a round leader to generate system calls for the proposing block
    pub fn generate_system_transactions<CCT, CRT>(
        proposed_seq_num: SeqNum,
        proposed_epoch: Epoch,
        parent_block_epoch: Epoch,
        block_author: Address,
        mut next_system_txn_nonce: u64,
        chain_config: &CCT,
    ) -> Vec<SystemTransaction>
    where
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        let system_calls = generate_system_calls(
            proposed_seq_num,
            proposed_epoch,
            parent_block_epoch,
            block_author,
            chain_config,
        );

        system_calls
            .into_iter()
            .map(|sys_call| {
                let system_txn = sys_call
                    .into_signed_transaction(chain_config.chain_id(), next_system_txn_nonce);
                next_system_txn_nonce += 1;

                system_txn
            })
            .collect()
    }
}

#[cfg(test)]
mod test_utils {
    use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy, transaction::Recovered};
    use alloy_primitives::{Address, Bytes, TxKind};
    use alloy_signer::SignerSync;
    use alloy_signer_local::LocalSigner;

    use crate::{SYSTEM_SENDER_ETH_ADDRESS, SYSTEM_SENDER_PRIV_KEY};

    pub fn get_valid_system_transaction() -> TxLegacy {
        TxLegacy {
            chain_id: Some(1337),
            nonce: 0,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(Address::new([0_u8; 20])),
            value: Default::default(),
            input: Bytes::new(),
        }
    }

    pub fn sign_with_system_sender(transaction: TxLegacy) -> Recovered<TxEnvelope> {
        let signature_hash = transaction.signature_hash();
        let local_signer = LocalSigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();
        let signature = local_signer.sign_hash_sync(&signature_hash).unwrap();

        Recovered::new_unchecked(
            TxEnvelope::Legacy(transaction.into_signed(signature)),
            SYSTEM_SENDER_ETH_ADDRESS,
        )
    }
}
