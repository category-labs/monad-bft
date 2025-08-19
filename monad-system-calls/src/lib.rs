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
use alloy_primitives::{Address, B256, FixedBytes, TxKind, hex};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_types::EthExecutionProtocol;
use monad_types::{Epoch, SeqNum};
use monad_validator::signature_collection::SignatureCollection;

pub mod validator;

// Private key used to sign system transactions
const SYSTEM_SENDER_PRIV_KEY: B256 = B256::new(hex!(
    "b0358e6d701a955d9926676f227e40172763296b317ff554e49cdf2c2c35f8a7"
));
pub const SYSTEM_SENDER_ETH_ADDRESS: Address =
    Address::new(hex!("0x6f49a8F621353f12378d0046E7d7e4b9B249DC9e"));

// System transactions related to staking
pub(crate) const STAKING_CONTRACT_ADDRESS: Address =
    Address::new(hex!("0x0000000000000000000000000000000000001000"));
pub(crate) const REWARD_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x00000064"));
pub(crate) const SNAPSHOT_FUNCTION_SELECTOR: FixedBytes<4> = FixedBytes::new(hex!("0x00000065"));
pub(crate) const EPOCH_CHANGE_FUNCTION_SELECTOR: FixedBytes<4> =
    FixedBytes::new(hex!("0x00000066"));

pub struct SystemCall(SystemCallInner);

impl SystemCall {
    fn into_system_transaction(self, chain_id: u64, nonce: u64) -> SystemTransaction {
        SystemTransaction(self.0.into_system_transaction_inner(chain_id, nonce))
    }
}

pub(crate) enum SystemCallInner {
    Reward { block_author_address: Address },
    SnapshotStakingContract,
    EpochChange { new_epoch: Epoch },
}

impl SystemCallInner {
    fn into_signed_transaction(self, chain_id: u64, nonce: u64) -> Recovered<TxEnvelope> {
        let (to, input) = match self {
            SystemCallInner::Reward {
                block_author_address,
            } => {
                let to = TxKind::Call(STAKING_CONTRACT_ADDRESS);
                let mut input = [0_u8; 24];
                input[0..4].copy_from_slice(REWARD_FUNCTION_SELECTOR.as_slice());
                input[4..24].copy_from_slice(block_author_address.as_slice());

                (to, input.into())
            }
            SystemCallInner::SnapshotStakingContract => {
                let to = TxKind::Call(STAKING_CONTRACT_ADDRESS);

                (to, SNAPSHOT_FUNCTION_SELECTOR.into())
            }
            SystemCallInner::EpochChange { new_epoch } => {
                let to = TxKind::Call(STAKING_CONTRACT_ADDRESS);
                let mut input = [0_u8; 12];
                input[0..4].copy_from_slice(EPOCH_CHANGE_FUNCTION_SELECTOR.as_slice());
                input[4..12].copy_from_slice(&new_epoch.0.to_be_bytes());

                (to, input.into())
            }
        };

        let transaction = TxLegacy {
            chain_id: Some(chain_id),
            nonce,
            gas_price: 0,
            gas_limit: 0,
            to,
            value: Default::default(),
            input,
        };

        let signer = PrivateKeySigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();
        let signed = transaction.into_signed(signature);

        Recovered::new_unchecked(TxEnvelope::Legacy(signed), SYSTEM_SENDER_ETH_ADDRESS)
    }

    fn into_system_transaction_inner(self, chain_id: u64, nonce: u64) -> SystemTransactionInner {
        match &self {
            SystemCallInner::Reward { .. } => {
                SystemTransactionInner::Reward(self.into_signed_transaction(chain_id, nonce))
            }
            Self::SnapshotStakingContract => SystemTransactionInner::SnapshotStakingContract(
                self.into_signed_transaction(chain_id, nonce),
            ),
            Self::EpochChange { .. } => {
                SystemTransactionInner::EpochChange(self.into_signed_transaction(chain_id, nonce))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SystemTransaction(SystemTransactionInner);

#[derive(Debug, Clone)]
enum SystemTransactionInner {
    Reward(Recovered<TxEnvelope>),
    SnapshotStakingContract(Recovered<TxEnvelope>),
    EpochChange(Recovered<TxEnvelope>),
}

impl From<SystemTransactionInner> for Recovered<TxEnvelope> {
    fn from(sys_txn_inner: SystemTransactionInner) -> Self {
        match sys_txn_inner {
            SystemTransactionInner::Reward(tx) => tx,
            SystemTransactionInner::SnapshotStakingContract(tx) => tx,
            SystemTransactionInner::EpochChange(tx) => tx,
        }
    }
}

impl SystemTransaction {
    pub fn signer(&self) -> Address {
        let signer = match &self.0 {
            SystemTransactionInner::Reward(txn) => txn.signer(),
            SystemTransactionInner::SnapshotStakingContract(txn) => txn.signer(),
            SystemTransactionInner::EpochChange(txn) => txn.signer(),
        };
        assert_eq!(signer, SYSTEM_SENDER_ETH_ADDRESS);

        signer
    }

    pub fn nonce(&self) -> u64 {
        match &self.0 {
            SystemTransactionInner::Reward(txn) => txn.nonce(),
            SystemTransactionInner::SnapshotStakingContract(txn) => txn.nonce(),
            SystemTransactionInner::EpochChange(txn) => txn.nonce(),
        }
    }

    pub fn length(&self) -> usize {
        match &self.0 {
            SystemTransactionInner::Reward(txn) => txn.length(),
            SystemTransactionInner::SnapshotStakingContract(txn) => txn.length(),
            SystemTransactionInner::EpochChange(txn) => txn.length(),
        }
    }
}

impl From<SystemTransaction> for Recovered<TxEnvelope> {
    fn from(sys_txn: SystemTransaction) -> Self {
        sys_txn.0.into()
    }
}

fn generate_system_calls(
    epoch_length: SeqNum,
    staking_activation: Epoch,
    proposed_seq_num: SeqNum,
    proposed_epoch: Epoch,
    parent_block_epoch: Epoch,
    block_author_address: Address,
) -> Vec<SystemCall> {
    let mut system_calls = Vec::new();

    if proposed_seq_num.is_epoch_end(epoch_length)
        && proposed_seq_num.get_locked_epoch(epoch_length) >= staking_activation
    {
        system_calls.push(SystemCall(SystemCallInner::SnapshotStakingContract));
    }

    if proposed_epoch >= staking_activation {
        if parent_block_epoch != proposed_epoch {
            system_calls.push(SystemCall(SystemCallInner::EpochChange {
                new_epoch: proposed_epoch,
            }));
        }

        system_calls.push(SystemCall(SystemCallInner::Reward {
            block_author_address,
        }));
    }

    system_calls
}

// Used by a validator to generate expected system calls for a block
fn generate_system_calls_from_header<ST, SCT>(
    epoch_length: SeqNum,
    staking_activation: Epoch,
    block_header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
) -> Vec<SystemCall>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let author_eth_address = block_header
        .author
        .pubkey()
        .get_eth_address()
        .expect("eth address available for signing key");

    generate_system_calls(
        epoch_length,
        staking_activation,
        block_header.seq_num,
        block_header.epoch,
        block_header.qc.get_epoch(),
        author_eth_address,
    )
}

#[derive(Clone, Debug)]
pub struct SystemTransactionGenerator {
    pub chain_id: u64,
    pub epoch_length: SeqNum,
    pub staking_activation: Epoch,
}

impl SystemTransactionGenerator {
    pub fn new(chain_id: u64, epoch_length: SeqNum, staking_activation: Epoch) -> Self {
        Self {
            chain_id,
            epoch_length,
            staking_activation,
        }
    }

    // Used by a round leader to generate system calls for the proposing block
    pub fn generate_system_transactions(
        &self,
        proposed_seq_num: SeqNum,
        proposed_epoch: Epoch,
        parent_block_epoch: Epoch,
        block_author: Address,
        mut next_system_txn_nonce: u64,
    ) -> Vec<SystemTransaction> {
        let system_calls = generate_system_calls(
            self.epoch_length,
            self.staking_activation,
            proposed_seq_num,
            proposed_epoch,
            parent_block_epoch,
            block_author,
        );

        system_calls
            .into_iter()
            .map(|sys_call| {
                let system_txn =
                    sys_call.into_system_transaction(self.chain_id, next_system_txn_nonce);
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
