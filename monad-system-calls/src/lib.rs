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

use alloy_primitives::{Address, B256, Bytes, hex};
use monad_consensus_types::block::ConsensusBlockHeader;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

pub mod validator;

// Private key used to sign system transactions
const SYSTEM_SENDER_PRIV_KEY: B256 = B256::new(hex!(
    "b0358e6d701a955d9926676f227e40172763296b317ff554e49cdf2c2c35f8a7"
));
const SYSTEM_SENDER_ETH_ADDRESS: Address =
    Address::new(hex!("0x6f49a8F621353f12378d0046E7d7e4b9B249DC9e"));

// A system call is a destination address, system call function selector
// and function input data
pub struct SystemCall(SystemCallInner);

enum SystemCallInner {}

impl SystemCall {
    fn get_dest_address(&self) -> Address {
        Address::new([0_u8; 20])
    }

    fn get_function_selector(&self) -> Bytes {
        Bytes::new()
    }

    fn get_input_data(&self) -> Bytes {
        Bytes::new()
    }
}

// Used by a round leader to generate system calls for the proposing block
pub fn generate_system_calls() -> Vec<SystemCall> {
    Vec::new()
}

// Used by a validator to generate expected system calls for a block
fn generate_system_calls_from_header<ST, SCT>(
    block_header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
) -> Vec<SystemCall>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    Vec::new()
}

#[derive(Debug, Clone)]
pub struct SystemTransaction(SystemTransactionInner);

#[derive(Debug, Clone)]
enum SystemTransactionInner {}

impl SystemTransaction {
    pub fn signer(&self) -> Address {
        SYSTEM_SENDER_ETH_ADDRESS
    }

    pub fn nonce(&self) -> u64 {
        // TODO use actual nonce from transaction
        0
    }

    pub fn length(&self) -> usize {
        // TODO use actual rlp length
        0
    }
}

impl From<SystemCall> for SystemTransaction {
    fn from(sys_call: SystemCall) -> Self {
        match sys_call {}
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
