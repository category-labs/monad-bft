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

use std::{fmt::Debug, ops::Deref};

use alloy_consensus::{transaction::Recovered, Header, TxEnvelope};
use alloy_eips::eip7702::RecoveredAuthorization;
use alloy_primitives::{Address, B256};
use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use monad_crypto::NopPubKey;
use monad_secp::PubKey as SecpPubkey;
use monad_types::{Balance, ExecutionProtocol, FinalizedHeader, Nonce, SeqNum};

pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;

pub trait ExtractEthAddress {
    fn get_eth_address(&self) -> Address;
}

impl ExtractEthAddress for NopPubKey {
    fn get_eth_address(&self) -> Address {
        Address::new([0_u8; 20])
    }
}

impl ExtractEthAddress for SecpPubkey {
    fn get_eth_address(&self) -> Address {
        Address::from_raw_public_key(&Self::bytes(self)[1..])
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EthAccount {
    pub nonce: Nonce,
    pub balance: Balance,
    pub code_hash: Option<B256>,
    pub is_delegated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Default)]
pub struct ProposedEthHeader {
    pub ommers_hash: [u8; 32],
    pub beneficiary: Address,
    pub transactions_root: [u8; 32],
    pub difficulty: u64,
    pub number: u64,
    pub gas_limit: u64,
    pub timestamp: u64,
    pub extra_data: [u8; 32],
    pub mix_hash: [u8; 32],
    pub nonce: [u8; 8],
    pub base_fee_per_gas: u64,
    pub withdrawals_root: [u8; 32],
    // cancun
    pub blob_gas_used: u64,
    pub excess_blob_gas: u64,
    pub parent_beacon_block_root: [u8; 32],
    // eip-7685
    pub requests_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct EthHeader(pub Header);

impl FinalizedHeader for EthHeader {
    fn seq_num(&self) -> SeqNum {
        SeqNum(self.0.number)
    }
}

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Default)]
pub struct EthBlockBody {
    // TODO consider storing recovered txs inline here
    pub transactions: Vec<TxEnvelope>,
    pub ommers: Vec<Ommer>,
    pub withdrawals: Vec<Withdrawal>,
}

#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Ommer {}
#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Withdrawal {}

impl Debug for EthBlockBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthBlockBody")
            .field("num_txns", &format!("{}", self.transactions.len()))
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, RlpEncodable, RlpDecodable)]
pub struct EthExecutionProtocol;
impl ExecutionProtocol for EthExecutionProtocol {
    type ProposedHeader = ProposedEthHeader;
    type FinalizedHeader = EthHeader;
    type Body = EthBlockBody;
}

#[derive(Clone, Debug)]
pub struct ValidatedTx {
    pub tx: Recovered<TxEnvelope>,
    pub authorizations_7702: Vec<RecoveredAuthorization>,
}

impl Deref for ValidatedTx {
    type Target = Recovered<TxEnvelope>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}
