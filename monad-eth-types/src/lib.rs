use std::fmt::Debug;

use alloy_consensus::{Header, TxEnvelope};
use alloy_primitives::{Address, B256, U256};
use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use monad_types::{ExecutionProtocol, FinalizedHeader, SeqNum};

pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;
pub const BASE_FEE_PER_GAS: u64 = 50_000_000_000;

pub type Nonce = u64;
pub type Balance = U256;

#[derive(Debug, Copy, Clone)]
pub struct EthAccount {
    pub nonce: Nonce,
    pub balance: Balance,
    pub code_hash: Option<B256>,
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
