use std::fmt::Debug;

use alloy_consensus::{
    constants::{EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
    Header, TxEnvelope, EMPTY_OMMER_ROOT_HASH,
};
use alloy_primitives::{Address, B256};
use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use monad_types::{
    ExecutionProtocol, FinalizedHeader, MockableFinalizedHeader, MockableProposedHeader, SeqNum,
};

pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;
pub const BASE_FEE_PER_GAS: u64 = 50_000_000_000;

pub type Nonce = u64;
pub type Balance = u128;

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

impl MockableProposedHeader for ProposedEthHeader {
    fn create(
        seq_num: SeqNum,
        timestamp_ns: u128,
        mix_hash: [u8; 32],
        proposal_gas_limit: u64,
    ) -> Self {
        Self {
            transactions_root: *EMPTY_TRANSACTIONS,
            ommers_hash: *EMPTY_OMMER_ROOT_HASH,
            withdrawals_root: *EMPTY_WITHDRAWALS,
            beneficiary: Default::default(),
            difficulty: 0,
            number: seq_num.0,
            gas_limit: proposal_gas_limit,
            timestamp: (timestamp_ns / 1_000_000_000) as u64,
            mix_hash,
            nonce: [0_u8; 8],
            extra_data: [0_u8; 32],
            base_fee_per_gas: BASE_FEE_PER_GAS,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct EthHeader(pub Header);

impl FinalizedHeader for EthHeader {
    fn seq_num(&self) -> SeqNum {
        SeqNum(self.0.number)
    }
}

impl MockableFinalizedHeader for EthHeader {
    fn from_seq_num(seq_num: SeqNum) -> Self {
        Self(Header {
            number: seq_num.0,
            ..Header::default()
        })
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
