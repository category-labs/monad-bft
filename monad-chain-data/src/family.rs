use crate::{
    core::header::EvmBlockHeader,
    logs::{family::LogsFamily, types::Log},
    traces::TracesFamily,
    txs::{family::TxsFamily, types::IngestTx},
};

pub type Hash32 = [u8; 32];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub block_num: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub header: EvmBlockHeader,
    pub logs: Vec<Log>,
    pub txs: Vec<IngestTx>,
    pub trace_rlp: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Families {
    pub logs: LogsFamily,
    pub txs: TxsFamily,
    pub traces: TracesFamily,
}
