use crate::{core::ids::TraceId, family::Hash32};

pub type Address20 = [u8; 20];
pub type Selector4 = [u8; 4];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Trace {
    pub block_num: u64,
    pub block_hash: Hash32,
    pub tx_idx: u32,
    pub trace_idx: u32,
    pub typ: u8,
    pub flags: u64,
    pub from: Address20,
    pub to: Option<Address20>,
    pub value: Vec<u8>,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Vec<u8>,
    pub output: Vec<u8>,
    pub status: u8,
    pub depth: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceSequencingState {
    pub next_trace_id: TraceId,
}
