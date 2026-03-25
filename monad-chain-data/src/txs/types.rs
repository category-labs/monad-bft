use crate::{core::ids::TxId, family::Hash32};

pub type Address20 = [u8; 20];
pub type Selector4 = [u8; 4];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct IngestTx {
    pub tx_idx: u32,
    pub tx_hash: Hash32,
    pub sender: Address20,
    pub signed_tx_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxFamilyState {
    pub next_tx_id: TxId,
}
