use alloy_consensus::TxEnvelope;
use alloy_rlp::{RlpDecodable, RlpEncodable};

pub const RPC_PRIORITY: u64 = 0x1000;

#[derive(RlpEncodable, RlpDecodable)]
pub struct EthTxPoolIpcTx {
    pub tx: TxEnvelope,
    pub priority: u64,
}

impl EthTxPoolIpcTx {
    pub fn new_with_rpc_priority(tx: TxEnvelope) -> Self {
        Self {
            tx,
            priority: RPC_PRIORITY,
        }
    }
}
