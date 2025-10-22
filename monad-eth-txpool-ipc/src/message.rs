use alloy_consensus::TxEnvelope;
use alloy_rlp::{RlpDecodable, RlpEncodable};

pub const RPC_PRIORITY: u64 = 0x1000;

#[derive(RlpEncodable, RlpDecodable)]
pub struct EthTxPoolIpcTx {
    pub tx: TxEnvelope,
    pub priority: u64,

    /// Used by forks to pass custom instructions to txpool
    pub extra_data: Vec<u8>,
}

impl EthTxPoolIpcTx {
    pub fn new_with_rpc_priority(tx: TxEnvelope, extra_data: Vec<u8>) -> Self {
        Self {
            tx,
            priority: RPC_PRIORITY,
            extra_data,
        }
    }
}
