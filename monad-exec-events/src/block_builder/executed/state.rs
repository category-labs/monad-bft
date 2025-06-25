use crate::{
    ffi::{
        monad_c_address, monad_c_bytes32, monad_c_eth_txn_header, monad_exec_block_start,
        monad_exec_txn_evm_output,
    },
    ExecutedTxnCallFrame, ExecutedTxnLog,
};

#[derive(Debug)]
pub(super) struct BlockReassemblyState {
    pub start: monad_exec_block_start,
    pub txns: Box<[Option<TxnReassemblyState>]>,
}

#[derive(Debug)]
pub(super) struct TxnReassemblyState {
    pub hash: monad_c_bytes32,
    pub sender: monad_c_address,
    pub header: monad_c_eth_txn_header,
    pub input: Box<[u8]>,
    pub logs: Vec<ExecutedTxnLog>,
    pub output: Option<monad_exec_txn_evm_output>,
    pub call_frames: Option<Vec<ExecutedTxnCallFrame>>,
}
