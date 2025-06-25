//! FFI docs

pub(crate) use self::bindings::{
    g_monad_exec_event_metadata_hash, monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS,
    monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS_LIST_HEADER,
    monad_exec_event_type_MONAD_EXEC_BLOCK_END, monad_exec_event_type_MONAD_EXEC_BLOCK_FINALIZED,
    monad_exec_event_type_MONAD_EXEC_BLOCK_PERF_EVM_ENTER,
    monad_exec_event_type_MONAD_EXEC_BLOCK_PERF_EVM_EXIT,
    monad_exec_event_type_MONAD_EXEC_BLOCK_QC, monad_exec_event_type_MONAD_EXEC_BLOCK_REJECT,
    monad_exec_event_type_MONAD_EXEC_BLOCK_START, monad_exec_event_type_MONAD_EXEC_BLOCK_VERIFIED,
    monad_exec_event_type_MONAD_EXEC_EVM_ERROR, monad_exec_event_type_MONAD_EXEC_NONE,
    monad_exec_event_type_MONAD_EXEC_STORAGE_ACCESS,
    monad_exec_event_type_MONAD_EXEC_TXN_CALL_FRAME, monad_exec_event_type_MONAD_EXEC_TXN_END,
    monad_exec_event_type_MONAD_EXEC_TXN_EVM_OUTPUT, monad_exec_event_type_MONAD_EXEC_TXN_LOG,
    monad_exec_event_type_MONAD_EXEC_TXN_PERF_EVM_ENTER,
    monad_exec_event_type_MONAD_EXEC_TXN_PERF_EVM_EXIT,
    monad_exec_event_type_MONAD_EXEC_TXN_REJECT, monad_exec_event_type_MONAD_EXEC_TXN_START,
    monad_exec_flow_type_MONAD_FLOW_ACCOUNT_INDEX, monad_exec_flow_type_MONAD_FLOW_BLOCK_SEQNO,
    monad_exec_flow_type_MONAD_FLOW_TXN_ID,
};
pub use self::bindings::{
    monad_c_address, monad_c_bytes32, monad_c_eth_txn_header, monad_c_eth_txn_receipt,
    monad_c_transaction_type_MONAD_TXN_EIP1559, monad_c_transaction_type_MONAD_TXN_EIP2930,
    monad_c_transaction_type_MONAD_TXN_LEGACY, monad_exec_account_access,
    monad_exec_account_access_list_header, monad_exec_block_finalized, monad_exec_block_header,
    monad_exec_block_qc, monad_exec_block_reject, monad_exec_block_result,
    monad_exec_block_verified, monad_exec_evm_error, monad_exec_storage_access,
    monad_exec_txn_call_frame, monad_exec_txn_evm_output, monad_exec_txn_log,
    monad_exec_txn_reject, monad_exec_txn_start,
};

#[allow(missing_docs)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
