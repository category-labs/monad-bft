use std::{
    collections::HashMap,
    ffi::{CStr, CString},
    path::Path,
    sync::Arc,
};

use alloy_consensus::{Header, Transaction as _, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bytes, B256, U256, U64};
use alloy_rlp::Encodable;
use alloy_sol_types::decode_revert_reason;
use bindings::monad_eth_call_result;
use futures::channel::oneshot::{channel, Sender};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/ethcall.rs"));
}

#[derive(Debug)]
pub struct EthCallExecutor {
    eth_call_executor: *mut bindings::monad_eth_call_executor,
}

unsafe impl Send for EthCallExecutor {}
unsafe impl Sync for EthCallExecutor {}

impl EthCallExecutor {
    pub fn new(num_fibers: u32, triedb_path: &Path) -> Self {
        let dbpath = CString::new(triedb_path.to_str().expect("invalid path"))
            .expect("failed to create CString");

        let eth_call_executor = unsafe {
            bindings::monad_eth_call_executor_create(num_fibers, dbpath.as_c_str().as_ptr())
        };

        Self { eth_call_executor }
    }
}

impl Drop for EthCallExecutor {
    fn drop(&mut self) {
        info!("dropping eth_call_executor");
        unsafe {
            bindings::monad_eth_call_executor_destroy(self.eth_call_executor);
        }
        info!("eth_call_executor successfully destroyed");
    }
}

// ensure that only one of {State, StateDiff} can be set
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum StorageOverride {
    State(HashMap<B256, B256>),
    StateDiff(HashMap<B256, B256>),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateOverrideObject {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<U256>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<U64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<Bytes>,
    #[serde(flatten, default, skip_serializing_if = "Option::is_none")]
    pub storage_override: Option<StorageOverride>,
}

pub const ETH_CALL_SUCCESS: i32 = 0;

#[derive(Clone, Debug)]
pub enum CallResult {
    Success(SuccessCallResult),
    Failure(FailureCallResult),
}

#[derive(Clone, Debug, Default)]
pub struct SuccessCallResult {
    pub gas_used: u64,
    pub gas_refund: u64,
    pub output_data: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct FailureCallResult {
    pub message: String,
    pub data: Option<String>,
}

pub struct SenderContext {
    sender: Sender<*mut monad_eth_call_result>,
}

/// # Safety
/// This should be used only as a callback for monad_eth_call_executor_submit
///
/// This function is called when the eth_call is finished and the result is returned over the
/// channel
pub unsafe extern "C" fn eth_call_submit_callback(
    result: *mut monad_eth_call_result,
    user: *mut std::ffi::c_void,
) {
    let user = unsafe { Box::from_raw(user as *mut SenderContext) };

    let _ = user.sender.send(result);
}

pub type StateOverrideSet = HashMap<Address, StateOverrideObject>;

pub async fn eth_call(
    chain_id: u64,
    transaction: TxEnvelope,
    block_header: Header,
    sender: Address,
    block_number: u64,
    block_round: Option<u64>,
    eth_call_executor: Arc<Mutex<EthCallExecutor>>,
    state_override_set: &StateOverrideSet,
) -> CallResult {
    // upper bound gas limit of transaction to block gas limit to prevent abuse of eth_call
    if transaction.gas_limit() > block_header.gas_limit {
        return CallResult::Failure(FailureCallResult {
            message: "gas limit too high".into(),
            data: None,
        });
    }

    let mut rlp_encoded_tx = vec![];
    transaction.encode_2718(&mut rlp_encoded_tx);

    let mut rlp_encoded_block_header = vec![];
    block_header.encode(&mut rlp_encoded_block_header);

    let mut rlp_encoded_sender = vec![];
    sender.encode(&mut rlp_encoded_sender);

    let override_ctx = unsafe { bindings::monad_state_override_create() };
    for (addr, obj) in state_override_set {
        let addr: &[u8] = addr.as_slice();

        unsafe {
            bindings::add_override_address(override_ctx, addr.as_ptr(), addr.len());

            if let Some(balance) = obj.balance {
                // Big Endianess is to match with decode in eth_call.cpp (intx::be::load)
                let balance_vec = balance.to_be_bytes_vec();

                bindings::set_override_balance(
                    override_ctx,
                    addr.as_ptr(),
                    addr.len(),
                    balance_vec.as_ptr(),
                    balance_vec.len(),
                );
            }

            if let Some(nonce) = obj.nonce {
                bindings::set_override_nonce(
                    override_ctx,
                    addr.as_ptr(),
                    addr.len(),
                    nonce.as_limbs()[0],
                )
            }

            if let Some(code) = &obj.code {
                bindings::set_override_code(
                    override_ctx,
                    addr.as_ptr(),
                    addr.len(),
                    code.as_ptr(),
                    code.len(),
                )
            }

            match &obj.storage_override {
                Some(StorageOverride::State(storage_override)) => {
                    for (k, v) in storage_override {
                        bindings::set_override_state(
                            override_ctx,
                            addr.as_ptr(),
                            addr.len(),
                            k.as_ptr(),
                            k.len(),
                            v.as_ptr(),
                            v.len(),
                        )
                    }
                }
                Some(StorageOverride::StateDiff(override_state_diff)) => {
                    for (k, v) in override_state_diff {
                        bindings::set_override_state_diff(
                            override_ctx,
                            addr.as_ptr(),
                            addr.len(),
                            k.as_ptr(),
                            k.len(),
                            v.as_ptr(),
                            v.len(),
                        )
                    }
                }
                None => {}
            }
        }
    }

    let chain_id = match chain_id {
        1 => bindings::monad_chain_config_CHAIN_CONFIG_ETHEREUM_MAINNET,
        20143 => bindings::monad_chain_config_CHAIN_CONFIG_MONAD_DEVNET,
        10143 => bindings::monad_chain_config_CHAIN_CONFIG_MONAD_TESTNET,
        _ => {
            unsafe { bindings::monad_state_override_destroy(override_ctx) };

            return CallResult::Failure(FailureCallResult {
                message: "unsupported chain id".to_string(),
                data: Some(chain_id.to_string()),
            });
        }
    };

    let (send, recv) = channel();
    let sender_ctx = Box::new(SenderContext { sender: send });

    // hold lock on executor while submitting the task
    let executor_lock = eth_call_executor.lock().await;
    let eth_call_executor = executor_lock.eth_call_executor;

    unsafe {
        let sender_ctx_ptr = Box::into_raw(sender_ctx);

        bindings::monad_eth_call_executor_submit(
            eth_call_executor,
            chain_id,
            rlp_encoded_tx.as_ptr(),
            rlp_encoded_tx.len(),
            rlp_encoded_block_header.as_ptr(),
            rlp_encoded_block_header.len(),
            rlp_encoded_sender.as_ptr(),
            rlp_encoded_sender.len(),
            block_number,
            block_round.unwrap_or(u64::MAX),
            override_ctx,
            Some(eth_call_submit_callback),
            sender_ctx_ptr as *mut std::ffi::c_void,
        )
    };

    let result = match recv.await {
        Ok(r) => r,
        Err(e) => {
            unsafe { bindings::monad_state_override_destroy(override_ctx) };

            warn!("callback from eth_call_executor failed: {:?}", e);

            return CallResult::Failure(FailureCallResult {
                message: "internal eth_call error".to_string(),
                data: None,
            });
        }
    };

    unsafe {
        let status_code = (*result).status_code;

        let call_result = match status_code {
            ETH_CALL_SUCCESS => {
                let gas_used = (*result).gas_used as u64;
                let gas_refund = (*result).gas_refund as u64;

                let output_data_len = (*result).output_data_len;
                let output_data = if output_data_len != 0 {
                    std::slice::from_raw_parts((*result).output_data, output_data_len).to_vec()
                } else {
                    vec![]
                };

                CallResult::Success(SuccessCallResult {
                    gas_used,
                    gas_refund,
                    output_data,
                })
            }
            _ => {
                if (*result).message.is_null() {
                    let output_data_len = (*result).output_data_len;
                    let output_data = if output_data_len != 0 {
                        std::slice::from_raw_parts((*result).output_data, output_data_len).to_vec()
                    } else {
                        vec![]
                    };

                    let message = String::from("execution reverted");
                    let formatted_message = match decode_revert_message(&output_data) {
                        Some(error_message) => format!("{}: {}", message, error_message),
                        None => message,
                    };
                    CallResult::Failure(FailureCallResult {
                        message: formatted_message,
                        data: Some(format!("0x{}", hex::encode(&output_data))),
                    })
                } else {
                    let message = if (*result).message.is_null() {
                        String::from("no message from eth_call")
                    } else {
                        let cstr_msg = CStr::from_ptr((*result).message.cast());
                        match cstr_msg.to_str() {
                            Ok(str) => String::from(str),
                            Err(_) => {
                                String::from("execution error eth_call message invalid utf-8")
                            }
                        }
                    };

                    CallResult::Failure(FailureCallResult {
                        message,
                        data: None,
                    })
                }
            }
        };

        bindings::monad_eth_call_result_release(result);
        bindings::monad_state_override_destroy(override_ctx);

        drop(executor_lock);
        call_result
    }
}

pub fn decode_revert_message(output_data: &[u8]) -> Option<String> {
    // https://docs.soliditylang.org/en/latest/control-structures.html#revert
    decode_revert_reason(output_data).and_then(|message| {
        let parsed_message = message
            .strip_prefix("revert: ")
            .or_else(|| message.strip_prefix("panic: "))
            .unwrap_or(&message)
            .trim();
        if parsed_message.is_empty() {
            None
        } else {
            Some(parsed_message.to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use alloy_primitives::hex;

    use super::*;

    #[test]
    fn test_decode_revert_message() {
        // https://github.com/ethereum/execution-apis/blob/37c2b9e/tests/eth_call/call-revert-abi-error.io
        let data = hex::decode(
            "0x08c379a00000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000a75736572206572726f72"
        ).unwrap();
        let message = decode_revert_message(&data).unwrap();
        assert_eq!(message, String::from("user error"));

        // https://github.com/ethereum/execution-apis/blob/37c2b9e/tests/eth_call/call-revert-abi-panic.io
        let data = hex::decode(
            "0x4e487b710000000000000000000000000000000000000000000000000000000000000001",
        )
        .unwrap();
        let message = decode_revert_message(&data).unwrap();
        assert_eq!(message, String::from("assertion failed (0x01)"));
    }
}
