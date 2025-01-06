#![allow(unused_imports)]

use std::{collections::HashMap, ops::Deref, path::Path, pin::pin};

use alloy_consensus::{Header, Transaction as _};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{bytes::BytesMut, Address, Bytes, PrimitiveSignature, B256, U256, U64};
use alloy_rlp::Encodable;
use autocxx::{block, moveit::moveit, WithinBox};
use futures::pin_mut;
use reth_primitives::{Transaction, TransactionSigned};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_string, Value};

autocxx::include_cpp! {
    #include "eth_call.hpp"
    #include "test_db.hpp"
    safety!(unsafe)
    generate!("monad_evmc_result")
    generate!("monad_state_override_set")
    generate!("eth_call")
    generate!("make_testdb")
    generate!("testdb_load_callenv")
    generate!("testdb_load_callcontract")
    generate!("testdb_load_transfer")
    generate!("testdb_path")
    generate!("destroy_testdb")
}

pub const EVMC_SUCCESS: i32 = 0;

pub enum CallResult {
    Success(SuccessCallResult),
    Failure(FailureCallResult),
}

pub struct SuccessCallResult {
    pub gas_used: u64,
    pub gas_refund: u64,
    pub output_data: Vec<u8>,
}

pub struct FailureCallResult {
    pub message: String,
    pub data: Option<String>,
}

// ensure that only one of {State, StateDiff} can be set
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum StorageOverride {
    State(HashMap<B256, B256>),
    StateDiff(HashMap<B256, B256>),
}

#[derive(Debug, Default, Deserialize, Serialize)]
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

pub type StateOverrideSet = HashMap<Address, StateOverrideObject>;

pub fn eth_call(
    transaction: Transaction,
    block_header: Header,
    sender: Address,
    block_number: u64,
    triedb_path: &Path,
    state_override_set: &StateOverrideSet,
) -> CallResult {
    // upper bound gas limit of transaction to block gas limit to prevent abuse of eth_call
    if transaction.gas_limit() > block_header.gas_limit {
        return CallResult::Failure(FailureCallResult {
            message: "gas limit too high".into(),
            data: None,
        });
    }

    // TODO: move the buffer copying into C++ for the reserve/push idiom
    let default_signature = PrimitiveSignature::new(U256::from(0), U256::from(0), false);
    let tx = TransactionSigned::new_unhashed(transaction, default_signature);
    let mut rlp_encoded_tx = Vec::new();
    tx.encode_2718(&mut rlp_encoded_tx);

    let mut cxx_rlp_encoded_tx: cxx::UniquePtr<cxx::CxxVector<u8>> = cxx::CxxVector::new();
    for byte in &rlp_encoded_tx {
        cxx_rlp_encoded_tx.pin_mut().push(*byte);
    }

    let mut rlp_encoded_block_header = vec![];
    block_header.encode(&mut rlp_encoded_block_header);
    let mut cxx_rlp_encoded_block_header: cxx::UniquePtr<cxx::CxxVector<u8>> =
        cxx::CxxVector::new();
    for byte in &rlp_encoded_block_header {
        cxx_rlp_encoded_block_header.pin_mut().push(*byte);
    }

    let mut rlp_encoded_sender = vec![];
    sender.encode(&mut rlp_encoded_sender);
    let mut cxx_rlp_encoded_sender: cxx::UniquePtr<cxx::CxxVector<u8>> = cxx::CxxVector::new();
    for byte in &rlp_encoded_sender {
        cxx_rlp_encoded_sender.pin_mut().push(*byte);
    }

    cxx::let_cxx_string!(triedb_path = triedb_path.to_str().unwrap().to_string());

    moveit! {
        let mut cxx_state_override_set = ffi::monad_state_override_set::new();
    }

    for (address, state_override_object) in state_override_set {
        let mut cxx_address: cxx::UniquePtr<cxx::CxxVector<u8>> = cxx::CxxVector::new();
        for byte in address {
            cxx_address.pin_mut().push(*byte);
        }
        cxx_state_override_set
            .as_mut()
            .add_override_address(&cxx_address);

        if let Some(balance) = &state_override_object.balance {
            let mut cxx_balance = cxx::CxxVector::new();

            // Big Endianess is to match with decode in eth_call.cpp (intx::be::load)
            for byte in balance.to_be_bytes_vec() {
                cxx_balance.pin_mut().push(byte);
            }

            cxx_state_override_set
                .as_mut()
                .set_override_balance(&cxx_address, &cxx_balance);
        }

        if let Some(nonce) = state_override_object.nonce {
            cxx_state_override_set
                .as_mut()
                .set_override_nonce(&cxx_address, &nonce.as_limbs()[0]);
        }

        if let Some(code) = &state_override_object.code {
            let mut cxx_code = cxx::CxxVector::new();

            for byte in code {
                cxx_code.pin_mut().push(*byte);
            }

            cxx_state_override_set
                .as_mut()
                .set_override_code(&cxx_address, &cxx_code);
        }

        if let Some(StorageOverride::State(override_state)) =
            &state_override_object.storage_override
        {
            for (key, value) in override_state {
                let mut cxx_key: cxx::UniquePtr<cxx::CxxVector<u8>> = cxx::CxxVector::new();
                let mut cxx_value: cxx::UniquePtr<cxx::CxxVector<u8>> = cxx::CxxVector::new();

                for byte in key {
                    cxx_key.pin_mut().push(*byte);
                }

                for byte in value {
                    cxx_value.pin_mut().push(*byte);
                }

                cxx_state_override_set.as_mut().set_override_state(
                    &cxx_address,
                    &cxx_key,
                    &cxx_value,
                );
            }
        } else if let Some(StorageOverride::StateDiff(override_state_diff)) =
            &state_override_object.storage_override
        {
            for (key, value) in override_state_diff {
                let mut cxx_key: cxx::UniquePtr<cxx::CxxVector<u8>> = cxx::CxxVector::new();
                let mut cxx_value: cxx::UniquePtr<cxx::CxxVector<u8>> = cxx::CxxVector::new();

                for byte in key {
                    cxx_key.pin_mut().push(*byte);
                }

                for byte in value {
                    cxx_value.pin_mut().push(*byte);
                }

                cxx_state_override_set.as_mut().set_override_state_diff(
                    &cxx_address,
                    &cxx_key,
                    &cxx_value,
                );
            }
        }
    }

    moveit! {
        let result = ffi::eth_call(
        &cxx_rlp_encoded_tx,
        &cxx_rlp_encoded_block_header,
        &cxx_rlp_encoded_sender,
        block_number,
        &triedb_path,
        &cxx_state_override_set);
    }

    let status_code = result.deref().get_status_code().0 as i32;
    let output_data = result.deref().get_output_data().as_slice().to_vec();
    let message = result.deref().get_message().to_string();
    let gas_used = result.deref().get_gas_used() as u64;
    let gas_refund = result.deref().get_gas_refund() as u64;

    match status_code {
        EVMC_SUCCESS => CallResult::Success(SuccessCallResult {
            gas_used,
            gas_refund,
            output_data,
        }),
        _ => {
            // if transaction fails, decode whether it's due to an invalid transaction
            // or due to a smart contract reversion
            if !message.is_empty() {
                // invalid transaction
                CallResult::Failure(FailureCallResult {
                    message,
                    data: None,
                })
            } else {
                // smart contract reversion
                let message = String::from("execution reverted");
                let error_message = decode_revert_message(&output_data);
                CallResult::Failure(FailureCallResult {
                    message: message + &error_message,
                    data: Some(format!("0x{}", hex::encode(&output_data))),
                })
            }
        }
    }
}

pub fn decode_revert_message(output_data: &[u8]) -> String {
    // https://docs.soliditylang.org/en/latest/control-structures.html#revert
    // https://github.com/ethereum/execution-apis/blob/main/tests/eth_call/call-revert-abi-error.io
    // if there is an error message to be decoded, output_data will be the following form:
    // 4 bytes function signature
    // 32 bytes data offset
    // 32 bytes error message length (let's call it x)
    // x bytes error message (padded to multiple of 32 bytes)
    let message_start_index = 68_usize;
    if output_data.len() > message_start_index {
        // we only return the first 256 bytes of the error message
        let message_length = output_data[message_start_index - 1] as usize;
        let message_end_index = message_start_index + message_length;
        if output_data.len() >= message_end_index {
            // extract the message bytes
            let message_bytes = &output_data[message_start_index..message_end_index];

            // attempt to decode the message bytes as UTF-8
            let message = match String::from_utf8(message_bytes.to_vec()) {
                Ok(message) => String::from(": ") + &message,
                Err(_) => String::new(),
            };
            return message;
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use alloy_consensus::TxLegacy;
    use alloy_primitives::{Bytes, TxKind};
    use alloy_rlp::Encodable;
    use hex::FromHex;
    use hex_literal::hex;

    use super::*;
    use crate::eth_call;

    #[derive(Deserialize)]
    struct TestStateOverrideSetParam {
        state_override_set: StateOverrideSet,
    }

    #[cfg(triedb)]
    #[test]
    fn test_callenv() {
        let db = ffi::make_testdb();
        let path = unsafe {
            ffi::testdb_load_callenv(db);
            let testdb_path = ffi::testdb_path(db).to_string();
            Path::new(&testdb_path).to_owned()
        };
        let result = eth_call(
            reth_primitives::transaction::Transaction::Legacy(reth_primitives::TxLegacy {
                chain_id: Some(41454),
                nonce: 0,
                gas_price: 0,
                gas_limit: 1000000000,
                to: reth_primitives::TransactionKind::Call(
                    hex!("9344b07175800259691961298ca11c824e65032d").into(),
                ),
                value: Default::default(),
                input: Default::default(),
            }),
            reth_primitives::Header {
                number: 1,
                beneficiary: hex!("0102030405010203040501020304050102030405").into(),
                gas_limit: 10000000000,
                ..Default::default()
            },
            hex!("0000000000000000000000000000000000000000").into(),
            0,
            path.as_path(),
            &StateOverrideSet::new(),
        );
        unsafe {
            ffi::destroy_testdb(db);
        };

        match result {
            CallResult::Failure(msg) => {
                panic!("Call failed: {}", msg.message);
            }
            CallResult::Success(res) => {
                assert_eq!(hex::encode(res.output_data), "0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000a1ee00000000000000000000000001020304050102030405010203040501020304050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
            }
        }
    }

    #[cfg(triedb)]
    #[test]
    fn test_transfer() {
        let db = ffi::make_testdb();
        let path = unsafe {
            ffi::testdb_load_transfer(db);
            let testdb_path = ffi::testdb_path(db).to_string();
            Path::new(&testdb_path).to_owned()
        };

        let txn: reth_primitives::transaction::Transaction =
            reth_primitives::Transaction::Legacy(reth_primitives::TxLegacy {
                chain_id: Some(41454),
                nonce: 0,
                gas_price: 0,
                gas_limit: 30000,
                to: reth_primitives::TransactionKind::Call(
                    hex!("0000000000000000000002000000000000000000").into(),
                ),
                value: TxValue::from(10000),
                input: Default::default(),
            });

        let header: reth_primitives::Header = reth_primitives::Header {
            number: 1,
            beneficiary: hex!("0102030405010203040501020304050102030405").into(),
            gas_limit: 100000,
            ..Default::default()
        };

        let sender: Address = hex!("0000000000000000000001000000000000000000").into();
        let block_number = 0;
        let triedb_path: &Path = path.as_path();

        // without override, passing
        {
            let state_overrides: StateOverrideSet = StateOverrideSet::new();
            let result = eth_call(
                txn.clone(),
                header.clone(),
                sender,
                block_number,
                triedb_path,
                &state_overrides,
            );

            match result {
                CallResult::Failure(msg) => {
                    panic!("Call failed: {}", msg.message);
                }
                CallResult::Success(res) => {
                    assert_eq!(hex::encode(res.output_data), "");
                    assert_eq!(res.gas_used, 21000)
                }
            }
        }

        // with balance override, failing
        {
            let state_overrides_string =
                "{\"state_override_set\": {\"0x0000000000000000000001000000000000000000\" : {
                \"balance\" : \"0x100\"
            } } }";

            let state_overrides_object: TestStateOverrideSetParam =
                match serde_json::from_str(&state_overrides_string) {
                    Ok(s) => s,
                    Err(e) => {
                        panic!("Can't parse string into json object!");
                    }
                };

            let result = eth_call(
                txn,
                header,
                sender,
                block_number,
                triedb_path,
                &state_overrides_object.state_override_set,
            );

            match result {
                CallResult::Failure(msg) => {
                    assert_eq!("insufficient balance", msg.message);
                }
                CallResult::Success(_res) => {
                    panic!("Expected Failure due to insufficient balance");
                }
            }
        }

        unsafe {
            ffi::destroy_testdb(db);
        };
    }

    #[cfg(triedb)]
    #[test]
    fn test_callcontract() {
        let db = ffi::make_testdb();
        let path = unsafe {
            ffi::testdb_load_callcontract(db);
            let testdb_path = ffi::testdb_path(db).to_string();
            Path::new(&testdb_path).to_owned()
        };

        let mut txn: reth_primitives::transaction::Transaction =
            reth_primitives::Transaction::Legacy(reth_primitives::TxLegacy {
                chain_id: Some(41454),
                nonce: 0,
                gas_price: 0,
                gas_limit: 1000000000,
                to: reth_primitives::TransactionKind::Call(
                    hex!("17e7eedce4ac02ef114a7ed9fe6e2f33feba1667").into(),
                ),
                value: Default::default(),
                input: hex!("ff01").into(),
            });

        let header: reth_primitives::Header = reth_primitives::Header {
            number: 0,
            beneficiary: hex!("0102030405010203040501020304050102030405").into(),
            gas_limit: 10000000000,
            ..Default::default()
        };

        let sender: Address = hex!("0000000000000000000000000000000000000000").into();
        let block_number = 0;
        let triedb_path: &Path = path.as_path();

        {
            let result = eth_call(
                txn.clone(),
                header.clone(),
                sender,
                block_number,
                triedb_path,
                &StateOverrideSet::new(),
            );
            match result {
                CallResult::Failure(msg) => {
                    panic!("Call failed: {}", msg.message);
                }
                CallResult::Success(res) => {
                    assert_eq!(hex::encode(res.output_data), "ffee")
                }
            }
        }

        // Code override: this should produce the same result as the above call
        {
            if let reth_primitives::Transaction::Legacy(ref mut legacy_tx) = txn {
                legacy_tx.to = reth_primitives::TransactionKind::Call(
                    hex!("000000000000000000000000000000000000000a").into(),
                );
            }

            let state_overrides_string = "{\"state_override_set\" : {\"0x000000000000000000000000000000000000000a\" : {
                \"code\" : \"0x366002146022577177726f6e672d63616c6c6461746173697a656000526012600efd5b60003560f01c61ff01146047576d77726f6e672d63616c6c64617461600052600e6012fd5b61ffee6000526002601ef3\"
            } } }";

            let state_overrides_object: TestStateOverrideSetParam =
                match serde_json::from_str(&state_overrides_string) {
                    Ok(s) => s,
                    Err(e) => {
                        panic!("Can't parse string into json object!");
                    }
                };

            let result = eth_call(
                txn,
                header.clone(),
                sender,
                block_number,
                triedb_path,
                &state_overrides_object.state_override_set,
            );
            match result {
                CallResult::Failure(msg) => {
                    panic!("Call failed: {}", msg.message);
                }
                CallResult::Success(res) => {
                    assert_eq!(hex::encode(res.output_data), "ffee")
                }
            }
        }

        unsafe {
            ffi::destroy_testdb(db);
        };
    }

    #[ignore]
    #[test]
    fn test_sha256_precompile() {
        let result = eth_call(
            reth_primitives::transaction::Transaction::Legacy(TxLegacy {
                chain_id: Some(1337),
                nonce: 0,
                gas_price: 0,
                gas_limit: 100000,
                to: TxKind::Call(hex!("0000000000000000000000000000000000000002").into()),
                value: Default::default(),
                input: hex!("deadbeef").into(),
            }),
            reth_primitives::Header::default(),
            hex!("95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5").into(),
            0,
            Path::new("/home/rgarc/test.db"),
            &StateOverrideSet::new(), // state overrides
        );

        match result {
            CallResult::Failure(res) => {
                panic!("Call failed: {}", res.message);
            }
            CallResult::Success(res) => {
                assert_eq!(
                    hex::encode(res.output_data),
                    "5f78c33274e43fa9de5659265c1d917e25c03722dcb0b8d27db8d5feaa813953"
                )
            }
        }
    }
}
