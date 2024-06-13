#![allow(unused_imports)]

use std::{ops::Deref, path::Path, pin::pin};

use alloy_primitives::{bytes::BytesMut, private::alloy_rlp::Encodable, Bytes};
use autocxx::{block, moveit::moveit, WithinBox};
use futures::pin_mut;

autocxx::include_cpp! {
    #include "eth_call.hpp"
    #include "test_db.hpp"
    safety!(unsafe)
    generate!("monad_evmc_result")
    generate!("eth_call")
    generate!("make_testdb")
    generate!("testdb_load_callenv")
    generate!("testdb_load_callcontract")
    generate!("testdb_path")
    generate!("destroy_testdb")
}

pub const EVMC_SUCCESS: i32 = 0;

pub enum CallResult {
    Success(SuccessCallResult),
    Failure(String),
}

pub struct SuccessCallResult {
    pub gas_used: u64,
    pub gas_refund: u64,
    pub output_data: Vec<u8>,
}

pub fn eth_call(
    transaction: reth_primitives::Transaction,
    block_header: reth_primitives::Header,
    sender: reth_primitives::Address,
    block_number: u64,
    triedb_path: &Path,
    blockdb_path: &Path,
) -> CallResult {
    // TODO: move the buffer copying into C++ for the reserve/push idiom
    let rlp_encoded_tx: Bytes = {
        let mut buf = BytesMut::new();
        transaction.encode_with_signature(&reth_primitives::Signature::default(), &mut buf, true);
        buf.freeze().into()
    };
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
    cxx::let_cxx_string!(blockdb_path = blockdb_path.to_str().unwrap().to_string());

    moveit! {
        let result = ffi::eth_call(
        &cxx_rlp_encoded_tx,
        &cxx_rlp_encoded_block_header,
        &cxx_rlp_encoded_sender,
        block_number,
        &triedb_path,
        &blockdb_path);
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
        _ => CallResult::Failure(message),
    }
}

#[cfg(test)]
mod test {
    use alloy_primitives::private::alloy_rlp::Encodable;
    use hex::FromHex;
    use hex_literal::hex;
    use monad_eth_tx::EthTransaction;
    use reth_primitives::{bytes::Bytes, hex::encode_to_slice, Address, TxValue};

    use super::*;
    use crate::eth_call;

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
                chain_id: Some(1),
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
                ..Default::default()
            },
            hex!("0000000000000000000000000000000000000000").into(),
            0,
            path.as_path(),
            Path::new(""),
        );
        unsafe {
            ffi::destroy_testdb(db);
        };

        match result {
            CallResult::Failure(msg) => {
                panic!("Call failed: {}", msg);
            }
            CallResult::Success(res) => {
                assert_eq!(hex::encode(res.output_data), "0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000100000000000000000000000001020304050102030405010203040501020304050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
            }
        }
    }

    #[cfg(triedb)]
    #[test]
    fn test_transfer() {
        let db = ffi::make_testdb();
        let path = unsafe {
            ffi::testdb_load_callenv(db);
            let testdb_path = ffi::testdb_path(db).to_string();
            Path::new(&testdb_path).to_owned()
        };
        let result = eth_call(
            reth_primitives::transaction::Transaction::Legacy(reth_primitives::TxLegacy {
                chain_id: Some(1),
                nonce: 0,
                gas_price: 0,
                gas_limit: 1000000000,
                to: reth_primitives::TransactionKind::Call(
                    hex!("9344b07175800259691961298ca11c824e65032d").into(),
                ),
                value: TxValue::from(10),
                input: Default::default(),
            }),
            reth_primitives::Header {
                number: 1,
                beneficiary: hex!("0102030405010203040501020304050102030405").into(),
                ..Default::default()
            },
            hex!("0000000000000000000000000000000000000000").into(),
            0,
            path.as_path(),
            Path::new(""),
        );
        unsafe {
            ffi::destroy_testdb(db);
        };

        match result {
            CallResult::Failure(msg) => {
                panic!("Call failed: {}", msg);
            }
            CallResult::Success(res) => {
                assert_eq!(hex::encode(res.output_data), "0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000100000000000000000000000001020304050102030405010203040501020304050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
                assert_eq!(res.gas_used, 21000)
            }
        }
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
        let result = eth_call(
            reth_primitives::transaction::Transaction::Legacy(reth_primitives::TxLegacy {
                chain_id: Some(1),
                nonce: 0,
                gas_price: 0,
                gas_limit: 1000000000,
                to: reth_primitives::TransactionKind::Call(
                    hex!("17e7eedce4ac02ef114a7ed9fe6e2f33feba1667").into(),
                ),
                value: Default::default(),
                input: hex!("ff01").into(),
            }),
            Default::default(),
            hex!("0000000000000000000000000000000000000000").into(),
            0,
            path.as_path(),
            Path::new(""),
        );
        unsafe {
            ffi::destroy_testdb(db);
        };
        match result {
            CallResult::Failure(msg) => {
                panic!("Call failed: {}", msg);
            }
            CallResult::Success(res) => {
                assert_eq!(hex::encode(res.output_data), "ffee")
            }
        }
    }

    #[ignore]
    #[test]
    fn test_sha256_precompile() {
        let temp_blockdb_file = tempfile::TempDir::with_prefix("blockdb").unwrap();
        let result = eth_call(
            reth_primitives::transaction::Transaction::Legacy(reth_primitives::TxLegacy {
                chain_id: Some(1337),
                nonce: 0,
                gas_price: 0,
                gas_limit: 100000,
                to: reth_primitives::TransactionKind::Call(
                    hex!("0000000000000000000000000000000000000002").into(),
                ),
                value: Default::default(),
                input: hex!("deadbeef").into(),
            }),
            reth_primitives::Header::default(),
            hex!("95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5").into(),
            0,
            Path::new("/home/rgarc/test.db"),
            temp_blockdb_file.path(),
        );

        match result {
            CallResult::Failure(msg) => {
                panic!("Call failed: {}", msg);
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
