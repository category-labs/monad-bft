#![allow(unused_imports)]

use std::{ops::Deref, path::Path, pin::pin};

use alloy_primitives::{bytes::BytesMut, private::alloy_rlp::Encodable, Bytes};
use autocxx::{block, moveit::moveit};
use futures::pin_mut;

use crate::bindings::{evmc_address, evmc_release_result_fn, evmc_status_code};

mod bindings {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]

    use cxx::ExternType;
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
    pub type evmc_host_context = ::std::os::raw::c_void;

    unsafe impl ExternType for evmc_result {
        type Id = cxx::type_id!("evmc_result");
        type Kind = cxx::kind::Opaque;
    }

    impl Default for evmc_address {
        fn default() -> Self {
            evmc_address { bytes: [0u8; 20] }
        }
    }

    impl Default for evmc_bytes32 {
        fn default() -> Self {
            evmc_bytes32 { bytes: [0u8; 32] }
        }
    }
}

autocxx::include_cpp! {
    #include "eth_call.hpp"
    safety!(unsafe)
    extern_cpp_type!("evmc_result", crate::bindings::evmc_result)
    generate!("monad::rpc::eth_call")
}

/// A RAII-wrapper around evmc_result, analogous to the evmc::Result C++ type.
#[derive(Debug)]
pub struct EvmcResult {
    evmc_raw_result: bindings::evmc_result,
}

impl Drop for EvmcResult {
    fn drop(&mut self) {
        if let Some(release_fn) = self.evmc_raw_result.release {
            // SAFETY: `release_fn` is a C function pointer that is set by execution to free
            // the dynamically allocated buffer containing the result of the message call
            unsafe {
                release_fn(&self.evmc_raw_result);
            }
        }
    }
}

impl EvmcResult {
    pub fn is_err(&self) -> bool {
        self.evmc_raw_result.status_code != evmc_status_code::EVMC_SUCCESS
    }

    pub fn output_data(&self) -> Vec<u8> {
        // SAFETY: the `output_data` and `output_size` in the `evmc_result` refers to a buffer that is
        // allocated in C++ to hold the result of a message call
        let raw_result = self.evmc_raw_result;
        unsafe {
            if raw_result.output_data.is_null() {
                vec![]
            } else {
                std::slice::from_raw_parts(raw_result.output_data, raw_result.output_size).to_vec()
            }
        }
    }

    pub fn status_code(&self) -> evmc_status_code {
        self.evmc_raw_result.status_code
    }
}

#[derive(Debug)]
pub struct EvmcError {
    pub status_code: evmc_status_code,
    pub message: String,
}

pub fn eth_call(
    transaction: reth_primitives::Transaction,
    block_header: reth_primitives::Header,
    sender: reth_primitives::Address,
    triedb_path: &Path,
    blockdb_path: &Path,
) -> Result<EvmcResult, EvmcError> {
    // TODO: move the buffer copying into C++ for the reserve/push idiom
    let rlp_encoded_tx: Bytes = {
        let mut buf = BytesMut::new();
        transaction.encode_without_signature(&mut buf);
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
        let result = crate::ffi::monad::rpc::eth_call(
        &cxx_rlp_encoded_tx,
        &cxx_rlp_encoded_block_header,
        &cxx_rlp_encoded_sender,
        0,
        &triedb_path,
        &blockdb_path);
    }

    let evmc_result = EvmcResult {
        evmc_raw_result: *result.deref(),
    };

    if evmc_result.is_err() {
        return Err(EvmcError {
            status_code: evmc_result.evmc_raw_result.status_code,
            message: String::from_utf8_lossy(&evmc_result.output_data()).to_string(),
        });
    }

    Ok(evmc_result)
}

#[cfg(test)]
mod test {
    use alloy_primitives::private::alloy_rlp::Encodable;
    use hex::FromHex;
    use hex_literal::hex;
    use monad_eth_tx::EthTransaction;
    use reth_primitives::{bytes::Bytes, hex::encode_to_slice, Address};

    use super::*;
    use crate::{bindings::*, eth_call, EvmcResult};

    #[test]
    fn test_basic_call() {
        let eth_call = eth_call(
            reth_primitives::transaction::Transaction::default(),
            reth_primitives::Header::default(),
            hex!("95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5").into(),
            Path::new(""),
            Path::new(""),
        )
        .unwrap();
        dbg!("{}", &eth_call);
        assert_eq!(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            hex::encode(eth_call.output_data())
        )
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
            Path::new("/home/rgarc/test.db"),
            temp_blockdb_file.path(),
        );
        dbg!(hex::encode(result.unwrap().output_data()));
    }
}
