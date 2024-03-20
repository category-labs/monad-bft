use std::path::{Path, PathBuf};

use alloy_primitives::{keccak256, Address, Bloom};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use log::debug;
use monad_triedb::Handle;
use serde::{Deserialize, Serialize};

use crate::eth_json_types::{EthAddress, EthStorageKey};

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
}

#[derive(Debug)]
pub enum TriedbResult {
    Null,
    EncodingError,
    // (nonce, balance, code_hash)
    Account(u128, u128, [u8; 32]),
    Storage([u8; 32]),
    Code(Vec<u8>),
    Receipt(Vec<u8>),
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
pub struct YpTransactionReceipt {
    pub txn_type: TransactionType,
    pub status: u64,
    pub cumulative_gas_used: u64,
    pub bloom: Bloom,
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone, RlpDecodable, RlpEncodable, Serialize, Deserialize)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<u32>,
    pub data: Vec<u8>,
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum TransactionType {
    Legacy = 0x0,
    Eip2930 = 0x1,
    Eip1559 = 0x2,
}

impl Encodable for TransactionType {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::Legacy => (),
            Self::Eip2930 | Self::Eip1559 => {
                let value = *self as u8;
                u8::encode(&value, out);
            }
        }
    }
}

impl Decodable for TransactionType {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        match buf.first() {
            None => Err(alloy_rlp::Error::InputTooShort),
            Some(x) if *x >= 0xc0 => Ok(Self::Legacy),
            Some(x) if *x == Self::Eip2930 as u8 => Ok(Self::Eip2930),
            Some(x) if *x == Self::Eip1559 as u8 => Ok(Self::Eip1559),
            Some(_) => Err(alloy_rlp::Error::Custom("InvalidTxnType")),
        }
    }
}

impl TriedbEnv {
    pub fn new(triedb_path: &Path) -> Self {
        Self {
            triedb_path: triedb_path.to_path_buf(),
        }
    }

    pub async fn get_account(&self, addr: EthAddress) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_addr_key(&addr);

            // FIXME: get the block_id
            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles, 0);
            let Some(result) = result else {
                let _ = send.send(TriedbResult::Null);
                return;
            };

            let mut buf = result.as_slice();
            let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
                debug!("rlp decode failed: {:?}", buf);
                let _ = send.send(TriedbResult::EncodingError);
                return;
            };

            let Ok(nonce) = u128::decode(&mut buf) else {
                debug!("rlp nonce decode failed: {:?}", buf);
                let _ = send.send(TriedbResult::EncodingError);
                return;
            };
            let Ok(balance) = u128::decode(&mut buf) else {
                debug!("rlp balance decode failed: {:?}", buf);
                let _ = send.send(TriedbResult::EncodingError);
                return;
            };

            let code_hash = match <[u8; 32]>::decode(&mut buf) {
                Ok(x) => x,
                Err(e) => {
                    debug!("rlp code_hash decode failed: {:?}", e);
                    [0; 32]
                }
            };

            let _ = send.send(TriedbResult::Account(nonce, balance, code_hash));
        });
        recv.await.expect("rayon panic get_account")
    }

    pub async fn get_storage_at(&self, addr: EthAddress, at: EthStorageKey) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_storage_at_key(&addr, &at);

            // FIXME: get the block_id
            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles, 0);
            let Some(result) = result else {
                let _ = send.send(TriedbResult::Null);
                return;
            };

            if result.len() > 32 {
                debug!("storage value is max 32 bytes");
                let _ = send.send(TriedbResult::EncodingError);
                return;
            }
            let mut storage_value = [0_u8; 32];
            for (byte, storage) in result.into_iter().rev().zip(storage_value.iter_mut().rev()) {
                *storage = byte;
            }
            let _ = send.send(TriedbResult::Storage(storage_value));
        });
        recv.await.expect("rayon panic get_storage_at")
    }

    pub async fn get_code(&self, code_hash: [u8; 32]) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_code_key(&code_hash);

            // FIXME: get the block_id
            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles, 0);
            let Some(result) = result else {
                let _ = send.send(TriedbResult::Null);
                return;
            };

            let _ = send.send(TriedbResult::Code(result));
        });
        recv.await.expect("rayon panic get_code")
    }

    pub async fn get_receipt(&self, txn_index: u64, block_num: u64) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_receipt_key(txn_index);

            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles, block_num);
            let Some(result) = result else {
                let _ = send.send(TriedbResult::Null);
                return;
            };

            let _ = send.send(TriedbResult::Receipt(result));
        });
        recv.await.expect("rayon panic get_receipt")
    }

    fn new_conn(triedb_path: &Path) -> Option<Handle> {
        Handle::try_new(triedb_path)
    }

    fn read(handle: &Handle, key: &[u8], key_len_nibbles: u8, block_id: u64) -> Option<Vec<u8>> {
        handle.read(key, key_len_nibbles, block_id)
    }

    fn create_addr_key(addr: &EthAddress) -> (Vec<u8>, u8) {
        let mut key_nibbles: Vec<u8> = vec![];

        let state_nibble = 0_u8;

        key_nibbles.push(state_nibble);

        let hashed_addr = keccak256(addr.0);
        for byte in &hashed_addr {
            key_nibbles.push(*byte >> 4);
            key_nibbles.push(*byte & 0xF);
        }

        let num_nibbles: u8 = key_nibbles.len().try_into().expect("key too big");
        if num_nibbles % 2 != 0 {
            key_nibbles.push(0);
        }
        let key: Vec<_> = key_nibbles
            .chunks(2)
            .map(|chunk| (chunk[0] << 4) | chunk[1])
            .collect();

        (key, num_nibbles)
    }

    fn create_storage_at_key(addr: &EthAddress, at: &EthStorageKey) -> (Vec<u8>, u8) {
        let mut key_nibbles: Vec<u8> = vec![];

        let state_nibble = 0_u8;
        key_nibbles.push(state_nibble);

        let hashed_addr = keccak256(addr.0);
        for byte in &hashed_addr {
            key_nibbles.push(*byte >> 4);
            key_nibbles.push(*byte & 0xF);
        }

        let hashed_at = keccak256(at.0);
        for byte in &hashed_at {
            key_nibbles.push(*byte >> 4);
            key_nibbles.push(*byte & 0xF);
        }

        let num_nibbles: u8 = key_nibbles.len().try_into().expect("key too big");
        if num_nibbles % 2 != 0 {
            key_nibbles.push(0);
        }
        let key: Vec<_> = key_nibbles
            .chunks(2)
            .map(|chunk| (chunk[0] << 4) | chunk[1])
            .collect();

        (key, num_nibbles)
    }

    fn create_code_key(code_hash: &[u8; 32]) -> (Vec<u8>, u8) {
        let mut key_nibbles: Vec<u8> = vec![];

        let code_nibble = 1_u8;
        key_nibbles.push(code_nibble);

        for byte in code_hash {
            key_nibbles.push(*byte >> 4);
            key_nibbles.push(*byte & 0xF);
        }

        let num_nibbles: u8 = key_nibbles.len().try_into().expect("key too big");
        if num_nibbles % 2 != 0 {
            key_nibbles.push(0);
        }
        let key: Vec<_> = key_nibbles
            .chunks(2)
            .map(|chunk| (chunk[0] << 4) | chunk[1])
            .collect();

        (key, num_nibbles)
    }

    pub fn path(&self) -> PathBuf {
        self.triedb_path.clone()
    }

    fn create_receipt_key(txn_index: u64) -> (Vec<u8>, u8) {
        let mut key_nibbles: Vec<u8> = vec![];

        let receipt_nibble = 2_u8;
        key_nibbles.push(receipt_nibble);

        let mut rlp_buf = vec![];
        txn_index.encode(&mut rlp_buf);

        for byte in rlp_buf {
            key_nibbles.push(byte >> 4);
            key_nibbles.push(byte & 0xF);
        }

        let num_nibbles: u8 = key_nibbles.len().try_into().expect("key too big");
        if num_nibbles % 2 != 0 {
            key_nibbles.push(0);
        }
        let key: Vec<_> = key_nibbles
            .chunks(2)
            .map(|chunk| (chunk[0] << 4) | chunk[1])
            .collect();

        (key, num_nibbles)
    }
}
