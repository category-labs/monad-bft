use std::path::{Path, PathBuf};

use alloy_primitives::{keccak256, Address, Bloom, Bytes};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use log::debug;
use monad_blockdb::BlockTagKey;
use monad_triedb::Handle;
use reth_primitives::{B256, U64, U8};
use serde::{Deserialize, Serialize};

use crate::eth_json_types::{BlockTags, EthAddress, EthStorageKey};

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
    BlockNum(u64),
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiptDetails {
    pub status: U64,
    pub cumulative_gas_used: U64,
    pub logs_bloom: Bloom,
    pub logs: Vec<ReceiptLog>,
}

#[derive(Debug, Clone, RlpDecodable, RlpEncodable, Serialize, Deserialize)]
pub struct ReceiptLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

pub fn decode_tx_type(rlp_buf: &mut &[u8]) -> Result<U8, alloy_rlp::Error> {
    match rlp_buf.first() {
        None => Err(alloy_rlp::Error::InputTooShort),
        Some(&x) if x < 0xc0 => {
            // first byte represents transaction type
            let tx_type = match x {
                1 => 1, // EIP2930
                2 => 2, // EIP1559
                // TODO: add support for EIP4844
                _ => return Err(alloy_rlp::Error::Custom("InvalidTxnType")),
            };
            *rlp_buf = &rlp_buf[1..]; // advance the buffer
            Ok(U8::from(tx_type))
        }
        Some(_) => Ok(U8::from(0)), // legacy transactions do not have first byte as transaction type
    }
}

impl TriedbEnv {
    pub fn new(triedb_path: &Path) -> Self {
        Self {
            triedb_path: triedb_path.to_path_buf(),
        }
    }

    pub async fn get_latest_block(&self) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let result = TriedbEnv::latest_block(&db);
            let _ = send.send(TriedbResult::BlockNum(result));
        });
        recv.await.expect("rayon panic get_latest_block")
    }

    pub async fn get_account(&self, addr: EthAddress, block_tag: BlockTags) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_addr_key(&addr);

            // parse block tag
            let block_num = match block_tag {
                BlockTags::Number(q) => q.0,
                BlockTags::Default(t) => match t {
                    BlockTagKey::Latest => TriedbEnv::latest_block(&db),
                    BlockTagKey::Finalized => TriedbEnv::latest_block(&db),
                },
            };

            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles, block_num);
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

    pub async fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_tag: BlockTags,
    ) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_storage_at_key(&addr, &at);

            // parse block tag
            let block_num = match block_tag {
                BlockTags::Number(q) => q.0,
                BlockTags::Default(t) => match t {
                    BlockTagKey::Latest => TriedbEnv::latest_block(&db),
                    BlockTagKey::Finalized => TriedbEnv::latest_block(&db),
                },
            };

            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles, block_num);
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

    pub async fn get_code(&self, code_hash: [u8; 32], block_tag: BlockTags) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_code_key(&code_hash);

            // parse block tag
            let block_num = match block_tag {
                BlockTags::Number(q) => q.0,
                BlockTags::Default(t) => match t {
                    BlockTagKey::Latest => TriedbEnv::latest_block(&db),
                    BlockTagKey::Finalized => TriedbEnv::latest_block(&db),
                },
            };

            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles, block_num);
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

    fn latest_block(handle: &Handle) -> u64 {
        handle.latest_block()
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
