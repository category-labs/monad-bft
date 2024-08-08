use std::{
    cell::RefCell,
    path::{Path, PathBuf},
};

use alloy_primitives::{keccak256, U256};
use alloy_rlp::{Decodable, Encodable};
use monad_blockdb::BlockTagKey;
use monad_blockdb_utils::BlockTags;
use monad_triedb::{key::create_addr_key, Handle};
use tracing::debug;

pub type EthAddress = [u8; 20];
pub type EthStorageKey = [u8; 32];

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

impl TriedbEnv {
    thread_local! {
        pub static DB: RefCell<Option<Handle>> = RefCell::new(None);
    }

    pub fn new(triedb_path: &Path) -> Self {
        Self {
            triedb_path: triedb_path.to_path_buf(),
        }
    }

    pub async fn get_latest_block(&self) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbEnv::new_conn(&triedb_path).expect("triedb should exist in path")
                });

                let result = TriedbEnv::latest_block(db);
                let _ = send.send(TriedbResult::BlockNum(result));
            });
        });
        recv.await.expect("rayon panic get_latest_block")
    }

    pub async fn get_account(&self, addr: EthAddress, block_tag: BlockTags) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbEnv::new_conn(&triedb_path).expect("triedb should exist in path")
                });
                let (triedb_key, key_len_nibbles) = create_addr_key(&addr);

                // parse block tag
                let block_num = match block_tag {
                    BlockTags::Number(q) => q,
                    BlockTags::Default(t) => match t {
                        BlockTagKey::Latest => TriedbEnv::latest_block(db),
                        BlockTagKey::Finalized => TriedbEnv::latest_block(db),
                    },
                };

                let result = TriedbEnv::read(db, &triedb_key, key_len_nibbles, block_num);
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

                // account incarnation decode (currently not needed)
                let Ok(_) = u64::decode(&mut buf) else {
                    debug!("rlp incarnation decode failed: {:?}", buf);
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

                let code_hash = if buf.is_empty() {
                    [0; 32]
                } else {
                    match <[u8; 32]>::decode(&mut buf) {
                        Ok(x) => x,
                        Err(e) => {
                            debug!("rlp code_hash decode failed: {:?}", e);
                            [0; 32]
                        }
                    }
                };

                let _ = send.send(TriedbResult::Account(nonce, balance, code_hash));
            });
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
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbEnv::new_conn(&triedb_path).expect("triedb should exist in path")
                });
                let (triedb_key, key_len_nibbles) = TriedbEnv::create_storage_at_key(&addr, &at);

                // parse block tag
                let block_num = match block_tag {
                    BlockTags::Number(q) => q,
                    BlockTags::Default(t) => match t {
                        BlockTagKey::Latest => TriedbEnv::latest_block(db),
                        BlockTagKey::Finalized => TriedbEnv::latest_block(db),
                    },
                };

                let result = TriedbEnv::read(db, &triedb_key, key_len_nibbles, block_num);
                let Some(result) = result else {
                    let _ = send.send(TriedbResult::Null);
                    return;
                };

                let _ = match U256::decode(&mut result.as_slice()) {
                    Ok(res) => {
                        let mut storage_value = [0_u8; 32];
                        for (byte, storage) in res
                            .to_be_bytes_vec()
                            .into_iter()
                            .zip(storage_value.iter_mut())
                        {
                            *storage = byte;
                        }
                        send.send(TriedbResult::Storage(storage_value))
                    }
                    Err(e) => {
                        debug!("rlp storage decode failed: {:?}", e);
                        send.send(TriedbResult::EncodingError)
                    }
                };
            });
        });
        recv.await.expect("rayon panic get_storage_at")
    }

    pub async fn get_code(&self, code_hash: [u8; 32], block_tag: BlockTags) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbEnv::new_conn(&triedb_path).expect("triedb should exist in path")
                });
                let (triedb_key, key_len_nibbles) = TriedbEnv::create_code_key(&code_hash);

                // parse block tag
                let block_num = match block_tag {
                    BlockTags::Number(q) => q,
                    BlockTags::Default(t) => match t {
                        BlockTagKey::Latest => TriedbEnv::latest_block(db),
                        BlockTagKey::Finalized => TriedbEnv::latest_block(db),
                    },
                };

                let result = TriedbEnv::read(db, &triedb_key, key_len_nibbles, block_num);
                let Some(result) = result else {
                    let _ = send.send(TriedbResult::Null);
                    return;
                };

                let _ = send.send(TriedbResult::Code(result));
            });
        });
        recv.await.expect("rayon panic get_code")
    }

    pub async fn get_receipt(&self, txn_index: u64, block_num: u64) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            TriedbEnv::DB.with_borrow_mut(|db_handle| {
                let db = db_handle.get_or_insert_with(|| {
                    TriedbEnv::new_conn(&triedb_path).expect("triedb should exist in path")
                });
                let (triedb_key, key_len_nibbles) = TriedbEnv::create_receipt_key(txn_index);

                let result = TriedbEnv::read(db, &triedb_key, key_len_nibbles, block_num);
                let Some(result) = result else {
                    let _ = send.send(TriedbResult::Null);
                    return;
                };

                let _ = send.send(TriedbResult::Receipt(result));
            });
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

    fn create_storage_at_key(addr: &EthAddress, at: &EthStorageKey) -> (Vec<u8>, u8) {
        let mut key_nibbles: Vec<u8> = vec![];

        let state_nibble = 0_u8;
        key_nibbles.push(state_nibble);

        let hashed_addr = keccak256(addr);
        for byte in &hashed_addr {
            key_nibbles.push(*byte >> 4);
            key_nibbles.push(*byte & 0xF);
        }

        let hashed_at = keccak256(at);
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
