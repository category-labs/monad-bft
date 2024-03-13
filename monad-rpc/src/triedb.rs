use std::path::{Path, PathBuf};

use alloy_primitives::keccak256;
use alloy_rlp::Decodable;
use log::debug;
use monad_triedb::Handle;

use crate::eth_json_types::{EthAddress, EthStorageKey};

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
}

pub enum TriedbResult {
    Null,
    EncodingError,
    // (nonce, balance, code_hash)
    Account(u128, u128, [u8; 32]),
    Storage([u8; 32]),
    Code(Vec<u8>),
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

            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles);
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
            let Ok(code_hash) = <[u8; 32]>::decode(&mut buf) else {
                debug!("rlp code_hash decode failed: {:?}", buf);
                let _ = send.send(TriedbResult::EncodingError);
                return;
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

            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles);
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
        recv.await.expect("rayon panic get_account")
    }

    pub async fn get_code(&self, code_hash: [u8; 32]) -> TriedbResult {
        let triedb_path = self.triedb_path.clone();
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let db = TriedbEnv::new_conn(&triedb_path).expect("triedb should exist");
            let (triedb_key, key_len_nibbles) = TriedbEnv::create_code_key(&code_hash);

            let result = TriedbEnv::read(&db, &triedb_key, key_len_nibbles);
            let Some(result) = result else {
                let _ = send.send(TriedbResult::Null);
                return;
            };

            let _ = send.send(TriedbResult::Code(result));
        });
        recv.await.expect("rayon panic get_account")
    }

    fn new_conn(triedb_path: &Path) -> Option<Handle> {
        Handle::try_new(triedb_path)
    }

    fn read(handle: &Handle, key: &[u8], key_len_nibbles: u8) -> Option<Vec<u8>> {
        handle.read(key, key_len_nibbles)
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
}
