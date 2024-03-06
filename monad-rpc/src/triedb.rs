use std::path::{Path, PathBuf};

use alloy_primitives::keccak256;
use alloy_rlp::Decodable;
use log::debug;
use monad_triedb::Handle;

use crate::eth_json_types::EthAddress;

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
}

pub enum TriedbResult {
    Null,
    RlpParseError,
    // (nonce, balance, code_hash)
    Account(u128, u128, [u8; 32]),
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
                let _ = send.send(TriedbResult::RlpParseError);
                return;
            };

            let Ok(nonce) = u128::decode(&mut buf) else {
                debug!("rlp nonce decode failed: {:?}", buf);
                let _ = send.send(TriedbResult::RlpParseError);
                return;
            };
            let Ok(balance) = u128::decode(&mut buf) else {
                debug!("rlp balance decode failed: {:?}", buf);
                let _ = send.send(TriedbResult::RlpParseError);
                return;
            };
            let Ok(code_hash) = <[u8; 32]>::decode(&mut buf) else {
                debug!("rlp code_hash decode failed: {:?}", buf);
                let _ = send.send(TriedbResult::RlpParseError);
                return;
            };

            let _ = send.send(TriedbResult::Account(nonce, balance, code_hash));
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
        let hashed_addr = keccak256(addr.0);

        let state_nibble = 0_u8;
        let mut key_nibbles: Vec<u8> = vec![];

        key_nibbles.push(state_nibble);
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
}
