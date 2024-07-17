use std::{
    ffi::CString,
    path::Path,
    ptr::{null, null_mut},
};

use alloy_primitives::{keccak256, B256};
use alloy_rlp::Decodable;
use key::create_addr_key;
use monad_eth_reserve_balance::state_backend::StateBackend;
use monad_eth_types::{EthAccount, EthAddress};
use tracing::{debug, error};

pub mod key;

mod bindings {
    include!(concat!(env!("OUT_DIR"), "/triedb.rs"));
}

const STATE_NIBBLE: u8 = 0x0;

// TODO: rename to TriedbHandle
#[derive(Clone, Debug)]
pub struct Handle {
    db_ptr: *mut bindings::triedb,
}

impl Handle {
    pub fn try_new(dbdir_path: &Path) -> Option<Self> {
        let path = CString::new(dbdir_path.to_str().expect("invalid path"))
            .expect("failed to create CString");

        let mut db_ptr = null_mut();

        let result = unsafe { bindings::triedb_open(path.as_c_str().as_ptr(), &mut db_ptr) };

        if result != 0 {
            debug!("triedb try_new error result: {}", result);
            return None;
        }

        Some(Self { db_ptr })
    }

    fn create_addr_key(addr: &EthAddress) -> (Vec<u8>, u8) {
        let mut key_nibbles: Vec<u8> = vec![];

        let state_nibble = 0_u8;

        key_nibbles.push(state_nibble);

        let hashed_addr = keccak256(addr);
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

    pub fn read(&self, key: &[u8], key_len_nibbles: u8, block_id: u64) -> Option<Vec<u8>> {
        let mut value_ptr = null();
        assert!(key_len_nibbles < u8::MAX - 1); // make sure doesn't overflow
        assert!((key_len_nibbles as usize + 1) / 2 <= key.len());
        let result = unsafe {
            bindings::triedb_read(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                &mut value_ptr,
                block_id,
            )
        };
        if result == -1 {
            return None;
        }

        if result == 0 {
            return Some(Vec::new());
        }

        // check that there's no unexpected error
        assert!(result > 0);

        let value_len = result.try_into().unwrap();
        let value = unsafe {
            let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
            bindings::triedb_finalize(value_ptr);
            value
        };

        Some(value)
    }

    pub fn get_state_root(&self, block_id: u64) -> Option<Vec<u8>> {
        let key: Vec<u8> = vec![STATE_NIBBLE];
        let mut value_ptr = null();
        let result = unsafe {
            bindings::triedb_read_data(self.db_ptr, key.as_ptr(), 1, &mut value_ptr, block_id)
        };
        if result == -1 {
            return None;
        }

        if result == 0 {
            return Some(Vec::new());
        }

        // check that there's no unexpected error
        assert_eq!(result, 32);

        let value_len = result.try_into().unwrap();
        let value = unsafe {
            let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
            bindings::triedb_finalize(value_ptr);
            value
        };

        Some(value)
    }

    pub fn latest_block(&self) -> u64 {
        unsafe { bindings::triedb_latest_block(self.db_ptr) }
    }

    pub fn get_account(&self, eth_address: &[u8; 20], block_id: u64) -> Option<EthAccount> {
        let (triedb_key, key_len_nibbles) = create_addr_key(eth_address);

        let result = self.read(&triedb_key, key_len_nibbles, block_id);

        let Some(account_rlp) = result else {
            debug!("account {:?} not found at {:?}", eth_address, block_id);
            return None;
        };

        let mut buf = account_rlp.as_slice();
        let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
            error!("rlp decode failed: {:?}", buf);
            return None;
        };

        // account incarnation decode (currently not needed)
        let Ok(_) = u64::decode(&mut buf) else {
            error!("rlp incarnation decode failed: {:?}", buf);
            return None;
        };

        let Ok(nonce) = u64::decode(&mut buf) else {
            error!("rlp nonce decode failed: {:?}", buf);

            return None;
        };
        let Ok(balance) = u128::decode(&mut buf) else {
            error!("rlp balance decode failed: {:?}", buf);
            return None;
        };

        let code_hash = if buf.is_empty() {
            None
        } else {
            match <[u8; 32]>::decode(&mut buf) {
                Ok(x) => Some(x),
                Err(e) => {
                    error!("rlp code_hash decode failed: {:?}", e);
                    return None;
                }
            }
        };

        Some(EthAccount {
            nonce,
            balance,
            code_hash: code_hash.map(B256::from),
        })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let result = unsafe { bindings::triedb_close(self.db_ptr) };
        assert_eq!(result, 0);
    }
}

impl StateBackend for Handle {
    fn get_account(&self, block: u64, eth_address: &EthAddress) -> Option<EthAccount> {
        self.get_account(eth_address.as_ref(), block)
    }

    fn is_available(&self, block: u64) -> bool {
        self.latest_block() >= block
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::Handle;

    #[test]
    fn read() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // this key is hardcoded into mock triedb
        let result = handle.read(&[1, 2, 3], 6, 0);
        assert_eq!(result, Some(vec![4, 5, 6]));

        // this key is hardcoded into mock triedb
        let result = handle.read(&[7, 8, 9], 6, 0);
        assert_eq!(result, Some(vec![10, 11, 12]));

        let result = handle.read(&[0], 2, 0);
        assert_eq!(result, None);
    }

    #[test]
    #[should_panic]
    fn read_invalid() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // too many nibbles
        let _ = handle.read(&[1, 2, 3], 7, 0);
    }

    #[test]
    fn read_latest_block() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // this value is hardcoded into mock triedb
        let result = handle.latest_block();
        assert_eq!(result, 20);
    }
}
