use alloy_primitives::keccak256;
use alloy_rlp::Encodable;
use tracing::warn;

pub fn create_addr_key(addr: &[u8; 20]) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    let state_nibble = 0_u8;

    key_nibbles.push(state_nibble);

    let hashed_addr = keccak256(addr);
    for byte in &hashed_addr {
        key_nibbles.push(*byte >> 4);
        key_nibbles.push(*byte & 0xF);
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}

pub fn create_storage_at_key(addr: &[u8; 20], at: &[u8; 32]) -> (Vec<u8>, u8) {
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

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}

pub fn create_code_key(code_hash: &[u8; 32]) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    let code_nibble = 1_u8;
    key_nibbles.push(code_nibble);

    for byte in code_hash {
        key_nibbles.push(*byte >> 4);
        key_nibbles.push(*byte & 0xF);
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}

pub fn create_receipt_key(txn_index: u64) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    let receipt_nibble = 2_u8;
    key_nibbles.push(receipt_nibble);

    let mut rlp_buf = vec![];
    txn_index.encode(&mut rlp_buf);

    for byte in rlp_buf {
        key_nibbles.push(byte >> 4);
        key_nibbles.push(byte & 0xF);
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}

// if no txn_index is provided, return the key to traverse the entire table
pub fn create_transaction_key(txn_index: Option<u64>) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    let transaction_nibble = 3_u8;
    key_nibbles.push(transaction_nibble);

    if let Some(index) = txn_index {
        let mut rlp_buf = vec![];
        index.encode(&mut rlp_buf);

        for byte in rlp_buf {
            key_nibbles.push(byte >> 4);
            key_nibbles.push(byte & 0xF);
        }
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}

pub fn create_block_header_key() -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    let block_header_nibble = 4_u8;
    key_nibbles.push(block_header_nibble);

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}

pub fn create_transaction_hash_key(tx_hash: &[u8; 32]) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    let tx_hash_nibble = 7_u8;
    key_nibbles.push(tx_hash_nibble);

    for byte in tx_hash {
        key_nibbles.push(*byte >> 4);
        key_nibbles.push(*byte & 0xF);
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if num_nibbles % 2 != 0 {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}
