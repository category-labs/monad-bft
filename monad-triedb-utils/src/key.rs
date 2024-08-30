use alloy_primitives::keccak256;
use alloy_rlp::Encodable;

pub fn create_addr_key(addr: &[u8; 20]) -> (Vec<u8>, u8) {
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

pub fn create_code_key(code_hash: &[u8; 32]) -> (Vec<u8>, u8) {
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
