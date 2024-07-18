use alloy_primitives::keccak256;

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
