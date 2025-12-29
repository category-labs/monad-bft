// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use alloy_primitives::keccak256;
use alloy_rlp::Encodable;
use monad_types::BlockId;
use tracing::warn;

// Table nibbles
const STATE_NIBBLE: u8 = 0;
const CODE_NIBBLE: u8 = 1;
const RECEIPT_NIBBLE: u8 = 2;
const TRANSACTION_NIBBLE: u8 = 3;
const BLOCK_HEADER_NIBBLE: u8 = 4;
const TRANSACTION_HASH_NIBBLE: u8 = 7;
const BLOCK_HASH_NIBBLE: u8 = 8;
const CALL_FRAME_NIBBLE: u8 = 9;

// table_key = concat(proposal nibble, little_endian(round - 8 bytes), table_nibble)
const PROPOSAL_NIBBLE: u8 = 0;
// table_key = concat(finalized nibble, table_nibble)
const FINALIZED_NIBBLE: u8 = 1;

#[derive(Debug, Clone, Copy)]
pub enum Version {
    Proposal(BlockId),
    Finalized,
}

pub enum KeyInput<'a> {
    State,
    Address(&'a [u8; 20]),
    Storage(&'a [u8; 20], &'a [u8; 32]),
    CodeHash(&'a [u8; 32]),
    ReceiptIndex(Option<u64>),
    TxIndex(Option<u64>),
    BlockHeader,
    TxHash(&'a [u8; 32]),
    BlockHash(&'a [u8; 32]),
    CallFrame,
}

pub fn create_triedb_key(version: Version, key: KeyInput) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];

    match version {
        Version::Proposal(block_id) => {
            key_nibbles.push(PROPOSAL_NIBBLE);
            for byte in block_id.0 .0 {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        Version::Finalized => {
            key_nibbles.push(FINALIZED_NIBBLE);
        }
    };

    match key {
        KeyInput::State => key_nibbles.push(STATE_NIBBLE),
        KeyInput::Address(addr) => {
            key_nibbles.push(STATE_NIBBLE);
            let hashed_addr = keccak256(addr);
            for byte in hashed_addr {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::Storage(addr, at) => {
            key_nibbles.push(STATE_NIBBLE);
            let hashed_addr = keccak256(addr);
            for byte in hashed_addr {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }

            let hashed_at = keccak256(at);
            for byte in hashed_at {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::CodeHash(code_hash) => {
            key_nibbles.push(CODE_NIBBLE);
            for byte in code_hash {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::ReceiptIndex(receipt_index) => {
            key_nibbles.push(RECEIPT_NIBBLE);

            if let Some(index) = receipt_index {
                let mut rlp_buf = vec![];
                index.encode(&mut rlp_buf);

                for byte in rlp_buf {
                    key_nibbles.push(byte >> 4);
                    key_nibbles.push(byte & 0xF);
                }
            }
        }
        KeyInput::TxIndex(tx_index) => {
            key_nibbles.push(TRANSACTION_NIBBLE);

            if let Some(index) = tx_index {
                let mut rlp_buf = vec![];
                index.encode(&mut rlp_buf);

                for byte in rlp_buf {
                    key_nibbles.push(byte >> 4);
                    key_nibbles.push(byte & 0xF);
                }
            }
        }
        KeyInput::BlockHeader => key_nibbles.push(BLOCK_HEADER_NIBBLE),
        KeyInput::TxHash(tx_hash) => {
            key_nibbles.push(TRANSACTION_HASH_NIBBLE);

            for byte in tx_hash {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::BlockHash(block_hash) => {
            key_nibbles.push(BLOCK_HASH_NIBBLE);

            for byte in block_hash {
                key_nibbles.push(byte >> 4);
                key_nibbles.push(byte & 0xF);
            }
        }
        KeyInput::CallFrame => key_nibbles.push(CALL_FRAME_NIBBLE),
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if !num_nibbles.is_multiple_of(2) {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}

pub fn create_range_key(tx_index: u64) -> (Vec<u8>, u8) {
    let mut key_nibbles: Vec<u8> = vec![];
    // call frame key takes tx index as 4 bytes
    // downcast index to u32
    let index: u32 = match tx_index.try_into() {
        Ok(value) => value,
        Err(_) => {
            warn!("Tx index too large, returning an empty key");
            return (vec![], 0);
        }
    };
    let bytes = index.to_be_bytes();
    for byte in bytes {
        key_nibbles.push(byte >> 4);
        key_nibbles.push(byte & 0xF)
    }

    let num_nibbles: u8 = match key_nibbles.len().try_into() {
        Ok(len) => len,
        Err(_) => {
            warn!("Key too big, returning an empty key");
            return (vec![], 0);
        }
    };

    if !num_nibbles.is_multiple_of(2) {
        key_nibbles.push(0);
    }

    let key: Vec<_> = key_nibbles
        .chunks(2)
        .map(|chunk| (chunk[0] << 4) | chunk[1])
        .collect();

    (key, num_nibbles)
}
