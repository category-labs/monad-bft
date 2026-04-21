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

pub type KeyLenNibbles = u8;

// Table nibbles
const STATE_NIBBLE: u8 = 0;
const CODE_NIBBLE: u8 = 1;
const RECEIPT_NIBBLE: u8 = 2;
const TRANSACTION_NIBBLE: u8 = 3;
const BLOCK_HEADER_NIBBLE: u8 = 4;
const TRANSACTION_HASH_NIBBLE: u8 = 7;
const BLOCK_HASH_NIBBLE: u8 = 8;
const CALL_FRAME_NIBBLE: u8 = 9;
const DOMAIN_STATE_NIBBLE: u8 = 10;
const DOMAIN_RECEIPT_NIBBLE: u8 = 11;
const DOMAIN_TRANSACTION_NIBBLE: u8 = 12;
const DOMAIN_TX_HASH_NIBBLE: u8 = 13;
const DOMAIN_BLOCK_HEADER_NIBBLE: u8 = 14;

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
    DomainAddress(&'a [u8; 20], &'a [u8; 20]),
    Storage(&'a [u8; 20], &'a [u8; 32]),
    DomainStorage(&'a [u8; 20], &'a [u8; 20], &'a [u8; 32]),
    CodeHash(&'a [u8; 32]),
    ReceiptIndex(Option<u64>),
    DomainReceiptIndex(&'a [u8; 20], Option<u64>),
    TxIndex(Option<u64>),
    DomainTxIndex(&'a [u8; 20], Option<u64>),
    BlockHeader,
    DomainBlockHeader(&'a [u8; 20]),
    TxHash(&'a [u8; 32]),
    DomainTxHash(&'a [u8; 20], &'a [u8; 32]),
    BlockHash(&'a [u8; 32]),
    CallFrame,
}

pub fn create_triedb_key(version: Version, key: KeyInput) -> (Vec<u8>, KeyLenNibbles) {
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
            append_hashed_nibbles(&mut key_nibbles, addr);
        }
        KeyInput::DomainAddress(domain, addr) => {
            key_nibbles.push(DOMAIN_STATE_NIBBLE);
            append_domain_nibbles(&mut key_nibbles, domain);
            append_hashed_nibbles(&mut key_nibbles, addr);
        }
        KeyInput::Storage(addr, at) => {
            key_nibbles.push(STATE_NIBBLE);
            append_hashed_nibbles(&mut key_nibbles, addr);
            append_hashed_nibbles(&mut key_nibbles, at);
        }
        KeyInput::DomainStorage(domain, addr, at) => {
            key_nibbles.push(DOMAIN_STATE_NIBBLE);
            append_domain_nibbles(&mut key_nibbles, domain);
            append_hashed_nibbles(&mut key_nibbles, addr);
            append_hashed_nibbles(&mut key_nibbles, at);
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
            append_optional_index_nibbles(&mut key_nibbles, receipt_index);
        }
        KeyInput::DomainReceiptIndex(domain, receipt_index) => {
            key_nibbles.push(DOMAIN_RECEIPT_NIBBLE);
            append_domain_nibbles(&mut key_nibbles, domain);
            append_optional_index_nibbles(&mut key_nibbles, receipt_index);
        }
        KeyInput::TxIndex(tx_index) => {
            key_nibbles.push(TRANSACTION_NIBBLE);
            append_optional_index_nibbles(&mut key_nibbles, tx_index);
        }
        KeyInput::DomainTxIndex(domain, tx_index) => {
            key_nibbles.push(DOMAIN_TRANSACTION_NIBBLE);
            append_domain_nibbles(&mut key_nibbles, domain);
            append_optional_index_nibbles(&mut key_nibbles, tx_index);
        }
        KeyInput::BlockHeader => key_nibbles.push(BLOCK_HEADER_NIBBLE),
        KeyInput::DomainBlockHeader(domain) => {
            key_nibbles.push(DOMAIN_BLOCK_HEADER_NIBBLE);
            append_domain_nibbles(&mut key_nibbles, domain);
        }
        KeyInput::TxHash(tx_hash) => {
            key_nibbles.push(TRANSACTION_HASH_NIBBLE);
            append_raw_nibbles(&mut key_nibbles, tx_hash);
        }
        KeyInput::DomainTxHash(domain, tx_hash) => {
            key_nibbles.push(DOMAIN_TX_HASH_NIBBLE);
            append_domain_nibbles(&mut key_nibbles, domain);
            append_raw_nibbles(&mut key_nibbles, tx_hash);
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

    let num_nibbles: KeyLenNibbles = match key_nibbles.len().try_into() {
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

fn append_hashed_nibbles(key_nibbles: &mut Vec<u8>, input: &[u8]) {
    let hashed = keccak256(input);
    append_raw_nibbles(key_nibbles, hashed.as_slice());
}

fn append_raw_nibbles(key_nibbles: &mut Vec<u8>, input: &[u8]) {
    for byte in input {
        key_nibbles.push(byte >> 4);
        key_nibbles.push(byte & 0xF);
    }
}

fn append_optional_index_nibbles(key_nibbles: &mut Vec<u8>, index: Option<u64>) {
    if let Some(index) = index {
        let mut rlp_buf = vec![];
        index.encode(&mut rlp_buf);
        append_raw_nibbles(key_nibbles, &rlp_buf);
    }
}

fn append_domain_nibbles(key_nibbles: &mut Vec<u8>, domain: &[u8; 20]) {
    debug_assert!(domain[..12].iter().all(|byte| *byte == 0));
    for byte in &domain[12..] {
        key_nibbles.push(byte >> 4);
        key_nibbles.push(byte & 0xF);
    }
}

pub fn create_range_key(tx_index: u64) -> (Vec<u8>, KeyLenNibbles) {
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

    let num_nibbles: KeyLenNibbles = match key_nibbles.len().try_into() {
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

#[cfg(test)]
mod test {
    use alloy_primitives::keccak256;
    use monad_types::{BlockId, Hash};

    use super::*;

    fn finalized_state_key(parts: &[&[u8]]) -> Vec<u8> {
        let mut key = vec![(FINALIZED_NIBBLE << 4) | STATE_NIBBLE];
        for part in parts {
            key.extend_from_slice(keccak256(part).as_slice());
        }
        key
    }

    fn finalized_domain_state_key(domain: &[u8; 20], parts: &[&[u8]]) -> Vec<u8> {
        let mut key = vec![(FINALIZED_NIBBLE << 4) | DOMAIN_STATE_NIBBLE];
        key.extend_from_slice(&domain[12..]);
        for part in parts {
            key.extend_from_slice(keccak256(part).as_slice());
        }
        key
    }

    #[test]
    fn create_triedb_key_keeps_global_state_keys_unchanged() {
        let address = [0x11; 20];
        let slot = [0x22; 32];

        let (address_key, address_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::Address(&address));
        let (storage_key, storage_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::Storage(&address, &slot));

        assert_eq!(address_key, finalized_state_key(&[&address]));
        assert_eq!(storage_key, finalized_state_key(&[&address, &slot]));
        assert_eq!(address_nibbles, 2 + 64);
        assert_eq!(storage_nibbles, 2 + 64 + 64);
    }

    #[test]
    fn create_triedb_key_separates_domains() {
        let domain_a = {
            let mut domain = [0_u8; 20];
            domain[12..].copy_from_slice(&0x114eafu64.to_be_bytes());
            domain
        };
        let domain_b = {
            let mut domain = [0_u8; 20];
            domain[12..].copy_from_slice(&0x224eafu64.to_be_bytes());
            domain
        };
        let address = [0x11; 20];
        let slot = [0x22; 32];

        let (global_address_key, _) =
            create_triedb_key(Version::Finalized, KeyInput::Address(&address));
        let (domain_address_key, domain_address_nibbles) = create_triedb_key(
            Version::Finalized,
            KeyInput::DomainAddress(&domain_a, &address),
        );
        let (other_domain_address_key, _) = create_triedb_key(
            Version::Finalized,
            KeyInput::DomainAddress(&domain_b, &address),
        );
        let (domain_storage_key, domain_storage_nibbles) = create_triedb_key(
            Version::Finalized,
            KeyInput::DomainStorage(&domain_a, &address, &slot),
        );

        assert_eq!(
            domain_address_key,
            finalized_domain_state_key(&domain_a, &[&address])
        );
        assert_eq!(
            domain_storage_key,
            finalized_domain_state_key(&domain_a, &[&address, &slot])
        );
        assert_ne!(global_address_key, domain_address_key);
        assert_ne!(domain_address_key, other_domain_address_key);
        assert_eq!(domain_address_nibbles, 2 + 16 + 64);
        assert_eq!(domain_storage_nibbles, 2 + 16 + 64 + 64);
    }

    #[test]
    fn create_triedb_key_supports_proposal_domain_storage() {
        let domain = {
            let mut domain = [0_u8; 20];
            domain[12..].copy_from_slice(&0x114eafu64.to_be_bytes());
            domain
        };
        let address = [0x11; 20];
        let slot = [0x22; 32];
        let block_id = BlockId(Hash([0x33; 32]));

        let (key, nibbles) = create_triedb_key(
            Version::Proposal(block_id),
            KeyInput::DomainStorage(&domain, &address, &slot),
        );

        assert_eq!(nibbles, 1 + 64 + 1 + 16 + 64 + 64);
        assert_eq!(key.len(), usize::from(nibbles).div_ceil(2));
    }

    #[test]
    fn create_private_domain_transaction_keys() {
        let mut domain = [0_u8; 20];
        domain[12..].copy_from_slice(&0xad4eafu64.to_be_bytes());
        let tx_hash = [0x44; 32];

        let (_, tx_nibbles) = create_triedb_key(
            Version::Finalized,
            KeyInput::DomainTxIndex(&domain, Some(3)),
        );
        let (_, receipt_nibbles) = create_triedb_key(
            Version::Finalized,
            KeyInput::DomainReceiptIndex(&domain, Some(3)),
        );
        let (_, hash_nibbles) = create_triedb_key(
            Version::Finalized,
            KeyInput::DomainTxHash(&domain, &tx_hash),
        );

        assert_eq!(tx_nibbles, 2 + 16 + 2);
        assert_eq!(receipt_nibbles, 2 + 16 + 2);
        assert_eq!(hash_nibbles, 2 + 16 + 64);
    }
}
