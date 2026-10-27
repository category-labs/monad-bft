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
// Private domain tables. Every key starts with the 8-byte big-endian domain
// chain id. Nibbles 10 and 11 are reserved for domain state and receipts. See
// util.hpp in monad-execution for the values.
const DOMAIN_TRANSACTION_NIBBLE: u8 = 12;
const DOMAIN_TRANSACTION_HASH_NIBBLE: u8 = 13;
const DOMAIN_BLOCK_HEADER_NIBBLE: u8 = 14;
const DOMAIN_BLOCK_NUMBER_NIBBLE: u8 = 15;

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
    /// rlp(index) -> encoded transaction and sender of the domain block
    /// committed with this L1 block. Stale unless the domain's header is from
    /// this L1 block.
    DomainTxIndex(u64, Option<u64>),
    /// tx hash -> rlp([l1_block_number, index])
    DomainTxHash(u64, &'a [u8; 32]),
    /// rlp(ProposedDomainHeader) of the domain's latest block.
    DomainBlockHeader(u64),
    /// domain block number -> rlp(l1_block_number)
    DomainBlockNumber(u64, u64),
}

fn push_nibbles(key_nibbles: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        key_nibbles.push(byte >> 4);
        key_nibbles.push(byte & 0xF);
    }
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
        KeyInput::DomainTxIndex(domain, tx_index) => {
            key_nibbles.push(DOMAIN_TRANSACTION_NIBBLE);
            push_nibbles(&mut key_nibbles, &domain.to_be_bytes());
            if let Some(index) = tx_index {
                let mut rlp_buf = vec![];
                index.encode(&mut rlp_buf);
                push_nibbles(&mut key_nibbles, &rlp_buf);
            }
        }
        KeyInput::DomainTxHash(domain, tx_hash) => {
            key_nibbles.push(DOMAIN_TRANSACTION_HASH_NIBBLE);
            push_nibbles(&mut key_nibbles, &domain.to_be_bytes());
            push_nibbles(&mut key_nibbles, tx_hash);
        }
        KeyInput::DomainBlockHeader(domain) => {
            key_nibbles.push(DOMAIN_BLOCK_HEADER_NIBBLE);
            push_nibbles(&mut key_nibbles, &domain.to_be_bytes());
        }
        KeyInput::DomainBlockNumber(domain, number) => {
            key_nibbles.push(DOMAIN_BLOCK_NUMBER_NIBBLE);
            push_nibbles(&mut key_nibbles, &domain.to_be_bytes());
            push_nibbles(&mut key_nibbles, &number.to_be_bytes());
        }
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

#[cfg(test)]
mod test {
    use super::*;

    const DOMAIN: u64 = 0x0000_0051_0000_279f;

    #[test]
    fn domain_keys() {
        // finalized(1) || table || domain, packed two nibbles per byte
        assert_eq!(
            create_triedb_key(Version::Finalized, KeyInput::DomainBlockHeader(DOMAIN)),
            (
                vec![0x1e, 0x00, 0x00, 0x00, 0x51, 0x00, 0x00, 0x27, 0x9f],
                18
            )
        );
        assert_eq!(
            create_triedb_key(
                Version::Finalized,
                KeyInput::DomainBlockNumber(DOMAIN, 0x0102)
            ),
            (
                vec![
                    0x1f, 0x00, 0x00, 0x00, 0x51, 0x00, 0x00, 0x27, 0x9f, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x01, 0x02
                ],
                34
            )
        );
        // rlp(5) = 0x05; an odd nibble count is padded with a zero nibble
        assert_eq!(
            create_triedb_key(Version::Finalized, KeyInput::DomainTxIndex(DOMAIN, Some(5))),
            (
                vec![0x1c, 0x00, 0x00, 0x00, 0x51, 0x00, 0x00, 0x27, 0x9f, 0x05],
                20
            )
        );
        let tx_hash = [0xab; 32];
        let (key, nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::DomainTxHash(DOMAIN, &tx_hash));
        assert_eq!(nibbles, 2 + 16 + 64);
        assert_eq!(
            &key[..9],
            &[0x1d, 0x00, 0x00, 0x00, 0x51, 0x00, 0x00, 0x27, 0x9f]
        );
        assert_eq!(&key[9..], &tx_hash);
    }
}
