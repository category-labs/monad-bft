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

use crate::{
    error::{ErrorKind, Result, ResultExt},
    model::RangeRlp,
    prelude::*,
};

struct RlpScanner<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> RlpScanner<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Decodes the RLP prefix at current position and returns (content_length, total_length)
    fn decode_length(&self) -> Result<(usize, usize)> {
        let remaining = &self.data[self.position..];
        if remaining.is_empty() {
            return Err(eyre!("unexpected end of input")).kind(ErrorKind::Parse);
        }

        let first_byte = remaining[0];
        match first_byte {
            0..=0x7f => Ok((1, 1)), // Single byte is its own encoding
            0x80..=0xb7 => {
                // Short string: first byte - 0x80 is the length
                let length = (first_byte - 0x80) as usize;
                Ok((length, length + 1)) // +1 for prefix
            }
            0xb8..=0xbf => {
                // Long string: length of length is first byte - 0xb7
                let length_of_length = (first_byte - 0xb7) as usize;
                if remaining.len() < 1 + length_of_length {
                    return Err(eyre!("input too short for string length")).kind(ErrorKind::Parse);
                }
                let length = self.decode_uint(&remaining[1..=length_of_length])?;
                Ok((length, 1 + length_of_length + length))
            }
            0xc0..=0xf7 => {
                // Short list: first byte - 0xc0 is the length
                let length = (first_byte - 0xc0) as usize;
                Ok((length, length + 1)) // +1 for prefix
            }
            0xf8..=0xff => {
                // Long list: length of length is first byte - 0xf7
                let length_of_length = (first_byte - 0xf7) as usize;
                if remaining.len() < 1 + length_of_length {
                    return Err(eyre!("input too short for list length")).kind(ErrorKind::Parse);
                }
                let length = self.decode_uint(&remaining[1..=length_of_length])?;
                Ok((length, 1 + length_of_length + length))
            }
        }
    }

    /// Helper to decode a big-endian unsigned integer
    fn decode_uint(&self, bytes: &[u8]) -> Result<usize> {
        if bytes.is_empty() {
            return Err(eyre!("empty bytes for uint")).kind(ErrorKind::Parse);
        }
        // Reject leading zeros
        if bytes[0] == 0 && bytes.len() > 1 {
            return Err(eyre!("leading zero in uint")).kind(ErrorKind::Parse);
        }

        let mut result = 0usize;
        for &byte in bytes {
            result = result
                .checked_mul(256)
                .ok_or_else(|| eyre!("uint overflow"))
                .kind(ErrorKind::Parse)?;
            result = result
                .checked_add(byte as usize)
                .ok_or_else(|| eyre!("uint overflow"))
                .kind(ErrorKind::Parse)?;
        }
        Ok(result)
    }
}

/// Returns a vector of (start, end) byte ranges for each transaction in the block
pub fn get_transaction_ranges(block_rlp: &[u8]) -> Result<Vec<(usize, usize)>> {
    let mut scanner = RlpScanner::new(block_rlp);
    let mut ranges = Vec::new();

    // Skip block prefix
    let (block_content_len, block_prefix_len) = scanner.decode_length()?;
    scanner.position += block_prefix_len - block_content_len;

    // Skip header
    let (_, header_len) = scanner.decode_length()?;
    scanner.position += header_len;

    // Get transactions list prefix
    let (txs_content_len, txs_prefix_len) = scanner.decode_length()?;
    let txs_start = scanner.position + (txs_prefix_len - txs_content_len);
    scanner.position = txs_start;

    // While we haven't consumed all transaction bytes
    let txs_end = txs_start + txs_content_len;
    while scanner.position < txs_end {
        let start = scanner.position;
        let (_, tx_len) = scanner.decode_length()?;
        scanner.position += tx_len;
        ranges.push((start, scanner.position));
    }

    Ok(ranges)
}

/// Get byte ranges for items in an RLP list
fn get_rlp_list_ranges(data: &[u8]) -> Result<Vec<RangeRlp>> {
    let mut scanner = RlpScanner::new(data);
    let mut ranges = Vec::new();

    // Get list prefix
    let (content_len, prefix_len) = scanner.decode_length()?;
    let items_start = scanner.position + (prefix_len - content_len);
    scanner.position = items_start;

    // Collect ranges for each item
    let items_end = items_start + content_len;
    while scanner.position < items_end {
        let start = scanner.position;
        let (_, item_len) = scanner.decode_length()?;
        scanner.position += item_len;
        ranges.push(RangeRlp {
            start,
            end: scanner.position,
        });
    }

    Ok(ranges)
}

/// Get offsets for all components of block data
/// WARN: extracting offsets is the same for all representations currently, but that may not always be the case
pub fn get_all_tx_offsets(
    block_rlp: &[u8],
    receipts_rlp: &[u8],
    traces_rlp: &[u8],
) -> Result<Vec<TxByteOffsets>> {
    // For block, we need to skip header first
    let mut scanner = RlpScanner::new(block_rlp);

    // Skip block prefix
    let (block_content_len, block_prefix_len) = scanner.decode_length()?;
    scanner.position += block_prefix_len - block_content_len;

    // Skip header
    let (_, header_len) = scanner.decode_length()?;
    scanner.position += header_len;

    // Get transactions list as slice
    let (_, txs_prefix_len) = scanner.decode_length()?;
    let txs_start = scanner.position;
    let txs_end = txs_start + txs_prefix_len;
    let txs_rlp = &block_rlp[txs_start..txs_end];

    // Get ranges for each component using generic function
    let tx_ranges = get_rlp_list_ranges(txs_rlp)?;
    let receipt_ranges = get_rlp_list_ranges(receipts_rlp)?;
    let trace_ranges = get_rlp_list_ranges(traces_rlp)?;

    // Adjust transaction ranges to account for offset in block
    let tx_ranges: Vec<_> = tx_ranges
        .into_iter()
        .map(|RangeRlp { start, end }| RangeRlp {
            start: start + txs_start,
            end: end + txs_start,
        })
        .collect();

    // Verify we have matching numbers of each
    if tx_ranges.len() != receipt_ranges.len() || tx_ranges.len() != trace_ranges.len() {
        dbg!(&tx_ranges, &receipt_ranges, &trace_ranges);
        info!(
            ?tx_ranges,
            ?receipt_ranges,
            ?trace_ranges,
            "Mismatched tx, receipts and traces counts"
        );
        return Err(eyre!(
            "Mismatched counts: {} transactions, {} receipts, {} traces",
            tx_ranges.len(),
            receipt_ranges.len(),
            trace_ranges.len()
        ))
        .kind(ErrorKind::Validation);
    }

    // Combine into TxByteOffsets structs
    Ok(tx_ranges
        .into_iter()
        .zip(receipt_ranges)
        .zip(trace_ranges)
        .map(|((tx, receipt), trace)| TxByteOffsets { tx, receipt, trace })
        .collect())
}

#[cfg(test)]
mod tests {
    const TEST_BLOCK: &str = "+QWh+QJloKSq1zbuA4rSAp0/EBEVZVtmIwXfHAteXRl48ZXLdyndoB3MTejex116q4W1Z7bM1BrTEkUblIp0E/ChQv1A1JNHlAAAAAAAAAAAAAAAAAAAAAAAAAAAoFnMCqDkgM2wLUKK9opNWj918d+lLBGkjaNVxgOrkHTKoEuicwIROzBpipCBq4BwlKqLnASmMdLmjctnnA+bg3xVoB0moWe9s04oyT0UXtgoMnALQPQK8l3jO4POdY7gtsCzuQEAAAAAGAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAADIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACICDC/inhBHhowCDAXYnhGeJnV+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACgD3+hDqIPFFBJ4PtjZ/MkvrTVEyZGwVMgt4IySFubwdeIAAAAAAAAAACFC6Q7dACgVugfFxvMVab/g0XmksD4bltI4BuZbK3AAWIvteNjtCGAgKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPkDNbkCegL5AnaCJ5+CO8CEdzWUAIUXv6x8AIMBDMCUGkkkHnG2C7pBHigp+n9U+AY5HFKAuQIEHnsulQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAeAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAGiB8+MZ/OHRjeMCTXrRfwpWGydCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAF0Pt6VoAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAbWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAABogfPjGfzh0Y3jAk160X8KVhsnQgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABdJ42RmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG1gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwICgP/iGy74YHj9fh81YUULb4jpB10jN4kugRMmJYR4geWOgWJt2pM4KHTnKvnxR2o1S+L98sD0w4+oVZNlCstwuOIm4tgL4s4InnwqFDBtxCACFDBtxCACCcueU0QBH3BCRZ6pSYXt0zY9OlMkOBHmAuEQJXqezAAAAAAAAAAAAAAAAGgxsJC8y4Z3r3EH4/y3CuoA4wwsAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ8M8GTdWSAAAMCAoJoGR3heQyaPmhjmgbCBEdOA/8CbY6bCgA/IUztHcR5ZoG3DW+j/zCQs/E1rzpebyDW/ssrEM6cb1S/BAWI6UZ98wA==";

    use alloy_consensus::{BlockBody, Header, SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_primitives::{B256, U256};
    use alloy_rlp::{Decodable, Encodable};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use base64::Engine;
    use eyre::Result;
    use monad_triedb_utils::triedb_env::TxEnvelopeWithSender;

    use super::*;
    use crate::prelude::Block;

    fn mock_tx(salt: u64) -> TxEnvelopeWithSender {
        let tx = TxEip1559 {
            nonce: salt,
            gas_limit: 456 + salt,
            max_fee_per_gas: 789,
            max_priority_fee_per_gas: 135,
            ..Default::default()
        };
        let signer = PrivateKeySigner::from_bytes(&B256::from(U256::from(123))).unwrap();
        let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let tx = tx.into_signed(sig);
        TxEnvelopeWithSender {
            sender: signer.address(),
            tx: TxEnvelope::from(tx),
        }
    }

    fn mock_block(number: u64, transactions: Vec<TxEnvelopeWithSender>) -> Block {
        Block {
            header: Header {
                number,
                ..Default::default()
            },
            body: BlockBody {
                transactions,
                ommers: vec![],
                withdrawals: None,
            },
        }
    }

    #[test]
    fn test_get_rlp_list_ranges() -> Result<()> {
        // Test with ["cat", "dog"] = [ 0xc8, 0x83, 'c', 'a', 't', 0x83, 'd', 'o', 'g' ]
        let data = [0xc8, 0x83, b'c', b'a', b't', 0x83, b'd', b'o', b'g'];
        let ranges = get_rlp_list_ranges(&data)?;

        assert_eq!(ranges.len(), 2);
        // First item ("cat") should be at range (1, 5)
        assert_eq!(ranges[0], RangeRlp { start: 1, end: 5 });
        // Second item ("dog") should be at range (5, 9)
        assert_eq!(ranges[1], RangeRlp { start: 5, end: 9 });

        Ok(())
    }

    #[test]
    fn test_empty_list() -> Result<()> {
        // Create block with 2 transactions
        let list = Vec::<u8>::new();

        // Get RLP encoding of the block
        let mut rlp = Vec::new();
        list.encode(&mut rlp);

        // Use get_rlp_list_ranges to get offsets
        let ranges = get_rlp_list_ranges(&rlp)?;
        assert_eq!(ranges.len(), 0);

        Ok(())
    }

    #[test]
    fn test_empty_block() -> Result<()> {
        // Create block with 2 transactions
        let block = mock_block(1, vec![]);

        // Get RLP encoding of the block
        let mut block_rlp = Vec::new();
        block.encode(&mut block_rlp);

        // Use get_transaction_ranges to get offsets
        let ranges = get_transaction_ranges(&block_rlp)?;
        assert_eq!(ranges.len(), 0);

        Ok(())
    }

    #[test]
    fn test_block_with_two_transactions_simple() -> Result<()> {
        // Create block with 2 transactions
        let tx1 = mock_tx(1);
        let tx2 = mock_tx(2);
        let block = mock_block(1, vec![tx1.clone(), tx2.clone()]);

        // Get RLP encoding of the block
        let mut block_rlp = Vec::new();
        block.encode(&mut block_rlp);

        // Get individual transaction encodings for comparison
        let mut tx1_encoded = Vec::new();
        tx1.encode(&mut tx1_encoded);
        let mut tx2_encoded = Vec::new();
        tx2.encode(&mut tx2_encoded);

        // Use get_transaction_ranges to get offsets
        let ranges = get_transaction_ranges(&block_rlp)?;
        assert_eq!(ranges.len(), 2);

        // Compare the transactions using the ranges
        assert_eq!(&block_rlp[ranges[0].0..ranges[0].1], &tx1_encoded[..]);
        assert_eq!(&block_rlp[ranges[1].0..ranges[1].1], &tx2_encoded[..]);

        Ok(())
    }

    #[test]
    fn test_get_all_tx_offsets() -> Result<()> {
        // Create test data
        let tx1 = mock_tx(1);
        let tx2 = mock_tx(2);
        let block = mock_block(1, vec![tx1.clone(), tx2.clone()]);

        // Encode block
        let mut block_rlp = Vec::new();
        block.encode(&mut block_rlp);

        // Create dummy receipts list (just use same transactions as dummy data)
        let mut receipts_rlp = Vec::new();
        vec![tx1.clone(), tx2.clone()].encode(&mut receipts_rlp);

        // Create dummy traces list (just use same transactions as dummy data)
        let mut traces_rlp = Vec::new();
        vec![tx1, tx2].encode(&mut traces_rlp);

        // Get all offsets
        let offsets = get_all_tx_offsets(&block_rlp, &receipts_rlp, &traces_rlp)?;

        assert_eq!(offsets.len(), 2);

        // Verify each component can be decoded
        for offset in &offsets {
            // Verify transaction slice is valid RLP
            let tx_slice = &block_rlp[offset.tx.start..offset.tx.end];
            assert!(TxEnvelopeWithSender::decode(&mut &*tx_slice).is_ok());

            // Verify receipt slice is valid RLP
            let receipt_slice = &receipts_rlp[offset.receipt.start..offset.receipt.end];
            assert!(!receipt_slice.is_empty());

            // Verify trace slice is valid RLP
            let trace_slice = &traces_rlp[offset.trace.start..offset.trace.end];
            assert!(!trace_slice.is_empty());
        }

        Ok(())
    }

    #[test]
    fn test_real_block() {
        let block_rlp = base64::prelude::BASE64_STANDARD.decode(TEST_BLOCK).unwrap();

        // Test both the direct transaction ranges and via get_all_tx_offsets
        let tx_ranges = get_transaction_ranges(&block_rlp).unwrap();

        // Create dummy receipts and traces (use transactions as dummy data)
        let mut receipts_rlp = Vec::new();
        let mut traces_rlp = Vec::new();
        let txs: Vec<_> = tx_ranges
            .iter()
            .map(|(start, end)| &block_rlp[*start..*end])
            .collect();
        txs.encode(&mut receipts_rlp);
        txs.encode(&mut traces_rlp);

        let offsets = get_all_tx_offsets(&block_rlp, &receipts_rlp, &traces_rlp).unwrap();

        // Verify we can decode transactions from both methods
        for ((start, end), offset) in tx_ranges.iter().zip(offsets.iter()) {
            let tx1 = TxEnvelope::decode(&mut &block_rlp[*start..*end]);
            let tx2 = TxEnvelope::decode(&mut &block_rlp[offset.tx.start..offset.tx.end]);

            assert!(tx1.is_ok());
            assert!(tx2.is_ok());
            // Could compare tx1 and tx2 if TxEnvelope implements PartialEq
        }
    }

    #[test]
    fn test_single_byte() -> Result<()> {
        // The byte '\x00' = [ 0x00 ]
        let scanner = RlpScanner::new(&[0x00]);
        let (content_len, total_len) = scanner.decode_length()?;
        assert_eq!((content_len, total_len), (1, 1));

        // The byte '\x0f' = [ 0x0f ]
        let scanner = RlpScanner::new(&[0x0f]);
        let (content_len, total_len) = scanner.decode_length()?;
        assert_eq!((content_len, total_len), (1, 1));
        Ok(())
    }

    #[test]
    fn test_short_string() -> Result<()> {
        // the string "dog" = [ 0x83, 'd', 'o', 'g' ]
        let scanner = RlpScanner::new(&[0x83, b'd', b'o', b'g']);
        let (content_len, total_len) = scanner.decode_length()?;
        assert_eq!((content_len, total_len), (3, 4));
        Ok(())
    }

    #[test]
    fn test_long_string() -> Result<()> {
        // Create a string longer than 55 bytes
        let long_str = vec![b'a'; 56];
        let mut encoded = vec![0xb8, 56];
        encoded.extend(long_str);

        let scanner = RlpScanner::new(&encoded);
        let (content_len, total_len) = scanner.decode_length()?;
        assert_eq!((content_len, total_len), (56, 58));
        Ok(())
    }

    #[test]
    fn test_nested_structure() -> Result<()> {
        // Test the set theoretical representation of three
        // [ [], [[]], [ [], [[]] ] ] = [ 0xc7, 0xc0, 0xc1, 0xc0, 0xc3, 0xc0, 0xc1, 0xc0 ]
        let data = [0xc7, 0xc0, 0xc1, 0xc0, 0xc3, 0xc0, 0xc1, 0xc0];
        let scanner = RlpScanner::new(&data);

        // First get outer list length
        let (content_len, _) = scanner.decode_length()?;
        assert_eq!(content_len, 7);
        Ok(())
    }

    #[test]
    fn test_error_cases() -> Result<()> {
        // Test empty input
        let scanner = RlpScanner::new(&[]);
        assert!(scanner.decode_length().is_err());

        // Test invalid long string length prefix
        let scanner = RlpScanner::new(&[0xb8]); // Missing length bytes
        assert!(scanner.decode_length().is_err());

        // Test uint with leading zeros
        let scanner = RlpScanner::new(&[0xb9, 0x00, 0x01]); // Leading zero in length
        assert!(scanner.decode_length().is_err());
        Ok(())
    }
}
