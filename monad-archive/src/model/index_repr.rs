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

use alloy_consensus::{ReceiptEnvelope, TxEnvelope};
use alloy_primitives::BlockHash;
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
use serde::{Deserialize, Serialize};

use crate::{
    error::{ErrorKind, OptionExt as _, Result, ResultExt, WrapErr},
    prelude::*,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct InlineV0 {
    pub tx: TxEnvelope,
    pub trace: Vec<u8>,
    pub receipt: ReceiptEnvelope,
    pub header_subset: HeaderSubsetV0,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ReferenceV0 {
    pub block_number: u64,
    pub header_subset: HeaderSubset,
    pub offsets: Option<TxByteOffsets>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
#[rlp(trailing)]
pub struct HeaderSubsetV0 {
    pub block_hash: BlockHash,
    pub block_number: u64,
    pub tx_index: u64,
    pub gas_used: u128,
    pub base_fee_per_gas: Option<u64>,
}

impl HeaderSubsetV0 {
    fn convert(self, block_timestamp: u64) -> HeaderSubset {
        HeaderSubset {
            block_timestamp,
            // copy existing fields
            block_hash: self.block_hash,
            block_number: self.block_number,
            tx_index: self.tx_index,
            gas_used: self.gas_used,
            base_fee_per_gas: self.base_fee_per_gas,
        }
    }
}

pub enum IndexDataStorageRepr {
    InlineV0(InlineV0),
    InlineV1(TxIndexedData),
    ReferenceV0(ReferenceV0),
}

impl IndexDataStorageRepr {
    pub(super) const SENTINEL: u8 = 50;
    pub(super) const INLINE_V0_MARKER: u8 = 0;
    pub(super) const INLINE_V1_MARKER: u8 = 1;
    pub(super) const REFERENCE_V0_MARKER: u8 = 2;

    fn marker_byte(&self) -> u8 {
        match self {
            IndexDataStorageRepr::InlineV0(_) => Self::INLINE_V0_MARKER,
            IndexDataStorageRepr::InlineV1(_) => Self::INLINE_V1_MARKER,
            IndexDataStorageRepr::ReferenceV0(_) => Self::REFERENCE_V0_MARKER,
        }
    }

    fn get_marker_bytes(buf: &[u8]) -> [u8; 2] {
        [buf[0], buf[1]]
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        {
            let buf = &mut buf as &mut dyn alloy_rlp::BufMut;
            buf.put_slice(&[Self::SENTINEL, self.marker_byte()]);
            match self {
                IndexDataStorageRepr::InlineV0(tx_indexed_data_v0) => {
                    tx_indexed_data_v0.encode(buf);
                }
                IndexDataStorageRepr::InlineV1(tx_indexed_data_v1) => {
                    tx_indexed_data_v1.encode(buf);
                }
                IndexDataStorageRepr::ReferenceV0(reference_v0) => {
                    reference_v0.encode(buf);
                }
            };
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 2 {
            return Err(eyre!(
                "Cannot decode IndexStorageRepr len too small. Len: {}",
                buf.len()
            ))
            .kind(ErrorKind::Parse);
        }
        let result = match Self::get_marker_bytes(buf) {
            [Self::SENTINEL, Self::INLINE_V0_MARKER] => {
                InlineV0::decode(&mut &buf[2..]) // fmt
                    .map(IndexDataStorageRepr::InlineV0)
            }
            [Self::SENTINEL, Self::INLINE_V1_MARKER] => {
                TxIndexedData::decode(&mut &buf[2..]) //fmt
                    .map(IndexDataStorageRepr::InlineV1)
            }
            [Self::SENTINEL, Self::REFERENCE_V0_MARKER] => {
                ReferenceV0::decode(&mut &buf[2..]) //fmt
                    .map(IndexDataStorageRepr::ReferenceV0)
            }
            // no sentinel bytes implies raw v0 encoding
            _ => InlineV0::decode(&mut &buf[..]) // fmt
                .map(IndexDataStorageRepr::InlineV0),
        };
        match result {
            Ok(d) => Ok(d),
            Err(e) => {
                info!(?e, "Failed to parse IndexDataStorageRepr despite sentinel bit being set. Falling back to raw InlineV0 decoding...");
                InlineV0::decode(&mut &buf[..])
                    .map(IndexDataStorageRepr::InlineV0)
                    .wrap_err("Failed to decode InlineV0 fallback")
                    .kind(ErrorKind::Parse)
            }
        }
    }

    pub async fn convert(self, block_reader: &impl BlockDataReader) -> Result<TxIndexedData> {
        Ok(match self {
            IndexDataStorageRepr::InlineV0(inline_v0) => {
                let (block, receipts) = try_join!(
                    block_reader.get_block_by_number(inline_v0.header_subset.block_number),
                    block_reader.get_block_receipts(inline_v0.header_subset.block_number)
                )
                .wrap_err("Failed to fetch block or receipts when converting from old to latest representation")?;

                TxIndexedData {
                    tx: TxEnvelopeWithSender {
                        sender: inline_v0
                            .tx
                            .recover_signer()
                            .wrap_err("Failed to recover signer")
                            .kind(ErrorKind::Parse)?,
                        tx: inline_v0.tx,
                    },
                    trace: inline_v0.trace,
                    receipt: receipts
                        .get(inline_v0.header_subset.tx_index as usize)
                        .ok_or_kind(ErrorKind::NotFound, "Failed to find receipt in block data")?
                        .clone(),
                    header_subset: inline_v0.header_subset.convert(block.header.timestamp),
                }
            }
            IndexDataStorageRepr::InlineV1(tx_indexed_data) => tx_indexed_data,
            IndexDataStorageRepr::ReferenceV0(reference_v0) => {
                // TODO: use byte offsets instead of fetching whole blob
                let (mut block, mut receipts, mut traces) = try_join!(
                    block_reader.get_block_by_number(reference_v0.header_subset.block_number),
                    block_reader.get_block_receipts(reference_v0.header_subset.block_number),
                    block_reader.get_block_traces(reference_v0.header_subset.block_number)
                )
                .wrap_err("Failed to fetch block, traces or receipts when converting from old to latest representation")?;

                let idx = reference_v0.header_subset.tx_index as usize;
                // Should we just use `get()` and clone instead of asserting len then using swap_remove?
                if block.body.transactions.len() <= idx {
                    return Err(eyre!("Block does not contain tx index"))
                        .kind(ErrorKind::Validation);
                }
                if traces.len() <= idx {
                    return Err(eyre!("traces does not contain tx index"))
                        .kind(ErrorKind::Validation);
                }
                if receipts.len() <= idx {
                    return Err(eyre!("receipts does not contain tx index"))
                        .kind(ErrorKind::Validation);
                }
                TxIndexedData {
                    tx: block.body.transactions.swap_remove(idx),
                    trace: traces.swap_remove(idx),
                    receipt: receipts.swap_remove(idx),
                    header_subset: reference_v0.header_subset,
                }
            }
        })
    }

    pub async fn get_trace(
        self,
        block_reader: &impl BlockDataReader,
    ) -> Result<(Vec<u8>, HeaderSubset)> {
        Ok(match self {
            IndexDataStorageRepr::InlineV0(inline_v0) => {
                let block = block_reader
                    .get_block_by_number(inline_v0.header_subset.block_number)
                    .await?;
                (
                    inline_v0.trace,
                    inline_v0.header_subset.convert(block.header.timestamp),
                )
            }
            IndexDataStorageRepr::InlineV1(tx_indexed_data) => {
                (tx_indexed_data.trace, tx_indexed_data.header_subset)
            }
            IndexDataStorageRepr::ReferenceV0(reference_v0) => {
                let mut traces = block_reader
                    .get_block_traces(reference_v0.header_subset.block_number)
                    .await?;
                let idx = reference_v0.header_subset.tx_index as usize;
                if traces.len() <= idx {
                    return Err(eyre!("traces does not contain tx index"))
                        .kind(ErrorKind::Validation);
                }
                (traces.swap_remove(idx), reference_v0.header_subset)
            }
        })
    }

    pub async fn get_tx(
        self,
        block_reader: &impl BlockDataReader,
    ) -> Result<(TxEnvelopeWithSender, HeaderSubset)> {
        Ok(match self {
            IndexDataStorageRepr::InlineV0(inline_v0) => {
                let mut block = block_reader
                    .get_block_by_number(inline_v0.header_subset.block_number)
                    .await?;
                (
                    block
                        .body
                        .transactions
                        .swap_remove(inline_v0.header_subset.tx_index as usize),
                    inline_v0.header_subset.convert(block.header.timestamp),
                )
            }
            IndexDataStorageRepr::InlineV1(tx_indexed_data) => {
                (tx_indexed_data.tx, tx_indexed_data.header_subset)
            }
            IndexDataStorageRepr::ReferenceV0(reference_v0) => {
                let mut block = block_reader
                    .get_block_by_number(reference_v0.header_subset.block_number)
                    .await?;
                let idx = reference_v0.header_subset.tx_index as usize;
                if block.body.transactions.len() <= idx {
                    return Err(eyre!("traces does not contain tx index"))
                        .kind(ErrorKind::Validation);
                }
                (
                    block.body.transactions.swap_remove(idx),
                    reference_v0.header_subset,
                )
            }
        })
    }

    pub async fn get_receipt(
        self,
        block_reader: &impl BlockDataReader,
    ) -> Result<(ReceiptWithLogIndex, HeaderSubset)> {
        Ok(match self {
            IndexDataStorageRepr::InlineV0(inline_v0) => {
                // No efficiency gained over regular convert
                let latest = IndexDataStorageRepr::InlineV0(inline_v0)
                    .convert(block_reader)
                    .await?;
                (latest.receipt, latest.header_subset)
            }
            IndexDataStorageRepr::InlineV1(tx_indexed_data) => {
                (tx_indexed_data.receipt, tx_indexed_data.header_subset)
            }
            IndexDataStorageRepr::ReferenceV0(reference_v0) => {
                let mut receipts = block_reader
                    .get_block_receipts(reference_v0.header_subset.block_number)
                    .await?;
                let idx = reference_v0.header_subset.tx_index as usize;
                if receipts.len() <= idx {
                    return Err(eyre!("receipts does not contain tx index"))
                        .kind(ErrorKind::Validation);
                }
                (receipts.swap_remove(idx), reference_v0.header_subset)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{kvstore::WritePolicy, test_utils::*};

    fn create_test_data() -> (InlineV0, Block, BlockReceipts) {
        let tx = mock_tx(123);
        let block = mock_block(251, vec![tx.clone()]);
        let rx = mock_rx(100, 21000);

        let inline_v0 = InlineV0 {
            tx: tx.tx,
            trace: vec![1, 2, 3],
            receipt: rx.receipt.clone(),
            header_subset: HeaderSubsetV0 {
                block_hash: block.header.hash_slow(),
                block_number: block.header.number,
                tx_index: 0,
                gas_used: 21000,
                base_fee_per_gas: Some(100),
            },
        };

        (inline_v0, block, vec![rx])
    }

    #[test]
    fn test_storage_repr_v0_encoding() {
        let (inline_v0, _, _) = create_test_data();

        let encoded = IndexDataStorageRepr::InlineV0(inline_v0.clone()).encode();
        assert_eq!(encoded[0], IndexDataStorageRepr::SENTINEL);
        assert_eq!(encoded[1], IndexDataStorageRepr::INLINE_V0_MARKER);

        let decoded = IndexDataStorageRepr::decode(&encoded).unwrap();
        match decoded {
            IndexDataStorageRepr::InlineV0(decoded_v0) => {
                assert_eq!(
                    decoded_v0.header_subset.block_number,
                    inline_v0.header_subset.block_number
                );
                assert_eq!(
                    decoded_v0.header_subset.gas_used,
                    inline_v0.header_subset.gas_used
                );
            }
            _ => panic!("Wrong version decoded"),
        }
    }

    #[tokio::test]
    async fn test_storage_repr_conversion() {
        let (inline_v0, block, receipts) = create_test_data();

        // Create a mock BlockDataReader that will return our test data
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(KVStoreErased::from(store.clone()));

        // Store the block and receipts
        archive
            .archive_block(block.clone(), WritePolicy::NoClobber)
            .await
            .unwrap();
        archive
            .archive_receipts(
                receipts.clone(),
                block.header.number,
                WritePolicy::NoClobber,
            )
            .await
            .unwrap();

        let repr = IndexDataStorageRepr::InlineV0(inline_v0);
        let converted = repr.convert(&archive).await.unwrap();

        assert_eq!(converted.header_subset.block_number, 251);
        assert_eq!(converted.header_subset.block_timestamp, 1234567);
        assert_eq!(converted.header_subset.gas_used, 21000);
        assert!(converted.header_subset.base_fee_per_gas.is_some());
    }

    #[test]
    fn test_invalid_data() {
        // Test empty data
        assert!(IndexDataStorageRepr::decode(&[]).is_err());

        // Test invalid marker
        let invalid_data = vec![IndexDataStorageRepr::SENTINEL, 99, 0, 0];
        assert!(IndexDataStorageRepr::decode(&invalid_data).is_err());

        // Test truncated data
        let (inline_v0, _, _) = create_test_data();
        let mut encoded = IndexDataStorageRepr::InlineV0(inline_v0).encode();
        encoded.truncate(encoded.len() / 2);
        assert!(IndexDataStorageRepr::decode(&encoded).is_err());
    }

    #[tokio::test]
    async fn test_it() {
        let tx = mock_tx(123);
        let log_size = 100 * 1024;
        let rx = mock_rx(log_size, 123); // test large receipt size
        let trace = vec![5, 2, 6, 12];
        let block = mock_block(251, vec![tx.clone()]);

        let inline_v0 = InlineV0 {
            tx: tx.tx,
            trace,
            receipt: rx.receipt,
            header_subset: HeaderSubsetV0 {
                block_hash: block.header.hash_slow(),
                block_number: block.header.number,
                tx_index: 0,
                gas_used: 1,
                base_fee_per_gas: Some(14),
            },
        };

        {
            let mut buf = Vec::new();
            inline_v0.encode(&mut buf);

            assert!(buf.len() > log_size);

            let decoded = IndexDataStorageRepr::decode(&buf).unwrap();
            assert!(matches!(decoded, IndexDataStorageRepr::InlineV0(_)));
        }

        {
            let buf = IndexDataStorageRepr::InlineV0(inline_v0).encode();

            assert!(buf.len() > log_size);

            let decoded = IndexDataStorageRepr::decode(&buf).unwrap();
            assert!(matches!(decoded, IndexDataStorageRepr::InlineV0(_)));
        }
    }
}
