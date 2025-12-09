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

use std::ops::Deref;

use alloy_primitives::{hex::ToHexExt, TxHash};
use eyre::bail;
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};

use super::{
    block_data_archive::BlockDataArchive,
    index_repr::{IndexDataStorageRepr, ReferenceV0},
};
use crate::{
    kvstore::{KVReaderErased, WritePolicy},
    prelude::*,
};

#[derive(Clone)]
pub struct TxIndexArchiver {
    pub index_store: KVStoreErased,
    pub block_data_archive: BlockDataArchive,
    pub max_inline_encoded_len: usize,
    pub reader: IndexReaderImpl,
}

// Allows archiver to also read without duplicated code
impl Deref for TxIndexArchiver {
    type Target = IndexReaderImpl;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

pub trait IndexReader {
    async fn resolve_from_bytes(&self, bytes: &[u8]) -> Result<TxIndexedData>;
    async fn get_latest_indexed(&self, async_backfill: bool) -> Result<Option<u64>>;
    async fn get_tx_indexed_data(&self, tx_hash: &TxHash) -> Result<Option<TxIndexedData>>;
    async fn get_tx_indexed_data_bulk(
        &self,
        tx_hashes: &[TxHash],
    ) -> Result<HashMap<TxHash, TxIndexedData>>;
    async fn get_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<(TxEnvelopeWithSender, HeaderSubset)>>;
    async fn get_trace(&self, tx_hash: &TxHash) -> Result<Option<(Vec<u8>, HeaderSubset)>>;
    async fn get_receipt(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<(ReceiptWithLogIndex, HeaderSubset)>>;
}

#[derive(Clone)]
pub struct IndexReaderImpl {
    pub index_store: KVReaderErased,
    pub block_data_reader: BlockDataReaderErased,
}

impl IndexReaderImpl {
    pub fn new(
        index_store: impl Into<KVReaderErased>,
        block_data_reader: impl Into<BlockDataReaderErased>,
    ) -> Self {
        Self {
            index_store: index_store.into(),
            block_data_reader: block_data_reader.into(),
        }
    }

    async fn get_repr(&self, tx_hash: &TxHash) -> Result<Option<IndexDataStorageRepr>> {
        let key = tx_hash.encode_hex();

        let Some(bytes) = self
            .index_store
            .get(&key)
            .await
            .wrap_err("Error getting index data")?
        else {
            return Ok(None);
        };
        let repr = IndexDataStorageRepr::decode(&bytes)?;
        Ok(Some(repr))
    }
}

impl IndexReader for IndexReaderImpl {
    async fn resolve_from_bytes(&self, bytes: &[u8]) -> Result<TxIndexedData> {
        let repr = IndexDataStorageRepr::decode(bytes)?;
        repr.convert(&self.block_data_reader).await
    }

    async fn get_latest_indexed(&self, async_backfill: bool) -> Result<Option<u64>> {
        if async_backfill {
            self.block_data_reader
                .get_latest(LatestKind::IndexedAsyncBackfill)
                .await
        } else {
            self.block_data_reader.get_latest(LatestKind::Indexed).await
        }
    }

    /// Prefer get_tx, get_receipt, get_trace where possible to avoid unecessary network calls
    async fn get_tx_indexed_data(&self, tx_hash: &TxHash) -> Result<Option<TxIndexedData>> {
        let Some(repr) = self.get_repr(tx_hash).await? else {
            return Ok(None);
        };
        let data = repr.convert(&self.block_data_reader).await?;
        Ok(Some(data))
    }

    async fn get_tx_indexed_data_bulk(
        &self,
        tx_hashes: &[TxHash],
    ) -> Result<HashMap<TxHash, TxIndexedData>> {
        let keys = tx_hashes
            .iter()
            .map(|h| h.encode_hex())
            .collect::<Vec<String>>();
        let reprs = self
            .index_store
            .bulk_get(&keys)
            .await
            .wrap_err("Error getting index data")?;

        let mut output = HashMap::new();
        for (hash, key) in tx_hashes.iter().zip(keys) {
            let Some(bytes) = reprs.get(&key) else {
                continue;
            };

            let decoded = IndexDataStorageRepr::decode(bytes)?;
            let converted = decoded.convert(&self.block_data_reader).await?;
            output.insert(*hash, converted);
        }

        Ok(output)
    }

    async fn get_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<(TxEnvelopeWithSender, HeaderSubset)>> {
        let Some(repr) = self.get_repr(tx_hash).await? else {
            return Ok(None);
        };
        let data = repr.get_tx(&self.block_data_reader).await?;
        Ok(Some(data))
    }

    async fn get_receipt(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<(ReceiptWithLogIndex, HeaderSubset)>> {
        let Some(repr) = self.get_repr(tx_hash).await? else {
            return Ok(None);
        };
        let data = repr.get_receipt(&self.block_data_reader).await?;
        Ok(Some(data))
    }

    async fn get_trace(&self, tx_hash: &TxHash) -> Result<Option<(Vec<u8>, HeaderSubset)>> {
        let Some(repr) = self.get_repr(tx_hash).await? else {
            return Ok(None);
        };
        let data = repr.get_trace(&self.block_data_reader).await?;
        Ok(Some(data))
    }
}

impl TxIndexArchiver {
    pub fn new(
        index_store: impl Into<KVStoreErased>,
        block_data_archive: BlockDataArchive,
        max_inline_encoded_len: usize,
    ) -> TxIndexArchiver {
        let index_store = index_store.into();
        Self {
            reader: IndexReaderImpl::new(index_store.clone(), block_data_archive.clone()),
            index_store,
            block_data_archive,
            max_inline_encoded_len,
        }
    }

    pub async fn update_latest_indexed(&self, block_num: u64, async_backfill: bool) -> Result<()> {
        if async_backfill {
            self.block_data_archive
                .update_latest(block_num, LatestKind::IndexedAsyncBackfill)
                .await
        } else {
            self.block_data_archive
                .update_latest(block_num, LatestKind::Indexed)
                .await
        }
    }

    pub async fn index_block(
        &self,
        block: Block,
        traces: BlockTraces,
        receipts: BlockReceipts,
        offsets: Option<Vec<TxByteOffsets>>,
    ) -> Result<()> {
        let block_number = block.header.number;
        let block_timestamp = block.header.timestamp;
        let block_hash = block.header.hash_slow();
        let base_fee_per_gas = block.header.base_fee_per_gas;

        if block.body.transactions.len() != traces.len()
            || traces.len() != receipts.len()
            || (offsets.is_some() && receipts.len() != offsets.as_ref().unwrap().len())
        {
            bail!("Block must have same number of txs as traces and receipts. num_txs: {}, num_traces: {}, num_receipts: {}", 
            block.body.transactions.len(), traces.len(), receipts.len());
        }

        let mut prev_cumulative_gas_used = 0;

        let requests = block
            .body
            .transactions
            .into_iter()
            .zip(traces)
            .zip(receipts)
            .enumerate()
            .map(|(idx, ((tx, trace), receipt))| {
                // calculate gas used by this tx
                let gas_used = receipt.receipt.cumulative_gas_used() - prev_cumulative_gas_used;
                prev_cumulative_gas_used = receipt.receipt.cumulative_gas_used();

                let key = tx.tx.tx_hash().encode_hex();
                let header_subset = || HeaderSubset {
                    block_hash,
                    block_number,
                    block_timestamp,
                    tx_index: idx as u64,
                    gas_used,
                    base_fee_per_gas,
                };
                let mut encoded = IndexDataStorageRepr::InlineV1(TxIndexedData {
                    tx,
                    trace,
                    receipt,
                    header_subset: header_subset(),
                })
                .encode();

                if encoded.len() > self.max_inline_encoded_len {
                    encoded = IndexDataStorageRepr::ReferenceV0(ReferenceV0 {
                        header_subset: header_subset(),
                        block_number,
                        offsets: offsets.as_ref().and_then(|v| v.get(idx).cloned()),
                    })
                    .encode();
                }

                (key, encoded)
            });

        self.index_store
            .bulk_put(requests, WritePolicy::NoClobber)
            .await
            .map(|_| ())
            .wrap_err("Error indexing block")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use alloy_rlp::Encodable;

    use super::*;
    use crate::{
        kvstore::memory::MemoryStorage, prelude::*, rlp_offset_scanner::get_all_tx_offsets,
        test_utils::*,
    };

    type FailurePtr = Arc<AtomicBool>;

    fn setup_indexer_with_should_fail() -> (BlockDataArchive, TxIndexArchiver, FailurePtr) {
        let sink = MemoryStorage::new("sink");
        let failure_ptr = sink.should_fail.clone();
        let archiver = BlockDataArchive::new(sink.clone());
        let index_archiver =
            TxIndexArchiver::new(KVStoreErased::from(sink), archiver.clone(), 1024);
        (archiver, index_archiver, failure_ptr)
    }

    fn setup_indexer() -> (BlockDataArchive, TxIndexArchiver) {
        let (archiver, index_archiver, _) = setup_indexer_with_should_fail();
        (archiver, index_archiver)
    }

    fn offsets_helper(
        block: &Block,
        traces: &BlockTraces,
        receipts: &BlockReceipts,
    ) -> Result<Option<Vec<TxByteOffsets>>> {
        let mut block_rlp = Vec::new();
        block.encode(&mut block_rlp);

        let mut traces_rlp = Vec::new();
        traces.encode(&mut traces_rlp);

        let mut receipts_rlp = Vec::new();
        receipts.encode(&mut receipts_rlp);

        get_all_tx_offsets(&block_rlp, &receipts_rlp, &traces_rlp).map(Option::Some)
    }

    #[tokio::test]
    async fn test_basic_indexing() {
        let (_, indexer) = setup_indexer();

        let tx = mock_tx(1);
        let block = mock_block(1, vec![tx.clone()]);
        let traces = vec![vec![1, 2, 3]];
        let receipts = vec![mock_rx(10, 21000)];

        indexer
            .index_block(
                block.clone(),
                traces.clone(),
                receipts.clone(),
                offsets_helper(&block, &traces, &receipts).unwrap(),
            )
            .await
            .unwrap();

        let indexed = indexer
            .get_tx_indexed_data(tx.tx.tx_hash())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(indexed.tx.sender, tx.sender);
        assert_eq!(indexed.trace, traces[0]);
        assert_eq!(indexed.header_subset.block_number, 1);
        assert_eq!(indexed.header_subset.block_hash, block.header.hash_slow());
        assert_eq!(indexed.header_subset.gas_used, 21000);
    }

    #[tokio::test]
    async fn test_store_errors_do_not_leak() {
        let (_, indexer, failure_ptr) = setup_indexer_with_should_fail();

        let tx = mock_tx(1);
        let block = mock_block(1, vec![tx.clone()]);
        let traces = vec![vec![1, 2, 3]];
        let receipts = vec![mock_rx(10, 21000)];

        indexer
            .index_block(
                block.clone(),
                traces.clone(),
                receipts.clone(),
                offsets_helper(&block, &traces, &receipts).unwrap(),
            )
            .await
            .unwrap();

        failure_ptr.store(true, Ordering::SeqCst);

        let res = indexer.get_tx_indexed_data(tx.tx.tx_hash()).await;
        dbg!(&res);
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(format!("{:?}", err).contains("MemoryStorage simulated failure"));
        assert!(
            !err.to_string().contains("MemoryStorage simulated failure"),
            "Top level error should not contain store specific error"
        );
    }

    #[tokio::test]
    async fn test_gas_calculation() {
        let (_, indexer) = setup_indexer();

        let tx1 = mock_tx(1);
        let tx2 = mock_tx(2);
        let block = mock_block(1, vec![tx1.clone(), tx2.clone()]);
        let traces = vec![vec![1], vec![2]];
        let receipts = vec![
            mock_rx(10, 21000), // First tx uses 21000
            mock_rx(10, 42000), // Second tx uses 21000 more
        ];

        let offsets = offsets_helper(&block, &traces, &receipts).unwrap();
        indexer
            .index_block(block, traces, receipts, offsets)
            .await
            .unwrap();

        let indexed1 = indexer
            .get_tx_indexed_data(tx1.tx.tx_hash())
            .await
            .unwrap()
            .unwrap();

        let indexed2 = indexer
            .get_tx_indexed_data(tx2.tx.tx_hash())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(indexed1.header_subset.gas_used, 21000);
        assert_eq!(indexed2.header_subset.gas_used, 21000); // 42000 - 21000
    }

    #[tokio::test]
    async fn test_mismatched_lengths() {
        let (_, indexer) = setup_indexer();

        let tx = mock_tx(1);
        let block = mock_block(1, vec![tx.clone()]);
        let traces = vec![]; // Empty traces
        let receipts = vec![mock_rx(10, 21000)];

        let result = offsets_helper(&block, &traces, &receipts);
        assert!(result.is_err());

        let result = indexer.index_block(block, traces, receipts, None).await;
        assert!(result.is_err());
    }
}
