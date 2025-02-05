use std::ops::Deref;

use alloy_consensus::{ReceiptEnvelope, TxEnvelope};
use alloy_primitives::{hex::ToHexExt, BlockHash, TxHash};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use eyre::{bail, ensure};
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
use serde::{Deserialize, Serialize};

use crate::{
    prelude::{BlockDataReader, *},
    storage::{KVReaderErased, TxByteOffsets},
};

#[derive(Clone)]
pub struct TxIndexArchiver {
    pub index_store: KVStoreErased,
    pub block_data_archive: BlockDataArchive,
    reader: IndexReader,
    pub max_inline_encoded_len: usize,
}

// Allows archiver to also read without duplicated code
impl Deref for TxIndexArchiver {
    type Target = IndexReader;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

#[derive(Clone)]
pub struct IndexReader {
    pub index_store: KVReaderErased,
    pub block_data_reader: BlockDataReaderErased,
}

impl IndexReader {
    pub fn new(index_store: KVReaderErased, block_data_reader: BlockDataReaderErased) -> Self {
        Self {
            index_store,
            block_data_reader,
        }
    }

    pub async fn get_latest_indexed(&self) -> Result<u64> {
        self.block_data_reader.get_latest(LatestKind::Indexed).await
    }

    async fn get_repr(&self, tx_hash: &TxHash) -> Result<IndexDataStorageRepr> {
        let key = tx_hash.encode_hex();
        let bytes = self
            .index_store
            .get(&key)
            .await?
            .wrap_err_with(|| format!("No data found in index for txhash: {}", &key))?;
        IndexDataStorageRepr::decode(&bytes)
    }

    /// Prefer get_tx, get_receipt, get_trace where possible to avoid unecessary network calls
    pub async fn get_tx_indexed_data(&self, tx_hash: &TxHash) -> Result<TxIndexedData> {
        self.get_repr(tx_hash)
            .await?
            .convert(&self.block_data_reader)
            .await
    }

    pub async fn get_tx_indexed_data_bulk(
        &self,
        tx_hashes: &[TxHash],
    ) -> Result<HashMap<TxHash, TxIndexedData>> {
        let keys = tx_hashes
            .iter()
            .map(|h| h.encode_hex())
            .collect::<Vec<String>>();
        let reprs = self.index_store.bulk_get(&keys).await?;

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

    pub async fn get_tx(&self, tx_hash: &TxHash) -> Result<(TxEnvelopeWithSender, HeaderSubset)> {
        self.get_repr(tx_hash)
            .await?
            .get_tx(&self.block_data_reader)
            .await
    }

    pub async fn get_receipt(
        &self,
        tx_hash: &TxHash,
    ) -> Result<(ReceiptWithLogIndex, HeaderSubset)> {
        self.get_repr(tx_hash)
            .await?
            .get_receipt(&self.block_data_reader)
            .await
    }

    pub async fn get_trace(&self, tx_hash: &TxHash) -> Result<(Vec<u8>, HeaderSubset)> {
        self.get_repr(tx_hash)
            .await?
            .get_trace(&self.block_data_reader)
            .await
    }
}

impl TxIndexArchiver {
    pub fn new(
        index_store: KVStoreErased,
        block_data_archive: BlockDataArchive,
        max_inline_encoded_len: usize,
    ) -> TxIndexArchiver {
        Self {
            reader: IndexReader::new(
                index_store.clone().into(),
                block_data_archive.clone().into(),
            ),
            index_store,
            block_data_archive,
            max_inline_encoded_len,
        }
    }

    pub async fn update_latest_indexed(&self, block_num: u64) -> Result<()> {
        self.block_data_archive
            .update_latest(block_num, LatestKind::Indexed)
            .await
    }

    pub async fn index_block(
        &self,
        block: Block,
        traces: Vec<Vec<u8>>,
        receipts: Vec<ReceiptWithLogIndex>,
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
            block.body.length(), traces.len(), receipts.len());
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

        self.index_store.bulk_put(requests).await
    }
}

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
    const SENTINEL: u8 = 50;
    const INLINE_V0_MARKER: u8 = 0;
    const INLINE_V1_MARKER: u8 = 1;
    const REFERENCE_V0_MARKER: u8 = 2;

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
            bail!(
                "Cannot decode IndexStorageRepr len too small. Len: {}",
                buf.len()
            );
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
                    .map_err(Into::into)
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
                        sender: inline_v0.tx.recover_signer()?,
                        tx: inline_v0.tx,
                    },
                    trace: inline_v0.trace,
                    receipt: receipts
                        .get(inline_v0.header_subset.tx_index as usize)
                        .context("Failed to find receipt in block data")?
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
                ensure!(
                    block.body.transactions.len() > idx,
                    "Block does not contain tx index"
                );
                ensure!(traces.len() > idx, "traces does not contain tx index");
                ensure!(receipts.len() > idx, "receipts does not contain tx index");
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
                ensure!(traces.len() > idx, "traces does not contain tx index");
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
                ensure!(
                    block.body.transactions.len() > idx,
                    "traces does not contain tx index"
                );
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
                ensure!(receipts.len() > idx, "traces does not contain tx index");
                (receipts.swap_remove(idx), reference_v0.header_subset)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat;

    use alloy_consensus::{
        BlockBody, Header, Receipt, ReceiptEnvelope, ReceiptWithBloom, SignableTransaction,
        TxEip1559,
    };
    use alloy_primitives::{Bloom, Log, LogData, B256, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    use super::*;
    use crate::{rlp_offset_scanner::get_all_tx_offsets, storage::memory::MemoryStorage};

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
            tx: tx.into(),
            sender: signer.address(),
        }
    }

    fn mock_rx(receipt_len: usize, cumulative_gas: u128) -> ReceiptWithLogIndex {
        let receipt = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt::<Log> {
                logs: vec![Log {
                    address: Default::default(),
                    data: LogData::new(
                        vec![],
                        repeat(42).take(receipt_len).collect::<Vec<u8>>().into(),
                    )
                    .unwrap(),
                }],
                status: alloy_consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: cumulative_gas,
            },
            Bloom::repeat_byte(b'a'),
        ));
        ReceiptWithLogIndex {
            receipt,
            starting_log_index: 0,
        }
    }

    fn mock_block(number: u64, transactions: Vec<TxEnvelopeWithSender>) -> Block {
        Block {
            header: Header {
                number,
                timestamp: 1234567,
                base_fee_per_gas: Some(100),
                ..Default::default()
            },
            body: BlockBody {
                transactions,
                ommers: vec![],
                withdrawals: None,
            },
        }
    }

    fn setup_indexer() -> (BlockDataArchive, TxIndexArchiver) {
        let sink = MemoryStorage::new("sink");
        let archiver = BlockDataArchive::new(sink.clone().into());
        let index_archiver =
            TxIndexArchiver::new(KVStoreErased::from(sink), archiver.clone(), 1024);
        (archiver, index_archiver)
    }

    fn offsets_helper(
        block: &Block,
        traces: &Vec<Vec<u8>>,
        receipts: &Vec<ReceiptWithLogIndex>,
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

        let indexed = indexer.get_tx_indexed_data(tx.tx.tx_hash()).await.unwrap();
        assert_eq!(indexed.tx.sender, tx.sender);
        assert_eq!(indexed.trace, traces[0]);
        assert_eq!(indexed.header_subset.block_number, 1);
        assert_eq!(indexed.header_subset.block_hash, block.header.hash_slow());
        assert_eq!(indexed.header_subset.gas_used, 21000);
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

        let indexed1 = indexer.get_tx_indexed_data(tx1.tx.tx_hash()).await.unwrap();
        let indexed2 = indexer.get_tx_indexed_data(tx2.tx.tx_hash()).await.unwrap();

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

    mod storage_repr {
        use super::*;

        fn create_test_data() -> (InlineV0, Block, Vec<ReceiptWithLogIndex>) {
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
            archive.archive_block(block.clone()).await.unwrap();
            archive
                .archive_receipts(receipts.clone(), block.header.number)
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
    }
}
