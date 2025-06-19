pub mod eviction;
pub mod logs_index;

use std::future::Future;

use alloy_consensus::Header;
use alloy_primitives::BlockHash;
use alloy_rlp::{Decodable, Encodable};
use eyre::OptionExt;
use futures::Stream;
use logs_index::ensure_logs_indexes;
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    options::{IndexOptions, ReplaceOptions},
    Client, Collection, IndexModel,
};
use serde::{Deserialize, Serialize};

use crate::{kvstore::object_store::ObjectStore, model::BlockArchiver, prelude::*};

#[derive(Serialize, Deserialize)]
pub struct BlockHeaderDoc {
    pub _id: i64,
    pub hash: Binary,
    pub tx_hashes: Vec<Binary>,
    pub header: Binary,
}

/// Latest block number document
/// Metadata docs inside Header collection
#[derive(Serialize, Deserialize)]
pub(super) struct LatestDoc {
    _id: String,
    block_number: i64,
}

impl LatestDoc {
    pub fn latest_query() -> Document {
        doc! { "_id": "latest" }
    }

    pub fn latest(block_number: i64) -> Self {
        Self {
            _id: "latest".to_string(),
            block_number,
        }
    }

    pub fn trace_eviction(block_number: i64) -> Self {
        Self {
            _id: "trace_eviction".to_string(),
            block_number,
        }
    }

    pub fn trace_eviction_query() -> Document {
        doc! { "_id": "trace_eviction" }
    }
}

pub struct BlockHeader {
    pub number: u64,
    pub hash: Binary,
    pub tx_hashes: Vec<Binary>,
    pub header: Header,
}

impl BlockHeader {
    fn to_header_subset(&self, tx_index: i64, gas_used: &BsonU128) -> HeaderSubset {
        HeaderSubset {
            block_hash: BlockHash::from_slice(&self.hash.bytes),
            block_number: self.number,
            block_timestamp: self.header.timestamp,
            base_fee_per_gas: self.header.base_fee_per_gas,
            tx_index: tx_index as u64,
            gas_used: gas_used.to_u128().unwrap(),
        }
    }
}

impl TryFrom<BlockHeaderDoc> for BlockHeader {
    type Error = eyre::Report;

    fn try_from(doc: BlockHeaderDoc) -> Result<Self, Self::Error> {
        Ok(BlockHeader {
            number: doc._id as u64,
            hash: doc.hash,
            tx_hashes: doc.tx_hashes,
            header: Header::decode(&mut doc.header.bytes.as_slice())
                .wrap_err("Failed to decode header")?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct BsonU128(pub Binary);

impl BsonU128 {
    pub fn new(value: u128) -> Self {
        Self(vec2bin(value.to_le_bytes().to_vec()))
    }

    pub fn to_u128(&self) -> Result<u128> {
        let mut array = [0_u8; 16];
        array.copy_from_slice(&self.0.bytes);
        Ok(u128::from_le_bytes(array))
    }
}

#[derive(Serialize, Deserialize)]
pub struct TxDoc {
    // Indexes
    pub _id: Binary,
    pub block_hash: Binary,
    pub block_number: i64,
    pub tx_index: i64,
    pub gas_used: BsonU128,

    // Either inline data or reference to S3 object
    // TODO: eventually move the inline data out of this document and into separate docs.
    //       All fields must be read from disk, can't selectively only read 1. Fine for first draft
    pub body: Option<Binary>,
    pub body_ref: Option<S3Ref>,

    pub receipt: Option<Binary>,
    pub receipt_ref: Option<S3Ref>,

    pub trace: Option<Binary>,
    pub trace_ref: Option<S3Ref>,

    // TODO: define LogDoc
    pub logs: Vec<Document>,
}

#[derive(Serialize, Deserialize)]
pub struct S3Ref {
    pub key: String,
    pub start: i32,
    pub end: i32,
}

impl From<S3Ref> for Bson {
    fn from(val: S3Ref) -> Self {
        Bson::Document(doc! { "key": val.key, "start": val.start, "end": val.end })
    }
}

#[derive(Serialize, Deserialize)]
pub struct TxProjectionResult {
    pub tx_index: i64,
    pub body: Option<Binary>,
    pub body_ref: Option<S3Ref>,

    pub receipt: Option<Binary>,
    pub receipt_ref: Option<S3Ref>,

    pub trace: Option<Binary>,
    pub trace_ref: Option<S3Ref>,
}

#[derive(Clone)]
pub struct ModelV2 {
    pub client: Client,
    pub s3_client: ObjectStore,
    pub headers: Collection<BlockHeaderDoc>,
    pub txs: Collection<TxDoc>,
    pub concurrency: usize,
    pub replica_name: String,
}

impl ModelV2 {
    pub async fn from_client(
        client: Client,
        s3_client: ObjectStore,
        concurrency: usize,
        replica_name: String,
    ) -> Result<Self> {
        let db_names = client.list_database_names().await?;
        if !db_names.contains(&replica_name) {
            return Err(eyre!("Database {replica_name} not found"));
        }

        let db = client.database(&replica_name);

        let collection_names = db.list_collection_names().await?;
        let headers = db.collection::<BlockHeaderDoc>("block_headers");
        let txs = db.collection::<TxDoc>("txs");

        if !collection_names.contains(&"block_headers".to_string()) {
            info!("block_headers collection not found, creating...");
            db.create_collection("block_headers").await?;
            info!("block_headers collection created, creating indexes...");
            headers
                .create_index(
                    IndexModel::builder()
                        .keys(doc! { "hash": 1 })
                        .options(IndexOptions::builder().unique(true).build())
                        .build(),
                )
                .await?;

            info!("block_headers collection and indexes created");
        }
        if !collection_names.contains(&"txs".to_string()) {
            info!("txs collection not found, creating...");
            db.create_collection("txs").await?;

            info!("txs collection created, creating indexes...");
            txs.create_index(
                IndexModel::builder()
                    .keys(doc! { "block_number": 1 })
                    .options(IndexOptions::builder().unique(false).build())
                    .build(),
            )
            .await?;
            info!("txs collection created, creating eth_getLogs indexes...");
            ensure_logs_indexes(&txs).await?;
            info!("eth_getLogs indexes created in txs collection");
        }

        Ok(Self {
            client,
            s3_client,
            headers,
            txs,
            concurrency,
            replica_name,
        })
    }

    pub async fn get_header(&self, number: u64) -> Result<BlockHeader> {
        let db_header = self
            .headers
            .find_one(doc! { "_id": number as i64 })
            .await
            .wrap_err("Failed to get header")?
            .ok_or_eyre(format!("Header not found for block {number}"))?;

        BlockHeader::try_from(db_header)
    }

    /// Proj must be a subset of TxProjectionResult
    pub async fn get_header_and_tx_projs(
        &self,
        number: u64,
        mut proj: Document,
    ) -> Result<(BlockHeader, impl Stream<Item = Result<TxProjectionResult>>)> {
        let header = self.get_header(number).await?;
        proj.insert("tx_index", 1);
        let tx_data_stream = self
            .txs
            .clone_with_type::<TxProjectionResult>()
            .find(doc! { "_id": { "$in": &header.tx_hashes } })
            .projection(proj)
            .sort(doc! { "tx_index": 1 })
            .await?
            .map(|x| x.wrap_err("Failed to get tx"));
        Ok((header, tx_data_stream))
    }

    async fn get_proj_and_subset(
        &self,
        tx_hash: &alloy_primitives::TxHash,
        field: &'static str,
    ) -> Result<(TxDoc, HeaderSubset)> {
        let tx_doc = self
            .txs
            .find_one(doc! { "_id": vec2bin(tx_hash.to_vec()) })
            .projection(doc! {
                "_id": 1,
                "block_hash": 1,
                "block_number": 1,
                "tx_index": 1,
                "gas_used": 1,
                "logs": 1,
                field: 1,
                format!("{}_ref", field): 1
            })
            .await?
            .wrap_err("Tx not found")?;

        let header = self.get_header(tx_doc.block_number as u64).await?;
        let header_subset = header.to_header_subset(tx_doc.tx_index, &tx_doc.gas_used);

        Ok((tx_doc, header_subset))
    }

    async fn get_block_traces_and_header(
        &self,
        block_number: u64,
    ) -> Result<(BlockHeader, BlockTraces)> {
        let (header, txs) = self
            .get_header_and_tx_projs(block_number, doc! {"trace": 1, "trace_ref": 1})
            .await?;
        let traces = txs
            .map(|x| x.map(|x| resolve_trace_inline_or_ref(&self.s3_client, x.trace, x.trace_ref)))
            .try_buffer_unordered(self.concurrency)
            .try_collect::<BlockTraces>()
            .await?;

        Ok((header, traces))
    }
}

/// Resolve inline or ref data
/// If ref, fetches async from S3 and decodes
/// If inline, decodes inline data (no async)
pub async fn resolve_inline_or_ref<T: Decodable>(
    s3_client: &ObjectStore,
    inline: Option<Binary>,
    s3_ref: Option<S3Ref>,
) -> Result<T> {
    if let Some(inline) = inline {
        T::decode(&mut inline.bytes.as_slice()).wrap_err("Failed to decode inline data")
    } else if let Some(s3_ref) = s3_ref {
        let blob: bytes::Bytes = s3_client
            .get_range(&s3_ref.key, s3_ref.start, s3_ref.end)
            .await
            .wrap_err("Failed to get ref data from s3")?;

        T::decode(&mut blob.as_ref()).wrap_err("Failed to decode ref data from s3")
    } else {
        Err(eyre!("No inline or ref data provided"))
    }
}

/// Resolve inline or ref data for traces (raw bytes, not RLP encoded)
pub async fn resolve_trace_inline_or_ref(
    s3_client: &ObjectStore,
    inline: Option<Binary>,
    s3_ref: Option<S3Ref>,
) -> Result<Vec<u8>> {
    if let Some(inline) = inline {
        Ok(inline.bytes)
    } else if let Some(s3_ref) = s3_ref {
        let blob: bytes::Bytes = s3_client
            .get_range(&s3_ref.key, s3_ref.start, s3_ref.end)
            .await
            .wrap_err("Failed to get ref data from s3")?;

        // When traces are stored in S3, they're RLP encoded as individual items
        // We need to decode the RLP wrapper to get the raw trace bytes
        let decoded: Vec<u8> =
            Decodable::decode(&mut blob.as_ref()).wrap_err("Failed to decode trace from S3")?;
        Ok(decoded)
    } else {
        Err(eyre!("No inline or ref data provided"))
    }
}

impl BlockArchiver for ModelV2 {
    fn object_store(&self) -> KVStoreErased {
        self.s3_client.clone().into()
    }

    async fn archive_block_data(
        &self,
        block: Block,
        receipts: BlockReceipts,
        traces: BlockTraces,
    ) -> Result<()> {
        let header = BlockHeaderDoc {
            _id: block.header.number as i64,
            hash: vec2bin(block.header.hash_slow().to_vec()),
            tx_hashes: block
                .body
                .transactions
                .iter()
                .map(|tx| vec2bin(tx.tx.tx_hash().to_vec()))
                .collect(),
            header: rlp_encode(&block.header),
        };

        let mut prev_cumulative_gas_used: u128 = 0;

        let txs: Vec<TxDoc> = block
            .body
            .transactions
            .iter()
            .zip(receipts)
            .zip(traces)
            .enumerate()
            .map(|(i, ((tx, receipt), trace))| TxDoc {
                _id: vec2bin(tx.tx.tx_hash().to_vec()),
                block_hash: vec2bin(block.header.hash_slow().to_vec()),
                block_number: block.header.number as i64,
                body: Some(rlp_encode(&tx)),
                body_ref: None,
                receipt: Some(rlp_encode(&receipt)),
                receipt_ref: None,
                trace: Some(vec2bin(trace)),
                trace_ref: None,
                logs: logs_index::logs_to_doc(receipt.receipt.logs()),
                tx_index: i as i64,
                gas_used: BsonU128::new({
                    // Not the nicest code, but it works
                    let gas_used = receipt.receipt.cumulative_gas_used() - prev_cumulative_gas_used;
                    prev_cumulative_gas_used = receipt.receipt.cumulative_gas_used();
                    gas_used
                }),
            })
            .collect();

        let headers = self.headers.clone();
        let header_req = spawn_mongo(
            async move {
                headers
                    .replace_one(doc! { "_id": header._id }, header)
                    .upsert(true)
                    .await
            },
            "Failed to insert header",
        );

        let tx_reqs = futures::stream::iter(txs.into_iter())
            .map(|tx| {
                let txs = self.txs.clone();
                spawn_mongo(
                    async move {
                        txs.replace_one(doc! { "_id": tx._id.clone() }, tx)
                            .upsert(true)
                            .await
                    },
                    "Failed to insert tx",
                )
            })
            .buffer_unordered(self.concurrency)
            .try_collect::<Vec<_>>();

        futures::try_join!(header_req, tx_reqs)?;
        Ok(())
    }

    async fn update_latest(&self, block_num: u64) -> Result<()> {
        self.headers
            .clone_with_type::<LatestDoc>()
            .replace_one(
                LatestDoc::latest_query(),
                LatestDoc::latest(block_num as i64),
            )
            .with_options(ReplaceOptions::builder().upsert(true).build())
            .await
            .wrap_err("Failed to update latest block number")?;
        Ok(())
    }
}

impl BlockDataReader for ModelV2 {
    async fn get_latest(&self) -> Result<Option<u64>> {
        Ok(self
            .headers
            .clone_with_type::<LatestDoc>()
            .find_one(LatestDoc::latest_query())
            .await?
            .map(|x| x.block_number as u64))
    }

    #[doc = "Get the storage bucket/table name"]
    fn get_replica(&self) -> &str {
        &self.replica_name
    }

    #[doc = "Get a block by its hash"]
    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        let header_fut = async {
            let header: Result<BlockHeader> = self
                .headers
                .find_one(doc! { "hash": vec2bin(block_hash.to_vec()) })
                .await?
                .wrap_err("Header not found")?
                .try_into();
            header
        };

        let txs_fut = async {
            self.txs
                .clone_with_type::<TxProjectionResult>()
                .find(doc! { "block_hash": vec2bin(block_hash.to_vec()) })
                .projection(doc! {"body": 1, "body_ref": 1})
                .await?
                .map(|x| x.wrap_err("Failed to get tx"))
                .and_then(|x| {
                    resolve_inline_or_ref::<TxEnvelopeWithSender>(
                        &self.s3_client,
                        x.body,
                        x.body_ref,
                    )
                })
                .try_collect::<Vec<_>>()
                .await
        };

        let (header, txs) = futures::try_join!(header_fut, txs_fut)?;
        Ok(make_block(header.header, txs))
    }

    #[doc = "Get a block by its number"]
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        let (header, txs) = self
            .get_header_and_tx_projs(block_num, doc! {"body": 1})
            .await?;
        let txs = txs
            .map(|x| x.map(|x| resolve_inline_or_ref(&self.s3_client, x.body, x.body_ref)))
            .try_buffer_unordered(self.concurrency)
            .try_collect::<Vec<TxEnvelopeWithSender>>()
            .await?;

        Ok(make_block(header.header, txs))
    }

    #[doc = "Get receipts for a block"]
    async fn get_block_receipts(&self, block_number: u64) -> Result<BlockReceipts> {
        let (_header, txs) = self
            .get_header_and_tx_projs(block_number, doc! {"receipt": 1, "receipt_ref": 1})
            .await?;
        let receipts = txs
            .map(|x| x.map(|x| resolve_inline_or_ref(&self.s3_client, x.receipt, x.receipt_ref)))
            .try_buffer_unordered(self.concurrency)
            .try_collect::<BlockReceipts>()
            .await?;

        Ok(receipts)
    }

    #[doc = "Get execution traces for a block"]
    async fn get_block_traces(&self, block_number: u64) -> Result<BlockTraces> {
        let (_, traces) = self.get_block_traces_and_header(block_number).await?;
        Ok(traces)
    }
}

impl IndexReader for ModelV2 {
    async fn get_latest_indexed(&self) -> Result<Option<u64>> {
        self.get_latest().await
    }

    async fn get_tx_indexed_data(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<TxIndexedData> {
        let tx_doc = self
            .txs
            .find_one(doc! { "_id": vec2bin(tx_hash.to_vec()) })
            .await?
            .wrap_err("Tx not found")?;

        let (header, tx, trace, receipt) = futures::try_join!(
            self.get_header(tx_doc.block_number as u64),
            resolve_inline_or_ref::<TxEnvelopeWithSender>(
                &self.s3_client,
                tx_doc.body,
                tx_doc.body_ref,
            ),
            resolve_trace_inline_or_ref(&self.s3_client, tx_doc.trace, tx_doc.trace_ref),
            resolve_inline_or_ref::<ReceiptWithLogIndex>(
                &self.s3_client,
                tx_doc.receipt,
                tx_doc.receipt_ref,
            ),
        )?;
        let header_subset = header.to_header_subset(tx_doc.tx_index, &tx_doc.gas_used);

        Ok(TxIndexedData {
            tx,
            trace,
            receipt,
            header_subset,
        })
    }

    async fn get_tx(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<(TxEnvelopeWithSender, HeaderSubset)> {
        let (tx_doc, header_subset) = self.get_proj_and_subset(tx_hash, "body").await?;

        let tx = resolve_inline_or_ref::<TxEnvelopeWithSender>(
            &self.s3_client,
            tx_doc.body,
            tx_doc.body_ref,
        )
        .await?;

        Ok((tx, header_subset))
    }

    async fn get_trace(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<(Vec<u8>, HeaderSubset)> {
        let (tx_doc, header_subset) = self.get_proj_and_subset(tx_hash, "trace").await?;
        let trace =
            resolve_trace_inline_or_ref(&self.s3_client, tx_doc.trace, tx_doc.trace_ref).await?;

        Ok((trace, header_subset))
    }

    async fn get_receipt(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<(ReceiptWithLogIndex, HeaderSubset)> {
        let (tx_doc, header_subset) = self.get_proj_and_subset(tx_hash, "receipt").await?;

        let receipt = resolve_inline_or_ref::<ReceiptWithLogIndex>(
            &self.s3_client,
            tx_doc.receipt,
            tx_doc.receipt_ref,
        )
        .await?;

        Ok((receipt, header_subset))
    }
}

/// Spawns a mongo future into a tokio task and flattens the task and future results into 1 eyre::Result
/// Note: cannot use generic error due to eyre reliance on a private trait that cannot be in bound
async fn spawn_mongo<
    T: Future<Output = Result<O, mongodb::error::Error>> + Send + 'static,
    O: Send + 'static,
>(
    req: T,
    msg: &'static str,
) -> Result<O> {
    let x = tokio::spawn(req).await.wrap_err(msg)?;
    x.wrap_err(msg)
}

pub fn vec2bin(vec: Vec<u8>) -> Binary {
    Binary {
        subtype: BinarySubtype::Generic,
        bytes: vec,
    }
}

pub fn rlp_encode<T: Encodable>(data: &T) -> Binary {
    let mut buf = Vec::new();
    data.encode(&mut buf);
    Binary {
        subtype: BinarySubtype::Generic,
        bytes: buf,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kvstore::{
            memory::MemoryStorage,
            mongo::new_client,
            object_store::ObjectStore,
        },
        test_utils::{mock_block, mock_rx, mock_trace, mock_tx, TestMongoContainer},
    };

    pub async fn setup() -> Result<(TestMongoContainer, ModelV2)> {
        let container = TestMongoContainer::new().await?;

        let client = new_client(&format!("mongodb://localhost:{}", container.port)).await?;

        // Create test database first
        client.database("test_db").create_collection("test").await?;

        // For tests, use MemoryStorage wrapped in ObjectStore enum
        let memory_storage = MemoryStorage::new("test-bucket");
        let model_v2 = ModelV2::from_client(
            client,
            ObjectStore::Memory(memory_storage),
            10,
            "test_db".to_string(),
        )
        .await?;

        Ok((container, model_v2))
    }

    #[ignore]
    #[tokio::test]
    async fn test_basic_operations() -> Result<()> {
        let (_container, model) = setup().await?;

        // Test archiving a block
        let block_num = 100;
        let txs = vec![mock_tx(1), mock_tx(2), mock_tx(3)];
        let block = mock_block(block_num, txs);
        let receipts = vec![mock_rx(100, 1000), mock_rx(200, 2000), mock_rx(300, 3000)];
        let traces = vec![mock_trace(1), mock_trace(2), mock_trace(3)];

        // Archive the block
        model
            .archive_block_data(block.clone(), receipts.clone(), traces.clone())
            .await?;

        // Update latest
        model.update_latest(block_num).await?;

        // Verify we can read it back
        let latest = model.get_latest().await?;
        assert_eq!(latest, Some(block_num));

        // Read block back
        let retrieved_block = model.get_block_by_number(block_num).await?;
        assert_eq!(retrieved_block.header.number, block_num);
        assert_eq!(retrieved_block.body.transactions.len(), 3);
        assert_eq!(retrieved_block, block);

        // Read receipts back
        let retrieved_receipts = model.get_block_receipts(block_num).await?;
        assert_eq!(retrieved_receipts.len(), 3);
        assert_eq!(retrieved_receipts, receipts);

        // Read traces back
        let retrieved_traces = model.get_block_traces(block_num).await?;
        assert_eq!(retrieved_traces.len(), 3);
        assert_eq!(retrieved_traces, traces);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_tx_index_operations() -> Result<()> {
        let (_container, model) = setup().await?;

        // Test archiving a block
        let block_num = 200;
        let txs = vec![mock_tx(10), mock_tx(20)];
        let tx_hashes: Vec<_> = txs.iter().map(|tx| tx.tx.tx_hash()).collect();
        let block = mock_block(block_num, txs.clone());
        let receipts = vec![mock_rx(150, 5000), mock_rx(250, 7500)];
        let traces = vec![mock_trace(10), mock_trace(20)];

        // Archive the block
        model
            .archive_block_data(block.clone(), receipts.clone(), traces.clone())
            .await?;

        // Test get_tx
        let (tx, header_subset) = model.get_tx(tx_hashes[0]).await?;
        assert_eq!(tx.tx.tx_hash(), tx_hashes[0]);
        assert_eq!(header_subset.block_number, block_num);
        assert_eq!(tx, txs[0]);

        // Test get_receipt
        let (receipt, header_subset) = model.get_receipt(tx_hashes[1]).await?;
        assert_eq!(receipt.receipt.cumulative_gas_used(), 7500);
        assert_eq!(header_subset.block_number, block_num);
        assert_eq!(receipt, receipts[1]);

        // Test get_trace
        let (trace, header_subset) = model.get_trace(tx_hashes[0]).await?;
        assert_eq!(trace, traces[0]);
        assert_eq!(header_subset.block_number, block_num);

        // Test get_tx_indexed_data
        let tx_indexed = model.get_tx_indexed_data(tx_hashes[0]).await?;
        assert_eq!(tx_indexed.tx.tx.tx_hash(), tx_hashes[0]);
        assert_eq!(tx_indexed.header_subset.block_number, block_num);
        assert_eq!(tx_indexed.trace, traces[0]);
        assert_eq!(tx_indexed.receipt, receipts[0]);
        assert_eq!(tx_indexed.tx, txs[0]);

        Ok(())
    }
}
