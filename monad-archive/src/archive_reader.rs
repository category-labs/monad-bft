use std::collections::HashMap;

use alloy_consensus::{ReceiptEnvelope, ReceiptWithBloom};
use alloy_primitives::{BlockHash, TxHash};
use eyre::Result;
use url::Url;

use crate::*;

pub enum LatestKind {
    Uploaded,
    Indexed,
}

#[derive(Clone)]
pub struct ArchiveReader<
    BReader: BlockDataReader = BlockDataReaderErased,
    IReader: IndexStoreReader = IndexStoreErased,
> {
    block_data_reader: BReader,
    index_reader: IReader,
}

impl<BStore: BlockDataReader, IStore: IndexStoreReader> ArchiveReader<BStore, IStore> {
    pub fn new(block_data_reader: BStore, index_reader: IStore) -> ArchiveReader<BStore, IStore> {
        ArchiveReader {
            block_data_reader,
            index_reader,
        }
    }

    pub async fn initialize_reader(
        bucket: String,
        region: Option<String>,
        url: &str,
        api_key: &str,
        concurrency: usize,
    ) -> Result<ArchiveReader<BlockDataArchive, CloudProxyReader>> {
        let url = url::Url::parse(url)?;
        let cloud_proxy_reader = CloudProxyReader::new(api_key, url, bucket.clone(), concurrency)?;
        let block_data_reader = BlockDataArchive::new(BlobStoreErased::S3Bucket(S3Bucket::new(
            bucket,
            &get_aws_config(region).await,
            Metrics::none(),
        )));

        Ok(ArchiveReader::new(block_data_reader, cloud_proxy_reader))
    }
}

impl<BStore: BlockDataReader, IStore: IndexStoreReader> IndexStoreReader
    for ArchiveReader<BStore, IStore>
{
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>> {
        self.index_reader.bulk_get(keys).await
    }

    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>> {
        self.index_reader.get(key).await
    }
}

impl<BStore: BlockDataReader, IStore: IndexStoreReader> BlockDataReader
    for ArchiveReader<BStore, IStore>
{
    fn get_bucket(&self) -> &str {
        self.block_data_reader.get_bucket()
    }

    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64> {
        self.block_data_reader.get_latest(latest_kind).await
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.block_data_reader.get_block_by_number(block_num).await
    }

    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        self.block_data_reader.get_block_by_hash(block_hash).await
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptEnvelope>> {
        self.block_data_reader
            .get_block_receipts(block_number)
            .await
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        self.block_data_reader.get_block_traces(block_number).await
    }
}
