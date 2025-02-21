use alloy_primitives::BlockHash;
use eyre::Result;
use monad_triedb_utils::triedb_env::ReceiptWithLogIndex;

use crate::{
    cli::AwsCliArgs,
    kvstore::{cloud_proxy::CloudProxyReader, mongo::MongoDbStorage},
    prelude::*,
};

#[derive(Clone, Copy)]
pub enum LatestKind {
    Uploaded,
    Indexed,
}

#[derive(Clone)]
pub struct ArchiveReader {
    block_data_reader: BlockDataReaderErased,
    index_reader: IndexReaderImpl,
    fallback: Option<Box<ArchiveReader>>,
}

impl ArchiveReader {
    pub fn new(
        block_data_reader: impl Into<BlockDataReaderErased>,
        index_store: impl Into<KVReaderErased>,
        fallback: Option<Box<ArchiveReader>>,
    ) -> ArchiveReader {
        let block_data_reader = block_data_reader.into();
        ArchiveReader {
            index_reader: IndexReaderImpl::new(index_store.into(), block_data_reader.clone()),
            block_data_reader,
            fallback,
        }
    }

    pub async fn init_mongo_reader(url: String, db: String) -> Result<ArchiveReader> {
        let block_store = MongoDbStorage::new_block_store(&url, &db, None).await?;
        let bdr = BlockDataArchive::new(block_store);
        let index_store = MongoDbStorage::new_index_store(&url, &db, None).await?;
        Ok(ArchiveReader::new(bdr, index_store, None))
    }

    pub async fn init_aws_reader(
        bucket: String,
        region: Option<String>,
        url: &str,
        api_key: &str,
        concurrency: usize,
    ) -> Result<ArchiveReader> {
        let url = url::Url::parse(url)?;
        let block_data_reader = BlockDataArchive::new(
            AwsCliArgs {
                bucket: bucket.clone(),
                concurrency,
                region,
            }
            .build_blob_store(&Metrics::none())
            .await,
        );
        let cloud_proxy_reader = CloudProxyReader::new(api_key, url, bucket)?;

        Ok(ArchiveReader::new(
            block_data_reader,
            cloud_proxy_reader,
            None,
        ))
    }

    pub fn with_fallback(mut self, fallback: Option<ArchiveReader>) -> Self {
        self.fallback = fallback.map(Box::new);
        self
    }

    async fn bdr_fallback_logic<'a, Ret, F, Fut>(&'a self, f: F) -> Result<Ret>
    where
        F: Fn(&'a BlockDataReaderErased) -> Fut,
        Fut: std::future::Future<Output = Result<Ret>>,
    {
        let Some(fallback) = self.fallback.as_ref() else {
            return f(&self.block_data_reader).await;
        };

        match f(&self.block_data_reader).await {
            Err(e) => {
                debug!(
                    ?e,
                    "ArchiveReader primary source returned an error, trying fallback..."
                );
                f(&fallback.block_data_reader).await
            }
            ok => ok,
        }
    }

    async fn index_fallback_logic<'a, Ret, F, Fut>(&'a self, f: F) -> Result<Ret>
    where
        F: Fn(&'a IndexReaderImpl) -> Fut,
        Fut: std::future::Future<Output = Result<Ret>>,
    {
        let Some(fallback) = self.fallback.as_ref() else {
            return f(&self.index_reader).await;
        };

        match f(&self.index_reader).await {
            Err(e) => {
                debug!(
                    ?e,
                    "ArchiveReader primary source returned an error, trying fallback..."
                );
                f(&fallback.index_reader).await
            }
            ok => ok,
        }
    }
}

impl IndexReader for ArchiveReader {
    async fn get_latest_indexed(&self) -> Result<Option<u64>> {
        self.index_fallback_logic(|idx| idx.get_latest_indexed())
            .await
    }

    async fn get_tx_indexed_data(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<TxIndexedData> {
        self.index_fallback_logic(|idx| idx.get_tx_indexed_data(tx_hash))
            .await
    }

    async fn get_tx_indexed_data_bulk(
        &self,
        tx_hashes: &[alloy_primitives::TxHash],
    ) -> Result<HashMap<alloy_primitives::TxHash, TxIndexedData>> {
        self.index_fallback_logic(|idx| idx.get_tx_indexed_data_bulk(tx_hashes))
            .await
    }

    async fn get_tx(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<(TxEnvelopeWithSender, HeaderSubset)> {
        self.index_fallback_logic(|idx| idx.get_tx(tx_hash)).await
    }

    async fn get_trace(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<(Vec<u8>, HeaderSubset)> {
        self.index_fallback_logic(|idx| idx.get_trace(tx_hash))
            .await
    }

    async fn get_receipt(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<(ReceiptWithLogIndex, HeaderSubset)> {
        self.index_fallback_logic(|idx| idx.get_receipt(tx_hash))
            .await
    }
}

impl BlockDataReader for ArchiveReader {
    fn get_bucket(&self) -> &str {
        self.block_data_reader.get_bucket()
    }

    async fn get_latest(&self, latest_kind: LatestKind) -> Result<Option<u64>> {
        self.bdr_fallback_logic(|bdr| bdr.get_latest(latest_kind))
            .await
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.bdr_fallback_logic(|bdr| bdr.get_block_by_number(block_num))
            .await
    }

    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        self.bdr_fallback_logic(|bdr| bdr.get_block_by_hash(block_hash))
            .await
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<BlockReceipts> {
        self.bdr_fallback_logic(|bdr| bdr.get_block_receipts(block_number))
            .await
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<BlockTraces> {
        self.bdr_fallback_logic(|bdr| bdr.get_block_traces(block_number))
            .await
    }

    async fn get_block_data_with_offsets(&self, block_num: u64) -> Result<BlockDataWithOffsets> {
        self.bdr_fallback_logic(|bdr| bdr.get_block_data_with_offsets(block_num))
            .await
    }

    #[doc = " Get a block by its number, or return None if not found"]
    async fn try_get_block_by_number(&self, block_num: u64) -> Result<Option<Block>> {
        self.bdr_fallback_logic(|bdr| bdr.try_get_block_by_number(block_num))
            .await
    }

    #[doc = " Get receipts for a block, or return None if not found"]
    async fn try_get_block_receipts(&self, block_number: u64) -> Result<Option<BlockReceipts>> {
        self.bdr_fallback_logic(|bdr| bdr.try_get_block_receipts(block_number))
            .await
    }

    #[doc = " Get execution traces for a block, or return None if not found"]
    async fn try_get_block_traces(&self, block_number: u64) -> Result<Option<BlockTraces>> {
        self.bdr_fallback_logic(|bdr| bdr.try_get_block_traces(block_number))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kvstore::memory::MemoryStorage,
        test_utils::{mock_block, mock_rx, mock_tx},
    };

    fn setup_index() -> (TxIndexArchiver, TxIndexArchiver) {
        let primary = MemoryStorage::new("primary");
        let bdr = BlockDataArchive::new(primary.clone());
        let primary = TxIndexArchiver::new(primary, bdr, 1000);

        let fallback = MemoryStorage::new("fallback");
        let bdr = BlockDataArchive::new(fallback.clone());
        let fallback = TxIndexArchiver::new(fallback, bdr, 1000);
        (primary, fallback)
    }

    #[tokio::test]
    async fn test_get_tx_primary() {
        let (primary, fallback) = setup_index();

        let tx = mock_tx(123);
        primary
            .index_block(
                mock_block(10, vec![tx.clone()]),
                vec![vec![]],
                vec![mock_rx(100, 10)],
                None,
            )
            .await
            .unwrap();

        let fallback = ArchiveReader::new(
            fallback.reader.block_data_reader,
            fallback.index_store,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader,
            primary.index_store,
            Some(Box::new(fallback)),
        );

        let (tx_ret, _) = reader.get_tx(tx.tx.tx_hash()).await.unwrap();
        assert_eq!(tx_ret, tx);
    }

    #[tokio::test]
    async fn test_get_tx_missing_no_fallback() {
        let (primary, _) = setup_index();
        let reader =
            ArchiveReader::new(primary.reader.block_data_reader, primary.index_store, None);

        let tx = mock_tx(123);
        let ret = reader.get_tx(tx.tx.tx_hash()).await;
        assert!(ret.is_err());
        assert!(ret.err().unwrap().to_string().contains("No data found in index for txhash: 159bcad22109fd9e0d5a3ba10d75a6f32386ca0112fabcc70ed100df937be54d"));
    }

    #[tokio::test]
    async fn test_get_tx_missing_both() {
        let (primary, fallback) = setup_index();
        let fallback = ArchiveReader::new(
            fallback.reader.block_data_reader,
            fallback.index_store,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader,
            primary.index_store,
            Some(Box::new(fallback)),
        );

        let tx = mock_tx(123);
        let ret = reader.get_tx(tx.tx.tx_hash()).await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    async fn test_get_tx_fallback() {
        let (primary, fallback) = setup_index();

        let tx = mock_tx(123);
        fallback
            .index_block(
                mock_block(10, vec![tx.clone()]),
                vec![vec![]],
                vec![mock_rx(100, 10)],
                None,
            )
            .await
            .unwrap();

        let fallback = ArchiveReader::new(
            fallback.reader.block_data_reader,
            fallback.index_store,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader,
            primary.index_store,
            Some(Box::new(fallback)),
        );

        let (tx_ret, _) = reader.get_tx(tx.tx.tx_hash()).await.unwrap();
        assert_eq!(tx_ret, tx);
    }

    fn setup_bdr() -> (BlockDataArchive, BlockDataArchive) {
        let primary = MemoryStorage::new("primary");
        let primary = BlockDataArchive::new(primary);
        let fallback = MemoryStorage::new("fallback");
        let fallback = BlockDataArchive::new(fallback);
        (primary, fallback)
    }

    #[tokio::test]
    async fn test_get_block_missing_in_both() {
        let (primary, fallback) = setup_bdr();
        let fallback = ArchiveReader::new(fallback, MemoryStorage::new("usused_index"), None);
        let reader = ArchiveReader::new(
            primary,
            MemoryStorage::new("usused_index"),
            Some(Box::new(fallback)),
        );

        let result = reader.get_block_by_number(10).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_block_missing_no_fallback() {
        let (primary, _) = setup_bdr();

        let reader = ArchiveReader::new(primary, MemoryStorage::new("usused_index"), None);

        let result = reader.get_block_by_number(12).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_block_present_in_fallback() {
        let (primary, fallback) = setup_bdr();

        let block = mock_block(12, vec![]);
        fallback.archive_block(block.clone()).await.unwrap();

        let fallback = ArchiveReader::new(fallback, MemoryStorage::new("usused_index"), None);
        let reader = ArchiveReader::new(
            primary,
            MemoryStorage::new("usused_index"),
            Some(Box::new(fallback)),
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }

    #[tokio::test]
    async fn test_get_block_present_in_primary_no_fallback() {
        let (primary, _) = setup_bdr();

        let block = mock_block(12, vec![]);
        primary.archive_block(block.clone()).await.unwrap();

        let reader = ArchiveReader::new(primary, MemoryStorage::new("usused_index"), None);

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }

    #[tokio::test]
    async fn test_get_block_present_in_primary() {
        let (primary, fallback) = setup_bdr();

        let block = mock_block(12, vec![]);
        primary.archive_block(block.clone()).await.unwrap();

        let fallback = ArchiveReader::new(fallback, MemoryStorage::new("usused_index"), None);
        let reader = ArchiveReader::new(
            primary,
            MemoryStorage::new("usused_index"),
            Some(Box::new(fallback)),
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }
}
