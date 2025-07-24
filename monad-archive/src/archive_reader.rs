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

use alloy_primitives::BlockHash;
use eyre::Result;
use monad_triedb_utils::triedb_env::ReceiptWithLogIndex;
use tracing::trace;

use crate::{
    cli::AwsCliArgs,
    kvstore::{cloud_proxy::CloudProxyReader, mongo::MongoDbStorage},
    model::logs_index::LogsIndexArchiver,
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
    tx_index_reader: IndexReaderImpl,
    pub log_index: Option<LogsIndexArchiver>,
    fallback: Option<Box<ArchiveReader>>,
}

impl ArchiveReader {
    pub fn new(
        block_data_reader: impl Into<BlockDataReaderErased>,
        tx_index_reader: IndexReaderImpl,
        fallback: Option<Box<ArchiveReader>>,
        log_index: Option<LogsIndexArchiver>,
    ) -> ArchiveReader {
        trace!(
            has_fallback = fallback.is_some(),
            "Creating new ArchiveReader instance"
        );
        let block_data_reader = block_data_reader.into();
        let reader = ArchiveReader {
            tx_index_reader,
            block_data_reader,
            fallback,
            log_index,
        };
        debug!("ArchiveReader instance created successfully");
        reader
    }

    pub async fn init_mongo_reader(
        url: String,
        db: String,
        metrics: Metrics,
    ) -> Result<ArchiveReader> {
        info!(url, db, "Initializing MongoDB ArchiveReader");
        trace!("Creating MongoDB block store");
        let block_store = MongoDbStorage::new_block_store(&url, &db, metrics.clone()).await?;
        let block_data_reader = BlockDataArchive::new(block_store);

        trace!("Creating MongoDB index store");
        let index_store = MongoDbStorage::new_index_store(&url, &db, metrics).await?;
        let index_reader = IndexReaderImpl::new(index_store, block_data_reader.clone());

        trace!("Creating MongoDB log index store");
        let log_index = LogsIndexArchiver::from_tx_index_archiver(&index_reader, 50, true)
            .await
            .wrap_err("Failed to create log index reader")?;

        debug!("MongoDB ArchiveReader initialization complete");
        Ok(ArchiveReader::new(
            block_data_reader,
            index_reader,
            None,
            Some(log_index),
        ))
    }

    pub async fn init_aws_reader(
        bucket: String,
        region: Option<String>,
        url: &str,
        api_key: &str,
        concurrency: usize,
    ) -> Result<ArchiveReader> {
        info!(
            cloud_proxy_url = url,
            bucket, region, "Initializing AWS ArchiveReader"
        );
        let url = url::Url::parse(url)?;

        trace!(
            "Creating AWS block data reader with concurrency: {}",
            concurrency
        );
        let block_data_reader = BlockDataArchive::new(
            AwsCliArgs {
                bucket: bucket.clone(),
                concurrency,
                region,
            }
            .build_blob_store(&Metrics::none())
            .await,
        );

        trace!("Creating cloud proxy reader");
        let cloud_proxy_reader = CloudProxyReader::new(api_key, url, bucket)?;
        let tx_index_reader = IndexReaderImpl::new(cloud_proxy_reader, block_data_reader.clone());

        debug!("AWS ArchiveReader initialization complete");
        Ok(ArchiveReader::new(
            block_data_reader,
            tx_index_reader,
            None,
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
            return f(&self.tx_index_reader).await;
        };

        match f(&self.tx_index_reader).await {
            Err(e) => {
                debug!(
                    ?e,
                    "ArchiveReader primary source returned an error, trying fallback..."
                );
                f(&fallback.tx_index_reader).await
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

    async fn resolve_from_bytes(&self, bytes: &[u8]) -> Result<TxIndexedData> {
        self.index_fallback_logic(|idx| idx.resolve_from_bytes(bytes))
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
            fallback.reader.block_data_reader.clone(),
            fallback.reader.clone(),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            Some(Box::new(fallback)),
            None,
        );

        let (tx_ret, _) = reader.get_tx(tx.tx.tx_hash()).await.unwrap();
        assert_eq!(tx_ret, tx);
    }

    #[tokio::test]
    async fn test_get_tx_missing_no_fallback() {
        let (primary, _) = setup_index();
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            None,
            None,
        );

        let tx = mock_tx(123);
        let ret = reader.get_tx(tx.tx.tx_hash()).await;
        assert!(ret.is_err());
        assert!(ret.err().unwrap().to_string().contains("No data found in index for txhash: 159bcad22109fd9e0d5a3ba10d75a6f32386ca0112fabcc70ed100df937be54d"));
    }

    #[tokio::test]
    async fn test_get_tx_missing_both() {
        let (primary, fallback) = setup_index();
        let fallback = ArchiveReader::new(
            fallback.reader.block_data_reader.clone(),
            fallback.reader.clone(),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            Some(Box::new(fallback)),
            None,
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
            fallback.reader.block_data_reader.clone(),
            fallback.reader.clone(),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            Some(Box::new(fallback)),
            None,
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
        let fallback = ArchiveReader::new(
            fallback,
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            Some(Box::new(fallback)),
            None,
        );

        let result = reader.get_block_by_number(10).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_block_missing_no_fallback() {
        let (primary, _) = setup_bdr();

        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            None,
            None,
        );

        let result = reader.get_block_by_number(12).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_block_present_in_fallback() {
        let (primary, fallback) = setup_bdr();

        let block = mock_block(12, vec![]);
        fallback.archive_block(block.clone()).await.unwrap();

        let fallback = ArchiveReader::new(
            fallback,
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            Some(Box::new(fallback)),
            None,
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }

    #[tokio::test]
    async fn test_get_block_present_in_primary_no_fallback() {
        let (primary, _) = setup_bdr();

        let block = mock_block(12, vec![]);
        primary.archive_block(block.clone()).await.unwrap();

        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }

    #[tokio::test]
    async fn test_get_block_present_in_primary() {
        let (primary, fallback) = setup_bdr();

        let block = mock_block(12, vec![]);
        primary.archive_block(block.clone()).await.unwrap();

        let fallback = ArchiveReader::new(
            fallback,
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            Some(Box::new(fallback)),
            None,
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }
}
