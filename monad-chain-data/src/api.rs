use crate::{
    blocks::Block,
    config::Config,
    core::header::EvmBlockHeader,
    error::{Error, Result},
    family::{Families, FinalizedBlock},
    ingest::authority::{LeaseAuthority, ReadOnlyAuthority, WriteAuthority},
    kernel::cache::BytesCacheMetrics,
    logs::{filter::LogFilter, log_ref::LogRef},
    store::{
        publication::MetaPublicationStore,
        traits::{BlobStore, MetaStore},
    },
    traces::{filter::TraceFilter, view::TraceRef},
    txs::{TxFilter, TxRef},
};
pub use crate::{
    core::{
        page::{QueryOrder, QueryPage, QueryPageMeta},
        refs::BlockRef,
    },
    status::ServiceStatus,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedQueryRequest<F> {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub resume_id: Option<u64>,
    pub limit: usize,
    pub filter: F,
}

pub type QueryLogsRequest = IndexedQueryRequest<LogFilter>;
pub type QueryTransactionsRequest = IndexedQueryRequest<TxFilter>;
pub type QueryTracesRequest = IndexedQueryRequest<TraceFilter>;

pub type BlockHeader = EvmBlockHeader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxReceipt {
    pub tx_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBlocksRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub from_block_hash: Option<[u8; 32]>,
    pub to_block_hash: Option<[u8; 32]>,
    pub order: QueryOrder,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionBudget {
    pub max_results: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub written_logs: usize,
    pub written_txs: usize,
    pub written_traces: usize,
}

#[allow(dead_code)]
pub struct FinalizedHistoryService<A: WriteAuthority, M: MetaStore, B: BlobStore> {
    config: Config,
    authority: A,
    publication_store: MetaPublicationStore<M>,
    families: Families,
    meta_store: M,
    blob_store: B,
    allows_writes: bool,
}

impl<A: WriteAuthority, M: MetaStore, B: BlobStore> FinalizedHistoryService<A, M, B> {
    pub(crate) fn with_authority(
        config: Config,
        meta_store: M,
        blob_store: B,
        authority: A,
        allows_writes: bool,
    ) -> Self {
        let publication_store = MetaPublicationStore::new(meta_store.clone());
        Self {
            config,
            authority,
            publication_store,
            families: Families::default(),
            meta_store,
            blob_store,
            allows_writes,
        }
    }

    pub fn cache_metrics(&self) -> BytesCacheMetrics {
        BytesCacheMetrics::default()
    }

    pub fn meta_store(&self) -> &M {
        &self.meta_store
    }

    pub fn blob_store(&self) -> &B {
        &self.blob_store
    }

    pub async fn query_blocks(
        &self,
        _request: QueryBlocksRequest,
        _budget: ExecutionBudget,
    ) -> Result<QueryPage<BlockHeader>> {
        Err(unsupported("query_blocks is not implemented in commit 2"))
    }

    pub async fn query_logs(
        &self,
        _request: QueryLogsRequest,
        _budget: ExecutionBudget,
    ) -> Result<QueryPage<LogRef>> {
        Err(unsupported("query_logs is not implemented in commit 2"))
    }

    pub async fn query_transactions(
        &self,
        _request: QueryTransactionsRequest,
        _budget: ExecutionBudget,
    ) -> Result<QueryPage<TxRef>> {
        Err(unsupported(
            "query_transactions is not implemented in commit 2",
        ))
    }

    pub async fn query_traces(
        &self,
        _request: QueryTracesRequest,
        _budget: ExecutionBudget,
    ) -> Result<QueryPage<TraceRef>> {
        Err(unsupported("query_traces is not implemented in commit 2"))
    }

    pub async fn get_tx(&self, _tx_hash: [u8; 32]) -> Result<Option<TxRef>> {
        Err(unsupported("get_tx is not implemented in commit 2"))
    }

    pub async fn get_block(&self, _number: u64) -> Result<Option<Block>> {
        Err(unsupported("get_block is not implemented in commit 2"))
    }

    pub async fn get_block_header(&self, _number: u64) -> Result<Option<BlockHeader>> {
        Err(unsupported(
            "get_block_header is not implemented in commit 2",
        ))
    }

    pub async fn get_tx_receipt(&self, _tx_hash: [u8; 32]) -> Result<Option<TxReceipt>> {
        Err(unsupported("get_tx_receipt is not implemented in commit 2"))
    }

    pub async fn get_block_receipts(&self, _number: u64) -> Result<Option<Vec<TxReceipt>>> {
        Err(unsupported(
            "get_block_receipts is not implemented in commit 2",
        ))
    }

    pub async fn ingest_finalized_block(&self, block: FinalizedBlock) -> Result<IngestOutcome> {
        self.ingest_finalized_blocks(vec![block]).await
    }

    pub async fn ingest_finalized_blocks(
        &self,
        _blocks: Vec<FinalizedBlock>,
    ) -> Result<IngestOutcome> {
        if !self.allows_writes {
            return Err(reader_only_mode_error());
        }
        Err(unsupported(
            "ingest_finalized_blocks is not implemented in commit 2",
        ))
    }

    pub async fn indexed_finalized_head(&self) -> Result<u64> {
        Err(unsupported(
            "indexed_finalized_head is not implemented in commit 2",
        ))
    }

    pub async fn status(&self) -> Result<ServiceStatus> {
        Err(unsupported("status is not implemented in commit 2"))
    }
}

fn unsupported(message: &'static str) -> Error {
    Error::Unsupported(message)
}

fn reader_only_mode_error() -> Error {
    Error::ReadOnlyMode("reader-only service cannot acquire write authority")
}

impl<M, B> FinalizedHistoryService<LeaseAuthority<MetaPublicationStore<M>>, M, B>
where
    M: MetaStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_writer(config: Config, meta_store: M, blob_store: B, owner_id: u64) -> Self {
        let authority = LeaseAuthority::new(
            MetaPublicationStore::new(meta_store.clone()),
            owner_id,
            config.publication_lease_blocks,
            config.publication_lease_renew_threshold_blocks,
        );
        Self::with_authority(config, meta_store, blob_store, authority, true)
    }
}

impl<M, B> FinalizedHistoryService<ReadOnlyAuthority, M, B>
where
    M: MetaStore + Clone,
    B: BlobStore,
{
    pub fn new_reader_only(config: Config, meta_store: M, blob_store: B) -> Self {
        Self::with_authority(config, meta_store, blob_store, ReadOnlyAuthority, false)
    }
}
