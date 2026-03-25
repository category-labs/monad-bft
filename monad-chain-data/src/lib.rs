pub mod api;
pub mod blocks;
pub mod config;
pub mod core;
pub mod error;
pub mod family;
pub mod ingest;
pub mod kernel;
pub mod logs;
pub mod status;
pub mod store;
pub mod traces;
pub mod txs;

pub use core::{
    clause::Clause,
    header::EvmBlockHeader,
    page::{QueryOrder, QueryPage, QueryPageMeta},
    refs::BlockRef,
};

pub use api::{
    BlockHeader, ExecutionBudget, FinalizedHistoryService, IngestOutcome, QueryBlocksRequest,
    QueryLogsRequest, QueryTracesRequest, QueryTransactionsRequest, TxReceipt,
};
pub use blocks::Block;
pub use config::Config;
pub use error::{Error, Result};
pub use family::{FinalizedBlock, Hash32};
pub use ingest::authority::{
    AuthorityState, LeaseAuthority, ReadOnlyAuthority, WriteAuthority, WriteContinuity,
    WriteSession,
};
pub use logs::{filter::LogFilter, log_ref::LogRef, types::Log};
pub use traces::{filter::TraceFilter, types::Trace, view::TraceRef};
pub use txs::{IngestTx, TxFilter, TxRef};

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn exposes_core_vocabulary_from_the_crate_root() {
        let header = EvmBlockHeader::minimal(7, [1; 32], [2; 32]);
        let page = QueryPage::<()> {
            items: Vec::new(),
            meta: QueryPageMeta {
                resolved_from_block: BlockRef::zero(7),
                resolved_to_block: BlockRef::zero(7),
                cursor_block: BlockRef::zero(7),
                has_more: false,
                next_resume_id: None,
            },
        };

        let clause = Clause::One([9u8; 32]);

        assert_eq!(header.number, 7);
        assert_eq!(page.meta.cursor_block.number, 7);
        assert_eq!(clause.or_terms(), 1);
    }

    #[test]
    fn reader_only_service_exposes_final_constructors_and_errors_honestly() {
        let service = FinalizedHistoryService::new_reader_only(Config::default(), (), ());

        assert_eq!(
            service.cache_metrics(),
            kernel::cache::BytesCacheMetrics::default()
        );
        assert!(matches!(
            block_on(service.query_blocks(
                QueryBlocksRequest {
                    from_block: None,
                    to_block: None,
                    from_block_hash: None,
                    to_block_hash: None,
                    order: QueryOrder::Ascending,
                    limit: 10,
                },
                ExecutionBudget::default(),
            )),
            Err(Error::Unsupported(
                "query_blocks is not implemented in commit 2"
            ))
        ));
        assert!(matches!(
            block_on(service.ingest_finalized_block(FinalizedBlock {
                block_num: 1,
                block_hash: [1; 32],
                parent_hash: [0; 32],
                header: EvmBlockHeader::minimal(1, [1; 32], [0; 32]),
                logs: Vec::new(),
                txs: Vec::new(),
                trace_rlp: Vec::new(),
            })),
            Err(Error::ReadOnlyMode(_))
        ));
    }

    #[test]
    fn reader_writer_service_uses_final_constructor_names() {
        let service = FinalizedHistoryService::new_reader_writer(Config::default(), (), (), 7);

        assert_eq!(
            service.cache_metrics(),
            kernel::cache::BytesCacheMetrics::default()
        );
        assert!(matches!(
            block_on(service.indexed_finalized_head()),
            Err(Error::Unsupported(
                "indexed_finalized_head is not implemented in commit 2"
            ))
        ));
    }
}
