use std::sync::Arc;

use actix::{Actor, Context, Handler};
use actix_web::{
    dev::{ServiceRequest, ServiceResponse},
    Error,
};
use monad_archive::prelude::ArchiveReader;
use monad_ethcall::EthCallExecutor;
use monad_triedb_utils::triedb_env::TriedbEnv;
use tokio::sync::{Mutex, Semaphore};
use tracing::debug;
use tracing_actix_web::RootSpanBuilder;

use super::eth::call::EthCallStatsTracker;
use crate::{fee::FixedFee, txpool::EthTxPoolBridgeClient, websocket::Disconnect};

#[derive(Clone)]
pub struct MonadRpcResources {
    pub txpool_bridge_client: EthTxPoolBridgeClient,
    pub triedb_reader: Option<TriedbEnv>,
    pub eth_call_executor: Option<Arc<Mutex<EthCallExecutor>>>,
    pub eth_call_executor_fibers: usize,
    pub eth_call_stats_tracker: Option<Arc<EthCallStatsTracker>>,
    pub archive_reader: Option<ArchiveReader>,
    pub base_fee_per_gas: FixedFee,
    pub chain_id: u64,
    pub batch_request_limit: u16,
    pub max_response_size: u32,
    pub allow_unprotected_txs: bool,
    pub rate_limiter: Arc<Semaphore>,
    pub total_permits: usize,
    pub logs_max_block_range: u64,
    pub eth_call_provider_gas_limit: u64,
    pub eth_estimate_gas_provider_gas_limit: u64,
    pub dry_run_get_logs_index: bool,
    pub use_eth_get_logs_index: bool,
    pub max_finalized_block_cache_len: u64,
    pub enable_eth_call_statistics: bool,
}

impl Handler<Disconnect> for MonadRpcResources {
    type Result = ();

    fn handle(&mut self, _msg: Disconnect, ctx: &mut Self::Context) -> Self::Result {
        debug!("received disconnect {:?}", ctx);
    }
}

impl MonadRpcResources {
    pub fn new(
        txpool_bridge_client: EthTxPoolBridgeClient,
        triedb_reader: Option<TriedbEnv>,
        eth_call_executor: Option<Arc<Mutex<EthCallExecutor>>>,
        eth_call_executor_fibers: usize,
        archive_reader: Option<ArchiveReader>,
        fixed_base_fee: u128,
        chain_id: u64,
        batch_request_limit: u16,
        max_response_size: u32,
        allow_unprotected_txs: bool,
        rate_limiter: Arc<Semaphore>,
        total_permits: usize,
        logs_max_block_range: u64,
        eth_call_provider_gas_limit: u64,
        eth_estimate_gas_provider_gas_limit: u64,
        dry_run_get_logs_index: bool,
        use_eth_get_logs_index: bool,
        max_finalized_block_cache_len: u64,
        enable_eth_call_statistics: bool,
    ) -> Self {
        Self {
            txpool_bridge_client,
            triedb_reader,
            eth_call_executor,
            eth_call_executor_fibers,
            eth_call_stats_tracker: if enable_eth_call_statistics {
                Some(Arc::new(EthCallStatsTracker::default()))
            } else {
                None
            },
            archive_reader,
            base_fee_per_gas: FixedFee::new(fixed_base_fee),
            chain_id,
            batch_request_limit,
            max_response_size,
            allow_unprotected_txs,
            rate_limiter,
            total_permits,
            logs_max_block_range,
            eth_call_provider_gas_limit,
            eth_estimate_gas_provider_gas_limit,
            dry_run_get_logs_index,
            use_eth_get_logs_index,
            max_finalized_block_cache_len,
            enable_eth_call_statistics,
        }
    }
}

impl Actor for MonadRpcResources {
    type Context = Context<Self>;
}

pub struct MonadJsonRootSpanBuilder;

impl RootSpanBuilder for MonadJsonRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> tracing::Span {
        tracing_actix_web::root_span!(request, json_method = tracing::field::Empty)
    }

    fn on_request_end<B: actix_web::body::MessageBody>(
        span: tracing::Span,
        outcome: &Result<ServiceResponse<B>, Error>,
    ) {
    }
}
