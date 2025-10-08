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

use std::sync::Arc;

use actix::{Actor, Context};
use actix_web::{
    dev::{ServiceRequest, ServiceResponse},
    Error,
};
use monad_archive::prelude::ArchiveReader;
use monad_ethcall::EthCallExecutor;
use monad_triedb_utils::triedb_env::TriedbEnv;
use tracing_actix_web::RootSpanBuilder;

use super::eth::call::EthCallStatsTracker;
use crate::{
    chainstate::ChainState, comparator::RpcComparator, metrics::Metrics,
    txpool::EthTxPoolBridgeClient,
};

#[derive(Clone)]
pub struct MonadRpcResources {
    pub txpool_bridge_client: EthTxPoolBridgeClient,
    pub triedb_reader: Option<TriedbEnv>,
    pub eth_call_executor: Option<Arc<EthCallExecutor>>,
    pub eth_call_stats_tracker: Option<Arc<EthCallStatsTracker>>,
    pub archive_reader: Option<ArchiveReader>,
    pub chain_id: u64,
    pub chain_state: Option<ChainState<TriedbEnv>>,
    pub batch_request_limit: u16,
    pub max_response_size: u32,
    pub allow_unprotected_txs: bool,
    pub logs_max_block_range: u64,
    pub eth_call_provider_gas_limit: u64,
    pub eth_estimate_gas_provider_gas_limit: u64,
    pub dry_run_get_logs_index: bool,
    pub use_eth_get_logs_index: bool,
    pub max_finalized_block_cache_len: u64,
    pub enable_eth_call_statistics: bool,
    pub metrics: Option<Metrics>,
    pub rpc_comparator: Option<RpcComparator>,
}

impl MonadRpcResources {
    pub fn new(
        txpool_bridge_client: EthTxPoolBridgeClient,
        triedb_reader: Option<TriedbEnv>,
        eth_call_executor: Option<Arc<EthCallExecutor>>,
        archive_reader: Option<ArchiveReader>,
        chain_id: u64,
        chain_state: Option<ChainState<TriedbEnv>>,
        batch_request_limit: u16,
        max_response_size: u32,
        allow_unprotected_txs: bool,
        logs_max_block_range: u64,
        eth_call_provider_gas_limit: u64,
        eth_estimate_gas_provider_gas_limit: u64,
        dry_run_get_logs_index: bool,
        use_eth_get_logs_index: bool,
        max_finalized_block_cache_len: u64,
        enable_eth_call_statistics: bool,
        metrics: Option<Metrics>,
        rpc_comparator: Option<RpcComparator>,
    ) -> Self {
        Self {
            txpool_bridge_client,
            triedb_reader,
            eth_call_executor,
            eth_call_stats_tracker: if enable_eth_call_statistics {
                Some(Arc::new(EthCallStatsTracker::default()))
            } else {
                None
            },
            archive_reader,
            chain_id,
            chain_state,
            batch_request_limit,
            max_response_size,
            allow_unprotected_txs,
            logs_max_block_range,
            eth_call_provider_gas_limit,
            eth_estimate_gas_provider_gas_limit,
            dry_run_get_logs_index,
            use_eth_get_logs_index,
            max_finalized_block_cache_len,
            enable_eth_call_statistics,
            metrics,
            rpc_comparator,
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
        _span: tracing::Span,
        _outcome: &Result<ServiceResponse<B>, Error>,
    ) {
    }
}
