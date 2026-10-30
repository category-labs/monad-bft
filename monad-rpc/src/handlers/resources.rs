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

use std::{collections::HashMap, sync::Arc};

use actix::{Actor, Context};
use actix_web::{
    dev::{ServiceRequest, ServiceResponse},
    Error,
};
use alloy_primitives::Address;
use monad_triedb_utils::triedb_env::TriedbEnv;
use tokio::sync::Semaphore;
use tracing_actix_web::RootSpanBuilder;

use crate::{
    comparator::RpcComparator,
    data::{eth_call_handler::EthCallHandler, DataProvider},
    event::EventServerClient,
    handlers::access::{DomainCaller, DomainOperators},
    middleware::Metrics,
    txpool::EthTxPoolBridgeClient,
};

#[derive(Clone)]
pub struct MonadRpcResources {
    pub txpool_bridge_client: Option<EthTxPoolBridgeClient>,
    pub eth_call_handler: Option<EthCallHandler>,
    pub base_chain_id: u64,
    pub chain_id: u64,
    pub domain: Option<Address>,
    /// Caller authenticated by the domain access token. `None` on the root route.
    pub caller: Option<DomainCaller>,
    /// Operator accounts of each private domain, keyed by domain chain id.
    pub domain_operators: std::sync::Arc<DomainOperators>,
    /// DomainSpoke of each private domain, keyed by domain chain id.
    pub domain_spokes: std::sync::Arc<HashMap<u64, Address>>,
    /// DomainSpoke of the route's domain, which decides who may read a contract's logs.
    pub domain_spoke: Option<Address>,
    pub data_provider: Option<DataProvider<TriedbEnv>>,
    pub event_server_client: Option<EventServerClient>,
    pub batch_request_limit: u16,
    pub batch_concurrent_limit: u16,
    pub max_response_size: u32,
    pub allow_unprotected_txs: bool,
    pub logs_max_block_range: u64,
    pub eth_send_raw_transaction_sync_default_timeout_ms: u64,
    pub eth_send_raw_transaction_sync_max_timeout_ms: u64,
    pub dry_run_get_logs_index: bool,
    pub use_eth_get_logs_index: bool,
    pub max_finalized_block_cache_len: u64,
    pub enable_eth_simulate_v1: bool,
    pub metrics: Option<Metrics>,
    pub rpc_comparator: Option<RpcComparator>,
    pub feehistory_limiter: Arc<Semaphore>,
}

impl MonadRpcResources {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        txpool_bridge_client: Option<EthTxPoolBridgeClient>,
        eth_call_handler: Option<EthCallHandler>,
        chain_id: u64,
        data_provider: Option<DataProvider<TriedbEnv>>,
        event_server_client: Option<EventServerClient>,
        batch_request_limit: u16,
        batch_concurrent_limit: u16,
        max_response_size: u32,
        allow_unprotected_txs: bool,
        logs_max_block_range: u64,
        eth_send_raw_transaction_sync_default_timeout_ms: u64,
        eth_send_raw_transaction_sync_max_timeout_ms: u64,
        dry_run_get_logs_index: bool,
        use_eth_get_logs_index: bool,
        max_finalized_block_cache_len: u64,
        enable_eth_simulate_v1: bool,
        metrics: Option<Metrics>,
        rpc_comparator: Option<RpcComparator>,
        feehistory_max_concurrent_requests: u32,
        domain_operators: DomainOperators,
        domain_spokes: HashMap<u64, Address>,
    ) -> Self {
        Self {
            txpool_bridge_client,
            eth_call_handler,
            base_chain_id: chain_id,
            chain_id,
            domain: None,
            caller: None,
            domain_operators: std::sync::Arc::new(domain_operators),
            domain_spokes: std::sync::Arc::new(domain_spokes),
            domain_spoke: None,
            data_provider,
            event_server_client,
            batch_request_limit,
            batch_concurrent_limit,
            max_response_size,
            allow_unprotected_txs,
            logs_max_block_range,
            eth_send_raw_transaction_sync_default_timeout_ms,
            eth_send_raw_transaction_sync_max_timeout_ms,
            dry_run_get_logs_index,
            use_eth_get_logs_index,
            max_finalized_block_cache_len,
            enable_eth_simulate_v1,
            metrics,
            rpc_comparator,
            feehistory_limiter: Arc::new(Semaphore::new(
                feehistory_max_concurrent_requests as usize,
            )),
        }
    }

    /// Scopes the resources to a domain route and the account that authenticated the
    /// request. The account is an operator if it is configured as one for `chain_id`.
    pub fn with_domain_context(
        &self,
        chain_id: u64,
        domain: Address,
        signer: Address,
    ) -> Self {
        let mut resources = self.clone();
        resources.chain_id = chain_id;
        resources.domain = Some(domain);
        resources.domain_spoke = self.domain_spokes.get(&chain_id).copied();
        resources.caller = Some(DomainCaller::new(
            signer,
            self.domain_operators.get(&chain_id),
        ));
        resources
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
