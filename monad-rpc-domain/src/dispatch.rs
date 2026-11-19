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

use actix_web::{web, HttpRequest, HttpResponse};
use monad_rpc::{
    handlers::{handle_request, resources::MonadRpcResources, rpc_select, RpcDispatcher},
    middleware::TimingRequestId,
    types::jsonrpc::{ErrorCode, JsonRpcError, RequestParams, Response},
};
use serde_json::value::RawValue;
use tracing::debug;
use tracing_actix_web::RootSpan;

use crate::auth::authenticate;

/// The methods a private domain serves. Any other method is not found.
const METHODS: &[&str] = &[
    "eth_blockNumber",
    "eth_call",
    "eth_chainId",
    "eth_createAccessList",
    "eth_estimateGas",
    "eth_feeHistory",
    "eth_fillTransaction",
    "eth_gasPrice",
    "eth_getBalance",
    "eth_getBlockByHash",
    "eth_getBlockByNumber",
    "eth_getBlockReceipts",
    "eth_getBlockTransactionCountByHash",
    "eth_getBlockTransactionCountByNumber",
    "eth_getCode",
    "eth_getLogs",
    "eth_getStorageAt",
    "eth_getTransactionByBlockHashAndIndex",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getTransactionByHash",
    "eth_getTransactionCount",
    "eth_getTransactionReceipt",
    "eth_maxPriorityFeePerGas",
    "eth_syncing",
    "net_version",
    "web3_clientVersion",
];

pub struct DomainRpc {
    resources: MonadRpcResources,
}

impl DomainRpc {
    pub fn new(resources: MonadRpcResources) -> Self {
        Self { resources }
    }
}

impl RpcDispatcher for DomainRpc {
    fn resources(&self) -> &MonadRpcResources {
        &self.resources
    }

    async fn dispatch(
        &self,
        method: &str,
        params: RequestParams<'_>,
        request_id: TimingRequestId,
    ) -> Result<Box<RawValue>, JsonRpcError> {
        if !METHODS.contains(&method) {
            return Err(JsonRpcError::new(ErrorCode::MethodNotFound));
        }
        rpc_select(&self.resources, method, params, request_id).await
    }
}

/// Answers the requests in an HTTP body if the HTTP request carries a valid access token.
pub async fn rpc_handler(
    root_span: RootSpan,
    req: HttpRequest,
    body: bytes::Bytes,
    rpc: web::Data<DomainRpc>,
    request_id: TimingRequestId,
) -> HttpResponse {
    if let Err(err) = authenticate(&req, rpc.resources.chain_id) {
        debug!(%err, "rejected unauthenticated request");
        return HttpResponse::Ok().json(Response::from_error(err.into()));
    }
    handle_request(root_span, body, rpc.get_ref(), request_id).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_web::{test, web, App};
    use alloy_primitives::B256;
    use alloy_signer_local::PrivateKeySigner;
    use monad_chain_config::MONAD_DEVNET_CHAIN_ID;
    use monad_rpc::{
        handlers::{
            resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
            rpc_select, RpcDispatcher,
        },
        middleware::TimingRequestId,
        txpool::EthTxPoolBridgeClient,
        types::jsonrpc::{ErrorCode, RequestParams},
    };
    use serde_json::{json, Value};
    use tokio::sync::Semaphore;
    use tracing_actix_web::TracingLogger;

    use super::{rpc_handler, DomainRpc};
    use crate::auth::{sign_token, AUTH_HEADER, UNAUTHORIZED_ERROR_CODE};

    const DOMAIN_CHAIN_ID: u64 = (1 << 16) | MONAD_DEVNET_CHAIN_ID;

    fn resources() -> MonadRpcResources {
        MonadRpcResources {
            txpool_bridge_client: Some(EthTxPoolBridgeClient::for_testing()),
            eth_call_handler: None,
            chain_id: DOMAIN_CHAIN_ID,
            data_provider: None,
            event_server_client: None,
            batch_request_limit: 5,
            batch_concurrent_limit: 5,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            logs_max_block_range: 1000,
            eth_send_raw_transaction_sync_default_timeout_ms: 0,
            eth_send_raw_transaction_sync_max_timeout_ms: 0,
            dry_run_get_logs_index: false,
            use_eth_get_logs_index: false,
            max_finalized_block_cache_len: 200,
            enable_eth_simulate_v1: false,
            metrics: None,
            rpc_comparator: None,
            feehistory_limiter: Arc::new(Semaphore::new(100)),
        }
    }

    #[tokio::test]
    async fn serves_the_domain_chain_id() {
        let rpc = DomainRpc::new(resources());
        let chain_id = rpc
            .dispatch(
                "eth_chainId",
                RequestParams::default(),
                TimingRequestId::random(),
            )
            .await
            .unwrap();
        assert_eq!(chain_id.get(), format!("\"{DOMAIN_CHAIN_ID:#x}\""));
    }

    #[tokio::test]
    async fn unlisted_method_is_not_found() {
        let resources = resources();
        let method = "txpool_statusByHash";
        let method_not_found = ErrorCode::MethodNotFound as i32;

        let served = rpc_select(
            &resources,
            method,
            RequestParams::default(),
            TimingRequestId::random(),
        )
        .await
        .unwrap_err();
        assert_ne!(served.code, method_not_found);

        let not_served = DomainRpc::new(resources)
            .dispatch(method, RequestParams::default(), TimingRequestId::random())
            .await
            .unwrap_err();
        assert_eq!(not_served.code, method_not_found);
    }
    /// The response to `eth_chainId` over HTTP with `token` as the access token.
    async fn chain_id_response(token: Option<String>) -> Value {
        let app = test::init_service(
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::Data::new(DomainRpc::new(resources())))
                .service(web::resource("/").route(web::post().to(rpc_handler))),
        )
        .await;
        let request = test::TestRequest::post()
            .uri("/")
            .set_json(json!({"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1}));
        let request = match token {
            Some(token) => request.insert_header((AUTH_HEADER, token)),
            None => request,
        };
        test::call_and_read_body_json(&app, request.to_request()).await
    }

    #[actix_web::test]
    async fn requires_an_access_token() {
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap();

        let response = chain_id_response(Some(sign_token(&signer, DOMAIN_CHAIN_ID))).await;
        assert_eq!(response["result"], format!("{DOMAIN_CHAIN_ID:#x}"));

        for token in [None, Some(sign_token(&signer, DOMAIN_CHAIN_ID + 1))] {
            let response = chain_id_response(token).await;
            assert_eq!(response["error"]["code"], UNAUTHORIZED_ERROR_CODE);
            assert!(response.get("result").is_none());
        }
    }
}
