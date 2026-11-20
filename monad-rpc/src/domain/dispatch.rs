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

use actix_web::{web, HttpResponse};
use serde_json::value::RawValue;
use tracing_actix_web::RootSpan;

use crate::{
    handlers::{handle_request, resources::MonadRpcResources, rpc_select, RpcDispatcher},
    middleware::TimingRequestId,
    types::jsonrpc::{ErrorCode, JsonRpcError, RequestParams},
};

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

pub async fn rpc_handler(
    root_span: RootSpan,
    body: bytes::Bytes,
    rpc: web::Data<DomainRpc>,
    request_id: TimingRequestId,
) -> HttpResponse {
    handle_request(root_span, body, rpc.get_ref(), request_id).await
}

#[cfg(test)]
mod tests {
    use monad_chain_config::MONAD_DEVNET_CHAIN_ID;

    use super::DomainRpc;
    use crate::{
        handlers::{resources::MonadRpcResources, rpc_select, RpcDispatcher},
        middleware::TimingRequestId,
        types::jsonrpc::{ErrorCode, RequestParams},
    };

    const DOMAIN_CHAIN_ID: u64 = (1 << 16) | MONAD_DEVNET_CHAIN_ID;

    fn resources() -> MonadRpcResources {
        MonadRpcResources {
            chain_id: DOMAIN_CHAIN_ID,
            ..crate::tests::resources()
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
}
