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

use std::collections::HashSet;

use actix_web::{web, HttpRequest, HttpResponse};
use alloy_primitives::Address;
use serde_json::value::RawValue;
use tracing::debug;
use tracing_actix_web::RootSpan;

use crate::{
    domain::{
        access::{Caller, Policy},
        auth::authenticate,
    },
    handlers::{handle_request, resources::MonadRpcResources, rpc_select, RpcDispatcher},
    middleware::TimingRequestId,
    types::jsonrpc::{ErrorCode, JsonRpcError, RequestParams, Response},
};

/// The methods a private domain serves and who each is served to. Any other method is not
/// found.
const METHODS: &[(&str, Policy)] = &[
    // Chain metadata and fee queries, which reveal no account state.
    ("eth_blockNumber", Policy::Open),
    ("eth_chainId", Policy::Open),
    ("eth_feeHistory", Policy::Open),
    ("eth_gasPrice", Policy::Open),
    ("eth_maxPriorityFeePerGas", Policy::Open),
    ("eth_syncing", Policy::Open),
    ("net_version", Policy::Open),
    ("web3_clientVersion", Policy::Open),
    // Blocks, and the code and storage of any account.
    ("eth_getBlockByHash", Policy::OperatorOnly),
    ("eth_getBlockByNumber", Policy::OperatorOnly),
    ("eth_getBlockReceipts", Policy::OperatorOnly),
    ("eth_getBlockTransactionCountByHash", Policy::OperatorOnly),
    ("eth_getBlockTransactionCountByNumber", Policy::OperatorOnly),
    ("eth_getCode", Policy::OperatorOnly),
    ("eth_getStorageAt", Policy::OperatorOnly),
    (
        "eth_getTransactionByBlockHashAndIndex",
        Policy::OperatorOnly,
    ),
    (
        "eth_getTransactionByBlockNumberAndIndex",
        Policy::OperatorOnly,
    ),
    // TODO: serve these to every account for its own balance, transactions, calls and logs.
    ("eth_call", Policy::OperatorOnly),
    ("eth_createAccessList", Policy::OperatorOnly),
    ("eth_estimateGas", Policy::OperatorOnly),
    ("eth_fillTransaction", Policy::OperatorOnly),
    ("eth_getBalance", Policy::OperatorOnly),
    ("eth_getLogs", Policy::OperatorOnly),
    ("eth_getTransactionByHash", Policy::OperatorOnly),
    ("eth_getTransactionCount", Policy::OperatorOnly),
    ("eth_getTransactionReceipt", Policy::OperatorOnly),
];

pub struct DomainRpc {
    resources: MonadRpcResources,
    operators: HashSet<Address>,
}

impl DomainRpc {
    pub fn new(resources: MonadRpcResources, operators: HashSet<Address>) -> Self {
        Self {
            resources,
            operators,
        }
    }

    /// The dispatcher of the requests that `signer` makes.
    fn on_behalf_of(&self, signer: Address) -> CallerRpc<'_> {
        CallerRpc {
            rpc: self,
            caller: Caller {
                signer,
                operator: self.operators.contains(&signer),
            },
        }
    }
}

/// Answers the requests of one caller.
struct CallerRpc<'a> {
    rpc: &'a DomainRpc,
    caller: Caller,
}

impl RpcDispatcher for CallerRpc<'_> {
    fn resources(&self) -> &MonadRpcResources {
        &self.rpc.resources
    }

    async fn dispatch(
        &self,
        method: &str,
        params: RequestParams<'_>,
        request_id: TimingRequestId,
    ) -> Result<Box<RawValue>, JsonRpcError> {
        let policy = METHODS
            .iter()
            .find_map(|&(name, policy)| (name == method).then_some(policy))
            .ok_or_else(|| JsonRpcError::new(ErrorCode::MethodNotFound))?;
        self.caller.authorize(policy)?;
        rpc_select(&self.rpc.resources, method, params, request_id).await
    }
}

/// Answers the requests in an HTTP body on behalf of the account whose access token the HTTP
/// request carries.
pub async fn rpc_handler(
    root_span: RootSpan,
    req: HttpRequest,
    body: bytes::Bytes,
    rpc: web::Data<DomainRpc>,
    request_id: TimingRequestId,
) -> HttpResponse {
    let signer = match authenticate(&req, rpc.resources.chain_id) {
        Ok(signer) => signer,
        Err(err) => {
            debug!(%err, "rejected unauthenticated request");
            return HttpResponse::Ok().json(Response::from_error(err.into()));
        }
    };
    handle_request(root_span, body, &rpc.on_behalf_of(signer), request_id).await
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use actix_web::{test, web, App};
    use alloy_primitives::B256;
    use alloy_signer_local::PrivateKeySigner;
    use monad_chain_config::MONAD_DEVNET_CHAIN_ID;
    use serde_json::{json, Value};
    use tracing_actix_web::TracingLogger;

    use super::{rpc_handler, DomainRpc, METHODS};
    use crate::{
        domain::{
            access::{Policy, FORBIDDEN_ERROR_CODE},
            auth::{sign_token, AUTH_HEADER, UNAUTHORIZED_ERROR_CODE},
        },
        handlers::{
            resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
            rpc_select, RpcDispatcher,
        },
        middleware::TimingRequestId,
        types::jsonrpc::{ErrorCode, RequestParams},
    };

    const DOMAIN_CHAIN_ID: u64 = (1 << 16) | MONAD_DEVNET_CHAIN_ID;

    fn operator() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap()
    }

    fn user() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x22)).unwrap()
    }

    fn resources() -> MonadRpcResources {
        MonadRpcResources {
            chain_id: DOMAIN_CHAIN_ID,
            ..crate::tests::resources()
        }
    }

    fn rpc() -> DomainRpc {
        DomainRpc::new(resources(), HashSet::from([operator().address()]))
    }

    /// The response to `method` over HTTP with `token` as the access token.
    async fn response(method: &str, token: Option<String>) -> Value {
        let app = test::init_service(
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::Data::new(rpc()))
                .service(web::resource("/").route(web::post().to(rpc_handler))),
        )
        .await;
        let request = test::TestRequest::post()
            .uri("/")
            .set_json(json!({"jsonrpc": "2.0", "method": method, "params": [], "id": 1}));
        let request = match token {
            Some(token) => request.insert_header((AUTH_HEADER, token)),
            None => request,
        };
        test::call_and_read_body_json(&app, request.to_request()).await
    }

    #[tokio::test]
    async fn serves_the_domain_chain_id() {
        let chain_id = rpc()
            .on_behalf_of(user().address())
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

        let not_served = rpc()
            .on_behalf_of(operator().address())
            .dispatch(method, RequestParams::default(), TimingRequestId::random())
            .await
            .unwrap_err();
        assert_eq!(not_served.code, method_not_found);
    }

    #[actix_web::test]
    async fn requires_an_access_token() {
        let response_to = |token| response("eth_chainId", token);

        let served = response_to(Some(sign_token(&user(), DOMAIN_CHAIN_ID))).await;
        assert_eq!(served["result"], format!("{DOMAIN_CHAIN_ID:#x}"));

        for token in [None, Some(sign_token(&user(), DOMAIN_CHAIN_ID + 1))] {
            let rejected = response_to(token).await;
            assert_eq!(rejected["error"]["code"], UNAUTHORIZED_ERROR_CODE);
            assert!(rejected.get("result").is_none());
        }
    }

    #[actix_web::test]
    async fn serves_operator_only_methods_to_operators_only() {
        for &(method, policy) in METHODS {
            let to_user = response(method, Some(sign_token(&user(), DOMAIN_CHAIN_ID))).await;
            let to_operator =
                response(method, Some(sign_token(&operator(), DOMAIN_CHAIN_ID))).await;

            let forbidden = |response: &Value| response["error"]["code"] == FORBIDDEN_ERROR_CODE;
            assert_eq!(
                forbidden(&to_user),
                policy == Policy::OperatorOnly,
                "{method}"
            );
            assert!(!forbidden(&to_operator), "{method}");
        }
    }
}
