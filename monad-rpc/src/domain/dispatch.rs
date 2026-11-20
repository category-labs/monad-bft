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
use monad_triedb_utils::triedb_env::Triedb;
use serde_json::value::RawValue;
use tracing::debug;
use tracing_actix_web::RootSpan;

use crate::{
    data::DataProvider,
    domain::{
        access::{Caller, Policy},
        auth::authenticate,
    },
    handlers::{
        eth::{
            account::{
                monad_eth_getBalance, monad_eth_getTransactionCount, MonadEthGetBalanceParams,
                MonadEthGetTransactionCountParams,
            },
            txn::{monad_eth_getTransactionByHash, monad_eth_getTransactionReceipt},
        },
        handle_request,
        resources::MonadRpcResources,
        rpc_select, RpcDispatcher,
    },
    middleware::TimingRequestId,
    types::{
        eth_json::serialize_result,
        jsonrpc::{ErrorCode, JsonRpcError, JsonRpcResultExt, RequestParams, Response},
    },
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
    // An account's balance, nonce and transactions, each under its rule in `own_data`.
    ("eth_getBalance", Policy::OwnData),
    ("eth_getTransactionByHash", Policy::OwnData),
    ("eth_getTransactionCount", Policy::OwnData),
    ("eth_getTransactionReceipt", Policy::OwnData),
    // TODO: serve these to every account for its own calls and logs.
    ("eth_call", Policy::OperatorOnly),
    ("eth_createAccessList", Policy::OperatorOnly),
    ("eth_estimateGas", Policy::OperatorOnly),
    ("eth_fillTransaction", Policy::OperatorOnly),
    ("eth_getLogs", Policy::OperatorOnly),
];

/// Answers a [`Policy::OwnData`] method with data of `caller` only.
async fn own_data<T: Triedb>(
    caller: &Caller,
    data_provider: &DataProvider<T>,
    method: &str,
    params: RequestParams<'_>,
) -> Result<Box<RawValue>, JsonRpcError> {
    match method {
        "eth_getBalance" => {
            let params: MonadEthGetBalanceParams =
                serde_json::from_str(params.get()).invalid_params()?;
            caller.require_account(Address::from(params.account.0))?;
            serialize_result(monad_eth_getBalance(data_provider, params).await?)
        }
        "eth_getTransactionCount" => {
            let params: MonadEthGetTransactionCountParams =
                serde_json::from_str(params.get()).invalid_params()?;
            caller.require_account(Address::from(params.account.0))?;
            serialize_result(monad_eth_getTransactionCount(data_provider, params).await?)
        }
        "eth_getTransactionByHash" => {
            let params = serde_json::from_str(params.get()).invalid_params()?;
            let transaction = monad_eth_getTransactionByHash(data_provider, params).await?;
            serialize_result(caller.require_sender(transaction, |tx| tx.0.inner.signer())?)
        }
        "eth_getTransactionReceipt" => {
            let params = serde_json::from_str(params.get()).invalid_params()?;
            let receipt = monad_eth_getTransactionReceipt(data_provider, params).await?;
            let mut receipt = caller.require_sender(receipt, |receipt| receipt.0.from)?;
            if let Some(receipt) = &mut receipt {
                caller.remove_logs(&mut receipt.0);
            }
            serialize_result(receipt)
        }
        _ => Err(JsonRpcError::internal_error(format!(
            "{method} has no access rule"
        ))),
    }
}

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
        match policy {
            Policy::Open => {}
            Policy::OperatorOnly => self.caller.require_operator()?,
            Policy::OwnData => {
                let data_provider = &self.rpc.resources.data_provider;
                let data_provider = data_provider.as_ref().method_not_found()?;
                return own_data(&self.caller, data_provider, method, params).await;
            }
        }
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
    use std::{collections::HashSet, sync::Arc};

    use actix_web::{test, web, App};
    use alloy_consensus::{
        Block, BlockBody, Eip658Value, Header, Receipt, ReceiptEnvelope, ReceiptWithBloom,
    };
    use alloy_primitives::{Address, Bloom, Log, B256, U256};
    use alloy_signer_local::PrivateKeySigner;
    use monad_chain_config::MONAD_DEVNET_CHAIN_ID;
    use monad_eth_testutil::make_legacy_tx;
    use monad_eth_types::{EthAccount, ReceiptWithLogIndex, TransactionLocation};
    use monad_triedb_utils::mock_triedb::MockTriedb;
    use monad_types::SeqNum;
    use serde_json::{json, value::to_raw_value, Value};
    use tracing_actix_web::TracingLogger;

    use super::{own_data, rpc_handler, DomainRpc, METHODS};
    use crate::{
        data::DataProvider,
        domain::{
            access::{Caller, Policy, FORBIDDEN_ERROR_CODE},
            auth::{sign_token, AUTH_HEADER, UNAUTHORIZED_ERROR_CODE},
        },
        handlers::{
            resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
            rpc_select, RpcDispatcher,
        },
        middleware::TimingRequestId,
        types::jsonrpc::{ErrorCode, JsonRpcError, RequestParams},
    };

    const DOMAIN_CHAIN_ID: u64 = (1 << 16) | MONAD_DEVNET_CHAIN_ID;
    const USER_KEY: B256 = B256::repeat_byte(0x22);
    const OTHER: Address = Address::repeat_byte(0x33);

    fn operator() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap()
    }

    fn user() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&USER_KEY).unwrap()
    }

    fn caller(signer: Address, operator: bool) -> Caller {
        Caller { signer, operator }
    }

    /// A chain of one transaction, sent by the user, and the hash of that transaction. Its
    /// receipt has one log. The user has balance 5 and nonce 1, the other account balance 9
    /// and nonce 3.
    fn chain() -> (DataProvider<MockTriedb>, B256) {
        let tx = make_legacy_tx(USER_KEY, 1, 21_000, 0, 0);
        let tx_hash = *tx.tx_hash();
        let receipt = Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 21_000,
            logs: vec![Log::new_unchecked(
                Address::repeat_byte(0xcc),
                vec![B256::repeat_byte(0xee), user().address().into_word()],
                Default::default(),
            )],
        };

        let mut triedb = MockTriedb::default();
        for (account, balance, nonce) in [(user().address(), 5, 1), (OTHER, 9, 3)] {
            triedb.set_account(
                account.into_array(),
                EthAccount {
                    balance: U256::from(balance),
                    nonce,
                    ..Default::default()
                },
            );
        }
        triedb.set_latest_block(1);
        triedb.set_finalized_block(
            SeqNum(1),
            Block {
                header: Header {
                    number: 1,
                    ..Default::default()
                },
                body: BlockBody {
                    transactions: vec![tx],
                    ommers: vec![],
                    withdrawals: None,
                },
            },
        );
        triedb.set_receipts(
            SeqNum(1),
            vec![ReceiptWithLogIndex {
                receipt: ReceiptEnvelope::Legacy(ReceiptWithBloom::from(receipt)),
                starting_log_index: 0,
            }],
        );
        triedb.set_transaction_location_by_hash(
            tx_hash.0,
            TransactionLocation {
                block_num: 1,
                tx_index: 0,
            },
        );
        (DataProvider::new(None, Arc::new(triedb), None), tx_hash)
    }

    /// The answer of `chain` to `method` called by `caller` with `params`.
    async fn own_data_of(
        chain: &DataProvider<MockTriedb>,
        caller: Caller,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let params = to_raw_value(&params).unwrap();
        own_data(&caller, chain, method, RequestParams::new(&params))
            .await
            .map(|result| serde_json::from_str(result.get()).unwrap())
    }

    fn assert_forbidden(result: Result<Value, JsonRpcError>) {
        assert_eq!(result.unwrap_err().code, FORBIDDEN_ERROR_CODE);
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

    #[tokio::test]
    async fn serves_an_account_its_own_balance_and_nonce() {
        let (chain, _) = chain();
        let user = user().address();

        for (method, own, of_other) in [
            ("eth_getBalance", "0x5", "0x9"),
            ("eth_getTransactionCount", "0x1", "0x3"),
        ] {
            let params = |account: Address| json!([account, "latest"]);

            let to_user = own_data_of(&chain, caller(user, false), method, params(user)).await;
            assert_eq!(to_user.unwrap(), own, "{method}");
            assert_forbidden(own_data_of(&chain, caller(user, false), method, params(OTHER)).await);

            let to_operator = own_data_of(&chain, caller(user, true), method, params(OTHER)).await;
            assert_eq!(to_operator.unwrap(), of_other, "{method}");
        }
    }

    #[tokio::test]
    async fn serves_a_transaction_to_its_sender_only() {
        let (chain, tx_hash) = chain();
        let user = user().address();
        let method = "eth_getTransactionByHash";

        for caller in [caller(user, false), caller(OTHER, true)] {
            let transaction = own_data_of(&chain, caller, method, json!([tx_hash])).await;
            assert_eq!(transaction.unwrap()["from"], json!(user));
        }
        assert_forbidden(own_data_of(&chain, caller(OTHER, false), method, json!([tx_hash])).await);

        let unknown = json!([B256::ZERO]);
        let missing = own_data_of(&chain, caller(OTHER, false), method, unknown).await;
        assert_eq!(missing.unwrap(), Value::Null);
    }

    #[tokio::test]
    async fn serves_a_receipt_to_its_sender_without_its_logs() {
        let (chain, tx_hash) = chain();
        let user = user().address();
        let method = "eth_getTransactionReceipt";

        let receipt = own_data_of(&chain, caller(user, false), method, json!([tx_hash]))
            .await
            .unwrap();
        assert_eq!(receipt["from"], json!(user));
        assert_eq!(receipt["logs"], json!([]));
        assert_eq!(receipt["logsBloom"], json!(Bloom::ZERO));

        let to_operator = own_data_of(&chain, caller(OTHER, true), method, json!([tx_hash]))
            .await
            .unwrap();
        assert_eq!(to_operator["logs"].as_array().unwrap().len(), 1);
        assert_ne!(to_operator["logsBloom"], json!(Bloom::ZERO));

        assert_forbidden(own_data_of(&chain, caller(OTHER, false), method, json!([tx_hash])).await);
    }

    #[tokio::test]
    async fn every_own_data_method_has_a_rule() {
        let (chain, _) = chain();
        for (method, _) in METHODS
            .iter()
            .filter(|(_, policy)| *policy == Policy::OwnData)
        {
            let err = own_data(
                &caller(OTHER, false),
                &chain,
                method,
                RequestParams::default(),
            )
            .await
            .unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidParams as i32, "{method}");
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
