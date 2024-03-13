use std::path::PathBuf;

use account_handlers::{
    monad_eth_accounts, monad_eth_coinbase, monad_eth_getBalance, monad_eth_getCode,
    monad_eth_getStorageAt, monad_eth_getTransactionCount, monad_eth_syncing,
};
use actix::prelude::*;
use actix_http::body::BoxBody;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest, ServiceResponse},
    web, App, Error, HttpResponse, HttpServer,
};
use blockdb_handlers::{
    monad_eth_blockNumber, monad_eth_chainId, monad_eth_getBlockByHash, monad_eth_getBlockByNumber,
};
use clap::Parser;
use cli::Cli;
use eth_txn_handlers::{
    monad_eth_getBlockTransactionCountByHash, monad_eth_getBlockTransactionCountByNumber,
    monad_eth_getTransactionByBlockHashAndIndex, monad_eth_getTransactionByBlockNumberAndIndex,
    monad_eth_getTransactionByHash,
};
use futures::SinkExt;
use log::{debug, info};
use reth_primitives::TransactionSigned;
use serde_json::Value;
use triedb::TriedbEnv;

use crate::{
    blockdb::BlockDbEnv,
    call::monad_eth_call,
    eth_txn_handlers::monad_eth_sendRawTransaction,
    gas_handlers::{monad_eth_estimateGas, monad_eth_gasPrice, monad_eth_maxPriorityFeePerGas},
    jsonrpc::{JsonRpcError, Request, RequestWrapper, Response, ResponseWrapper},
    mempool_tx::MempoolTxIpcSender,
    websocket::Disconnect,
};

mod account_handlers;
mod blockdb;
mod blockdb_handlers;
mod call;
mod cli;
mod eth_json_types;
mod eth_txn_handlers;
mod gas_handlers;
mod hex;
mod jsonrpc;
mod mempool_tx;
mod triedb;
mod websocket;

async fn rpc_handler(body: bytes::Bytes, app_state: web::Data<MonadRpcResources>) -> HttpResponse {
    let request: RequestWrapper<Value> = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            debug!("parse error: {e} {body:?}");
            return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
        }
    };

    let response = match request {
        RequestWrapper::Single(json_request) => {
            let Ok(request) = serde_json::from_value::<Request>(json_request) else {
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
            };
            ResponseWrapper::Single(Response::from_result(
                request.id,
                rpc_select(&app_state, &request.method, request.params).await,
            ))
        }
        RequestWrapper::Batch(json_batch_request) => {
            if json_batch_request.is_empty() {
                return HttpResponse::Ok()
                    .json(Response::from_error(JsonRpcError::invalid_request()));
            }
            let batch_response =
                futures::future::join_all(json_batch_request.into_iter().map(|json_request| {
                    let app_state = app_state.clone(); // cheap copy
                    async move {
                        let Ok(request) = serde_json::from_value::<Request>(json_request) else {
                            return (Value::Null, Err(JsonRpcError::invalid_request()));
                        };
                        let (state, id, method, params) =
                            (app_state, request.id, request.method, request.params);
                        (id, rpc_select(&state, &method, params).await)
                    }
                }))
                .await
                .into_iter()
                .map(|(request_id, response)| Response::from_result(request_id, response))
                .collect::<Vec<_>>();
            ResponseWrapper::Batch(batch_response)
        }
    };

    info!("rpc_request/response: {body:?} => {response:?}");
    HttpResponse::Ok().json(&response)
}

async fn rpc_select(
    app_state: &MonadRpcResources,
    method: &str,
    params: Value,
) -> Result<Value, JsonRpcError> {
    match method {
        "eth_call" => {
            let Some(reader) = &app_state.blockdb_reader else {
                return Err(JsonRpcError::method_not_supported());
            };

            let Some(triedb_env) = &app_state.triedb_reader else {
                return Err(JsonRpcError::method_not_supported());
            };

            let Some(execution_ledger_path) = &app_state.execution_ledger_path.0 else {
                debug!("execution ledger path was not set");
                return Err(JsonRpcError::method_not_supported());
            };

            monad_eth_call(
                reader,
                &triedb_env.path(),
                execution_ledger_path.as_path(),
                params,
            )
            .await
        }
        "eth_sendRawTransaction" => {
            monad_eth_sendRawTransaction(app_state.mempool_sender.clone(), params).await
        }
        "eth_getTransactionByHash" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getTransactionByHash(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockByHash" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getBlockByHash(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockByNumber" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getBlockByNumber(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getTransactionByBlockHashAndIndex" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getTransactionByBlockHashAndIndex(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getTransactionByBlockNumberAndIndex" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getTransactionByBlockNumberAndIndex(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockTransactionCountByHash" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getBlockTransactionCountByHash(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockTransactionCountByNumber" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getBlockTransactionCountByNumber(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBalance" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_getBalance(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getCode" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_getCode(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getStorageAt" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_getStorageAt(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getTransactionCount" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_getTransactionCount(reader, params).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_blockNumber" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_blockNumber(reader).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_chainId" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_chainId(reader).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_syncing" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_syncing(reader).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_coinbase" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_coinbase(reader).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_accounts" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_accounts(reader).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_estimateGas" => monad_eth_estimateGas(params).await,
        "eth_gasPrice" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_gasPrice(reader).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_maxPriorityFeePerGas" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_maxPriorityFeePerGas(reader).await
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_sendTransaction" => Err(JsonRpcError::method_not_supported()),
        "eth_signTransaction" => Err(JsonRpcError::method_not_supported()),
        "eth_sign" => Err(JsonRpcError::method_not_supported()),
        "eth_hashrate" => Err(JsonRpcError::method_not_supported()),
        _ => Err(JsonRpcError::method_not_found()),
    }
}

#[derive(Debug, Clone)]
struct ExecutionLedgerPath(pub Option<PathBuf>);

#[derive(Clone)]
struct MonadRpcResources {
    mempool_sender: flume::Sender<TransactionSigned>,
    blockdb_reader: Option<BlockDbEnv>,
    triedb_reader: Option<TriedbEnv>,
    execution_ledger_path: ExecutionLedgerPath,
}

impl Handler<Disconnect> for MonadRpcResources {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, ctx: &mut Self::Context) -> Self::Result {
        debug!("received disconnect {:?}", ctx);
    }
}

impl MonadRpcResources {
    pub fn new(
        mempool_sender: flume::Sender<TransactionSigned>,
        blockdb_reader: Option<BlockDbEnv>,
        triedb_reader: Option<TriedbEnv>,
        execution_ledger_path: Option<PathBuf>,
    ) -> Self {
        Self {
            mempool_sender,
            blockdb_reader,
            triedb_reader,
            execution_ledger_path: ExecutionLedgerPath(execution_ledger_path),
        }
    }
}

impl Actor for MonadRpcResources {
    type Context = Context<Self>;
}

pub fn create_app<S: 'static>(
    app_data: S,
) -> App<
    impl ServiceFactory<
        ServiceRequest,
        Response = ServiceResponse<BoxBody>,
        Config = (),
        InitError = (),
        Error = Error,
    >,
> {
    App::new()
        .app_data(web::JsonConfig::default().limit(8192))
        .app_data(web::Data::new(app_data))
        .service(web::resource("/").route(web::post().to(rpc_handler)))
        .service(web::resource("/ws/").route(web::get().to(websocket::handler)))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    env_logger::try_init().expect("failed to initialize logger");

    // channels and thread for communicating over the mempool ipc socket
    // RPC handlers that need to send to the mempool can clone the ipc_sender
    // channel to send
    let (ipc_sender, ipc_receiver) = flume::unbounded::<TransactionSigned>();
    tokio::spawn(async move {
        // FIXME: continuously try and reconnect if fail
        let mut sender = MempoolTxIpcSender::new(args.ipc_path)
            .await
            .expect("IPC sock must exist");
        while let Ok(tx) = ipc_receiver.recv_async().await {
            sender.send(tx).await.expect("IPC send failed");
        }
    });

    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build_global()
        .expect("thread pool with 4 threads");

    let resources = MonadRpcResources::new(
        ipc_sender.clone(),
        args.blockdb_path
            .clone()
            .map(|p| BlockDbEnv::new(&p))
            .flatten(),
        args.triedb_path.clone().as_deref().map(TriedbEnv::new),
        args.execution_ledger_path,
    );

    // main server app
    HttpServer::new(move || create_app(resources.clone()))
        .bind((args.rpc_addr, args.rpc_port))?
        .run()
        .await
}

#[cfg(test)]
mod tests {
    use actix_http::Request;
    use actix_web::{
        body::{to_bytes, MessageBody},
        dev::{Service, ServiceResponse},
        test, Error,
    };
    use reth_primitives::{
        sign_message, AccessList, Address, Transaction, TransactionKind, TransactionSigned,
        TxEip1559, TxLegacy, B256,
    };
    use serde_json::{json, Number};
    use test_case::test_case;

    use super::*;

    pub struct MonadRpcResourcesState {
        pub ipc_receiver: flume::Receiver<TransactionSigned>,
    }

    pub async fn init_server() -> (
        impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error>,
        MonadRpcResourcesState,
    ) {
        let (ipc_sender, ipc_receiver) = flume::unbounded::<TransactionSigned>();
        let m = MonadRpcResourcesState { ipc_receiver };
        let app = test::init_service(create_app(MonadRpcResources {
            mempool_sender: ipc_sender.clone(),
            blockdb_reader: None,
            triedb_reader: None,
            execution_ledger_path: ExecutionLedgerPath(None),
        }))
        .await;
        (app, m)
    }

    fn make_tx_legacy() -> (B256, String) {
        let input = vec![0; 64];
        let transaction = Transaction::Legacy(TxLegacy {
            chain_id: Some(1337),
            nonce: 0,
            gas_price: 1,
            gas_limit: 6400,
            to: TransactionKind::Call(Address::random()),
            value: 0.into(),
            input: input.into(),
        });

        let hash = transaction.signature_hash();

        let sender_secret_key = B256::repeat_byte(0xcc);
        let signature =
            sign_message(sender_secret_key, hash).expect("signature should always succeed");
        let txn = TransactionSigned {
            transaction,
            hash,
            signature,
        };

        (txn.recalculate_hash(), txn.envelope_encoded().to_string())
    }

    fn make_tx_eip1559() -> (B256, String) {
        let input = vec![0; 64];
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: 1337,
            nonce: 0,
            max_fee_per_gas: 123,
            max_priority_fee_per_gas: 456,
            gas_limit: 6400,
            to: TransactionKind::Call(Address::random()),
            value: 0.into(),
            input: input.into(),
            access_list: AccessList::default(),
        });

        let hash = transaction.signature_hash();

        let sender_secret_key = B256::repeat_byte(0xcc);
        let signature =
            sign_message(sender_secret_key, hash).expect("signature should always succeed");
        let txn = TransactionSigned {
            transaction,
            hash,
            signature,
        };

        (txn.recalculate_hash(), txn.envelope_encoded().to_string())
    }

    async fn recover_response_body(resp: ServiceResponse<impl MessageBody>) -> serde_json::Value {
        let b = to_bytes(resp.into_body())
            .await
            .unwrap_or_else(|_| panic!("body to_bytes failed"));
        serde_json::from_slice(&b)
            .map_err(|e| {
                println!("failed to serialize {:?}", &b);
                e
            })
            .unwrap()
    }

    #[actix_web::test]
    async fn test_rpc_method_not_found() {
        let (app, _) = init_server().await;

        let payload = json!(
            {
                "jsonrpc": "2.0",
                "method": "subtract",
                "params": [42, 43],
                "id": 1
            }
        );
        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::Response =
            serde_json::from_value(recover_response_body(resp).await).unwrap();

        match resp.error {
            Some(e) => assert_eq!(e.code, -32601),
            None => panic!("expected error in response"),
        }
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_sendRawTransaction() {
        let (app, monad) = init_server().await;

        let test_input = [make_tx_legacy, make_tx_eip1559];
        for (i, f) in test_input.iter().enumerate() {
            let (expected_hash, rawtx) = f();
            let payload = json!(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_sendRawTransaction",
                    "params": [rawtx],
                    "id": 1
                }
            );

            let req = test::TestRequest::post()
                .uri("/")
                .set_payload(payload.to_string())
                .to_request();

            let resp = app.call(req).await.unwrap();
            let resp: jsonrpc::Response =
                serde_json::from_value(recover_response_body(resp).await).unwrap();

            match resp.result {
                Some(r) => assert_eq!(r, Value::String(expected_hash.to_string())),
                None => panic!("expected a result in response"),
            }

            let txn = monad
                .ipc_receiver
                .try_recv()
                .unwrap_or_else(|_| panic!("testcase {i}: nothing was sent on channel"));
            assert_eq!(expected_hash, txn.hash());
        }
    }

    #[allow(non_snake_case)]
    #[test_case(json!([]), ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)); "empty batch")]
    #[test_case(json!([1]), ResponseWrapper::Batch(vec![Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)]); "invalid batch but not empty")]
    #[test_case(json!([1, 2, 3, 4]),
    ResponseWrapper::Batch(vec![
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
    ]); "multiple invalid batch")]
    #[test_case(json!([
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1},
        1,
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![
            Response::new(None, Some(JsonRpcError::method_not_found()), Value::Number(Number::from(1))),
            Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
            Response::new(None, Some(JsonRpcError::method_not_found()), Value::Number(Number::from(1))),
        ],
    ); "partial success")]
    #[actix_web::test]
    async fn json_rpc_specification_batch_compliance(
        payload: Value,
        expected: ResponseWrapper<Response>,
    ) {
        let (app, _) = init_server().await;

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::ResponseWrapper<Response> =
            serde_json::from_value(recover_response_body(resp).await).unwrap();
        assert_eq!(resp, expected);
    }
}
