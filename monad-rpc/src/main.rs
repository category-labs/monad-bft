use account_handlers::monad_eth_getBalance;
use actix_web::{web, App, HttpResponse, HttpServer};
use clap::Parser;
use cli::Cli;
use eth_txn_handlers::{
    monad_eth_getBlockTransactionCountByHash, monad_eth_getBlockTransactionCountByNumber,
    monad_eth_getTransactionByBlockHashAndIndex, monad_eth_getTransactionByHash,
};
use futures::SinkExt;
use log::{debug, trace};
use reth_primitives::TransactionSigned;
use serde_json::Value;
use triedb::TriedbEnv;

use crate::{
    blockdb::BlockDbEnv,
    eth_txn_handlers::{monad_eth_getBlockByHash, monad_eth_sendRawTransaction},
    jsonrpc::JsonRpcError,
    mempool_tx::MempoolTxIpcSender,
};

mod account_handlers;
mod blockdb;
mod cli;
mod eth_json_types;
mod eth_txn_handlers;
mod hex;
mod jsonrpc;
mod mempool_tx;
mod triedb;

async fn rpc_handler(
    body: bytes::Bytes,
    app_state: web::Data<MonadRpcResources>,
) -> Result<HttpResponse, actix_web::Error> {
    trace!("rpc_handler: {body:?}");

    let request: Result<jsonrpc::Request, serde_json::Error> = serde_json::from_slice(&body);
    let request = match request {
        Ok(req) => req,
        Err(e) => {
            debug!("parse error: {e}");
            return Ok(HttpResponse::Ok().json(jsonrpc::JsonRpcError::parse_error()));
        }
    };

    let response = match rpc_select(&app_state, &request.method, request.params).await {
        Ok(v) => jsonrpc::Response::new(Some(v), None, request.id),
        Err(e) => jsonrpc::Response::new(None, Some(e), request.id),
    };

    Ok(HttpResponse::Ok().json(response))
}

async fn rpc_select(
    app_state: &MonadRpcResources,
    method: &str,
    params: Value,
) -> Result<Value, JsonRpcError> {
    match method {
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
        "eth_getTransactionByBlockHashAndIndex" => {
            if let Some(reader) = &app_state.blockdb_reader {
                monad_eth_getTransactionByBlockHashAndIndex(reader, params).await
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
        _ => Err(JsonRpcError::method_not_found()),
    }
}

#[derive(Clone)]
struct MonadRpcResources {
    mempool_sender: flume::Sender<TransactionSigned>,
    blockdb_reader: Option<BlockDbEnv>,
    triedb_reader: Option<TriedbEnv>,
}

impl MonadRpcResources {
    pub fn new(
        mempool_sender: flume::Sender<TransactionSigned>,
        blockdb_reader: Option<BlockDbEnv>,
        triedb_reader: Option<TriedbEnv>,
    ) -> Self {
        Self {
            mempool_sender,
            blockdb_reader,
            triedb_reader,
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse();

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

    // main server app
    HttpServer::new(move || {
        App::new()
            .app_data(web::JsonConfig::default().limit(8192))
            .service(
                web::resource("/")
                    .app_data(web::Data::new(MonadRpcResources::new(
                        ipc_sender.clone(),
                        args.blockdb_path
                            .clone()
                            .map(|p| BlockDbEnv::new(&p))
                            .flatten(),
                        args.triedb_path.clone().as_deref().map(TriedbEnv::new),
                    )))
                    .route(web::post().to(rpc_handler)),
            )
    })
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
        test, web, App, Error,
    };
    use reth_primitives::{
        sign_message, AccessList, Address, Transaction, TransactionKind, TransactionSigned,
        TxEip1559, TxLegacy, B256,
    };
    use serde_json::json;

    use super::*;

    struct MonadRpcResourcesState {
        ipc_receiver: flume::Receiver<TransactionSigned>,
    }

    async fn init_server() -> (
        impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error>,
        MonadRpcResourcesState,
    ) {
        let (ipc_sender, ipc_receiver) = flume::unbounded::<TransactionSigned>();
        let m = MonadRpcResourcesState { ipc_receiver };
        let app = test::init_service(
            App::new().service(
                web::resource("/")
                    .app_data(web::Data::new(MonadRpcResources {
                        mempool_sender: ipc_sender.clone(),
                        blockdb_reader: None,
                        triedb_reader: None,
                    }))
                    .route(web::post().to(rpc_handler)),
            ),
        )
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
        serde_json::from_slice(&b).unwrap()
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
}
