use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_channel::Receiver;
use monad_secp::KeyPair;
use rand::RngCore;
use reqwest::Url;
use reth_primitives::{
    keccak256, sign_message, AccessList, Address, Bytes, Transaction, TransactionKind,
    TransactionSigned, TxEip1559, B256,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::MissedTickBehavior;

#[derive(Clone)]
pub struct Client {
    url: Url,
    client: reqwest::Client,
}

impl Client {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn rpc(&self, req: Value) -> Result<reqwest::Response, reqwest::Error> {
        self.client.post(self.url.clone()).json(&req).send().await
    }
}

#[derive(Debug, Deserialize)]
pub struct JsonResponse {
    pub result: Option<Value>,
    pub id: usize,
}

impl JsonResponse {
    pub fn get_result_u128(&self) -> u128 {
        let s = self.result.clone().unwrap();
        let s = s
            .as_str()
            .expect("result must be string")
            .trim_start_matches("0x");

        u128::from_str_radix(s, 16).unwrap()
    }
}

/// Represents an EOA and contains the private key for signing transactions.
/// Tracks the Nonce for the account
#[derive(Clone)]
pub struct Account {
    pub priv_key: B256,
    pub address: Address,

    // Local view of nonce and balance. Can be refreshed.
    // Initially assumed to be 0.
    // NOTE: If a transaction is sent to or from this account, refreshing immediately
    // could be incorrect because of execution delay.
    pub nonce: u64,
    // NOTE: If user updates this local view of balance after sending a transaction from this
    // account, make sure to periodically refresh to ensure exact gas fees are accounted for.
    pub balance: u128,
}

impl Account {
    pub fn new(private_key: String) -> Self {
        let pk = B256::from_str(&private_key).unwrap();
        Account::create_account(pk)
    }

    pub fn new_random() -> Self {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let pk = B256::from_slice(&bytes);
        Account::create_account(pk)
    }

    pub fn create_account(pk: B256) -> Self {
        let address = private_key_to_addr(pk);

        Self {
            priv_key: pk,
            address,
            nonce: 0,
            balance: 0,
        }
    }

    /// creates the raw RLP encoded bytes of the transaction
    pub fn create_raw_xfer_transaction(&mut self, dst: Address, value: u128) -> Bytes {
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: 41454,
            nonce: self.nonce,
            max_fee_per_gas: 10000,
            max_priority_fee_per_gas: 1000,
            gas_limit: 21000,
            to: TransactionKind::Call(dst),
            value: value.into(),
            input: vec![].into(),
            access_list: AccessList::default(),
        });

        let hash = transaction.signature_hash();
        let signature =
            sign_message(self.priv_key, hash).expect("signing transaction should succeed");

        let signed_transaction =
            TransactionSigned::from_transaction_and_signature(transaction, signature);
        let raw_tx = signed_transaction.envelope_encoded();

        self.nonce += 1;
        // TODO: Also update the expected balance of the receiver account

        raw_tx
    }

    pub async fn refresh_nonce(&mut self, client: Client) {
        let req = json!(
            {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionCount",
                "params": [self.address, "latest"],
                "id": 1,
            }
        );

        let res = match client.rpc(req).await {
            Ok(res) => res,
            Err(err) => {
                println!("rpc not responding for refresh nonce: {}", err);
                return;
            }
        };
        let res = match res.json::<JsonResponse>().await {
            Ok(x) => x,
            Err(e) => {
                println!("json response decoding should not fail: {e}");
                return;
            }
        };

        self.nonce = res.get_result_u128() as u64;
    }

    pub async fn refresh_balance(&mut self, client: Client) {
        let req = json!(
            {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [self.address, "latest"],
                "id": 1,
            }
        );

        let res = match client.rpc(req).await {
            Ok(res) => res,
            Err(err) => {
                println!("rpc not responding for refresh balance: {}", err);
                return;
            }
        };
        let res = match res.json::<JsonResponse>().await {
            Ok(x) => x,
            Err(e) => {
                println!("json response decoding should not fail: {e}");
                return;
            }
        };

        self.balance = res.get_result_u128();
    }
}

// ------------------- RPC functions -------------------

// sends the requests in a batch list in the order of the raw_txns input vector. The index of each
// request is used as the id field in the batch list. The return value is a vector of the indices
// that returned success
async fn send_batch_raw_txns(raw_txns: Vec<Bytes>, client: Client) -> Vec<usize> {
    // TODO, not sure if there a batch size limit so may need to make multiple batches
    // enumerate the batch calls with id so that we can match with the array of responses that the
    // server returns. The list of responses
    let json_values = raw_txns
        .iter()
        .enumerate()
        .map(|(id, txn)| {
            json!({
                    "jsonrpc": "2.0",
                    "method": "eth_sendRawTransaction",
                    "params": [txn],
                    "id": id,
            })
        })
        .collect::<Vec<Value>>();

    let batch_req = serde_json::Value::Array(json_values);

    // the result of a batch request should be an array of responses for each req in the batch
    // list, but the ordering does not need to be maintained -- the id field is used to identify
    // which response goes with which request
    let res = match client.rpc(batch_req).await {
        Ok(res) => res,
        Err(err) => {
            println!("rpc not responding for send batch raw txns: {}", err);
            return Vec::new();
        }
    };

    let res_ids = match res.json::<Vec<JsonResponse>>().await {
        Ok(res_ids) => res_ids
            .iter()
            .filter_map(|resp| resp.result.as_ref().map(|_| resp.id))
            .collect(),
        Err(err) => {
            println!("balances refresh response, json deser error: {}", err);
            Vec::new()
        }
    };

    res_ids
}

fn private_key_to_addr(mut pk: B256) -> Address {
    let kp = KeyPair::from_bytes(pk.as_mut_slice()).unwrap();
    let pubkey_bytes = kp.pubkey().bytes();
    assert!(pubkey_bytes.len() == 65);
    let hash = keccak256(&pubkey_bytes[1..]);
    Address::from_slice(&hash[12..])
}

// single RPC sender worker.
// waits for a batch of transactions (every second) from the txn receiver and forwards to RPC
pub async fn rpc_sender(
    rpc_sender_id: usize,
    rpc_sender_interval: Duration,
    txn_batch_receiver: Receiver<Vec<Bytes>>,
    txn_batch_size: usize,
    client: Client,
    tx_counter: Arc<AtomicUsize>,
) {
    println!("spawned rpc sender id: {}", rpc_sender_id);

    // interval, not sleep
    let mut interval = tokio::time::interval(rpc_sender_interval);
    // to ensure TPS doesn't spike if there is a gap between tx gen
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;

        match txn_batch_receiver.recv().await {
            Ok(next_batch) => {
                // TODO: Use the response to update nonces
                send_batch_raw_txns(next_batch, client.clone()).await;
                tx_counter.fetch_add(txn_batch_size, Ordering::SeqCst);
            }
            Err(err) => {
                // channel is closed and no more txns exist
                println!("rpc sender: {}: {}", rpc_sender_id, err);
                break;
            }
        }
    }
}
