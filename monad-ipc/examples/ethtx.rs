use std::{
    error::Error,
    path::{Path, PathBuf},
    task::Poll,
};

use clap::{Parser, Subcommand};
use futures::{Sink, SinkExt};
use itertools::Itertools;
use rand::RngCore;
use reth_primitives::{
    sign_message, Address, Transaction, TransactionKind, TransactionSigned, TxLegacy, B256,
};
use serde_json::json;
use tokio::{net::UnixStream, time};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

pub struct MempoolTxIpcSender {
    writer: FramedWrite<UnixStream, LengthDelimitedCodec>,
}

impl MempoolTxIpcSender {
    pub async fn new<P>(bind_path: P) -> Result<Self, std::io::Error>
    where
        P: AsRef<Path>,
    {
        Ok(Self {
            writer: FramedWrite::new(
                UnixStream::connect(bind_path).await?,
                LengthDelimitedCodec::default(),
            ),
        })
    }
}

impl Sink<TransactionSigned> for MempoolTxIpcSender {
    type Error = std::io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        tx: TransactionSigned,
    ) -> Result<(), Self::Error> {
        let buf = tx.envelope_encoded();

        self.writer.start_send_unpin(buf.into())
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_close_unpin(cx)
    }
}

#[derive(Subcommand, Debug)]
pub enum Transport {
    Ipc {
        #[arg(long)]
        ipc_path: PathBuf,

        #[arg(long, default_value_t = u32::MAX)]
        tps: u32,
    },
    Rpc {
        #[arg(long, default_value_t = String::from("0.0.0.0"))]
        rpc_addr: String,

        #[arg(long, default_value_t = 8080)]
        rpc_port: u16,

        /// If set, will divide `num_tx` in `Args` into chunks of size `batch_size` to submit over a JSON-RPC batch.
        #[arg(long, default_value_t = 1)]
        batch_size: usize,

        /// If set will issue the JSON-RPC HTTP requests concurrently
        #[arg(long, default_value_t = false)]
        concurrent: bool,
    },
}

/// Tool to create and send Ethereum Txs to monad-bft directly through IPC
#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 250)]
    input_len: usize,

    #[arg(short, long, default_value_t = 1000)]
    num_tx: usize,

    #[command(subcommand)]
    transport: Transport,
}

async fn send_requests<'a, C: Iterator<Item = &'a TransactionSigned>>(
    transaction_chunk: C,
    rpc_addr: &String,
    rpc_port: u16,
) -> String {
    let client = reqwest::Client::new();
    let url = reqwest::Url::parse(&format!("http://{}:{}", rpc_addr, rpc_port)).unwrap();
    let payload = transaction_chunk
        .into_iter()
        .map(|tx| {
            let encoded_tx = format!("0x{}", hex::encode(tx.envelope_encoded()));
            json!({
                "jsonrpc": "2.0",
                "method": "eth_sendRawTransaction",
                "params": [
                    encoded_tx
                ],
                "id": 1
            })
        })
        .collect::<Vec<serde_json::Value>>();
    client
        .post(url)
        .header(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        )
        .body(serde_json::to_string(&payload).unwrap())
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()
        .to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let txs: Vec<_> = (0..args.num_tx).map(|_| make_tx(args.input_len)).collect();
    match args.transport {
        Transport::Ipc { ipc_path, tps } => {
            let mut sender = MempoolTxIpcSender::new(ipc_path).await?;

            let interval = time::Duration::from_secs(1) / tps;
            for tx in txs {
                let start_time = time::Instant::now();
                sender.send(tx).await?;

                if let Some(sleep_duration) = interval.checked_sub(start_time.elapsed()) {
                    time::sleep(sleep_duration).await;
                }
            }

            Ok(())
        }
        Transport::Rpc {
            rpc_addr,
            rpc_port,
            batch_size,
            concurrent,
        } => {
            if concurrent {
                let responses =
                    futures::future::join_all(txs.iter().chunks(batch_size).into_iter().map(
                        |txs_batch| async { send_requests(txs_batch, &rpc_addr, rpc_port).await },
                    ))
                    .await
                    .into_iter()
                    .collect::<Vec<String>>();

                for response in responses {
                    println!("{}", response);
                }
            } else {
                let mut responses = Vec::new();
                for txs_batch in &txs.iter().chunks(batch_size) {
                    responses.push(send_requests(txs_batch, &rpc_addr, rpc_port).await);
                }
                for response in responses {
                    println!("{}", response);
                }
            }

            Ok(())
        }
    }
}

fn make_tx(input_len: usize) -> TransactionSigned {
    let mut input = vec![0; input_len];
    rand::thread_rng().fill_bytes(&mut input);
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

    let sender_secret_key = B256::random();
    let signature = sign_message(sender_secret_key, hash).expect("signature should always succeed");

    TransactionSigned {
        transaction,
        hash,
        signature,
    }
}
