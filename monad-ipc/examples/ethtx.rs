use std::{
    error::Error,
    path::{Path, PathBuf},
    task::Poll,
};

use clap::Parser;
use futures::{Sink, SinkExt};
use rand::RngCore;
use reth_primitives::{
    sign_message, Address, Transaction, TransactionKind, TransactionSigned, TxLegacy, B256,
};
use tokio::net::UnixStream;
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

/// Tool to create and send Ethereum Txs to monad-bft directly through IPC
#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 250)]
    input_len: u32,

    #[arg(short, long, default_value_t = 1000)]
    num_tx: u32,

    #[arg(long)]
    ipc_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let mut sender = MempoolTxIpcSender::new(args.ipc_path).await?;

    let txs: Vec<_> = (0..args.num_tx).map(|_| make_tx(args.input_len)).collect();
    for tx in txs {
        sender.send(tx).await?;
    }

    Ok(())
}

fn make_tx(input_len: u32) -> TransactionSigned {
    let mut input = vec![0; input_len as usize];
    rand::thread_rng().fill_bytes(&mut input);
    let transaction = Transaction::Legacy(TxLegacy {
        chain_id: Some(1337),
        nonce: 0,
        gas_price: 1,
        gas_limit: 9999999999,
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
