use std::{
    error::Error,
    path::{Path, PathBuf},
    str::FromStr,
    task::Poll,
    thread::sleep,
    time::Duration,
};

use alloy_rlp::Decodable;
use clap::Parser;
use futures::{Sink, SinkExt, StreamExt};
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_tx::EthTransaction;
use monad_executor_glue::{MempoolEvent, MonadEvent};
use monad_ipc::IpcReceiver;
use monad_secp::SecpSignature;
use reth_primitives::{hex_literal::hex, Address};
use tokio::net::UnixStream;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use tracing::debug;

type MessageSignatureType = SecpSignature;
type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<MessageSignatureType>>;

const MSG_PER_SENDER: u64 = 10;
const SENDERS: u64 = 4;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    ipc_path: PathBuf,
}

struct IpcSender {
    writer: FramedWrite<UnixStream, LengthDelimitedCodec>,
}

impl IpcSender {
    async fn new(ipc_path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            writer: FramedWrite::new(
                UnixStream::connect(ipc_path).await?,
                LengthDelimitedCodec::default(),
            ),
        })
    }
}

impl Sink<EthTransaction> for IpcSender {
    type Error = std::io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        tx: EthTransaction,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let ipc_path = args.ipc_path;

    if ipc_path.exists() {
        std::fs::remove_file(ipc_path.clone()).unwrap();
    }

    let mut receiver =
        IpcReceiver::<MessageSignatureType, SignatureCollectionType>::new(ipc_path.clone(), 100)?;

    // https://etherscan.io/tx/0xc97438c9ac71f94040abec76967bcaf16445ff747bcdeb383e5b94033cbed201
    let raw_tx = hex!("02f871018302877a8085070adf56b2825208948880bb98e7747f73b52a9cfa34dab9a4a06afa3887eecbb1ada2fad280c080a0d5e6f03b507cc86b59bed88c201f98c9ca6514dc5825f41aa923769cf0402839a0563f21850c0c212ce6f402f140acdcebbb541c9bb6a051070851efec99e4dd8d");
    let author_address = Address::from_str("0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97").unwrap();

    let eth_tx = EthTransaction::decode(&mut &raw_tx[..]).unwrap();

    // spawn three sender connections, 100ms from each other
    let spawn_sender = |eth_tx: EthTransaction, ipc_path: PathBuf, start_time: Duration| {
        std::thread::spawn(move || {
            for i in 0..SENDERS {
                sleep(start_time);
                debug!("spawning sender {:?}", i);
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let mut sender = IpcSender::new(ipc_path.clone())
                        .await
                        .expect("creating sender to succeed");
                    for _ in 0..MSG_PER_SENDER {
                        sender.feed(eth_tx.clone()).await.expect("feed to succeed");
                    }
                    sender.close().await.expect("close sender to succeed");
                });
            }
        })
    };

    let handler = spawn_sender(eth_tx.clone(), ipc_path.clone(), Duration::from_millis(100));

    for _ in 0..MSG_PER_SENDER * SENDERS {
        let monad_event = receiver.next().await.expect("never terminates");
        match monad_event {
            MonadEvent::MempoolEvent(event) => match event {
                MempoolEvent::UserTxns(txns) => {
                    for tx in txns {
                        let tx =
                            EthTransaction::decode(&mut tx.as_ref()).expect("must be valid eth tx");

                        let signer = tx.signer();
                        assert_eq!(signer, author_address);
                        assert_eq!(tx.transaction, eth_tx.transaction);
                        debug!("received tx");
                    }
                }
            },

            _ => Err("Wrong MonadEvent variant")?,
        }
    }
    handler.join().unwrap();
    Ok(())
}
