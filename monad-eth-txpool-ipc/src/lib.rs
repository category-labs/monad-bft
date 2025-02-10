use std::{
    io::{self, ErrorKind},
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolSnapshot};
use tokio::{net::UnixStream, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::warn;

pub struct EthTxPoolIpcStream {
    // It's really ugly to emulate Sink for a Framed<UnixStream, ...> in a sync
    // context, so the next best option is to have the stream live on a tokio
    // thread for now ...
    // TODO(andr-dev): Remove tokio_util and write a custom sync framer/codec
    // implementation simlar to LengthDelimnitedCodec
    tx: mpsc::Sender<Vec<EthTxPoolEvent>>,
    rx: ReceiverStream<Recovered<TxEnvelope>>,

    handle: tokio::task::JoinHandle<io::Result<()>>,
}

impl EthTxPoolIpcStream {
    pub fn new(stream: UnixStream, snapshot: EthTxPoolSnapshot) -> Self {
        let (batch_tx, batch_rx) = mpsc::channel(8 * 1024);
        let (event_tx, event_rx) = mpsc::channel(8 * 1024);

        Self {
            tx: event_tx,
            rx: ReceiverStream::new(batch_rx),

            handle: tokio::spawn(Self::run(stream, snapshot, batch_tx, event_rx)),
        }
    }

    async fn run(
        stream: UnixStream,
        snapshot: EthTxPoolSnapshot,
        tx_sender: mpsc::Sender<Recovered<TxEnvelope>>,
        mut event_rx: mpsc::Receiver<Vec<EthTxPoolEvent>>,
    ) -> io::Result<()> {
        let mut stream = Framed::new(stream, LengthDelimitedCodec::default());

        let snapshot_bytes = bincode::serialize(&snapshot).expect("snapshot is serializable");

        stream.send(snapshot_bytes.into()).await?;

        loop {
            tokio::select! {
                result = stream.next() => {
                    let Some(result) = result else {
                        break;
                    };

                    let Ok(tx) = alloy_rlp::decode_exact::<TxEnvelope>(result?.as_ref()) else {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "EthTxPoolIpcStream received invalid tx serialized bytes!"
                        ));
                    };

                    let Ok(signer) = tx.recover_signer() else {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "EthTxPoolIpcStream received tx with invalid signer!"
                        ));
                    };

                    let tx = Recovered::new_unchecked(tx, signer);

                    let Err(error) = tx_sender.try_send(tx) else {
                        continue;
                    };

                    match error {
                        mpsc::error::TrySendError::Full(_) => {
                            // TODO(andr-dev): Make "overloaded" IPC type that RPC can monitor to pace out
                            // tx sends
                            warn!("dropping tx, reason: channel full");
                        },
                        mpsc::error::TrySendError::Closed(_) => break,
                    }
                }

                result = event_rx.recv() => {
                    let Some(events) = result else {
                        break;
                    };

                    let events = bincode::serialize(&events).expect("txpool events are serializable");

                    stream.send(events.into()).await?;
                }
            }
        }

        Ok(())
    }

    pub fn send_tx_events(
        &self,
        events: Vec<EthTxPoolEvent>,
    ) -> Result<(), mpsc::error::TrySendError<Vec<EthTxPoolEvent>>> {
        self.tx.try_send(events)
    }
}

impl Stream for EthTxPoolIpcStream {
    type Item = Recovered<TxEnvelope>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.handle.poll_unpin(cx) {
            match result {
                Ok(Ok(())) => {
                    warn!("txpool stream handler exited");
                }
                Ok(Err(error)) => {
                    warn!("txpool stream crashed, reason: {error:?}")
                }
                Err(error) => {
                    warn!("txpool stream crashed, reason: {error:?}")
                }
            }

            return Poll::Ready(None);
        }

        self.rx.poll_next_unpin(cx)
    }
}

pub struct EthTxPoolIpcClient {
    stream: Framed<UnixStream, LengthDelimitedCodec>,
}

impl EthTxPoolIpcClient {
    pub async fn new<P>(path: P) -> io::Result<(Self, EthTxPoolSnapshot)>
    where
        P: AsRef<Path>,
    {
        let stream = UnixStream::connect(path).await?;
        let mut stream = Framed::new(stream, LengthDelimitedCodec::default());

        let snapshot_bytes = stream.next().await.ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "EthTxPoolIpcClient must recieve snapshot on connectino",
            )
        })??;

        let snapshot = bincode::deserialize::<EthTxPoolSnapshot>(&snapshot_bytes).unwrap();

        Ok((Self { stream }, snapshot))
    }
}

impl<'a> Sink<&'a TxEnvelope> for EthTxPoolIpcClient {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.stream.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, tx: &'a TxEnvelope) -> Result<(), Self::Error> {
        let bytes = alloy_rlp::encode(tx);
        self.stream.start_send_unpin(bytes.into())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.stream.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.stream.poll_close_unpin(cx)
    }
}

impl Stream for EthTxPoolIpcClient {
    type Item = Vec<EthTxPoolEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Poll::Ready(result) = self.stream.poll_next_unpin(cx) else {
            return Poll::Pending;
        };

        let Some(bytes) = result.transpose().ok().flatten() else {
            return Poll::Ready(None);
        };

        let Ok(event) = bincode::deserialize(bytes.as_ref()) else {
            return Poll::Ready(None);
        };

        Poll::Ready(Some(event))
    }
}
