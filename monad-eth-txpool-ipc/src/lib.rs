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

use std::{
    future::Future,
    io::{self, ErrorKind},
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolIpcTx, EthTxPoolSnapshot};
use tokio::{net::UnixStream, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::warn;

const TX_CHANNEL_SEND_TIMEOUT: Duration = Duration::from_millis(1_000);
const SOCKET_SEND_TIMEOUT: Duration = Duration::from_millis(1_000);

fn build_length_delimited_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(64 * 1024 * 1024)
        .new_codec()
}

pub struct EthTxPoolIpcStream {
    tx: mpsc::Sender<Vec<EthTxPoolEvent>>,
    rx: ReceiverStream<EthTxPoolIpcTx>,

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
        tx_sender: mpsc::Sender<EthTxPoolIpcTx>,
        mut event_rx: mpsc::Receiver<Vec<EthTxPoolEvent>>,
    ) -> io::Result<()> {
        let mut stream = Framed::new(stream, build_length_delimited_codec());

        let snapshot_bytes = bincode::serialize(&snapshot)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;

        stream.send(snapshot_bytes.into()).await?;

        loop {
            tokio::select! {
                biased;

                result = stream.next() => {
                    let Some(result) = result else {
                        break;
                    };

                    let Ok(tx) = alloy_rlp::decode_exact::<EthTxPoolIpcTx>(result?.as_ref()) else {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "EthTxPoolIpcStream received invalid tx serialized bytes!"
                        ));
                    };

                    if !Self::with_timeout(
                        tx_sender.send(tx),
                        TX_CHANNEL_SEND_TIMEOUT,
                        "tx channel",
                    ).await {
                        break;
                    }
                }

                result = event_rx.recv() => {
                    let Some(events) = result else {
                        break;
                    };

                    let events = bincode::serialize(&events)
                        .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;

                    if !Self::with_timeout(
                        stream.send(events.into()),
                        SOCKET_SEND_TIMEOUT,
                        "socket",
                    ).await {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn with_timeout<T, E: std::fmt::Debug>(
        fut: impl Future<Output = Result<T, E>>,
        timeout: Duration,
        context: &'static str,
    ) -> bool {
        match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(_)) => true,
            Ok(Err(err)) => {
                warn!(?err, "EthTxPoolIpcStream {context} error, disconnecting");
                false
            }
            Err(elapsed) => {
                warn!(
                    ?elapsed,
                    "EthTxPoolIpcStream {context} timed out, disconnecting"
                );
                false
            }
        }
    }

    pub fn send_tx_events(
        &self,
        events: Vec<EthTxPoolEvent>,
    ) -> Result<(), mpsc::error::TrySendError<Vec<EthTxPoolEvent>>> {
        if events.is_empty() {
            return Ok(());
        }

        self.tx.try_send(events)
    }
}

impl Stream for EthTxPoolIpcStream {
    type Item = EthTxPoolIpcTx;

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
        let mut stream = Framed::new(stream, build_length_delimited_codec());

        let snapshot_bytes = stream.next().await.ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "EthTxPoolIpcClient must receive snapshot on connection",
            )
        })??;

        let snapshot = bincode::deserialize::<EthTxPoolSnapshot>(&snapshot_bytes)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;

        Ok((Self { stream }, snapshot))
    }
}

impl Sink<EthTxPoolIpcTx> for EthTxPoolIpcClient {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.stream.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, tx: EthTxPoolIpcTx) -> Result<(), Self::Error> {
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

        let bytes = match result {
            None => return Poll::Ready(None),
            Some(Err(err)) => {
                warn!(?err, "EthTxPoolIpcClient stream error");
                return Poll::Ready(None);
            }
            Some(Ok(bytes)) => bytes,
        };

        match bincode::deserialize(bytes.as_ref()) {
            Ok(event) => Poll::Ready(Some(event)),
            Err(err) => {
                warn!(?err, "EthTxPoolIpcClient failed to deserialize event");
                Poll::Ready(None)
            }
        }
    }
}
