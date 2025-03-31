use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use flume::Receiver;
use futures::{ready, Future, Sink, SinkExt, Stream, StreamExt};
use monad_eth_txpool_ipc::EthTxPoolIpcClient;
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolSnapshot};
use pin_project::pin_project;
use tokio::pin;
use tracing::warn;

pub use self::{client::EthTxPoolBridgeClient, handle::EthTxPoolBridgeHandle, types::TxStatus};
use self::{socket::SocketWatcher, state::EthTxPoolBridgeState};

mod client;
mod handle;
mod socket;
mod state;
mod types;

#[pin_project(project = EthTxPoolBridgeIpcStateProjection)]
enum EthTxPoolBridgeIpcState {
    Ready,
    Reconnect(
        Pin<Box<dyn Future<Output = io::Result<(EthTxPoolIpcClient, EthTxPoolSnapshot)>> + Send>>,
    ),
    BrokenPipe(#[pin] SocketWatcher),
}

#[pin_project]
pub struct EthTxPoolBridge {
    socket_path: PathBuf,
    ipc_client: EthTxPoolIpcClient,
    #[pin]
    ipc_state: EthTxPoolBridgeIpcState,

    state: EthTxPoolBridgeState,
}

impl EthTxPoolBridge {
    pub async fn start<P>(
        bind_path: P,
    ) -> io::Result<(EthTxPoolBridgeClient, EthTxPoolBridgeHandle)>
    where
        P: AsRef<Path>,
    {
        let socket_path = bind_path.as_ref().to_path_buf();

        let (ipc_client, snapshot) = EthTxPoolIpcClient::new(bind_path).await?;

        let state: EthTxPoolBridgeState = EthTxPoolBridgeState::new(snapshot);

        let (tx_sender, tx_receiver) = flume::bounded(1024);

        let client = EthTxPoolBridgeClient::new(tx_sender, state.create_view());

        let bridge = Self {
            socket_path,
            ipc_client,
            ipc_state: EthTxPoolBridgeIpcState::Ready,

            state,
        };

        let handle = EthTxPoolBridgeHandle::new(tokio::task::spawn(bridge.run(tx_receiver)));

        Ok((client, handle))
    }

    async fn run(mut self, tx_receiver: Receiver<TxEnvelope>) {
        let mut cleanup_timer = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                result = tx_receiver.recv_async() => {
                    let tx = result.unwrap();

                    for tx in std::iter::once(tx).chain(tx_receiver.drain()) {
                        self.state.add_tx(&tx);

                        if let Err(e) = self.feed(&tx).await {
                            warn!("IPC feed failed, monad-bft likely crashed: {}", e);
                        }
                    }

                    if let Err(e) = self.flush().await {
                        warn!("IPC flush failed, monad-bft likely crashed: {}", e);
                    }
                }

                result = self.next() => {
                    let Some(events) = result else {
                        continue;
                    };

                    self.state.handle_events(events);
                }

                now = cleanup_timer.tick() => {
                    self.state.cleanup(now);
                }
            }
        }
    }
}

impl<'a> Sink<&'a TxEnvelope> for EthTxPoolBridge {
    type Error = io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.ipc_client.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        tx: &'a TxEnvelope,
    ) -> Result<(), Self::Error> {
        self.ipc_client.start_send_unpin(tx)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut().project();
        let mut ipc_state = this.ipc_state.as_mut();

        match ipc_state.as_mut().project() {
            EthTxPoolBridgeIpcStateProjection::Ready => {
                let writer: Poll<Result<(), std::io::Error>> = this.ipc_client.poll_flush_unpin(cx);
                match writer {
                    Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::BrokenPipe => {
                        let sw = SocketWatcher::try_new(this.socket_path.clone())?;
                        ipc_state.set(EthTxPoolBridgeIpcState::BrokenPipe(sw));
                        cx.waker().wake_by_ref();
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    v => return v,
                }
            }
            EthTxPoolBridgeIpcStateProjection::BrokenPipe(mut sw) => {
                ready!(sw.as_mut().poll(cx))?;
                ipc_state.set(EthTxPoolBridgeIpcState::Reconnect(Box::pin(
                    EthTxPoolIpcClient::new(this.socket_path.to_path_buf()),
                )));
                cx.waker().wake_by_ref();
            }
            EthTxPoolBridgeIpcStateProjection::Reconnect(task) => {
                match task.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        let (ipc_client, snapshot) = result?;
                        this.state.apply_snapshot(snapshot);

                        *this.ipc_client = ipc_client;
                        ipc_state.set(EthTxPoolBridgeIpcState::Ready);
                        cx.waker().wake_by_ref();
                    }
                    Poll::Pending => return Poll::Pending,
                };
            }
        }
        Poll::Pending
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.ipc_client.poll_close_unpin(cx)
    }
}

impl Stream for EthTxPoolBridge {
    type Item = Vec<EthTxPoolEvent>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.ipc_client.poll_next_unpin(cx)
    }
}
