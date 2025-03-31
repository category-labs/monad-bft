use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use alloy_consensus::TxEnvelope;
use futures::{ready, Future, Sink, SinkExt, Stream, StreamExt};
use monad_eth_txpool_ipc::EthTxPoolIpcClient;
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolSnapshot};
use pin_project::pin_project;
use tokio::pin;

use self::socket::SocketWatcher;
pub use self::state::{EthTxPoolBridgeState, TxStatus};

mod socket;
mod state;

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
    client: EthTxPoolIpcClient,
    #[pin]
    ipc_state: EthTxPoolBridgeIpcState,

    state: Arc<EthTxPoolBridgeState>,
}

impl EthTxPoolBridge {
    pub async fn new<P>(bind_path: P, state: Arc<EthTxPoolBridgeState>) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let socket_path = bind_path.as_ref().to_path_buf();

        let (client, snapshot) = EthTxPoolIpcClient::new(bind_path).await?;
        state.apply_snapshot(snapshot);

        Ok(Self {
            socket_path,
            client,
            ipc_state: EthTxPoolBridgeIpcState::Ready,

            state,
        })
    }
}

impl<'a> Sink<&'a TxEnvelope> for EthTxPoolBridge {
    type Error = io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.client.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        tx: &'a TxEnvelope,
    ) -> Result<(), Self::Error> {
        self.client.start_send_unpin(tx)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut().project();
        let mut ipc_state = this.ipc_state.as_mut();

        match ipc_state.as_mut().project() {
            EthTxPoolBridgeIpcStateProjection::Ready => {
                let writer: Poll<Result<(), std::io::Error>> = this.client.poll_flush_unpin(cx);
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
                        let (client, snapshot) = result?;
                        this.state.apply_snapshot(snapshot);

                        *this.client = client;
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
        self.client.poll_close_unpin(cx)
    }
}

impl Stream for EthTxPoolBridge {
    type Item = Vec<EthTxPoolEvent>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.client.poll_next_unpin(cx)
    }
}
