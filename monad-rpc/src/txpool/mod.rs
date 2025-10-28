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
use state::TxStatusSender;
use tokio::pin;
use tracing::warn;

pub use self::{client::EthTxPoolBridgeClient, handle::EthTxPoolBridgeHandle, types::TxStatus};
use self::{
    socket::SocketWatcher,
    state::{EthTxPoolBridgeEvictionQueue, EthTxPoolBridgeState},
};

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
    eviction_queue: EthTxPoolBridgeEvictionQueue,
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

        let mut eviction_queue = EthTxPoolBridgeEvictionQueue::default();
        let state: EthTxPoolBridgeState = EthTxPoolBridgeState::new(&mut eviction_queue, snapshot);

        let (tx_sender, tx_receiver) = flume::bounded(1024);

        let client = EthTxPoolBridgeClient::new(tx_sender, state.create_view());

        let bridge = Self {
            socket_path,
            ipc_client,
            ipc_state: EthTxPoolBridgeIpcState::Ready,

            state,
            eviction_queue,
        };

        let handle = EthTxPoolBridgeHandle::new(tokio::task::spawn(bridge.run(tx_receiver)));

        Ok((client, handle))
    }

    async fn run(mut self, tx_receiver: Receiver<(TxEnvelope, TxStatusSender, bool)>) {
        let mut cleanup_timer = tokio::time::interval(Duration::from_secs(5));

        cleanup_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let err = loop {
            tokio::select! {
                result = tx_receiver.recv_async() => {
                    let tx_triplet = match result {
                        Ok(tx_triplet) => tx_triplet,
                        Err(e) => break e,
                    };

                    for (tx, tx_status_send, bypass_transfer_balance_check) in std::iter::once(tx_triplet).chain(tx_receiver.drain()) {
                        self.state.add_tx(&mut self.eviction_queue, &tx, tx_status_send);

                        if let Err(e) = self.feed(&(tx, bypass_transfer_balance_check)).await {
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

                    self.state.handle_events(&mut self.eviction_queue, events);
                }

                now = cleanup_timer.tick() => {
                    self.state.cleanup(&mut self.eviction_queue, now);
                }
            }
        };

        warn!(?err, "TxPoolBridge shutting down")
    }
}

impl<'a> Sink<&'a (TxEnvelope, bool)> for EthTxPoolBridge {
    type Error = io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.ipc_client.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        tx_with_flag: &'a (TxEnvelope, bool),
    ) -> Result<(), Self::Error> {
        self.ipc_client.start_send_unpin(tx_with_flag)
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
                        this.state.apply_snapshot(this.eviction_queue, snapshot);

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
