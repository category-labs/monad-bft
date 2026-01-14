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
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use flume::Receiver;
use futures::{SinkExt, StreamExt};
use monad_eth_txpool_ipc::{EthTxPoolIpcClient, EthTxPoolIpcTx};
use state::TxStatusReceiverSender;
use tracing::{debug, error, info, warn};

pub use self::{client::EthTxPoolBridgeClient, handle::EthTxPoolBridgeHandle, types::TxStatus};
use self::{
    socket::SocketWatcher,
    state::{EthTxPoolBridgeEvictionQueue, EthTxPoolBridgeState},
};
use crate::txpool::socket::SocketWatcherEvent;

mod client;
mod handle;
mod socket;
mod state;
mod types;

pub const ETH_TXPOOL_BRIDGE_CHANNEL_SIZE: usize = 1024;

pub struct EthTxPoolBridge {
    ipc_client: Option<EthTxPoolIpcClient>,

    bind_path: PathBuf,
    socket_watcher: SocketWatcher,

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
        let (ipc_client, snapshot) = EthTxPoolIpcClient::new(&bind_path).await?;

        let mut eviction_queue = EthTxPoolBridgeEvictionQueue::default();
        let state: EthTxPoolBridgeState = EthTxPoolBridgeState::new(&mut eviction_queue, snapshot);

        let (tx_sender, tx_receiver) = flume::bounded(ETH_TXPOOL_BRIDGE_CHANNEL_SIZE);

        let client = EthTxPoolBridgeClient::new(tx_sender, state.create_view());

        let socket_watcher = SocketWatcher::try_new(&bind_path)?;

        let bridge = Self {
            ipc_client: Some(ipc_client),

            bind_path: bind_path.as_ref().to_path_buf(),
            socket_watcher,

            state,
            eviction_queue,
        };

        let handle = EthTxPoolBridgeHandle::new(tokio::task::spawn(bridge.run(tx_receiver)));

        Ok((client, handle))
    }

    async fn run(mut self, tx_receiver: Receiver<(TxEnvelope, TxStatusReceiverSender)>) {
        let mut cleanup_timer = tokio::time::interval(Duration::from_secs(5));

        cleanup_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let err = loop {
            let ipc_client = match self.ipc_client.as_mut() {
                Some(ipc_client) => ipc_client,
                None => match self.try_create_ipc_client().await {
                    Err(err) => break err,
                    Ok(()) => {
                        tx_receiver.drain();
                        continue;
                    }
                },
            };

            tokio::select! {
                biased;

                result = ipc_client.next() => {
                    if let Some(events) = result {
                        self.state.handle_events(&mut self.eviction_queue, events);
                    } else {
                        error!("TxPoolBridge txpool ipc client died, trying to reconnect");

                        if let Err(err) = self.try_reconnect().await {
                            error!(?err, "TxPoolBridge txpool ipc client failed to reconnect, falling back to SocketWatcher");
                            self.ipc_client = None;
                        }
                    }
                }

                result = tx_receiver.recv_async() => {
                    let tx_pair = match result {
                        Ok(tx_pair) => tx_pair,
                        Err(flume::RecvError::Disconnected) => {
                            error!("TxPoolBridge tx receiver disconnected");
                            break None;
                        },
                    };

                    for (tx, tx_status_recv_send) in std::iter::once(tx_pair).chain(tx_receiver.drain()) {
                        if !self.state.add_tx(&mut self.eviction_queue, &tx, tx_status_recv_send) {
                            continue;
                        }

                        if let Err(e) = ipc_client.feed(EthTxPoolIpcTx::new_with_default_priority(
                            tx,
                            Vec::default(),
                        )).await {
                            warn!("TxPoolBridge IPC feed failed, monad-bft likely crashed: {}", e);
                        }
                    }

                    if let Err(e) = ipc_client.flush().await {
                        error!("TxPoolBridge IPC flush failed, monad-bft likely crashed: {}", e);
                    }
                }

                result = self.socket_watcher.next() => {
                    let Some(result) = result else {
                        error!("TxPoolBridge SocketWatcher died while txpool ipc client alive");
                        break None;
                    };

                    match result {
                        Err(err) => {
                            error!(?err, "TxPoolBridge SocketWatcher error while txpool ipc client alive");
                            break Some(err);
                        },
                        Ok(event) => {
                            warn!(?event, "TxPoolBridge SocketWatcher detected event while txpool ipc client alive");
                        }
                    }
                }

                now = cleanup_timer.tick() => {
                    debug!("TxPoolBridge running state cleanup");
                    self.state.cleanup(&mut self.eviction_queue, now);
                }
            }
        };

        warn!(?err, "TxPoolBridge shutting down")
    }

    async fn try_create_ipc_client(&mut self) -> Result<(), Option<io::Error>> {
        loop {
            let Some(result) = self.socket_watcher.next().await else {
                error!("TxPoolBridge SocketWatcher died while attempting to reconnect");
                return Err(None);
            };

            let event = match result {
                Err(err) => {
                    error!(
                        ?err,
                        "TxPoolBridge SocketWatcher error while attempting to reconnect"
                    );
                    return Err(Some(err));
                }
                Ok(event) => event,
            };

            match event {
                SocketWatcherEvent::Create(bind_path) => {
                    if self.bind_path != bind_path {
                        error!(
                            expected_bind_path =? self.bind_path,
                            socket_watcher_bind_path =? bind_path,
                            "TxPoolBridge received different socket bind path from SocketWatcher"
                        );
                        return Err(None);
                    }

                    match self.try_reconnect().await {
                        Ok(()) => return Ok(()),
                        Err(err) => {
                            error!(?err, "TxPoolBridge failed to reconnect txpool ipc client");
                        }
                    }
                }
                SocketWatcherEvent::Delete => {
                    info!(
                        ?event,
                        "TxPoolBridge detected socket delete event while trying to reconnect"
                    );
                }
            }
        }
    }

    async fn try_reconnect(&mut self) -> io::Result<()> {
        info!(bind_path =? self.bind_path, "TxPoolBridge reconnecting txpool ipc client");

        let (ipc_client, snapshot) = EthTxPoolIpcClient::new(&self.bind_path).await?;

        info!("TxPoolBridge txpool ipc client reconnected");

        self.ipc_client = Some(ipc_client);

        self.state
            .apply_snapshot(&mut self.eviction_queue, snapshot);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use flume::TrySendError;
    use futures::StreamExt;
    use itertools::Itertools;
    use monad_eth_testutil::{make_legacy_tx, S1, S2};
    use monad_eth_txpool_ipc::EthTxPoolIpcStream;
    use monad_eth_txpool_types::EthTxPoolSnapshot;
    use tempfile::TempDir;
    use test_case::test_matrix;
    use tokio::{net::UnixListener, time::Duration};

    use super::{
        client::EthTxPoolBridgeClient, handle::EthTxPoolBridgeHandle,
        state::EthTxPoolBridgeStateView, EthTxPoolBridge, TxStatus,
    };

    const BASE_FEE_PER_GAS: u64 = 100_000_000_000;

    #[test_matrix([1, 16, 64, 128, 1024])]
    fn test_client_acquire_tx_inflight_guard_up_to_capacity(capacity: usize) {
        let (tx_sender, _rx) = flume::bounded(capacity);

        let state_view = EthTxPoolBridgeStateView::for_testing();
        let client = EthTxPoolBridgeClient::new(tx_sender, state_view);

        // We intentionally only allow up to capacity - 1
        let guards = (0..(capacity - 1))
            .map(|_| {
                client
                    .acquire_tx_inflight_guard()
                    .expect("Can acquire up to capacity")
            })
            .collect_vec();

        assert!(client.acquire_tx_inflight_guard().is_none());

        drop(guards);
    }

    #[test]
    fn test_client_try_send_disconnected() {
        let (tx_sender, rx) = flume::bounded(1);
        let state_view = EthTxPoolBridgeStateView::for_testing();
        let client = EthTxPoolBridgeClient::new(tx_sender, state_view);

        // Drop the receiver to disconnect
        drop(rx);

        let tx = make_legacy_tx(S1, BASE_FEE_PER_GAS.into(), 100_000, 0, 0);
        let (status_sender, _status_recv) = tokio::sync::oneshot::channel();

        let result = client.try_send(tx, status_sender);
        assert!(matches!(result, Err(TrySendError::Disconnected(_))));
    }

    #[test]
    fn test_client_try_send_success() {
        let (tx_sender, rx) = flume::bounded(1);
        let state_view = EthTxPoolBridgeStateView::for_testing();
        let client = EthTxPoolBridgeClient::new(tx_sender, state_view);

        let tx = make_legacy_tx(S1, BASE_FEE_PER_GAS.into(), 100_000, 0, 0);
        let tx_hash = *tx.tx_hash();
        let (status_sender, _status_recv) = tokio::sync::oneshot::channel();

        let result = client.try_send(tx, status_sender);
        assert!(result.is_ok());

        let (received_tx, _) = rx.try_recv().unwrap();
        assert_eq!(*received_tx.tx_hash(), tx_hash);
    }

    #[tokio::test]
    async fn test_handle_awaits_task_completion() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = EthTxPoolBridgeHandle::new(tokio::task::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            completed_clone.store(true, Ordering::SeqCst);
        }));

        handle.await;

        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_bridge_reconnects_after_ipc_client_dies() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let socket_path = temp_dir.path().join("test.sock");

        // Start persistent listener that will handle reconnections
        let listener = UnixListener::bind(&socket_path).expect("Failed to bind socket");
        let listener = Arc::new(listener);
        let listener_clone = listener.clone();

        // Spawn task to handle first connection
        let first_connection = tokio::spawn(async move {
            let (stream, _) = listener_clone.accept().await.expect("Failed to accept");
            let ipc_stream = EthTxPoolIpcStream::new(
                stream,
                EthTxPoolSnapshot {
                    txs: HashSet::default(),
                },
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(ipc_stream); // Simulate crash
        });

        let (client, _handle) = EthTxPoolBridge::start(&socket_path)
            .await
            .expect("Bridge should start");

        // Wait for first connection to crash
        first_connection
            .await
            .expect("First connection task failed");

        // Bridge should attempt immediate reconnection (socket still exists)
        let accept_result = tokio::time::timeout(Duration::from_secs(2), listener.accept()).await;

        assert!(
            accept_result.is_ok(),
            "Bridge should attempt to reconnect after IPC client dies"
        );

        // Clean up
        drop(client);
    }

    #[tokio::test]
    async fn test_bridge_handles_rapid_reconnects() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let socket_path = temp_dir.path().join("test.sock");

        let listener = UnixListener::bind(&socket_path).expect("Failed to bind socket");
        let listener = Arc::new(listener);
        let listener_clone = listener.clone();

        let connections = tokio::spawn(async move {
            // Simulate multiple rapid disconnects and reconnects
            for _ in 0..3 {
                let (stream, _) =
                    tokio::time::timeout(Duration::from_secs(2), listener_clone.accept())
                        .await
                        .expect("Should accept connection")
                        .expect("Failed to accept connection");

                let ipc_stream = EthTxPoolIpcStream::new(
                    stream,
                    EthTxPoolSnapshot {
                        txs: HashSet::default(),
                    },
                );

                drop(ipc_stream);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        let (client, handle) = EthTxPoolBridge::start(&socket_path)
            .await
            .expect("Bridge should start");

        connections.await.unwrap();

        // Final reconnection should still work
        let accept_result = tokio::time::timeout(Duration::from_secs(2), listener.accept()).await;
        assert!(
            accept_result.is_ok(),
            "Should reconnect after rapid disconnects"
        );

        drop(handle);
        drop(client);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bridge_handles_transactions_from_client() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let socket_path = temp_dir.path().join("test.sock");

        let listener = UnixListener::bind(&socket_path).expect("Failed to bind socket");

        let ipc_task = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("Failed to accept connection");
            let mut ipc_stream = EthTxPoolIpcStream::new(
                stream,
                EthTxPoolSnapshot {
                    txs: HashSet::default(),
                },
            );

            // The IPC stream should receive the transaction
            let received_tx = tokio::time::timeout(Duration::from_millis(500), ipc_stream.next())
                .await
                .expect("Should receive transaction within timeout")
                .expect("Stream should not be closed");

            (received_tx, ipc_stream)
        });

        let (client, _handle) = EthTxPoolBridge::start(&socket_path)
            .await
            .expect("Bridge should start");

        // Send a transaction through the client
        let tx = make_legacy_tx(S1, BASE_FEE_PER_GAS.into(), 100_000, 0, 0);
        let tx_hash = *tx.tx_hash();
        let (status_sender, mut status_recv) = tokio::sync::oneshot::channel();

        let result = client.try_send(tx.clone(), status_sender);
        assert!(result.is_ok(), "Transaction should be sent successfully");

        let (received_tx, ipc_stream) = ipc_task.await.expect("IPC task should complete");

        assert_eq!(
            *received_tx.tx.tx_hash(),
            tx_hash,
            "Received transaction should match sent transaction"
        );

        // Status should be available
        let status_receiver = status_recv
            .try_recv()
            .expect("Status receiver should be available");
        assert_eq!(
            *status_receiver.borrow(),
            TxStatus::Unknown,
            "Initial status should be Unknown"
        );

        // Clean up
        drop(ipc_stream);
        drop(client);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bridge_applies_snapshot_on_reconnect() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let socket_path = temp_dir.path().join("test.sock");

        let tx1 = make_legacy_tx(S1, BASE_FEE_PER_GAS.into(), 100_000, 0, 0);
        let tx2 = make_legacy_tx(S2, BASE_FEE_PER_GAS.into(), 100_000, 0, 0);
        let tx1_hash = *tx1.tx_hash();
        let tx2_hash = *tx2.tx_hash();

        // Start with a snapshot containing tx1
        let listener = UnixListener::bind(&socket_path).expect("Failed to bind socket");
        let listener = Arc::new(listener);
        let listener_clone = listener.clone();

        // Spawn task to handle first connection with tx1 in snapshot
        let ipc_task1 = tokio::spawn(async move {
            let (stream1, _) = listener_clone
                .accept()
                .await
                .expect("Failed to accept connection");
            let ipc_stream1 = EthTxPoolIpcStream::new(
                stream1,
                EthTxPoolSnapshot {
                    txs: HashSet::from_iter([tx1_hash]),
                },
            );

            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(ipc_stream1);
        });

        let (client, _handle) = EthTxPoolBridge::start(&socket_path)
            .await
            .expect("Bridge should start");

        // Verify tx1 is tracked
        tokio::time::sleep(Duration::from_millis(50)).await;
        let tx1_status = client.get_status_by_hash(&tx1_hash);
        assert_eq!(
            tx1_status,
            Some(TxStatus::Tracked),
            "tx1 should be tracked initially"
        );

        // Wait for simulated crash
        ipc_task1.await.expect("IPC task 1 should complete");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Spawn task to handle reconnection with new snapshot (tx2 instead of tx1)
        let ipc_task2 = tokio::spawn(async move {
            let (stream2, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                .await
                .expect("Should reconnect within timeout")
                .expect("Failed to accept reconnection");

            let ipc_stream2 = EthTxPoolIpcStream::new(
                stream2,
                EthTxPoolSnapshot {
                    txs: HashSet::from_iter([tx2_hash]),
                },
            );

            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(ipc_stream2);
        });

        // Give time for snapshot to be applied
        tokio::time::sleep(Duration::from_millis(100)).await;

        // tx1 should be removed, tx2 should be tracked
        let tx1_status = client.get_status_by_hash(&tx1_hash);
        let tx2_status = client.get_status_by_hash(&tx2_hash);

        assert_eq!(tx1_status, None, "tx1 should be removed after reconnect");
        assert_eq!(
            tx2_status,
            Some(TxStatus::Tracked),
            "tx2 should be tracked after reconnect"
        );

        // Clean up
        ipc_task2.await.expect("IPC task 2 should complete");
        drop(client);
    }
}
