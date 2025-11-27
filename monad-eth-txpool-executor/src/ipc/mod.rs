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
    collections::{BTreeMap, VecDeque},
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use alloy_primitives::{TxHash, U256};
use futures::StreamExt;
use monad_eth_txpool_ipc::{EthTxPoolIpcStream, EthTxPoolIpcTx};
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolEventType, EthTxPoolSnapshot};
use pin_project::pin_project;
use tokio::{
    net::UnixListener,
    time::{self, Sleep},
};
use tracing::{info, warn};

pub use self::config::EthTxPoolIpcConfig;

mod config;

const MAX_BATCH_LEN: usize = 128;
const BATCH_TIMER_INTERVAL_MS: u64 = 8;

#[pin_project(project = EthTxPoolIpcServerProjected)]
pub struct EthTxPoolIpcServer {
    #[pin]
    listener: UnixListener,

    connections: Vec<EthTxPoolIpcStream>,

    queue: BTreeMap<U256, VecDeque<EthTxPoolIpcTx>>,
    queue_len: usize,
    #[pin]
    queue_timer: Sleep,
}

impl EthTxPoolIpcServer {
    pub fn new(
        EthTxPoolIpcConfig {
            bind_path,
            tx_batch_size: _,
            max_queued_batches,
            queued_batches_watermark,
        }: EthTxPoolIpcConfig,
    ) -> Result<Self, io::Error> {
        assert!(queued_batches_watermark <= max_queued_batches);

        let listener = UnixListener::bind(bind_path)?;

        Ok(Self {
            listener,

            connections: Vec::default(),

            queue: BTreeMap::default(),
            queue_len: 0,
            queue_timer: time::sleep(Duration::ZERO),
        })
    }

    pub fn broadcast_tx_events(self: Pin<&mut Self>, events: BTreeMap<TxHash, EthTxPoolEventType>) {
        if events.is_empty() {
            return;
        }

        let events: Vec<EthTxPoolEvent> = events
            .into_iter()
            .map(|(tx_hash, action)| EthTxPoolEvent { tx_hash, action })
            .collect();

        self.project()
            .connections
            .retain(|stream| match stream.send_tx_events(events.clone()) {
                Ok(()) => true,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    warn!("dropping ipc stream, reason: channel full!");
                    false
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    info!("dropping ipc stream, reason: channel closed!");
                    false
                }
            });
    }

    pub fn poll_txs(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        generate_snapshot: impl Fn() -> EthTxPoolSnapshot,
    ) -> Poll<Vec<EthTxPoolIpcTx>> {
        let EthTxPoolIpcServerProjected {
            listener,

            connections,

            queue,
            queue_len,
            mut queue_timer,
        } = self.project();

        while let Poll::Ready(result) = listener.poll_accept(cx) {
            match result {
                Err(error) => {
                    warn!("listener poll accept error={error:?}");
                    continue;
                }
                Ok((stream, _)) => {
                    connections.push(EthTxPoolIpcStream::new(stream, generate_snapshot()));
                }
            }
        }

        let queue_was_empty = *queue_len == 0;

        connections.retain_mut(|stream| {
            loop {
                if *queue_len >= MAX_BATCH_LEN {
                    break;
                }

                let Poll::Ready(result) = stream.poll_next_unpin(cx) else {
                    break;
                };

                let Some(tx) = result else {
                    return false;
                };

                queue.entry(tx.priority).or_default().push_back(tx);
                *queue_len += 1;
            }

            true
        });

        if *queue_len == 0 {
            return Poll::Pending;
        }

        if queue_was_empty {
            queue_timer.set(time::sleep(Duration::from_millis(BATCH_TIMER_INTERVAL_MS)));
        }

        if *queue_len < MAX_BATCH_LEN && queue_timer.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }

        let mut batch = Vec::default();

        while let Some(batch_remaining_capacity) = MAX_BATCH_LEN.checked_sub(batch.len()) {
            if batch_remaining_capacity == 0 {
                break;
            }

            let Some(top_priority) = queue.last_entry() else {
                break;
            };

            if batch_remaining_capacity < top_priority.get().len() {
                batch.extend(top_priority.into_mut().drain(0..batch_remaining_capacity));
                break;
            } else {
                batch.extend(top_priority.remove());
            }
        }

        *queue_len -= batch.len();

        Poll::Ready(batch)
    }
}
