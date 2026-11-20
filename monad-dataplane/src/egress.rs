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

//! Shared TCP/UDP egress scheduling and UDP worker dispatch.

use std::{net::SocketAddr, num::NonZeroU64, time::Instant};

use async_channel::Sender;
#[cfg(test)]
use monoio::spawn;
use monoio::{select, time::sleep};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tracing::{debug, trace};

use crate::{
    metrics::DataplaneMetrics,
    pacing::{PacingItem, PacingQueue, PacingRequest},
    udp::UdpSend,
    UdpMsg, UdpPacingConfig,
};

pub(crate) const MAX_BURST_SIZE: usize = 128 * 1024;
const MAX_BATCH_ITEMS: usize = 128;
const MAX_CHANNEL_DRAIN: usize = 64;

pub(crate) enum PacedMessage {
    Udp(UdpMsg),
    Tcp {
        addr: SocketAddr,
        priority: monad_types::EgressPriority,
        request: PacingRequest,
    },
}

impl PacedMessage {
    fn addr(&self) -> SocketAddr {
        match self {
            Self::Udp(message) => message.dst,
            Self::Tcp { addr, .. } => *addr,
        }
    }

    fn priority(&self) -> monad_types::EgressPriority {
        match self {
            Self::Udp(message) => message.priority,
            Self::Tcp { priority, .. } => *priority,
        }
    }
}

impl From<UdpMsg> for PacedMessage {
    fn from(message: UdpMsg) -> Self {
        Self::Udp(message)
    }
}

impl PacingItem for PacedMessage {
    fn queued_bytes(&self) -> usize {
        match self {
            Self::Udp(message) => message.queued_bytes(),
            Self::Tcp { request, .. } => request.queued_bytes(),
        }
    }

    fn record_memory_limit_drop(&self, metrics: &DataplaneMetrics) {
        match self {
            Self::Udp(message) => message.record_memory_limit_drop(metrics),
            Self::Tcp { request, .. } => request.record_memory_limit_drop(metrics),
        }
    }

    fn next_payload_bytes(&self) -> usize {
        match self {
            Self::Udp(message) => message.next_payload_bytes(),
            Self::Tcp { request, .. } => request.next_payload_bytes(),
        }
    }

    fn next_pacing_bytes(&self) -> usize {
        match self {
            Self::Udp(message) => message.next_pacing_bytes(),
            Self::Tcp { request, .. } => request.next_pacing_bytes(),
        }
    }
}

fn enqueue_message(
    queue: &mut PacingQueue<PacedMessage>,
    metrics: &DataplaneMetrics,
    message: PacedMessage,
) {
    let destination = message.addr();
    if matches!(&message, PacedMessage::Udp(_)) && destination.is_ipv6() {
        metrics.udp_egress_messages_dropped.inc();
        debug!(?destination, "IPv6 UDP message is not supported");
        return;
    }
    let _ = queue.enqueue(destination.ip(), message.priority(), message);
}

pub(crate) async fn task(
    dispatch_tx: Sender<Vec<UdpSend>>,
    mut egress_rx: mpsc::Receiver<PacedMessage>,
    up_bandwidth_mbps: u64,
    pacing_config: UdpPacingConfig,
    metrics: DataplaneMetrics,
) {
    let global_bytes_per_second = NonZeroU64::new(
        u64::try_from(u128::from(up_bandwidth_mbps) * 1_000_000 / 8)
            .expect("egress bandwidth overflows bytes per second"),
    )
    .expect("egress bandwidth must be non-zero");
    let peer_bytes_per_second = NonZeroU64::new(
        u64::try_from(u128::from(pacing_config.peer_bandwidth_mbps) * 1_000_000 / 8)
            .expect("egress peer bandwidth overflows bytes per second"),
    )
    .expect("egress peer bandwidth must be non-zero");
    let mut queue = PacingQueue::new(
        global_bytes_per_second,
        peer_bytes_per_second,
        pacing_config.max_queued_bytes,
        metrics.clone(),
    );
    let max_batch_items = MAX_BATCH_ITEMS;
    let mut incoming = Vec::with_capacity(MAX_CHANNEL_DRAIN);

    loop {
        for _ in 0..MAX_CHANNEL_DRAIN {
            match egress_rx.try_recv() {
                Ok(message) => enqueue_message(&mut queue, &metrics, message),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }

        if queue.is_empty() {
            incoming.clear();
            if egress_rx.recv_many(&mut incoming, MAX_CHANNEL_DRAIN).await == 0 {
                return;
            }
            for message in incoming.drain(..) {
                enqueue_message(&mut queue, &metrics, message);
            }
        }

        let now = queue.elapsed();
        let Some(at) = queue.next_wakeup(now) else {
            continue;
        };
        let delay = at.saturating_sub(now);
        if !delay.is_zero() {
            select! {
                message = egress_rx.recv() => {
                    let Some(message) = message else { return };
                    enqueue_message(&mut queue, &metrics, message);
                    continue;
                }
                _ = sleep(delay) => {}
            }
        }

        let now = queue.elapsed();
        if queue.next_wakeup(now).is_none_or(|at| at > now) {
            continue;
        }

        let queue_len = queue.len();
        let mut batch = Vec::with_capacity(max_batch_items);
        let mut burst_size = 0;
        for _ in 0..max_batch_items {
            if burst_size >= MAX_BURST_SIZE {
                break;
            }
            let Some(mut scheduled) = queue.dequeue(now, MAX_BURST_SIZE - burst_size) else {
                break;
            };
            burst_size += scheduled.batch_bytes;
            let msg = match &mut scheduled.item {
                PacedMessage::Udp(msg) => msg,
                PacedMessage::Tcp { request, .. } => {
                    request.grant();
                    continue;
                }
            };
            let chunk = msg.payload.split_to(scheduled.batch_bytes);
            let socket_id = msg.socket_id;
            let dst = msg.dst;

            if !msg.payload.is_empty() {
                assert!(
                    queue.requeue(dst.ip(), scheduled).is_ok(),
                    "requeueing admitted UDP bytes cannot exceed memory"
                );
            }

            trace!(?socket_id, dst_addr = ?dst, chunk_len = chunk.len(), "preparing udp send");
            batch.push(UdpSend {
                socket_id,
                dst,
                payload: chunk,
            });
        }

        if batch.is_empty() {
            continue;
        }
        let batch_count = batch.len();
        if batch_count > 1 {
            trace!(
                batch_size = batch_count,
                burst_size,
                queue_size = queue_len,
                "sending udp batch"
            );
        }

        let started = Instant::now();
        let result = dispatch_tx.send(batch).await;
        metrics
            .udp_tx_queue_wait_micros
            .add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        if let Err(error) = result {
            metrics
                .udp_egress_messages_dropped
                .add(u64::try_from(error.0.len()).unwrap_or(u64::MAX));
            return;
        }
    }
}

#[cfg(test)]
pub(crate) fn test_pacing() -> mpsc::Sender<PacedMessage> {
    let (sender, receiver) = mpsc::channel(crate::TCP_EGRESS_CHANNEL_SIZE);
    let (dispatch, received) = async_channel::bounded(1);
    spawn(task(
        dispatch,
        receiver,
        1_000,
        UdpPacingConfig::for_global_bandwidth(1_000),
        DataplaneMetrics::new(),
    ));
    spawn(async move { while received.recv().await.is_ok() {} });
    sender
}
