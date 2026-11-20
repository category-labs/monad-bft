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

//! UDP egress scheduling and worker dispatch.

use std::{net::SocketAddr, num::NonZeroU64, time::Instant};

use async_channel::Sender;
use monoio::{select, time::sleep};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tracing::{debug, trace};

use crate::{
    metrics::DataplaneMetrics,
    pacing::PacingQueue,
    udp::{
        max_write_size_for_segment_size, UdpSend, DEFAULT_SEGMENT_SIZE, MAX_AGGREGATED_SEGMENTS,
    },
    UdpMsg, UdpPacingConfig,
};

const MAX_CHANNEL_DRAIN: usize = 64;

fn enqueue_udp_msg(queue: &mut PacingQueue<UdpMsg>, metrics: &DataplaneMetrics, msg: UdpMsg) {
    let SocketAddr::V4(destination) = msg.dst else {
        metrics.udp_egress_messages_dropped.inc();
        debug!(destination = ?msg.dst, "IPv6 UDP message is not supported");
        return;
    };
    let priority = msg.priority;
    let _ = queue.enqueue(destination, priority, msg);
}

pub(crate) async fn task(
    dispatch_tx: Sender<Vec<UdpSend>>,
    mut udp_egress_rx: mpsc::Receiver<UdpMsg>,
    up_bandwidth_mbps: u64,
    pacing_config: UdpPacingConfig,
    metrics: DataplaneMetrics,
) {
    let global_bytes_per_second = NonZeroU64::new(
        u64::try_from(u128::from(up_bandwidth_mbps) * 1_000_000 / 8)
            .expect("UDP bandwidth overflows bytes per second"),
    )
    .expect("UDP bandwidth must be non-zero");
    let peer_bytes_per_second = NonZeroU64::new(
        u64::try_from(u128::from(pacing_config.peer_bandwidth_mbps) * 1_000_000 / 8)
            .expect("UDP peer bandwidth overflows bytes per second"),
    )
    .expect("UDP peer bandwidth must be non-zero");
    let mut queue = PacingQueue::new(
        global_bytes_per_second,
        peer_bytes_per_second,
        pacing_config.max_queued_bytes,
        metrics.clone(),
    );
    let max_batch_bytes = max_write_size_for_segment_size(DEFAULT_SEGMENT_SIZE) as usize;
    let max_batch_items = MAX_AGGREGATED_SEGMENTS as usize;
    let mut incoming = Vec::with_capacity(MAX_CHANNEL_DRAIN);

    loop {
        for _ in 0..MAX_CHANNEL_DRAIN {
            match udp_egress_rx.try_recv() {
                Ok(msg) => enqueue_udp_msg(&mut queue, &metrics, msg),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }

        if queue.is_empty() {
            incoming.clear();
            if udp_egress_rx
                .recv_many(&mut incoming, MAX_CHANNEL_DRAIN)
                .await
                == 0
            {
                return;
            }
            for msg in incoming.drain(..) {
                enqueue_udp_msg(&mut queue, &metrics, msg);
            }
        }

        let now = queue.elapsed();
        let Some(at) = queue.next_wakeup(now) else {
            continue;
        };
        let delay = at.saturating_sub(now);
        if !delay.is_zero() {
            select! {
                message = udp_egress_rx.recv() => {
                    let Some(message) = message else { return };
                    enqueue_udp_msg(&mut queue, &metrics, message);
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
        let mut total_bytes = 0;
        while batch.len() < max_batch_items && total_bytes < max_batch_bytes {
            let Some(mut scheduled) = queue.dequeue(now, max_batch_bytes - total_bytes) else {
                break;
            };
            let chunk = scheduled.item.payload.split_to(scheduled.batch_bytes);
            let msg = &scheduled.item;
            let socket_id = msg.socket_id;
            let dst = msg.dst;

            if !msg.payload.is_empty() {
                let SocketAddr::V4(destination) = dst else {
                    unreachable!("only IPv4 messages enter the pacing queue")
                };
                assert!(
                    queue.requeue(destination, scheduled).is_ok(),
                    "requeueing admitted UDP bytes cannot exceed memory"
                );
            }

            total_bytes += chunk.len();
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
                total_bytes,
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
