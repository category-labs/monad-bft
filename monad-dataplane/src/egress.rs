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
use bytes::Bytes;
#[cfg(test)]
use monoio::spawn;
use monoio::{select, time::sleep};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tracing::{debug, trace};

#[cfg(test)]
use crate::UdpSocketId;
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
fn test_pacer(
    capacity: usize,
    bandwidth_mbps: u64,
    config: UdpPacingConfig,
    metrics: DataplaneMetrics,
) -> (
    mpsc::Sender<PacedMessage>,
    async_channel::Receiver<Vec<UdpSend>>,
) {
    let (sender, receiver) = mpsc::channel(capacity);
    let (dispatch, received) = async_channel::bounded(1);
    spawn(task(dispatch, receiver, bandwidth_mbps, config, metrics));
    (sender, received)
}

#[cfg(test)]
pub(crate) fn test_pacing() -> mpsc::Sender<PacedMessage> {
    let (sender, received) = test_pacer(
        crate::TCP_EGRESS_CHANNEL_SIZE,
        1_000,
        UdpPacingConfig::for_global_bandwidth(1_000),
        DataplaneMetrics::new(),
    );
    spawn(async move { while received.recv().await.is_ok() {} });
    sender
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monoio::time;

    use super::*;

    fn queue<T: PacingItem>(global: u64, peer: u64, memory: usize) -> PacingQueue<T> {
        PacingQueue::new(
            NonZeroU64::new(global).unwrap(),
            NonZeroU64::new(peer).unwrap(),
            memory,
            DataplaneMetrics::new(),
        )
    }

    fn enqueue(queue: &mut PacingQueue<PacedMessage>, message: PacedMessage) {
        assert!(queue
            .enqueue(message.addr().ip(), message.priority(), message)
            .is_ok());
    }

    fn udp_message(
        dst: SocketAddr,
        payload: impl Into<Bytes>,
        priority: monad_types::EgressPriority,
    ) -> PacedMessage {
        let payload = payload.into();
        PacedMessage::Udp(UdpMsg {
            socket_id: UdpSocketId::Raptorcast,
            dst,
            stride: payload.len().try_into().unwrap(),
            payload,
            priority,
        })
    }

    #[test]
    fn ipv6_udp_is_dropped_before_admission() {
        let metrics = DataplaneMetrics::new();
        let mut queue = queue(1000, 1000, 1024);
        let addr = "[::1]:1".parse().unwrap();
        enqueue_message(
            &mut queue,
            &metrics,
            udp_message(
                addr,
                Bytes::from_static(b"udp"),
                monad_types::EgressPriority::High,
            ),
        );
        assert!(queue.is_empty());
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 1);

        let (ready, mut grants) = mpsc::channel(1);
        enqueue_message(
            &mut queue,
            &metrics,
            PacedMessage::Tcp {
                addr,
                priority: monad_types::EgressPriority::Background,
                request: PacingRequest::new(100, ready),
            },
        );
        let PacedMessage::Tcp { mut request, .. } =
            queue.dequeue(Duration::ZERO, MAX_BURST_SIZE).unwrap().item
        else {
            panic!("expected a TCP reservation");
        };
        request.grant();
        assert_eq!(grants.try_recv(), Ok(Ok(())));
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 1);
    }

    #[rstest::rstest]
    #[case::udp_high(
        monad_types::EgressPriority::High,
        monad_types::EgressPriority::Regular,
        true
    )]
    #[case::tcp_high(
        monad_types::EgressPriority::Regular,
        monad_types::EgressPriority::High,
        false
    )]
    #[case::regular_before_background(
        monad_types::EgressPriority::Regular,
        monad_types::EgressPriority::Background,
        true
    )]
    #[case::high_before_background(
        monad_types::EgressPriority::High,
        monad_types::EgressPriority::Background,
        true
    )]
    #[case::equal_fifo(
        monad_types::EgressPriority::Regular,
        monad_types::EgressPriority::Regular,
        false
    )]
    fn mixed_queue_preserves_caller_priority_and_fifo(
        #[case] udp_priority: monad_types::EgressPriority,
        #[case] tcp_priority: monad_types::EgressPriority,
        #[case] udp_first: bool,
        #[values(false, true)] same_peer: bool,
    ) {
        let mut queue = queue(1000, 1000, 1024);
        let addr = "127.0.0.1:1".parse().unwrap();
        let (ready, mut grants) = mpsc::channel(1);
        let tcp = PacedMessage::Tcp {
            addr,
            priority: tcp_priority,
            request: PacingRequest::new(100, ready),
        };
        enqueue(&mut queue, tcp);
        let udp = udp_message(
            if same_peer {
                "127.0.0.1:2"
            } else {
                "127.0.0.2:2"
            }
            .parse()
            .unwrap(),
            vec![1; 100],
            udp_priority,
        );
        enqueue(&mut queue, udp);
        let first = queue.dequeue(Duration::ZERO, MAX_BURST_SIZE).unwrap();
        assert_eq!(matches!(first.item, PacedMessage::Udp(_)), udp_first);
        let now = queue.next_wakeup(Duration::ZERO).unwrap();
        let second = queue.dequeue(now, MAX_BURST_SIZE).unwrap();
        assert_eq!(matches!(second.item, PacedMessage::Udp(_)), !udp_first);
        assert!(queue.is_empty());
        let mut tcp = if udp_first { second } else { first };
        let PacedMessage::Tcp { request, .. } = &mut tcp.item else {
            panic!("expected a TCP reservation")
        };
        request.grant();
        assert_eq!(grants.try_recv(), Ok(Ok(())));
    }

    #[rstest::rstest]
    #[case::same_ip("127.0.0.1:2")]
    #[case::different_ip("127.0.0.2:2")]
    fn tcp_chunks_share_service_across_connections(#[case] other_addr: &str) {
        let mut queue = queue(1_000_000, 1_000_000_000, 1024 * 1024);
        let addrs = ["127.0.0.1:1".parse().unwrap(), other_addr.parse().unwrap()];
        let (ready_a, grants_a) = mpsc::channel(1);
        let (ready_b, grants_b) = mpsc::channel(1);
        let ready = [ready_a, ready_b];
        let mut grants = [grants_a, grants_b];
        let reservation = |index: usize| PacedMessage::Tcp {
            addr: addrs[index],
            priority: monad_types::EgressPriority::Background,
            request: PacingRequest::new(64 * 1024, ready[index].clone()),
        };
        let mut grant_next = |queue: &mut PacingQueue<PacedMessage>, index: usize| {
            let at = queue.next_wakeup(Duration::ZERO).unwrap();
            let PacedMessage::Tcp {
                addr, mut request, ..
            } = queue.dequeue(at, MAX_BURST_SIZE).unwrap().item
            else {
                panic!("expected a TCP reservation");
            };
            assert_eq!(addr, addrs[index]);
            request.grant();
            assert_eq!(grants[index].try_recv(), Ok(Ok(())));
        };

        enqueue(&mut queue, reservation(0));
        grant_next(&mut queue, 0);
        enqueue(&mut queue, reservation(1));
        enqueue(&mut queue, reservation(0));
        grant_next(&mut queue, 1);
        enqueue(&mut queue, reservation(1));
        grant_next(&mut queue, 0);
        grant_next(&mut queue, 1);
        assert!(queue.is_empty());
    }

    #[rstest::rstest]
    #[case::global_limit(1000, 10_000, false)]
    #[case::shared_peer_limit(10_000, 1000, true)]
    fn tcp_reservations_and_udp_share_rate_limits(
        #[case] global: u64,
        #[case] peer: u64,
        #[case] same_peer: bool,
    ) {
        let mut queue = queue(global, peer, 4096);
        let addr = "127.0.0.1:1".parse().unwrap();
        let (ready, mut grants) = mpsc::channel(1);
        let tcp = PacedMessage::Tcp {
            addr,
            priority: monad_types::EgressPriority::Regular,
            request: PacingRequest::new(1000, ready),
        };
        enqueue(&mut queue, tcp);
        let mut scheduled = queue.dequeue(Duration::ZERO, MAX_BURST_SIZE).unwrap();
        let PacedMessage::Tcp { request, .. } = &mut scheduled.item else {
            panic!("expected reservation")
        };
        request.grant();
        assert_eq!(grants.try_recv(), Ok(Ok(())));
        let udp = udp_message(
            if same_peer {
                "127.0.0.1:2"
            } else {
                "127.0.0.2:2"
            }
            .parse()
            .unwrap(),
            vec![1; 100],
            monad_types::EgressPriority::High,
        );
        enqueue(&mut queue, udp);
        assert_eq!(
            queue.next_wakeup(Duration::ZERO),
            Some(Duration::from_secs(1))
        );
        assert!(matches!(
            queue
                .dequeue(Duration::from_secs(1), MAX_BURST_SIZE)
                .unwrap()
                .item,
            PacedMessage::Udp(_)
        ));
    }

    #[monoio::test(timer_enabled = true)]
    async fn pacing_grants_tcp_without_udp_and_reuses_the_reply_channel() {
        let (sender, dispatch_rx) = test_pacer(
            4,
            1000,
            UdpPacingConfig::for_global_bandwidth(1000),
            DataplaneMetrics::new(),
        );
        let (ready, mut grants) = mpsc::channel(1);
        for _ in 0..4 {
            assert!(sender
                .try_send(PacedMessage::Tcp {
                    addr: "127.0.0.1:1".parse().unwrap(),
                    priority: monad_types::EgressPriority::Regular,
                    request: PacingRequest::new(64 * 1024, ready.clone())
                })
                .is_ok());
            assert_eq!(
                time::timeout(Duration::from_secs(1), grants.recv())
                    .await
                    .unwrap(),
                Some(Ok(()))
            );
        }
        assert!(dispatch_rx.try_recv().is_err());
    }

    #[monoio::test(timer_enabled = true)]
    async fn new_udp_peer_sends_while_tcp_waits_for_its_peer_budget() {
        let (sender, dispatch_rx) = test_pacer(
            4,
            1000,
            UdpPacingConfig {
                peer_bandwidth_mbps: 1,
                max_queued_bytes: 1024 * 1024,
            },
            DataplaneMetrics::new(),
        );
        let (ready, mut grants) = mpsc::channel(1);
        let tcp = || PacedMessage::Tcp {
            addr: "127.0.0.1:1".parse().unwrap(),
            priority: monad_types::EgressPriority::Regular,
            request: PacingRequest::new(64 * 1024, ready.clone()),
        };
        assert!(sender.try_send(tcp()).is_ok());
        assert_eq!(
            time::timeout(Duration::from_secs(1), grants.recv())
                .await
                .unwrap(),
            Some(Ok(()))
        );
        assert!(sender.try_send(tcp()).is_ok());
        assert!(sender
            .try_send(udp_message(
                "127.0.0.2:2".parse().unwrap(),
                Bytes::from_static(b"high"),
                monad_types::EgressPriority::High
            ))
            .is_ok());
        let batch = time::timeout(Duration::from_millis(100), dispatch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].payload, Bytes::from_static(b"high"));
        assert!(matches!(grants.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(
            time::timeout(Duration::from_secs(1), grants.recv())
                .await
                .unwrap(),
            Some(Ok(()))
        );
    }

    #[test]
    fn tcp_reservation_waits_until_it_fits_the_remaining_burst() {
        let mut queue = queue(1_000_000, 1_000_000, 1024);
        let (ready, mut grants) = mpsc::channel(1);
        queue
            .enqueue(
                "::1".parse::<std::net::IpAddr>().unwrap(),
                monad_types::EgressPriority::Regular,
                PacingRequest::new(64 * 1024, ready),
            )
            .unwrap();
        assert!(queue.dequeue(Duration::ZERO, 64 * 1024 - 1).is_none());
        assert_eq!(queue.len(), 1);
        assert!(matches!(grants.try_recv(), Err(TryRecvError::Empty)));
        let mut scheduled = queue.dequeue(Duration::ZERO, MAX_BURST_SIZE).unwrap();
        assert_eq!(scheduled.batch_bytes, 64 * 1024);
        scheduled.item.grant();
        assert_eq!(grants.try_recv(), Ok(Ok(())));
        assert!(queue.is_empty());
    }

    #[monoio::test(timer_enabled = true)]
    async fn pacing_waits_for_worker_queue_space_and_counts_closed_queue_drops() {
        use futures::FutureExt;

        let metrics = DataplaneMetrics::new();
        let (sender, receiver) = mpsc::channel(4);
        let (dispatch_tx, dispatch_rx) = async_channel::bounded(1);
        dispatch_tx.try_send(Vec::new()).unwrap();
        let message = || UdpMsg {
            socket_id: UdpSocketId::Raptorcast,
            dst: "127.0.0.1:1".parse().unwrap(),
            payload: Bytes::from_static(&[1]),
            stride: 1,
            priority: monad_types::EgressPriority::High,
        };
        assert!(sender.try_send(message().into()).is_ok());
        let mut pacing = std::pin::pin!(task(
            dispatch_tx,
            receiver,
            1_000,
            UdpPacingConfig::for_global_bandwidth(1_000),
            metrics.clone(),
        ));
        assert!(pacing.as_mut().now_or_never().is_none());
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 0);
        time::sleep(Duration::from_millis(1)).await;
        dispatch_rx.try_recv().unwrap();
        assert!(pacing.as_mut().now_or_never().is_none());
        let batch = dispatch_rx.try_recv().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].payload, Bytes::from_static(&[1]));
        assert!(metrics.udp_tx_queue_wait_micros.get() > 0);
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 0);

        dispatch_rx.close();
        assert!(sender.try_send(message().into()).is_ok());
        time::timeout(Duration::from_millis(100), pacing)
            .await
            .unwrap();
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 1);
    }
}
