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
    collections::HashMap, net::SocketAddr, num::NonZeroU64, rc::Rc, sync::mpsc as std_mpsc, thread,
    time::Instant,
};

use async_channel::{Receiver, Sender};
use bytes::Bytes;
use futures::future::join_all;
use monoio::{net::udp::UdpSocket, select, spawn, time::sleep, IoUringDriver, RuntimeBuilder};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tracing::{debug, trace};

use crate::{
    metrics::DataplaneMetrics,
    pacing::{PacingItem, PacingKey, PacingPriority, PacingQueue, Scheduled},
    tcp::{
        tx::{chunk_header_bytes, Chunk, Message, TxState, TCP_CHUNK_BYTES},
        TCP_MESSAGE_LENGTH_LIMIT,
    },
    udp::{
        max_write_size_for_segment_size, record_send_error, DEFAULT_SEGMENT_SIZE,
        MAX_AGGREGATED_SEGMENTS,
    },
    RecvUdpMsg, TcpMsg, UdpMsg, UdpPacingConfig, UdpSocketId, IPV4_HDR_SIZE, UDP_HDR_SIZE,
};
const MAX_CHANNEL_DRAIN: usize = 64;
const UDP_TX_QUEUE_SIZE: usize = 128;

struct UdpSend {
    socket_id: UdpSocketId,
    dst: SocketAddr,
    payload: Bytes,
}

impl PacingItem for UdpMsg {
    fn queued_bytes(&self) -> usize {
        self.payload.len()
    }

    fn next_payload_bytes(&self) -> usize {
        self.payload
            .len()
            .min(self.stride.max(1) as usize)
            .min(usize::from(u16::MAX - IPV4_HDR_SIZE - UDP_HDR_SIZE))
            .min(max_write_size_for_segment_size(DEFAULT_SEGMENT_SIZE) as usize)
    }
}

struct TcpWork {
    addr: SocketAddr,
    payload: Bytes,
    message: Rc<Message>,
    first: bool,
}

enum PacedMessage {
    Udp(UdpMsg),
    Tcp(TcpWork),
}

impl PacingItem for PacedMessage {
    fn queued_bytes(&self) -> usize {
        match self {
            Self::Udp(work) => work.queued_bytes(),
            Self::Tcp(work) => work.payload.len(),
        }
    }

    fn next_payload_bytes(&self) -> usize {
        match self {
            Self::Udp(work) => work.next_payload_bytes(),
            Self::Tcp(work) => work
                .payload
                .len()
                .min(TCP_CHUNK_BYTES - chunk_header_bytes(work.first)),
        }
    }

    fn next_pacing_bytes(&self) -> usize {
        match self {
            Self::Udp(work) => work.next_pacing_bytes(),
            Self::Tcp(work) => self.next_payload_bytes() + chunk_header_bytes(work.first),
        }
    }

    fn peer_bytes_per_second(&self, configured: NonZeroU64) -> NonZeroU64 {
        match self {
            Self::Udp(_) => configured,
            Self::Tcp(_) => NonZeroU64::new(u64::MAX).unwrap(),
        }
    }

    fn is_udp(&self) -> bool {
        matches!(self, Self::Udp(_))
    }
}

impl Scheduled<PacedMessage> {
    fn take_chunk(&mut self) -> Bytes {
        match &mut self.item {
            PacedMessage::Udp(message) => message.payload.split_to(self.batch_bytes),
            PacedMessage::Tcp(work) => work.payload.split_to(self.batch_bytes),
        }
    }
}

pub(crate) struct TxConfig {
    pub up_bandwidth_mbps: u64,
    pub pacing: UdpPacingConfig,
    pub buffer_size: Option<usize>,
    pub use_multishot: bool,
    pub workers: usize,
}

pub(crate) fn spawn_tasks(
    socket_configs: Vec<(UdpSocketId, SocketAddr, mpsc::Sender<RecvUdpMsg>)>,
    udp_egress_rx: mpsc::Receiver<UdpMsg>,
    tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>,
    tcp: TxState,
    config: TxConfig,
    bound_addrs_tx: std_mpsc::SyncSender<Vec<(UdpSocketId, SocketAddr)>>,
    metrics: DataplaneMetrics,
) {
    let TxConfig {
        up_bandwidth_mbps,
        pacing,
        buffer_size,
        use_multishot,
        workers: worker_count,
    } = config;
    assert!(worker_count != 0, "at least one UDP TX worker is required");
    let (sockets, bound_addrs) =
        crate::udp::spawn_tasks(socket_configs, buffer_size, use_multishot, &metrics);
    let worker_sockets: Vec<Vec<_>> = (0..worker_count)
        .map(|_| {
            sockets
                .iter()
                .map(|(id, socket)| {
                    (
                        *id,
                        socket.try_clone().expect("failed to clone UDP TX socket"),
                    )
                })
                .collect()
        })
        .collect();
    drop(sockets);

    let (dispatch_tx, dispatch_rx) = async_channel::bounded(UDP_TX_QUEUE_SIZE);
    let (startup_tx, startup_rx) = std_mpsc::channel();
    for (worker, sockets) in worker_sockets.into_iter().enumerate() {
        let metrics = metrics.clone();
        let startup_tx = startup_tx.clone();
        let dispatch_rx = dispatch_rx.clone();
        thread::Builder::new()
            .name(format!("monad-udp-tx-{worker}"))
            .spawn(move || {
                RuntimeBuilder::<IoUringDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("failed building UDP TX runtime")
                    .block_on(tx_worker(sockets, dispatch_rx, metrics, startup_tx));
            })
            .expect("failed to spawn UDP TX thread");
    }
    drop(startup_tx);

    for _ in 0..worker_count {
        startup_rx
            .recv()
            .expect("UDP TX worker exited during startup");
    }
    bound_addrs_tx.send(bound_addrs).unwrap();
    spawn(tx_pacing(
        dispatch_tx,
        udp_egress_rx,
        tcp_egress_rx,
        up_bandwidth_mbps,
        pacing,
        tcp,
        metrics,
    ));
}

fn enqueue_udp_msg(queue: &mut PacingQueue<PacedMessage>, metrics: &DataplaneMetrics, msg: UdpMsg) {
    let SocketAddr::V4(destination) = msg.dst else {
        metrics.udp_egress_messages_dropped.inc();
        debug!(destination = ?msg.dst, "IPv6 UDP message is not supported");
        return;
    };
    let priority = msg.priority;
    let _ = queue.enqueue(destination, priority, PacedMessage::Udp(msg));
}

fn enqueue_tcp_msg(
    queue: &mut PacingQueue<PacedMessage>,
    metrics: &DataplaneMetrics,
    addr: SocketAddr,
    message: TcpMsg,
) {
    let message_len = message.msg.len();
    if message_len > TCP_MESSAGE_LENGTH_LIMIT {
        metrics.tcp_egress_messages_dropped.inc();
        return;
    }
    let work = PacedMessage::Tcp(TcpWork {
        addr,
        payload: message.msg,
        message: Message::new(message_len, message.completion, metrics.clone()),
        first: true,
    });
    let _ = queue.enqueue(PacingKey::Tcp(addr), PacingPriority::Background, work);
}

// A dataplane may have only UDP or only TCP socket handles. Closing one ingress
// must not stop the other protocol or leave a permanently ready receive future.
async fn recv_open<T>(receiver: &mut mpsc::Receiver<T>) -> T {
    match receiver.recv().await {
        Some(message) => message,
        None => std::future::pending().await,
    }
}

async fn yield_to_writers() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

async fn tx_pacing(
    dispatch_tx: Sender<Vec<UdpSend>>,
    mut udp_egress_rx: mpsc::Receiver<UdpMsg>,
    mut tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>,
    up_bandwidth_mbps: u64,
    pacing_config: UdpPacingConfig,
    mut tcp: TxState,
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

    loop {
        while let Ok(event) = tcp.events.try_recv() {
            if let Some(addr) = tcp.handle_event(event) {
                queue.remove_peer(PacingKey::Tcp(addr));
            }
        }
        for _ in 0..MAX_CHANNEL_DRAIN {
            match udp_egress_rx.try_recv() {
                Ok(msg) => enqueue_udp_msg(&mut queue, &metrics, msg),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        for _ in 0..MAX_CHANNEL_DRAIN {
            match tcp_egress_rx.try_recv() {
                Ok((addr, msg)) => enqueue_tcp_msg(&mut queue, &metrics, addr, msg),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }

        let now = queue.elapsed();
        let wake_at = queue.next_wakeup(now);
        if wake_at.is_none_or(|at| at > now) {
            let timer = async {
                match wake_at {
                    Some(at) => sleep(at.saturating_sub(now)).await,
                    None => std::future::pending().await,
                }
            };
            select! {
                message = recv_open(&mut udp_egress_rx) => {
                    enqueue_udp_msg(&mut queue, &metrics, message);
                }
                message = recv_open(&mut tcp_egress_rx) => {
                    enqueue_tcp_msg(&mut queue, &metrics, message.0, message.1);
                }
                event = tcp.events.recv() => {
                    if let Some(addr) = event.and_then(|event| tcp.handle_event(event)) {
                        queue.remove_peer(PacingKey::Tcp(addr));
                    }
                }
                _ = timer => {}
            }
            continue;
        }

        let queue_len = queue.len();
        let mut batch = Vec::with_capacity(max_batch_items);
        let mut total_bytes = 0;
        while batch.len() < max_batch_items && total_bytes < max_batch_bytes {
            // The first selection may be a TCP chunk; subsequent UDP selections
            // retain the original UDP batch limit.
            let limit = if batch.is_empty() {
                TCP_CHUNK_BYTES.max(max_batch_bytes)
            } else {
                max_batch_bytes - total_bytes
            };
            let Some(mut scheduled) = queue.dequeue(now, limit) else {
                break;
            };
            let chunk = scheduled.take_chunk();
            match &mut scheduled.item {
                PacedMessage::Udp(msg) => {
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
                PacedMessage::Tcp(work) => {
                    let addr = work.addr;
                    let last = work.payload.is_empty();
                    let chunk = Chunk {
                        payload: chunk,
                        message: Rc::clone(&work.message),
                        first: work.first,
                        last,
                    };
                    if !tcp.send(addr, chunk) {
                        queue.remove_peer(PacingKey::Tcp(addr));
                    } else if !last {
                        work.first = false;
                        assert!(queue.requeue(PacingKey::Tcp(addr), scheduled).is_ok());
                    }
                    // Reconsider ingress and priority after each 128 KiB TCP chunk.
                    break;
                }
            }
        }

        if batch.is_empty() {
            yield_to_writers().await;
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
        // TCP writers share this runtime and must run even while pacing is ready.
        yield_to_writers().await;
    }
}

async fn tx_worker(
    tx_sockets: Vec<(UdpSocketId, std::net::UdpSocket)>,
    dispatch_rx: Receiver<Vec<UdpSend>>,
    metrics: DataplaneMetrics,
    startup_tx: std_mpsc::Sender<()>,
) {
    let tx_sockets = tx_sockets
        .into_iter()
        .map(|(id, socket)| (id, UdpSocket::from_std(socket).unwrap()))
        .collect::<HashMap<_, _>>();
    if startup_tx.send(()).is_err() {
        return;
    }
    drop(startup_tx);

    while let Ok(dispatch) = dispatch_rx.recv().await {
        let sends = dispatch.into_iter().map(|message| {
            let socket = tx_sockets.get(&message.socket_id).expect("valid socket_id");
            socket.send_to(message.payload, message.dst)
        });
        for (ret, chunk) in join_all(sends).await {
            match ret {
                Ok(payload_bytes_sent) => {
                    metrics.udp_messages_sent.inc();
                    metrics.udp_bytes_sent.add(payload_bytes_sent as u64);
                }
                Err(err) => record_send_error(&metrics, err, chunk.len()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monoio::time;

    use super::*;

    #[test]
    fn udp_memory_accounts_for_remaining_payload_not_next_packet() {
        let metrics = DataplaneMetrics::new();
        let mut queue = PacingQueue::new(
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::new(u64::MAX).unwrap(),
            6,
            metrics.clone(),
        );
        let key = "127.0.0.1:1".parse::<std::net::SocketAddrV4>().unwrap();
        let message = |bytes| {
            PacedMessage::Udp(UdpMsg {
                socket_id: crate::UdpSocketId::Raptorcast,
                dst: key.into(),
                payload: Bytes::from(vec![0; bytes]),
                stride: 2,
                priority: monad_types::UdpPriority::Regular,
            })
        };
        assert!(queue
            .enqueue(key, monad_types::UdpPriority::Regular, message(7))
            .is_err());
        assert_eq!(metrics.udp_pacing_queued_bytes.get(), 0);
        assert!(queue
            .enqueue(key, monad_types::UdpPriority::Regular, message(5))
            .is_ok());
        assert_eq!(metrics.udp_pacing_queued_bytes.get(), 5);

        let mut scheduled = queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(scheduled.batch_bytes, 2);
        assert_eq!(metrics.udp_pacing_queued_bytes.get(), 0);
        assert_eq!(scheduled.take_chunk(), Bytes::from_static(&[0, 0]));
        assert_eq!(scheduled.item.queued_bytes(), 3);
        assert!(queue.requeue(key, scheduled).is_ok());
        assert_eq!(metrics.udp_pacing_queued_bytes.get(), 3);
        assert!(queue
            .enqueue(key, monad_types::UdpPriority::Regular, message(4))
            .is_err());
        assert!(queue
            .enqueue(key, monad_types::UdpPriority::Regular, message(3))
            .is_ok());
        assert_eq!(metrics.udp_pacing_queued_bytes.get(), 6);
    }
    fn tcp_state(metrics: DataplaneMetrics) -> TxState {
        TxState::new(
            std::sync::Arc::new(crate::Addrlist::new_with_trusted(std::iter::empty())),
            64,
            metrics,
        )
    }

    #[monoio::test(timer_enabled = true)]
    async fn udp_preempts_queued_tcp() {
        let metrics = DataplaneMetrics::new();
        let (udp_tx, udp_rx) = mpsc::channel(4);
        let (tcp_tx, tcp_rx) = mpsc::channel(4);
        let (dispatch_tx, dispatch_rx) = async_channel::bounded(4);
        let addr = "127.0.0.1:1".parse().unwrap();
        tcp_tx
            .try_send((
                addr,
                TcpMsg {
                    msg: vec![0; TCP_CHUNK_BYTES].into(),
                    completion: None,
                },
            ))
            .unwrap();
        udp_tx
            .try_send(UdpMsg {
                socket_id: UdpSocketId::Raptorcast,
                dst: addr,
                payload: vec![1; 100].into(),
                stride: 100,
                priority: monad_types::UdpPriority::High,
            })
            .unwrap();
        spawn(tx_pacing(
            dispatch_tx,
            udp_rx,
            tcp_rx,
            1,
            UdpPacingConfig::for_global_bandwidth(1),
            tcp_state(metrics.clone()),
            metrics.clone(),
        ));
        let batch = time::timeout(Duration::from_millis(100), dispatch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(metrics.egress_pacing_background_grants.get(), 0);
    }

    #[monoio::test(timer_enabled = true)]
    async fn udp_preempts_between_tcp_chunks() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let metrics = DataplaneMetrics::new();
        let (udp_tx, udp_rx) = mpsc::channel(4);
        let (tcp_tx, tcp_rx) = mpsc::channel(4);
        let (dispatch_tx, dispatch_rx) = async_channel::bounded(4);
        tcp_tx
            .try_send((
                listener.local_addr().unwrap(),
                TcpMsg {
                    msg: vec![7; TCP_MESSAGE_LENGTH_LIMIT].into(),
                    completion: None,
                },
            ))
            .unwrap();
        spawn(tx_pacing(
            dispatch_tx,
            udp_rx,
            tcp_rx,
            100,
            UdpPacingConfig::for_global_bandwidth(100),
            tcp_state(metrics.clone()),
            metrics.clone(),
        ));
        time::timeout(Duration::from_millis(100), async {
            while metrics.egress_pacing_background_grants.get() == 0 {
                sleep(Duration::from_micros(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            metrics.egress_pacing_background_granted_bytes.get(),
            TCP_CHUNK_BYTES as u64
        );
        udp_tx
            .try_send(UdpMsg {
                socket_id: UdpSocketId::Raptorcast,
                dst: listener.local_addr().unwrap(),
                payload: Bytes::from_static(&[1]),
                stride: 1,
                priority: monad_types::UdpPriority::High,
            })
            .unwrap();
        // Reserving the whole message would delay UDP for 252 ms at 100 Mbps.
        let batch = time::timeout(Duration::from_millis(50), dispatch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch[0].payload, Bytes::from_static(&[1]));
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
            priority: monad_types::UdpPriority::High,
        };
        assert!(sender.try_send(message()).is_ok());
        let mut pacing = std::pin::pin!(tx_pacing(
            dispatch_tx,
            receiver,
            mpsc::channel(1).1,
            1_000,
            UdpPacingConfig::for_global_bandwidth(1_000),
            tcp_state(metrics.clone()),
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
        assert!(sender.try_send(message()).is_ok());
        time::timeout(Duration::from_millis(100), pacing)
            .await
            .unwrap();
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 1);
    }

    #[monoio::test(timer_enabled = true)]
    async fn new_peer_wakes_transmit_while_another_peer_is_cooling() {
        let regular = UdpSocket::bind("127.0.0.1:0").unwrap();
        let high = UdpSocket::bind("127.0.0.1:0").unwrap();
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let (dispatch_tx, dispatch_rx) = async_channel::bounded(UDP_TX_QUEUE_SIZE);
        let (startup_tx, _startup_rx) = std_mpsc::channel();
        spawn(tx_worker(
            vec![(UdpSocketId::Raptorcast, socket)],
            dispatch_rx,
            DataplaneMetrics::new(),
            startup_tx,
        ));
        let (sender, receiver) = mpsc::channel(4);
        for _ in 0..2 {
            assert!(sender
                .try_send(UdpMsg {
                    socket_id: UdpSocketId::Raptorcast,
                    dst: regular.local_addr().unwrap(),
                    payload: vec![1; 60_000].into(),
                    stride: 60_000,
                    priority: monad_types::UdpPriority::Regular,
                })
                .is_ok());
        }
        spawn(tx_pacing(
            dispatch_tx,
            receiver,
            mpsc::channel(1).1,
            1_000,
            UdpPacingConfig {
                peer_bandwidth_mbps: 1,
                max_queued_bytes: 1024 * 1024,
            },
            tcp_state(DataplaneMetrics::new()),
            DataplaneMetrics::new(),
        ));
        let (result, _) = time::timeout(Duration::from_secs(1), regular.recv_from(vec![0; 60_000]))
            .await
            .unwrap();
        assert_eq!(result.unwrap().0, 60_000);
        // Regular traffic is cooling for about 480 ms; the new peer need not wait.
        assert!(sender
            .try_send(UdpMsg {
                socket_id: UdpSocketId::Raptorcast,
                dst: high.local_addr().unwrap(),
                payload: Bytes::from_static(&[2]),
                stride: 1,
                priority: monad_types::UdpPriority::High,
            })
            .is_ok());
        let (result, payload) =
            time::timeout(Duration::from_millis(100), high.recv_from(vec![0; 1]))
                .await
                .unwrap();
        assert_eq!(result.unwrap().0, 1);
        assert_eq!(payload, [2]);
    }
}
