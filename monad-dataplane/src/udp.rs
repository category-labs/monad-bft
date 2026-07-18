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
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    os::fd::{AsRawFd, FromRawFd},
};

use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use monoio::{
    buf::{Ipv4RecvMsgParser, UserRecvMsgRingBuf},
    net::udp::UdpSocket,
    spawn,
};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tracing::{debug, error, trace, warn};

use super::{RecvUdpMsg, UdpMsg, UdpPacingConfig, UdpSocketId};
use crate::{
    buffer_ext::SocketBufferExt,
    metrics::DataplaneMetrics,
    pacing::{BatchLimits, EnqueueError, PacingQueue, QueueCost},
};

const DEFAULT_RINGBUF_COUNT: u32 = 2048;
const DEFAULT_RINGBUF_SIZE: u32 = ETHERNET_SEGMENT_SIZE as u32;

pub const DEFAULT_MTU: u16 = monad_types::DEFAULT_MTU;

const IPV4_HDR_SIZE: u16 = 20;
const IPV6_HDR_SIZE: u16 = 40;
const UDP_HDR_SIZE: u16 = 8;
const MAX_AGGREGATED_WRITE_SIZE: u16 = u16::MAX - IPV4_HDR_SIZE - UDP_HDR_SIZE;
const MAX_AGGREGATED_SEGMENTS: u16 = 128;

pub const fn segment_size_for_mtu(mtu: u16) -> u16 {
    mtu - IPV4_HDR_SIZE - UDP_HDR_SIZE
}

pub const DEFAULT_SEGMENT_SIZE: u16 = segment_size_for_mtu(DEFAULT_MTU);
pub const ETHERNET_SEGMENT_SIZE: u16 = segment_size_for_mtu(monad_types::ETHERNET_MTU);

pub(crate) struct UdpTaskConfig {
    pub up_bandwidth_mbps: u64,
    pub pacing: UdpPacingConfig,
    pub buffer_size: Option<usize>,
    pub use_multishot: bool,
}

fn configure_socket(socket: &UdpSocket, buffer_size: Option<usize>) {
    if let Some(size) = buffer_size {
        set_socket_buffer_sizes(socket, size);
    }
    set_mtu_discovery(socket);
}

fn set_socket_buffer_sizes(socket: &UdpSocket, requested_size: usize) {
    set_recv_buffer_size(socket, requested_size);
    set_send_buffer_size(socket, requested_size);
}

fn set_recv_buffer_size(socket: &UdpSocket, requested_size: usize) {
    if let Err(e) = socket.set_recv_buffer_size(requested_size) {
        panic!("set_recv_buffer_size to {requested_size} failed with: {e}");
    }
    let actual_size = socket.recv_buffer_size().expect("get recv buffer size");
    if actual_size < requested_size {
        panic!("unable to set udp receive buffer size to {requested_size}. Got {actual_size} instead. Set net.core.rmem_max to at least {requested_size}");
    }
}

fn set_send_buffer_size(socket: &UdpSocket, requested_size: usize) {
    if let Err(e) = socket.set_send_buffer_size(requested_size) {
        panic!("set_send_buffer_size to {requested_size} failed with: {e}");
    }
    let actual_size = socket.send_buffer_size().expect("get send buffer size");
    if actual_size < requested_size {
        panic!("unable to set udp send buffer size to {requested_size}. got {actual_size} instead. set net.core.wmem_max to at least {requested_size}");
    }
}

fn set_mtu_discovery(socket: &UdpSocket) {
    const MTU_DISCOVER: libc::c_int = libc::IP_PMTUDISC_OMIT;
    let raw_fd = socket.as_raw_fd();

    if unsafe {
        libc::setsockopt(
            raw_fd,
            libc::SOL_IP,
            libc::IP_MTU_DISCOVER,
            &MTU_DISCOVER as *const _ as _,
            std::mem::size_of_val(&MTU_DISCOVER) as _,
        )
    } != 0
    {
        panic!(
            "set IP_MTU_DISCOVER failed with: {}",
            Error::last_os_error()
        );
    }
}

pub(crate) fn spawn_tasks(
    socket_configs: Vec<(UdpSocketId, SocketAddr, mpsc::Sender<RecvUdpMsg>)>,
    udp_egress_rx: mpsc::Receiver<UdpMsg>,
    config: UdpTaskConfig,
    bound_addrs_tx: std::sync::mpsc::SyncSender<Vec<(UdpSocketId, SocketAddr)>>,
    metrics: DataplaneMetrics,
) {
    let UdpTaskConfig {
        up_bandwidth_mbps,
        pacing,
        buffer_size,
        use_multishot,
    } = config;
    let mut tx_sockets = Vec::new();
    let mut bound_addrs = Vec::with_capacity(socket_configs.len());

    for (socket_id, socket_addr, ingress_tx) in socket_configs {
        let socket = std::net::UdpSocket::bind(socket_addr).unwrap();
        let tx = UdpSocket::from_std(socket).unwrap();
        configure_socket(&tx, buffer_size);
        let actual_addr = tx.local_addr().unwrap();
        bound_addrs.push((socket_id, actual_addr));

        let group_id = socket_id as u16;
        if use_multishot {
            let rx = tx.dup().expect("failed to dup socket");
            spawn(rx_multishot_socket(
                rx,
                ingress_tx.clone(),
                group_id,
                metrics.clone(),
            ));
            trace!(
                ?socket_id,
                ?socket_addr,
                ?actual_addr,
                "created multishot socket"
            );
        } else {
            let rx = tx.dup().expect("failed to dup socket");
            spawn(rx_single_socket(rx, ingress_tx.clone(), metrics.clone()));
            trace!(?socket_id, ?socket_addr, ?actual_addr, "created socket");
        }

        tx_sockets.push((socket_id, tx));
    }

    bound_addrs_tx.send(bound_addrs).unwrap();
    spawn(tx(
        tx_sockets,
        udp_egress_rx,
        up_bandwidth_mbps,
        pacing,
        metrics,
    ));
}

async fn rx_single_socket(
    socket: UdpSocket,
    udp_ingress_tx: mpsc::Sender<RecvUdpMsg>,
    metrics: DataplaneMetrics,
) {
    loop {
        let buf = BytesMut::with_capacity(ETHERNET_SEGMENT_SIZE.into());

        match socket.recv_from(buf).await {
            (Ok((len, src_addr)), buf) => {
                let payload = buf.freeze();
                metrics.udp_messages_received.inc();
                metrics.udp_bytes_received.add(len as u64);

                let msg = RecvUdpMsg {
                    src_addr,
                    payload,
                    stride: len.max(1).try_into().unwrap(),
                };

                if let Err(err) = udp_ingress_tx.send(msg).await {
                    warn!(?src_addr, ?err, "error queueing up received UDP message");
                    break;
                }
            }
            (Err(err), _buf) => {
                metrics.udp_receive_errors.inc();
                warn!("socket.recv_from() error {}", err);
            }
        }
    }
}

enum MultishotResult<R> {
    ReuseRing(R),
    RecreateRing,
    ChannelClosed,
}

async fn run_multishot_stream(
    socket: &UdpSocket,
    udp_ingress_tx: &mpsc::Sender<RecvUdpMsg>,
    ring: UserRecvMsgRingBuf<Ipv4RecvMsgParser>,
    metrics: &DataplaneMetrics,
) -> MultishotResult<UserRecvMsgRingBuf<Ipv4RecvMsgParser>> {
    let mut multishot = socket
        .recvmsg_multishot(ring)
        .expect("failed to create multishot stream");
    let mut stream = multishot.stream();

    while let Some(result) = stream.next().await {
        match result {
            Ok((src_addr, buf)) => {
                let payload = Bytes::copy_from_slice(&buf);
                let len = payload.len();
                metrics.udp_messages_received.inc();
                metrics.udp_bytes_received.add(len as u64);

                let msg = RecvUdpMsg {
                    src_addr: src_addr.into(),
                    payload,
                    stride: len.max(1).try_into().unwrap(),
                };

                if let Err(err) = udp_ingress_tx.send(msg).await {
                    warn!(?err, "error queueing up received UDP message (multishot)");
                    return MultishotResult::ChannelClosed;
                }
            }
            Err(e) if e.raw_os_error() == Some(libc::ENOBUFS) => {
                metrics.udp_receive_errors.inc();
                debug!("ringbuf exhausted, recreating stream");
            }
            Err(e) => {
                metrics.udp_receive_errors.inc();
                warn!("multishot recv error: {:?}", e);
            }
        }
    }

    debug!("multishot stream needs to be recreated");
    match multishot.try_into_ring() {
        Ok(ring) => MultishotResult::ReuseRing(ring),
        Err(_) => {
            error!("multishot stream not terminated after poll returned None, recreating ring");
            MultishotResult::RecreateRing
        }
    }
}

async fn rx_multishot_socket(
    socket: UdpSocket,
    udp_ingress_tx: mpsc::Sender<RecvUdpMsg>,
    group_id: u16,
    metrics: DataplaneMetrics,
) {
    let create_ring = || {
        UserRecvMsgRingBuf::<Ipv4RecvMsgParser>::new(
            DEFAULT_RINGBUF_COUNT as u16,
            DEFAULT_RINGBUF_SIZE as usize,
            group_id,
        )
        .expect("failed to create buffer ring")
    };

    let mut ring = create_ring();

    loop {
        match run_multishot_stream(&socket, &udp_ingress_tx, ring, &metrics).await {
            MultishotResult::ReuseRing(r) => ring = r,
            MultishotResult::RecreateRing => ring = create_ring(),
            MultishotResult::ChannelClosed => return,
        }
    }
}

async fn tx(
    tx_sockets: Vec<(UdpSocketId, UdpSocket)>,
    mut udp_egress_rx: mpsc::Receiver<UdpMsg>,
    up_bandwidth_mbps: u64,
    pacing_config: UdpPacingConfig,
    metrics: DataplaneMetrics,
) {
    let tx_sockets: HashMap<UdpSocketId, UdpSocket> = tx_sockets.into_iter().collect();
    let global_bytes_per_second = mbps_to_bytes_per_second(up_bandwidth_mbps);
    let peer_bytes_per_second = mbps_to_bytes_per_second(pacing_config.peer_bandwidth_mbps);
    let mut queue = PacingQueue::new(
        global_bytes_per_second,
        peer_bytes_per_second,
        pacing_config.max_queued_bytes,
    );
    let batch_limits = BatchLimits {
        max_bytes: max_write_size_for_segment_size(DEFAULT_SEGMENT_SIZE) as usize,
        max_items: MAX_AGGREGATED_SEGMENTS as usize,
    };
    let mut send_futures = Vec::with_capacity(batch_limits.max_items);
    let mut channel_open = true;

    loop {
        for _ in 0..256 {
            if !channel_open {
                break;
            }
            match udp_egress_rx.try_recv() {
                Ok(msg) => enqueue_udp_msg(&mut queue, &metrics, msg),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    channel_open = false;
                    break;
                }
            }
        }

        if queue.is_empty() {
            if !channel_open {
                return;
            }
            match udp_egress_rx.recv().await {
                Some(msg) => enqueue_udp_msg(&mut queue, &metrics, msg),
                None => channel_open = false,
            }
            continue;
        }

        let queue_len = queue.len();
        send_futures.clear();
        let (batch_count, total_bytes) = {
            let mut batch = queue.batch(batch_limits);

            while let Some(scheduled) = batch.next() {
                let mut msg = scheduled.item;
                let chunk = msg.payload.split_to(next_payload_bytes(&msg));
                let socket_id = msg.socket_id;
                let dst = msg.dst;
                let socket = tx_sockets.get(&socket_id).expect("valid socket_id");

                if !msg.payload.is_empty() {
                    let cost = udp_queue_cost(&msg);
                    if let Err(error) = batch.reenqueue(dst, msg.priority, msg, cost) {
                        record_enqueue_error(&metrics, error);
                    }
                }

                trace!(?socket_id, dst_addr = ?dst, chunk_len = chunk.len(), "preparing udp send");
                send_futures.push(socket.send_to(chunk, dst));
            }

            batch.wait().await;
            (batch.items(), batch.bytes())
        };

        if batch_count > 1 {
            trace!(
                batch_size = batch_count,
                total_bytes,
                queue_size = queue_len,
                "sending udp batch"
            );
        }

        for (ret, chunk) in join_all(send_futures.drain(..)).await {
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

fn mbps_to_bytes_per_second(mbps: u64) -> NonZeroU64 {
    NonZeroU64::new(
        mbps.checked_mul(125_000)
            .expect("UDP bandwidth overflows bytes per second"),
    )
    .expect("UDP bandwidth must be non-zero")
}

fn next_payload_bytes(msg: &UdpMsg) -> usize {
    let ip_header = if msg.dst.is_ipv4() {
        IPV4_HDR_SIZE
    } else {
        IPV6_HDR_SIZE
    };
    let max_payload = usize::from(u16::MAX - ip_header - UDP_HDR_SIZE);

    msg.payload
        .len()
        .min(msg.stride.max(1) as usize)
        .min(max_payload)
        .min(max_write_size_for_segment_size(DEFAULT_SEGMENT_SIZE) as usize)
}

fn udp_wire_bytes(destination: SocketAddr, payload_bytes: usize) -> NonZeroUsize {
    let ip_header_bytes = if destination.is_ipv4() {
        IPV4_HDR_SIZE
    } else {
        IPV6_HDR_SIZE
    };
    NonZeroUsize::new(payload_bytes + usize::from(UDP_HDR_SIZE + ip_header_bytes))
        .expect("UDP headers make wire cost non-zero")
}

fn udp_queue_cost(msg: &UdpMsg) -> QueueCost {
    let batch_bytes = next_payload_bytes(msg);
    QueueCost {
        wire_bytes: udp_wire_bytes(msg.dst, batch_bytes),
        batch_bytes,
        memory_bytes: msg.payload.len(),
    }
}

fn enqueue_udp_msg(queue: &mut PacingQueue<UdpMsg>, metrics: &DataplaneMetrics, msg: UdpMsg) {
    let destination = msg.dst;
    let cost = udp_queue_cost(&msg);
    if let Err(error) = queue.enqueue(destination, msg.priority, msg, cost) {
        record_enqueue_error(metrics, error);
    }
}

fn record_enqueue_error<T>(metrics: &DataplaneMetrics, error: EnqueueError<T>) {
    metrics.udp_egress_messages_dropped.inc();
    match error {
        EnqueueError::MemoryLimit(_) => metrics.udp_pacing_memory_limit_drops.inc(),
        EnqueueError::PeerLimit(_) => metrics.udp_pacing_peer_limit_drops.inc(),
    }
}

fn record_send_error(metrics: &DataplaneMetrics, err: Error, len: usize) {
    metrics.udp_send_errors.inc();
    match err.kind() {
        ErrorKind::NetworkUnreachable => {
            debug!("send address family mismatch. message is dropped")
        }
        ErrorKind::InvalidInput => warn!(len, "got EINVAL on send. message is dropped"),
        _ if is_eafnosupport(&err) => {
            debug!("send address family mismatch. message is dropped")
        }
        _ => error!(len, ?err, "unexpected send error. message is dropped"),
    }
}

fn max_write_size_for_segment_size(segment_size: u16) -> u16 {
    (MAX_AGGREGATED_WRITE_SIZE / segment_size).min(MAX_AGGREGATED_SEGMENTS) * segment_size
}

fn is_eafnosupport(err: &Error) -> bool {
    const EAFNOSUPPORT: &str = "Address family not supported by protocol";

    let err = format!("{}", err);

    err.len() >= EAFNOSUPPORT.len() && &err[0..EAFNOSUPPORT.len()] == EAFNOSUPPORT
}

pub trait UdpSocketExt: AsRawFd {
    fn dup(&self) -> std::io::Result<UdpSocket> {
        let fd = self.as_raw_fd();
        let new_fd = unsafe { libc::dup(fd) };
        if new_fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let std_socket = unsafe { std::net::UdpSocket::from_raw_fd(new_fd) };
        UdpSocket::from_std(std_socket)
    }
}

impl UdpSocketExt for UdpSocket {}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use bytes::Bytes;
    use monad_types::UdpPriority;

    use super::*;

    fn create_test_msg(dst: SocketAddr, priority: UdpPriority, payload_size: usize) -> UdpMsg {
        UdpMsg {
            socket_id: UdpSocketId::Raptorcast,
            dst,
            payload: Bytes::from(vec![0u8; payload_size]),
            stride: 1024,
            priority,
        }
    }

    #[test]
    fn udp_adapter_records_memory_limit_metric() {
        let mut queue = PacingQueue::new(
            mbps_to_bytes_per_second(1_000),
            mbps_to_bytes_per_second(1_000),
            1_000,
        );
        let metrics = DataplaneMetrics::new();
        let destination = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        enqueue_udp_msg(
            &mut queue,
            &metrics,
            create_test_msg(destination, UdpPriority::Regular, 800),
        );
        enqueue_udp_msg(
            &mut queue,
            &metrics,
            create_test_msg(destination, UdpPriority::High, 800),
        );

        assert_eq!(metrics.udp_pacing_memory_limit_drops.get(), 1);
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 1);
    }

    #[test]
    fn udp_adapter_records_peer_limit_metric() {
        let metrics = DataplaneMetrics::new();
        record_enqueue_error(&metrics, EnqueueError::PeerLimit(()));

        assert_eq!(metrics.udp_pacing_peer_limit_drops.get(), 1);
        assert_eq!(metrics.udp_pacing_memory_limit_drops.get(), 0);
        assert_eq!(metrics.udp_egress_messages_dropped.get(), 1);
    }

    #[test]
    fn udp_wire_cost_includes_ip_version_headers() {
        let ipv4: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let ipv6: SocketAddr = "[::1]:8080".parse().unwrap();
        let payload_bytes = 1_024;
        assert_eq!(udp_wire_bytes(ipv4, payload_bytes).get(), 1_024 + 20 + 8);
        assert_eq!(udp_wire_bytes(ipv6, payload_bytes).get(), 1_024 + 40 + 8);
    }

    #[monoio::test]
    async fn test_dup_independent() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let main_socket = monoio::net::udp::UdpSocket::bind(addr).unwrap();
        let actual_addr = main_socket.local_addr().unwrap();

        let dup1 = main_socket.dup().unwrap();
        let dup2 = main_socket.dup().unwrap();

        drop(dup1);

        let result = dup2.send_to(b"test", actual_addr).await.0.map(|_| ());
        assert!(result.is_ok(), "dup2 should still work after dup1 dropped");

        let result = main_socket
            .send_to(b"test", actual_addr)
            .await
            .0
            .map(|_| ());
        assert!(
            result.is_ok(),
            "main socket should still work after dup1 dropped"
        );
    }
}
