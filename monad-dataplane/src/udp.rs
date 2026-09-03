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
    num::NonZeroU64,
    os::fd::{AsRawFd, FromRawFd},
};

use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use monoio::{
    buf::{Ipv4RecvMsgParser, UserRecvMsgRingBuf},
    net::udp::UdpSocket,
    spawn,
    time::sleep,
};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tracing::{debug, error, trace, warn};

use super::{RecvUdpMsg, UdpMsg, UdpPacingConfig, UdpSocketId, IPV4_HDR_SIZE, UDP_HDR_SIZE};
use crate::{
    buffer_ext::SocketBufferExt,
    metrics::DataplaneMetrics,
    pacing::{PacingItem, PacingQueue},
};

const DEFAULT_RINGBUF_COUNT: u32 = 2048;
const DEFAULT_RINGBUF_SIZE: u32 = ETHERNET_SEGMENT_SIZE as u32;

pub const DEFAULT_MTU: u16 = monad_types::DEFAULT_MTU;

const MAX_AGGREGATED_WRITE_SIZE: u16 = u16::MAX - IPV4_HDR_SIZE - UDP_HDR_SIZE;
const MAX_AGGREGATED_SEGMENTS: u16 = 128;
const MAX_CHANNEL_DRAIN: usize = 64;

impl PacingItem for UdpMsg {
    fn next_payload_bytes(&self) -> usize {
        self.payload
            .len()
            .min(self.stride.max(1) as usize)
            .min(usize::from(u16::MAX - IPV4_HDR_SIZE - UDP_HDR_SIZE))
            .min(max_write_size_for_segment_size(DEFAULT_SEGMENT_SIZE) as usize)
    }
}

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
    };

    let mut ring = match create_ring() {
        Ok(ring) => ring,
        Err(err) => {
            warn!(
                ?err,
                "failed to create multishot buffer ring, falling back to single-shot UDP receiver"
            );
            return rx_single_socket(socket, udp_ingress_tx, metrics).await;
        }
    };

    loop {
        match run_multishot_stream(&socket, &udp_ingress_tx, ring, &metrics).await {
            MultishotResult::ReuseRing(r) => ring = r,
            MultishotResult::RecreateRing => {
                ring = create_ring().expect("failed to recreate multishot buffer ring")
            }
            MultishotResult::ChannelClosed => return,
        }
    }
}

fn enqueue_udp_msg(queue: &mut PacingQueue<UdpMsg>, metrics: &DataplaneMetrics, msg: UdpMsg) {
    let SocketAddr::V4(destination) = msg.dst else {
        metrics.udp_egress_messages_dropped.inc();
        debug!(destination = ?msg.dst, "IPv6 UDP message is not supported");
        return;
    };
    let priority = msg.priority;
    let queued_bytes = msg.payload.len();
    let _ = queue.enqueue(destination, priority, msg, queued_bytes);
}

async fn tx(
    tx_sockets: Vec<(UdpSocketId, UdpSocket)>,
    mut udp_egress_rx: mpsc::Receiver<UdpMsg>,
    up_bandwidth_mbps: u64,
    pacing_config: UdpPacingConfig,
    metrics: DataplaneMetrics,
) {
    let tx_sockets: HashMap<UdpSocketId, UdpSocket> = tx_sockets.into_iter().collect();
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
    let mut send_futures = Vec::with_capacity(max_batch_items);
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
            sleep(delay).await;
        }

        let now = queue.elapsed();
        if queue.next_wakeup(now).is_none_or(|at| at > now) {
            continue;
        }

        let queue_len = queue.len();
        send_futures.clear();
        let mut total_bytes = 0;
        while send_futures.len() < max_batch_items && total_bytes < max_batch_bytes {
            let Some(scheduled) = queue.dequeue(now, max_batch_bytes - total_bytes) else {
                break;
            };
            let chunk_bytes = scheduled.batch_bytes;
            let mut msg = scheduled.item;
            let chunk = msg.payload.split_to(chunk_bytes);
            let socket_id = msg.socket_id;
            let dst = msg.dst;
            let socket = tx_sockets.get(&socket_id).expect("valid socket_id");

            if !msg.payload.is_empty() {
                let SocketAddr::V4(destination) = dst else {
                    unreachable!("only IPv4 messages enter the pacing queue")
                };
                let queued_bytes = msg.payload.len();
                let _ = queue.enqueue(destination, msg.priority, msg, queued_bytes);
            }

            total_bytes += chunk_bytes;
            trace!(?socket_id, dst_addr = ?dst, chunk_len = chunk.len(), "preparing udp send");
            send_futures.push(socket.send_to(chunk, dst));
        }

        let batch_count = send_futures.len();
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
    use std::time::Duration;

    use monoio::time;

    use super::*;

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

    #[monoio::test(timer_enabled = true)]
    async fn test_multishot_falls_back_when_ring_creation_fails() {
        const GROUP_ID: u16 = 42;

        let existing_ring = match UserRecvMsgRingBuf::<Ipv4RecvMsgParser>::new(
            DEFAULT_RINGBUF_COUNT as u16,
            DEFAULT_RINGBUF_SIZE as usize,
            GROUP_ID,
        ) {
            Ok(ring) => Some(ring),
            Err(err) if err.raw_os_error() == Some(libc::EINVAL) => None,
            Err(err) => panic!("unexpected buffer ring creation error: {err}"),
        };

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let rx_socket = UdpSocket::bind(addr).unwrap();
        let rx_addr = rx_socket.local_addr().unwrap();
        let tx_socket = UdpSocket::bind(addr).unwrap();
        let (ingress_tx, mut ingress_rx) = mpsc::channel(1);

        spawn(rx_multishot_socket(
            rx_socket,
            ingress_tx,
            GROUP_ID,
            DataplaneMetrics::new(),
        ));

        let payload = b"fallback";
        let bytes_sent = tx_socket.send_to(payload, rx_addr).await.0.unwrap();
        assert_eq!(bytes_sent, payload.len());

        let msg = time::timeout(Duration::from_secs(1), ingress_rx.recv())
            .await
            .expect("timed out waiting for UDP message")
            .expect("UDP ingress channel closed");
        assert_eq!(msg.payload.as_ref(), payload);

        drop(existing_ring);
    }
}
