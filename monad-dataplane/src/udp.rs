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
    collections::{HashMap, VecDeque},
    io::{Error, ErrorKind},
    net::SocketAddr,
    os::fd::{AsRawFd, FromRawFd},
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use monad_types::UdpPriority;
use monoio::{
    buf::{Ipv4RecvMsgParser, UserRecvMsgRingBuf},
    net::udp::UdpSocket,
    spawn, time,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};

use super::{RecvUdpMsg, UdpMsg, UdpSocketId};
use crate::buffer_ext::SocketBufferExt;

const PRIORITY_QUEUE_BYTES_CAPACITY: usize = 100 * 1024 * 1024;

const DEFAULT_RINGBUF_COUNT: u32 = 2048;
const DEFAULT_RINGBUF_SIZE: u32 = ETHERNET_SEGMENT_SIZE as u32;

#[derive(Error, Debug)]
#[error("priority queue capacity exceeded: priority={priority:?} current={current_bytes} capacity={capacity_bytes}")]
struct QueueCapacityError {
    priority: UdpPriority,
    current_bytes: usize,
    capacity_bytes: usize,
}

struct PriorityQueues {
    queues: [VecDeque<UdpMsg>; 2],
    current_bytes: [usize; 2],
    capacity_bytes: usize,
}

impl PriorityQueues {
    fn new() -> Self {
        Self::with_bytes_capacity(PRIORITY_QUEUE_BYTES_CAPACITY)
    }

    fn with_bytes_capacity(capacity_bytes: usize) -> Self {
        Self {
            queues: [VecDeque::new(), VecDeque::new()],
            current_bytes: [0, 0],
            capacity_bytes,
        }
    }

    fn try_push(&mut self, msg: UdpMsg) -> Result<(), QueueCapacityError> {
        let msg_bytes = msg.payload.len();
        let priority_idx = msg.priority as usize;
        if self.current_bytes[priority_idx] + msg_bytes > self.capacity_bytes {
            return Err(QueueCapacityError {
                priority: msg.priority,
                current_bytes: self.current_bytes[priority_idx],
                capacity_bytes: self.capacity_bytes,
            });
        }
        self.current_bytes[priority_idx] += msg_bytes;
        self.queues[priority_idx].push_back(msg);
        Ok(())
    }

    fn pop_highest_priority(&mut self) -> Option<UdpMsg> {
        for (priority_idx, queue) in self.queues.iter_mut().enumerate() {
            if let Some(msg) = queue.pop_front() {
                self.current_bytes[priority_idx] =
                    self.current_bytes[priority_idx].saturating_sub(msg.payload.len());
                return Some(msg);
            }
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.is_empty())
    }
}

pub const DEFAULT_MTU: u16 = monad_types::DEFAULT_MTU;

const IPV4_HDR_SIZE: u16 = 20;
const UDP_HDR_SIZE: u16 = 8;
pub const fn segment_size_for_mtu(mtu: u16) -> u16 {
    mtu - IPV4_HDR_SIZE - UDP_HDR_SIZE
}

pub const DEFAULT_SEGMENT_SIZE: u16 = segment_size_for_mtu(DEFAULT_MTU);
pub const ETHERNET_SEGMENT_SIZE: u16 = segment_size_for_mtu(monad_types::ETHERNET_MTU);

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
    up_bandwidth_mbps: u64,
    buffer_size: Option<usize>,
    use_multishot: bool,
    bound_addrs_tx: std::sync::mpsc::SyncSender<Vec<(UdpSocketId, SocketAddr)>>,
) {
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
            spawn(rx_multishot_socket(rx, ingress_tx.clone(), group_id));
            trace!(
                ?socket_id,
                ?socket_addr,
                ?actual_addr,
                "created multishot socket"
            );
        } else {
            let rx = tx.dup().expect("failed to dup socket");
            spawn(rx_single_socket(rx, ingress_tx.clone()));
            trace!(?socket_id, ?socket_addr, ?actual_addr, "created socket");
        }

        tx_sockets.push((socket_id, tx));
    }

    bound_addrs_tx.send(bound_addrs).unwrap();
    spawn(tx(tx_sockets, udp_egress_rx, up_bandwidth_mbps));
}

async fn rx_single_socket(socket: UdpSocket, udp_ingress_tx: mpsc::Sender<RecvUdpMsg>) {
    loop {
        let buf = BytesMut::with_capacity(ETHERNET_SEGMENT_SIZE.into());

        match socket.recv_from(buf).await {
            (Ok((len, src_addr)), buf) => {
                let payload = buf.freeze();

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
                debug!("ringbuf exhausted, recreating stream");
            }
            Err(e) => {
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
        match run_multishot_stream(&socket, &udp_ingress_tx, ring).await {
            MultishotResult::ReuseRing(r) => ring = r,
            MultishotResult::RecreateRing => ring = create_ring(),
            MultishotResult::ChannelClosed => return,
        }
    }
}

const PACING_SLEEP_OVERSHOOT_DETECTION_WINDOW: Duration = Duration::from_millis(100);

async fn tx(
    tx_sockets: Vec<(UdpSocketId, UdpSocket)>,
    mut udp_egress_rx: mpsc::Receiver<UdpMsg>,
    up_bandwidth_mbps: u64,
) {
    let tx_sockets: HashMap<UdpSocketId, UdpSocket> = tx_sockets.into_iter().collect();
    let mut next_transmit = Instant::now();

    let mut priority_queues = PriorityQueues::new();

    let max_batch_bytes = max_write_size_for_segment_size(DEFAULT_SEGMENT_SIZE) as usize;
    let mut send_futures = Vec::with_capacity(MAX_AGGREGATED_SEGMENTS as usize);

    loop {
        let now = Instant::now();
        if next_transmit > now {
            time::sleep(next_transmit - now).await;
        } else {
            let late = now - next_transmit;

            if late > PACING_SLEEP_OVERSHOOT_DETECTION_WINDOW {
                next_transmit = now;
            }
        }

        if fill_message_queues(&mut udp_egress_rx, &mut priority_queues)
            .await
            .is_err()
        {
            return;
        }

        let queue_len = priority_queues
            .queues
            .iter()
            .map(|q| q.len())
            .sum::<usize>();
        let mut total_bytes = 0usize;
        let mut batch_count = 0usize;
        send_futures.clear();

        while !priority_queues.is_empty()
            && total_bytes < max_batch_bytes
            && batch_count < MAX_AGGREGATED_SEGMENTS as usize
        {
            let mut msg = priority_queues.pop_highest_priority().unwrap();
            let chunk_size = msg
                .payload
                .len()
                .min(msg.stride as usize)
                .min(max_batch_bytes);

            if chunk_size + total_bytes > max_batch_bytes {
                if let Err(err) = priority_queues.try_push(msg) {
                    warn!(?err, "failed to re-queue message");
                }
                break;
            }

            let chunk = msg.payload.split_to(chunk_size);
            total_bytes += chunk.len();

            let socket_id = msg.socket_id;
            let dst = msg.dst;

            let socket = tx_sockets.get(&socket_id).expect("valid socket_id");

            if !msg.payload.is_empty() {
                if let Err(err) = priority_queues.try_push(msg) {
                    warn!(?err, "failed to re-queue message with remaining payload");
                }
            }

            trace!(
                ?socket_id,
                dst_addr = ?dst,
                chunk_len = chunk.len(),
                "preparing udp send"
            );

            send_futures.push(socket.send_to(chunk, dst));
            batch_count += 1;
        }

        if batch_count > 1 {
            trace!(
                batch_size = batch_count,
                total_bytes = total_bytes,
                queue_size = queue_len,
                "sending udp batch"
            );
        }

        for (ret, chunk) in join_all(send_futures.drain(..)).await {
            if let Err(err) = &ret {
                match err.kind() {
                    ErrorKind::NetworkUnreachable => {
                        debug!("send address family mismatch. message is dropped")
                    }
                    ErrorKind::InvalidInput => {
                        warn!(len = chunk.len(), "got EINVAL on send. message is dropped")
                    }
                    _ => {
                        if is_eafnosupport(err) {
                            debug!("send address family mismatch. message is dropped");
                        } else {
                            error!(
                                len = chunk.len(),
                                ?err,
                                "unexpected send error. message is dropped"
                            );
                        }
                    }
                }
            }
        }

        if total_bytes > 0 {
            next_transmit +=
                Duration::from_nanos((total_bytes as u64) * 8 * 1000 / up_bandwidth_mbps);
        }
    }
}

async fn fill_message_queues(
    udp_egress_rx: &mut mpsc::Receiver<UdpMsg>,
    priority_queues: &mut PriorityQueues,
) -> Result<(), ()> {
    while priority_queues.is_empty() || !udp_egress_rx.is_empty() {
        match udp_egress_rx.recv().await {
            Some(udp_msg) => {
                if let Err(err) = priority_queues.try_push(udp_msg) {
                    warn!(?err, "priority queue capacity exceeded, dropping message");
                }
            }
            None => return Err(()),
        }
    }
    Ok(())
}

const MAX_AGGREGATED_WRITE_SIZE: u16 = 65535 - IPV4_HDR_SIZE - UDP_HDR_SIZE;
const MAX_AGGREGATED_SEGMENTS: u16 = 128;

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

    fn create_test_msg(priority: UdpPriority, payload_size: usize) -> UdpMsg {
        UdpMsg {
            socket_id: UdpSocketId::Raptorcast,
            dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            payload: Bytes::from(vec![0u8; payload_size]),
            stride: 1024,
            priority,
        }
    }

    #[test]
    fn test_priority_queue_capacity() {
        let mut queue = PriorityQueues::with_bytes_capacity(1000);

        assert!(queue
            .try_push(create_test_msg(UdpPriority::High, 800))
            .is_ok());
        assert!(queue
            .try_push(create_test_msg(UdpPriority::Regular, 800))
            .is_ok());

        assert!(queue
            .try_push(create_test_msg(UdpPriority::High, 300))
            .is_err());
        assert!(queue
            .try_push(create_test_msg(UdpPriority::Regular, 300))
            .is_err());

        let popped = queue.pop_highest_priority();
        assert_eq!(popped.unwrap().priority, UdpPriority::High);

        assert!(queue
            .try_push(create_test_msg(UdpPriority::High, 500))
            .is_ok());
        assert!(queue
            .try_push(create_test_msg(UdpPriority::Regular, 300))
            .is_err());
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
