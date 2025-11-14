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
    collections::VecDeque,
    io::{Error, ErrorKind},
    net::SocketAddr,
    os::fd::{AsRawFd, FromRawFd},
    time::{Duration, Instant},
};

use bytes::BytesMut;
use futures::future::join_all;
use monad_types::UdpPriority;
use monoio::{net::udp::UdpSocket, spawn, time};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use super::{RecvUdpMsg, UdpMsg};
use crate::buffer_ext::SocketBufferExt;

#[derive(Debug, Clone, Copy)]
pub(crate) enum UdpMessageType {
    Broadcast,
    Direct,
}

const PRIORITY_QUEUE_BYTES_CAPACITY: usize = 100 * 1024 * 1024;

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

pub const DEFAULT_MTU: u16 = ETHERNET_MTU;

const IPV4_HDR_SIZE: u16 = 20;
const UDP_HDR_SIZE: u16 = 8;
pub const fn segment_size_for_mtu(mtu: u16) -> u16 {
    mtu - IPV4_HDR_SIZE - UDP_HDR_SIZE
}

pub const DEFAULT_SEGMENT_SIZE: u16 = segment_size_for_mtu(DEFAULT_MTU);

const ETHERNET_MTU: u16 = 1500;
pub const ETHERNET_SEGMENT_SIZE: u16 = segment_size_for_mtu(ETHERNET_MTU);

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
    socket_configs: Vec<(usize, SocketAddr, String, mpsc::Sender<RecvUdpMsg>)>,
    udp_egress_rx: mpsc::Receiver<UdpMsg>,
    up_bandwidth_mbps: u64,
    buffer_size: Option<usize>,
    uring_config: Option<crate::UringConfig>,
    socket_readers: usize,
    max_write_size: usize,
) {
    let mut tx_sockets = Vec::new();

    if let Some(uring_cfg) = uring_config {
        use std::collections::HashMap;

        let uring_configs: Vec<crate::uring::reader::SocketConfig> = socket_configs
            .iter()
            .map(
                |(socket_id, socket_addr, label, _)| crate::uring::reader::SocketConfig {
                    socket_id: *socket_id,
                    socket_addr: *socket_addr,
                    label: label.clone(),
                },
            )
            .collect();

        let tx_map: HashMap<usize, mpsc::Sender<RecvUdpMsg>> = socket_configs
            .iter()
            .map(|(socket_id, _, _, ingress_tx)| (*socket_id, ingress_tx.clone()))
            .collect();

        let fds = crate::uring::reader::spawn(
            uring_configs,
            move |socket_id, msg| {
                tx_map
                    .get(&socket_id)
                    .expect("valid socket_id")
                    .blocking_send(msg)
                    .map_err(|e| {
                        warn!(socket_id, error = %e, "failed to send message to ingress channel");
                    })
            },
            uring_cfg.num_readers,
            uring_cfg.batch_size,
            buffer_size,
            uring_cfg.use_ringbuf,
        );

        for ((socket_id, socket_addr, label, _), fd) in socket_configs.into_iter().zip(fds) {
            let tx = UdpSocket::from_std(unsafe { std::net::UdpSocket::from_raw_fd(fd) })
                .expect("failed to create tx socket from fd");
            trace!(socket_id, label = %label, ?socket_addr, "created socket with io_uring");
            tx_sockets.push(tx);
        }
    } else {
        for (socket_id, socket_addr, label, ingress_tx) in socket_configs {
            let socket = std::net::UdpSocket::bind(socket_addr).unwrap();
            let monoio_socket = UdpSocket::from_std(socket).unwrap();
            configure_socket(&monoio_socket, buffer_size);
            let raw_fd = monoio_socket.as_raw_fd();

            let tx_fd = unsafe { libc::dup(raw_fd) };
            if tx_fd < 0 {
                panic!(
                    "failed to duplicate socket fd for tx: {}",
                    Error::last_os_error()
                );
            }
            let tx =
                UdpSocket::from_std(unsafe { std::net::UdpSocket::from_raw_fd(tx_fd) }).unwrap();

            for _ in 0..socket_readers {
                let rx_fd = unsafe { libc::dup(raw_fd) };
                if rx_fd < 0 {
                    panic!(
                        "failed to duplicate socket fd for rx: {}",
                        Error::last_os_error()
                    );
                }
                let rx_clone =
                    unsafe { UdpSocket::from_std(std::net::UdpSocket::from_raw_fd(rx_fd)) }
                        .unwrap();
                spawn(rx_single_socket(rx_clone, ingress_tx.clone()));
            }

            std::mem::forget(monoio_socket);

            trace!(socket_id, label = %label, ?socket_addr, readers = socket_readers, "created socket");
            tx_sockets.push(tx);
        }
    }

    spawn(tx(
        tx_sockets,
        udp_egress_rx,
        up_bandwidth_mbps,
        max_write_size,
    ));
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

const PACING_SLEEP_OVERSHOOT_DETECTION_WINDOW: Duration = Duration::from_millis(100);

async fn tx(
    tx_sockets: Vec<UdpSocket>,
    mut udp_egress_rx: mpsc::Receiver<UdpMsg>,
    up_bandwidth_mbps: u64,
    max_write_size: usize,
) {
    let mut next_transmit = Instant::now();

    let mut priority_queues = PriorityQueues::new();

    let max_batch_bytes = max_write_size;
    let max_segments =
        (max_write_size / DEFAULT_SEGMENT_SIZE as usize).min(MAX_AGGREGATED_SEGMENTS as usize);
    let mut send_futures = Vec::with_capacity(max_segments);

    let mut total_bytes_sent = 0u64;
    let mut total_packets_sent = 0u64;
    let mut last_log = Instant::now();
    let log_interval = Duration::from_secs(1);

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
            && batch_count < max_segments
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

            let socket = tx_sockets.get(socket_id).expect("valid socket_id");

            if !msg.payload.is_empty() {
                if let Err(err) = priority_queues.try_push(msg) {
                    warn!(?err, "failed to re-queue message with remaining payload");
                }
            }

            trace!(
                socket_id,
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
            total_bytes_sent += total_bytes as u64;
            total_packets_sent += batch_count as u64;

            let now = Instant::now();
            if now.duration_since(last_log) >= log_interval {
                let elapsed = now.duration_since(last_log).as_secs_f64();
                let packets_per_sec = total_packets_sent as f64 / elapsed;
                let mbps = (total_bytes_sent as f64 * 8.0) / elapsed / 1_000_000.0;

                info!(
                    packets_sent = total_packets_sent,
                    packets_per_sec = format!("{:.0}", packets_per_sec),
                    mbps = format!("{:.2}", mbps),
                    "dataplane tx throughput"
                );

                total_bytes_sent = 0;
                total_packets_sent = 0;
                last_log = now;
            }

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

const MAX_AGGREGATED_SEGMENTS: u16 = 128;

fn is_eafnosupport(err: &Error) -> bool {
    const EAFNOSUPPORT: &str = "Address family not supported by protocol";

    let err = format!("{}", err);

    err.len() >= EAFNOSUPPORT.len() && &err[0..EAFNOSUPPORT.len()] == EAFNOSUPPORT
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use bytes::Bytes;
    use monad_types::UdpPriority;

    use super::*;

    fn create_test_msg(priority: UdpPriority, payload_size: usize) -> UdpMsg {
        UdpMsg {
            socket_id: 0,
            dst: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            payload: Bytes::from(vec![0u8; payload_size]),
            stride: 1024,
            priority,
            msg_type: UdpMessageType::Broadcast,
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
}
