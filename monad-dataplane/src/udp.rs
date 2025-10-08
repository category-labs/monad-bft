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

use bytes::{Bytes, BytesMut};
use monoio::{net::udp::UdpSocket, spawn, time};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use super::{RecvUdpMsg, UdpMsg};
use crate::buffer_ext::SocketBufferExt;

#[derive(Debug, Clone)]
pub(crate) enum UdpMessageType {
    Broadcast,
    Direct,
}

pub const DEFAULT_MTU: u16 = ETHERNET_MTU;

const IPV4_HDR_SIZE: u16 = 20;
const UDP_HDR_SIZE: u16 = 8;
pub const fn segment_size_for_mtu(mtu: u16) -> u16 {
    mtu - IPV4_HDR_SIZE - UDP_HDR_SIZE
}

pub const DEFAULT_SEGMENT_SIZE: u16 = segment_size_for_mtu(DEFAULT_MTU);

const ETHERNET_MTU: u16 = 1500;
const ETHERNET_SEGMENT_SIZE: u16 = segment_size_for_mtu(ETHERNET_MTU);

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
    local_addr: SocketAddr,
    direct_socket_port: Option<u16>,
    udp_ingress_tx: mpsc::Sender<RecvUdpMsg>,
    udp_direct_ingress_tx: mpsc::Sender<RecvUdpMsg>,
    udp_egress_rx: mpsc::Receiver<(SocketAddr, UdpMsg)>,
    up_bandwidth_mbps: u64,
    buffer_size: Option<usize>,
) {
    let (udp_socket_rx, udp_socket_tx) = create_socket_pair(local_addr, buffer_size);
    let (direct_socket_rx, direct_socket_tx) = direct_socket_port
        .map(|port| {
            let mut direct_addr = local_addr;
            direct_addr.set_port(port);
            let (rx, tx) = create_socket_pair(direct_addr, buffer_size);
            (Some(rx), Some(tx))
        })
        .unwrap_or((None, None));

    spawn(rx(
        udp_socket_rx,
        direct_socket_rx,
        udp_ingress_tx,
        udp_direct_ingress_tx,
    ));
    spawn(tx(
        udp_socket_tx,
        direct_socket_tx,
        udp_egress_rx,
        up_bandwidth_mbps,
    ));
}

fn create_socket_pair(addr: SocketAddr, buffer_size: Option<usize>) -> (UdpSocket, UdpSocket) {
    let rx = UdpSocket::bind(addr).unwrap();
    configure_socket(&rx, buffer_size);
    let tx =
        UdpSocket::from_std(unsafe { std::net::UdpSocket::from_raw_fd(rx.as_raw_fd()) }).unwrap();
    (rx, tx)
}

async fn rx(
    udp_socket_rx: UdpSocket,
    direct_socket_rx: Option<UdpSocket>,
    udp_ingress_tx: mpsc::Sender<RecvUdpMsg>,
    udp_direct_ingress_tx: mpsc::Sender<RecvUdpMsg>,
) {
    match direct_socket_rx {
        Some(direct_socket) => {
            spawn(rx_single_socket(udp_socket_rx, udp_ingress_tx));
            spawn(rx_single_socket(direct_socket, udp_direct_ingress_tx));
        }
        None => {
            rx_single_socket(udp_socket_rx, udp_ingress_tx).await;
        }
    }
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
    socket_tx: UdpSocket,
    direct_socket_tx: Option<UdpSocket>,
    mut udp_egress_rx: mpsc::Receiver<(SocketAddr, UdpMsg)>,
    up_bandwidth_mbps: u64,
) {
    let mut udp_segment_size: u16 = DEFAULT_SEGMENT_SIZE;
    set_udp_segment_size(&socket_tx, udp_segment_size);
    if let Some(ref direct_socket) = direct_socket_tx {
        set_udp_segment_size(direct_socket, udp_segment_size);
    }
    let mut max_chunk: u16 = max_write_size_for_segment_size(udp_segment_size);

    let mut next_transmit = Instant::now();

    let mut messages_to_send: VecDeque<(SocketAddr, Bytes, u16, UdpMessageType)> = VecDeque::new();

    loop {
        while messages_to_send.is_empty() || !udp_egress_rx.is_empty() {
            let Some((addr, udp_msg)) = udp_egress_rx.recv().await else {
                return;
            };

            messages_to_send.push_back((addr, udp_msg.payload, udp_msg.stride, udp_msg.msg_type));
        }

        let (addr, mut payload, stride, msg_type) = messages_to_send.pop_front().unwrap();

        if udp_segment_size != stride {
            udp_segment_size = stride;
            set_udp_segment_size(&socket_tx, udp_segment_size);
            max_chunk = max_write_size_for_segment_size(udp_segment_size);
        }

        // Transmit the first max_chunk bytes of this (addr, payload) pair.
        let chunk = payload.split_to(payload.len().min(max_chunk.into()));
        let chunk_len = chunk.len();

        let now = Instant::now();

        if next_transmit > now {
            time::sleep(next_transmit - now).await;
        } else {
            let late = now - next_transmit;

            if late > PACING_SLEEP_OVERSHOOT_DETECTION_WINDOW {
                next_transmit = now;
            }
        }

        let socket = match (&msg_type, &direct_socket_tx) {
            (UdpMessageType::Direct, Some(direct_socket)) => direct_socket,
            _ => &socket_tx,
        };

        let (ret, chunk) = socket.send_to(chunk, addr).await;

        if let Err(err) = &ret {
            match err.kind() {
                // ENETUNREACH is returned when trying to send to an IPv4 address from a
                // socket bound to a local IPv6 address.
                ErrorKind::NetworkUnreachable => debug!(
                    local_addr =? socket.local_addr().unwrap(),
                    ?addr,
                    "send address family mismatch. message is dropped"
                ),

                // TODO: An EINVAL return is likely due to MTU/GSO issues -- we should fall
                // back to disabling GSO for this chunk and transmitting the constituent
                // segments individually.
                ErrorKind::InvalidInput => warn!(
                    local_addr =? socket.local_addr().unwrap(),
                    udp_segment_size,
                    max_chunk,
                    ?addr,
                    len = chunk.len(),
                    "got EINVAL on send. message is dropped"
                ),

                // EAFNOSUPPORT is returned when trying to send to an IPv6 address from a
                // socket bound to an IPv4 address.
                //
                // EAFNOSUPPORT is returned as ErrorKind::Uncategorized, which can't be
                // matched against, so it has to be tested for under the wildcard match.
                _ => {
                    if is_eafnosupport(err) {
                        debug!(
                            local_addr =? socket.local_addr().unwrap(),
                            ?addr,
                            "send address family mismatch. message is dropped"
                        );
                    } else {
                        error!(
                            local_addr =? socket.local_addr().unwrap(),
                            udp_segment_size,
                            max_chunk,
                            ?addr,
                            len = chunk.len(),
                            ?err,
                            "unexpected send error. message is dropped"
                        );
                    }
                }
            }
        }

        // the remainder of the message is re-queued only if the send is succesful
        if ret.is_ok() {
            next_transmit +=
                Duration::from_nanos((chunk_len as u64) * 8 * 1000 / up_bandwidth_mbps);

            // Re-queue (addr, payload) at the end of the list if there are bytes left to transmit.
            if !payload.is_empty() {
                messages_to_send.push_back((addr, payload, stride, msg_type));
            }
        }
    }
}

fn set_udp_segment_size(socket: &UdpSocket, udp_segment_size: u16) {
    let udp_segment_size: libc::c_int = udp_segment_size as i32;

    if unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_UDP,
            libc::UDP_SEGMENT,
            &udp_segment_size as *const _ as _,
            std::mem::size_of_val(&udp_segment_size) as _,
        )
    } != 0
    {
        panic!("set UDP_SEGMENT failed with: {}", Error::last_os_error());
    }
}

const MAX_AGGREGATED_WRITE_SIZE: u16 = 65535 - IPV4_HDR_SIZE - UDP_HDR_SIZE;
const MAX_AGGREGATED_SEGMENTS: u16 = 128;

fn max_write_size_for_segment_size(segment_size: u16) -> u16 {
    (MAX_AGGREGATED_WRITE_SIZE / segment_size).min(MAX_AGGREGATED_SEGMENTS) * segment_size
}

// This is very very ugly, but there is no other way to figure this out.
fn is_eafnosupport(err: &Error) -> bool {
    const EAFNOSUPPORT: &str = "Address family not supported by protocol";

    let err = format!("{}", err);

    err.len() >= EAFNOSUPPORT.len() && &err[0..EAFNOSUPPORT.len()] == EAFNOSUPPORT
}
