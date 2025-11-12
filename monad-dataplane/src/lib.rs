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
    fmt::Debug,
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use addrlist::Addrlist;
use bytes::Bytes;
use futures::channel::oneshot;
use monad_types::UdpPriority;
use monoio::{spawn, time::Instant, IoUringDriver, RuntimeBuilder};
use tcp::{TcpConfig, TcpControl, TcpRateLimit};
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::{debug, warn};

pub(crate) mod addrlist;
pub(crate) mod ban_expiry;
pub(crate) mod buffer_ext;
pub mod tcp;
pub mod udp;

pub struct UdpSocketConfig {
    pub socket_addr: SocketAddr,
    pub label: String,
}

pub struct DataplaneBuilder {
    local_addr: SocketAddr,
    trusted_addresses: Vec<IpAddr>,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    udp_up_bandwidth_mbps: u64,
    udp_buffer_size: Option<usize>,
    tcp_config: TcpConfig,
    ban_duration: Duration,
    udp_sockets: Vec<UdpSocketConfig>,
}

impl DataplaneBuilder {
    pub fn new(local_addr: &SocketAddr, up_bandwidth_mbps: u64) -> Self {
        Self {
            local_addr: *local_addr,
            udp_up_bandwidth_mbps: up_bandwidth_mbps,
            udp_buffer_size: None,
            trusted_addresses: vec![],
            tcp_config: TcpConfig {
                rate_limit: TcpRateLimit {
                    rps: NonZeroU32::new(10000).unwrap(),
                    rps_burst: NonZeroU32::new(2000).unwrap(),
                },
                connections_limit: 10000,
                per_ip_connections_limit: 100,
            },
            ban_duration: Duration::from_secs(5 * 60), // 5 minutes
            udp_sockets: vec![],
        }
    }

    pub fn with_udp_buffer_size(mut self, buffer_size: usize) -> Self {
        self.udp_buffer_size = Some(buffer_size);
        self
    }

    pub fn with_tcp_connections_limit(mut self, total: usize, per_ip: usize) -> Self {
        self.tcp_config.connections_limit = total;
        self.tcp_config.per_ip_connections_limit = if per_ip == 0 { total } else { per_ip };
        self
    }

    pub fn with_tcp_rps_burst(mut self, rps: u32, burst: u32) -> Self {
        self.tcp_config.rate_limit.rps = NonZeroU32::new(rps).expect("rps must be non-zero");
        self.tcp_config.rate_limit.rps_burst =
            NonZeroU32::new(burst).expect("burst must be non-zero");
        self
    }

    pub fn with_trusted_ips(mut self, ips: Vec<IpAddr>) -> Self {
        self.trusted_addresses = ips;
        self
    }

    pub fn extend_udp_sockets(mut self, sockets: Vec<UdpSocketConfig>) -> Self {
        self.udp_sockets.extend(sockets);
        self
    }

    pub fn build(self) -> Dataplane {
        let DataplaneBuilder {
            local_addr,
            udp_up_bandwidth_mbps: up_bandwidth_mbps,
            udp_buffer_size,
            trusted_addresses: trusted,
            tcp_config,
            ban_duration,
            udp_sockets,
        } = self;

        let mut seen_labels = std::collections::HashSet::new();
        let mut seen_ports = std::collections::HashSet::new();
        for socket in &udp_sockets {
            assert!(
                seen_labels.insert(socket.label.clone()),
                "duplicate udp socket label: {}",
                socket.label
            );
            assert!(
                seen_ports.insert(socket.socket_addr.port()),
                "duplicate udp socket port: {}",
                socket.socket_addr.port()
            );
        }

        let (tcp_ingress_tx, tcp_ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);

        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);

        let mut udp_socket_handles = Vec::new();
        let mut socket_configs = Vec::new();

        for (socket_id, UdpSocketConfig { socket_addr, label }) in
            udp_sockets.into_iter().enumerate()
        {
            let (handle, config) =
                create_socket_handle(socket_id, socket_addr, label, udp_egress_tx.clone());
            udp_socket_handles.push(handle);
            socket_configs.push(config);
        }

        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();

        let (banned_ips_tx, banned_ips_rx) = mpsc::unbounded_channel();
        let addrlist = Arc::new(Addrlist::new_with_trusted(trusted.into_iter()));
        let tcp_control_map = TcpControl::new();
        thread::Builder::new()
            .name("monad-dataplane".into())
            .spawn({
                let tcp_control_map = tcp_control_map.clone();
                let addrlist = addrlist.clone();
                move || {
                    RuntimeBuilder::<IoUringDriver>::new()
                        .enable_timer()
                        .build()
                        .expect("Failed building the Runtime")
                        .block_on(async move {
                            spawn(ban_expiry::task(
                                addrlist.clone(),
                                banned_ips_rx,
                                ban_duration,
                            ));

                            tcp::spawn_tasks(
                                tcp_config,
                                tcp_control_map,
                                addrlist.clone(),
                                local_addr,
                                tcp_ingress_tx,
                                tcp_egress_rx,
                            );
                            udp::spawn_tasks(
                                socket_configs,
                                udp_egress_rx,
                                up_bandwidth_mbps,
                                udp_buffer_size,
                            );

                            ready_clone.store(true, Ordering::Release);

                            futures::future::pending::<()>().await;
                        });
                }
            })
            .expect("failed to spawn dataplane thread");

        let control = DataplaneControl::new(tcp_control_map, banned_ips_tx, addrlist);

        let tcp_reader = TcpSocketReader {
            ingress_rx: tcp_ingress_rx,
        };
        let tcp_writer = TcpSocketWriter {
            egress_tx: tcp_egress_tx,
            msgs_dropped: Arc::new(AtomicUsize::new(0)),
        };
        let tcp_socket = TcpSocketHandle {
            reader: tcp_reader,
            writer: tcp_writer,
        };

        Dataplane {
            tcp_socket: Some(tcp_socket),
            udp_socket_handles,
            control,
            ready,
        }
    }
}

pub struct Dataplane {
    tcp_socket: Option<TcpSocketHandle>,
    udp_socket_handles: Vec<UdpSocketHandle>,
    control: DataplaneControl,
    ready: Arc<AtomicBool>,
}

pub struct UdpSocketReader {
    socket_id: usize,
    label: String,
    ingress_rx: mpsc::Receiver<RecvUdpMsg>,
}

impl UdpSocketReader {
    pub async fn recv(&mut self) -> RecvUdpMsg {
        self.ingress_rx.recv().await.unwrap_or_else(|| {
            panic!(
                "socket {} ({}) ingress channel closed",
                self.socket_id, self.label
            )
        })
    }
}

#[derive(Clone)]
pub struct UdpSocketWriter {
    socket_id: usize,
    socket_addr: SocketAddr,
    label: String,
    egress_tx: mpsc::Sender<UdpMsg>,
    msgs_dropped: Arc<AtomicUsize>,
}

pub struct UdpSocketHandle {
    reader: UdpSocketReader,
    writer: UdpSocketWriter,
}

impl UdpSocketHandle {
    pub fn split(self) -> (UdpSocketReader, UdpSocketWriter) {
        (self.reader, self.writer)
    }

    pub async fn recv(&mut self) -> RecvUdpMsg {
        self.reader.recv().await
    }

    pub fn write(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
        self.writer.write(dst, payload, stride)
    }

    pub fn write_broadcast(&self, msg: BroadcastMsg) {
        self.writer.write_broadcast(msg)
    }

    pub fn write_broadcast_with_priority(&self, msg: BroadcastMsg, priority: UdpPriority) {
        self.writer.write_broadcast_with_priority(msg, priority)
    }

    pub fn write_unicast(&self, msg: UnicastMsg) {
        self.writer.write_unicast(msg)
    }

    pub fn write_unicast_with_priority(&self, msg: UnicastMsg, priority: UdpPriority) {
        self.writer.write_unicast_with_priority(msg, priority)
    }

    pub fn writer(&self) -> &UdpSocketWriter {
        &self.writer
    }

    pub fn label(&self) -> &str {
        &self.writer.label
    }
}

impl UdpSocketWriter {
    pub fn write(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
        let msg_length = payload.len();
        let result = self.egress_tx.try_send(UdpMsg {
            socket_id: self.socket_id,
            dst,
            payload,
            stride,
            priority: UdpPriority::Regular,
        });

        match result {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let total = self.msgs_dropped.fetch_add(1, Ordering::Relaxed);
                warn!(
                    socket_id = self.socket_id,
                    label = %self.label,
                    ?dst,
                    msg_length,
                    total_msgs_dropped = total,
                    "udp egress channel full, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => {
                panic!(
                    "socket {} ({}) egress channel closed",
                    self.socket_id, self.label
                )
            }
        }
    }

    pub fn write_broadcast(&self, msg: BroadcastMsg) {
        self.write_broadcast_with_priority(msg, UdpPriority::Regular);
    }

    pub fn write_broadcast_with_priority(&self, msg: BroadcastMsg, priority: UdpPriority) {
        let msg_len = msg.payload.len();
        let mut pending_count = msg.msg_count();

        for udp_msg in msg.into_iter_with_priority(self.socket_id, priority) {
            match self.egress_tx.try_send(udp_msg) {
                Ok(()) => pending_count -= 1,
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Closed(_)) => {
                    panic!(
                        "socket {} ({}) egress channel closed",
                        self.socket_id, self.label
                    )
                }
            }
        }

        if pending_count > 0 {
            let total = self
                .msgs_dropped
                .fetch_add(pending_count, Ordering::Relaxed);
            warn!(
                socket_id = self.socket_id,
                label = %self.label,
                num_msgs_dropped = pending_count,
                total_msgs_dropped = total,
                msg_length = msg_len,
                ?priority,
                "udp egress channel full, dropping broadcast messages"
            );
        }
    }

    pub fn write_unicast(&self, msg: UnicastMsg) {
        self.write_unicast_with_priority(msg, UdpPriority::Regular);
    }

    pub fn write_unicast_with_priority(&self, msg: UnicastMsg, priority: UdpPriority) {
        let mut pending_count = msg.msg_count();

        for udp_msg in msg.into_iter_with_priority(self.socket_id, priority) {
            match self.egress_tx.try_send(udp_msg) {
                Ok(()) => pending_count -= 1,
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Closed(_)) => {
                    panic!(
                        "socket {} ({}) egress channel closed",
                        self.socket_id, self.label
                    )
                }
            }
        }

        if pending_count > 0 {
            let total = self
                .msgs_dropped
                .fetch_add(pending_count, Ordering::Relaxed);
            warn!(
                socket_id = self.socket_id,
                label = %self.label,
                num_msgs_dropped = pending_count,
                total_msgs_dropped = total,
                ?priority,
                "udp egress channel full, dropping unicast messages"
            );
        }
    }
}

impl Debug for UdpSocketHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpSocketHandle")
            .field("socket_id", &self.writer.socket_id)
            .field("label", &self.writer.label)
            .field("socket_addr", &self.writer.socket_addr)
            .finish()
    }
}

pub struct UdpDataplane {
    socket_handles: Vec<UdpSocketHandle>,
}

impl UdpDataplane {
    pub fn take_socket(&mut self, label: &str) -> Option<UdpSocketHandle> {
        self.socket_handles
            .iter()
            .position(|h| h.label() == label)
            .map(|idx| self.socket_handles.swap_remove(idx))
    }
}

pub struct TcpSocketReader {
    ingress_rx: mpsc::Receiver<RecvTcpMsg>,
}

impl TcpSocketReader {
    pub async fn recv(&mut self) -> RecvTcpMsg {
        self.ingress_rx
            .recv()
            .await
            .unwrap_or_else(|| panic!("tcp ingress channel closed"))
    }
}

#[derive(Clone)]
pub struct TcpSocketWriter {
    egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
    msgs_dropped: Arc<AtomicUsize>,
}

impl TcpSocketWriter {
    pub fn write(&self, addr: SocketAddr, msg: TcpMsg) {
        let msg_length = msg.msg.len();

        match self.egress_tx.try_send((addr, msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let total = self.msgs_dropped.fetch_add(1, Ordering::Relaxed);
                warn!(
                    ?addr,
                    msg_length,
                    total_msgs_dropped = total,
                    "tcp egress channel full, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => panic!("tcp egress channel closed"),
        }
    }
}

pub struct TcpSocketHandle {
    reader: TcpSocketReader,
    writer: TcpSocketWriter,
}

impl TcpSocketHandle {
    pub fn split(self) -> (TcpSocketReader, TcpSocketWriter) {
        (self.reader, self.writer)
    }

    pub async fn recv(&mut self) -> RecvTcpMsg {
        self.reader.recv().await
    }

    pub fn write(&self, addr: SocketAddr, msg: TcpMsg) {
        self.writer.write(addr, msg)
    }
}

#[derive(Clone)]
pub struct DataplaneControl {
    inner: Arc<DataplaneControlInner>,
}

struct DataplaneControlInner {
    tcp_control_map: TcpControl,
    notify_ban_expiry: mpsc::UnboundedSender<(IpAddr, Instant)>,
    addrlist: Arc<Addrlist>,
}

impl DataplaneControl {
    fn new(
        tcp_control_map: TcpControl,
        notify_ban_expiry: mpsc::UnboundedSender<(IpAddr, Instant)>,
        addrlist: Arc<Addrlist>,
    ) -> Self {
        let inner = DataplaneControlInner {
            tcp_control_map,
            notify_ban_expiry,
            addrlist,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// add_trusted marks ip address as trusted.
    /// connections limits are not applied to trusted ips.
    pub fn add_trusted(&self, addr: IpAddr) {
        self.inner.addrlist.add_trusted(&addr);
    }

    /// remove_trusted removes ip address from trusted list.
    pub fn remove_trusted(&self, addr: IpAddr) {
        self.inner.addrlist.remove_trusted(&addr);
    }

    /// update_trusted updates the trusted addresses.
    pub fn update_trusted(&self, added: Vec<IpAddr>, removed: Vec<IpAddr>) {
        debug!(?added, ?removed, "updating trusted entities");

        self.inner
            .addrlist
            .update_trusted(added.into_iter(), removed.into_iter());
    }

    /// ban ip address. ban duration is specified in dataplane config.
    pub fn ban(&self, ip: IpAddr) {
        let now = Instant::now();
        self.inner.addrlist.ban(&ip, now);
        self.inner.notify_ban_expiry.send((ip, now)).unwrap();
        self.disconnect_ip(ip);
    }

    /// disconnect all connections from specified ip address.
    pub fn disconnect_ip(&self, ip: IpAddr) {
        self.inner.tcp_control_map.disconnect_ip(ip);
    }

    /// disconnect single connection.
    pub fn disconnect(&self, addr: SocketAddr) {
        self.inner
            .tcp_control_map
            .disconnect_socket(addr.ip(), addr.port());
    }
}

#[derive(Clone)]
pub struct BroadcastMsg {
    pub targets: Vec<SocketAddr>,
    pub payload: Bytes,
    pub stride: u16,
}

impl BroadcastMsg {
    fn msg_count(&self) -> usize {
        self.targets.len()
    }

    fn into_iter_with_priority(
        self,
        socket_id: usize,
        priority: UdpPriority,
    ) -> impl Iterator<Item = UdpMsg> {
        let Self {
            targets,
            payload,
            stride,
        } = self;
        targets.into_iter().map(move |dst| UdpMsg {
            socket_id,
            dst,
            payload: payload.clone(),
            stride,
            priority,
        })
    }
}

#[derive(Clone)]
pub struct UnicastMsg {
    pub msgs: Vec<(SocketAddr, Bytes)>,
    pub stride: u16,
}

impl UnicastMsg {
    fn msg_count(&self) -> usize {
        self.msgs.len()
    }

    fn into_iter_with_priority(
        self,
        socket_id: usize,
        priority: UdpPriority,
    ) -> impl Iterator<Item = UdpMsg> {
        let Self { msgs, stride } = self;
        msgs.into_iter().map(move |(dst, payload)| UdpMsg {
            socket_id,
            dst,
            payload,
            stride,
            priority,
        })
    }
}

#[derive(Clone)]
pub struct RecvUdpMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub stride: u16,
}

#[derive(Clone)]
pub struct RecvTcpMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
}

pub struct TcpMsg {
    pub msg: Bytes,
    pub completion: Option<oneshot::Sender<()>>,
}

pub(crate) struct UdpMsg {
    pub(crate) socket_id: usize,
    pub(crate) dst: SocketAddr,
    pub(crate) payload: Bytes,
    pub(crate) stride: u16,
    pub(crate) priority: UdpPriority,
}

const TCP_INGRESS_CHANNEL_SIZE: usize = 1024;
const TCP_EGRESS_CHANNEL_SIZE: usize = 1024;
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;
const UDP_EGRESS_CHANNEL_SIZE: usize = 12_800;

fn create_socket_handle(
    socket_id: usize,
    socket_addr: SocketAddr,
    label: String,
    egress_tx: mpsc::Sender<UdpMsg>,
) -> (
    UdpSocketHandle,
    (usize, SocketAddr, String, mpsc::Sender<RecvUdpMsg>),
) {
    let (ingress_tx, ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
    let msgs_dropped = Arc::new(AtomicUsize::new(0));

    let reader = UdpSocketReader {
        socket_id,
        label: label.clone(),
        ingress_rx,
    };

    let writer = UdpSocketWriter {
        socket_id,
        socket_addr,
        label: label.clone(),
        egress_tx,
        msgs_dropped,
    };

    let handle = UdpSocketHandle { reader, writer };
    let config = (socket_id, socket_addr, label, ingress_tx);
    (handle, config)
}

impl Dataplane {
    pub fn split(self) -> (TcpSocketHandle, UdpDataplane, DataplaneControl) {
        let tcp_socket = self.tcp_socket.expect("tcp socket already taken");
        let udp = UdpDataplane {
            socket_handles: self.udp_socket_handles,
        };
        (tcp_socket, udp, self.control)
    }

    pub fn take_tcp_socket(&mut self) -> Option<TcpSocketHandle> {
        self.tcp_socket.take()
    }

    pub fn udp_socket_handles(&mut self) -> &mut [UdpSocketHandle] {
        &mut self.udp_socket_handles
    }

    pub fn take_udp_socket_handle(&mut self, label: &str) -> Option<UdpSocketHandle> {
        self.udp_socket_handles
            .iter()
            .position(|h| h.label() == label)
            .map(|idx| self.udp_socket_handles.swap_remove(idx))
    }

    pub fn add_trusted(&self, addr: IpAddr) {
        self.control.add_trusted(addr);
    }

    pub fn remove_trusted(&self, addr: IpAddr) {
        self.control.remove_trusted(addr);
    }

    pub fn update_trusted(&self, added: Vec<IpAddr>, removed: Vec<IpAddr>) {
        self.control.update_trusted(added, removed);
    }

    pub fn ban(&self, ip: IpAddr) {
        self.control.ban(ip);
    }

    pub fn disconnect_ip(&self, ip: IpAddr) {
        self.control.disconnect_ip(ip);
    }

    pub fn disconnect(&self, addr: SocketAddr) {
        self.control.disconnect(addr);
    }

    pub async fn tcp_read(&mut self) -> RecvTcpMsg {
        self.tcp_socket
            .as_mut()
            .expect("tcp socket already taken")
            .recv()
            .await
    }

    pub fn tcp_write(&self, addr: SocketAddr, msg: TcpMsg) {
        self.tcp_socket
            .as_ref()
            .expect("tcp socket already taken")
            .write(addr, msg);
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn block_until_ready(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while !self.ready() {
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
    }
}
