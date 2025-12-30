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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TcpSocketId {
    Raptorcast,
    AuthenticatedRaptorcast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UdpSocketId {
    Raptorcast,
    AuthenticatedRaptorcast,
}

#[derive(Default)]
pub struct TcpSocketHandles {
    handles: Vec<(TcpSocketId, TcpSocketHandle)>,
}

impl TcpSocketHandles {
    fn push(&mut self, id: TcpSocketId, handle: TcpSocketHandle) {
        self.handles.push((id, handle));
    }

    pub fn take(&mut self, id: TcpSocketId) -> Option<TcpSocketHandle> {
        self.handles
            .iter()
            .position(|(h_id, _)| *h_id == id)
            .map(|idx| self.handles.swap_remove(idx).1)
    }
}

#[derive(Default)]
pub struct UdpSocketHandles {
    handles: Vec<(UdpSocketId, UdpSocketHandle)>,
}

impl UdpSocketHandles {
    fn push(&mut self, id: UdpSocketId, handle: UdpSocketHandle) {
        self.handles.push((id, handle));
    }

    pub fn take(&mut self, id: UdpSocketId) -> Option<UdpSocketHandle> {
        self.handles
            .iter()
            .position(|(h_id, _)| *h_id == id)
            .map(|idx| self.handles.swap_remove(idx).1)
    }
}

pub struct DataplaneBuilder {
    trusted_addresses: Vec<IpAddr>,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    udp_up_bandwidth_mbps: u64,
    udp_buffer_size: Option<usize>,
    tcp_config: TcpConfig,
    ban_duration: Duration,
    udp_sockets: Vec<(UdpSocketId, SocketAddr)>,
    tcp_sockets: Vec<(TcpSocketId, SocketAddr)>,
    udp_multishot: bool,
}

impl DataplaneBuilder {
    pub fn new(up_bandwidth_mbps: u64) -> Self {
        Self {
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
            ban_duration: Duration::from_secs(5 * 60),
            udp_sockets: Vec::new(),
            tcp_sockets: Vec::new(),
            udp_multishot: true,
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

    pub fn with_udp_sockets(
        mut self,
        sockets: impl IntoIterator<Item = (UdpSocketId, SocketAddr)>,
    ) -> Self {
        self.udp_sockets.extend(sockets);
        self
    }

    pub fn with_tcp_sockets(
        mut self,
        sockets: impl IntoIterator<Item = (TcpSocketId, SocketAddr)>,
    ) -> Self {
        self.tcp_sockets.extend(sockets);
        self
    }

    pub fn with_udp_multishot(mut self, enabled: bool) -> Self {
        self.udp_multishot = enabled;
        self
    }

    pub fn build(self) -> Dataplane {
        let DataplaneBuilder {
            udp_up_bandwidth_mbps: up_bandwidth_mbps,
            udp_buffer_size,
            trusted_addresses: trusted,
            tcp_config,
            ban_duration,
            udp_sockets,
            tcp_sockets,
            udp_multishot,
        } = self;

        validate_sockets(udp_sockets.iter(), "udp");
        validate_sockets(tcp_sockets.iter(), "tcp");

        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);
        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);

        let mut udp_socket_configs = Vec::new();
        let mut udp_socket_handles = UdpSocketHandles::default();
        for (id, addr) in udp_sockets {
            let (handle, config) = create_udp_socket_handle(id, addr, udp_egress_tx.clone());
            udp_socket_configs.push(config);
            udp_socket_handles.push(id, handle);
        }

        let mut tcp_socket_configs = Vec::new();
        let mut tcp_socket_handles = TcpSocketHandles::default();
        for (id, addr) in tcp_sockets {
            let (handle, config) = create_tcp_socket_handle(id, addr, tcp_egress_tx.clone());
            tcp_socket_configs.push(config);
            tcp_socket_handles.push(id, handle);
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
                                tcp_socket_configs,
                                tcp_egress_rx,
                            );
                            udp::spawn_tasks(
                                udp_socket_configs,
                                udp_egress_rx,
                                up_bandwidth_mbps,
                                udp_buffer_size,
                                udp_multishot,
                            );

                            ready_clone.store(true, Ordering::Release);

                            futures::future::pending::<()>().await;
                        });
                }
            })
            .expect("failed to spawn dataplane thread");

        let control = DataplaneControl::new(tcp_control_map, banned_ips_tx, addrlist);

        Dataplane {
            tcp_sockets: tcp_socket_handles,
            udp_sockets: udp_socket_handles,
            control,
            ready,
        }
    }
}

pub struct Dataplane {
    pub tcp_sockets: TcpSocketHandles,
    pub udp_sockets: UdpSocketHandles,
    pub control: DataplaneControl,
    ready: Arc<AtomicBool>,
}

pub struct UdpSocketReader {
    socket_id: UdpSocketId,
    ingress_rx: mpsc::Receiver<RecvUdpMsg>,
}

impl UdpSocketReader {
    pub async fn recv(&mut self) -> RecvUdpMsg {
        self.ingress_rx
            .recv()
            .await
            .unwrap_or_else(|| panic!("socket {:?} ingress channel closed", self.socket_id))
    }
}

#[derive(Clone)]
pub struct UdpSocketWriter {
    socket_id: UdpSocketId,
    socket_addr: SocketAddr,
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

    pub fn id(&self) -> UdpSocketId {
        self.writer.socket_id
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
                    socket_id = ?self.socket_id,
                    ?dst,
                    msg_length,
                    total_msgs_dropped = total,
                    "udp egress channel full, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => {
                panic!("socket {:?} egress channel closed", self.socket_id)
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
                    panic!("socket {:?} egress channel closed", self.socket_id)
                }
            }
        }

        if pending_count > 0 {
            let total = self
                .msgs_dropped
                .fetch_add(pending_count, Ordering::Relaxed);
            warn!(
                socket_id = ?self.socket_id,
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
                    panic!("socket {:?} egress channel closed", self.socket_id)
                }
            }
        }

        if pending_count > 0 {
            let total = self
                .msgs_dropped
                .fetch_add(pending_count, Ordering::Relaxed);
            warn!(
                socket_id = ?self.socket_id,
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
            .field("socket_addr", &self.writer.socket_addr)
            .finish()
    }
}

pub struct TcpSocketReader {
    socket_id: TcpSocketId,
    ingress_rx: mpsc::Receiver<RecvTcpMsg>,
}

impl TcpSocketReader {
    pub async fn recv(&mut self) -> RecvTcpMsg {
        self.ingress_rx
            .recv()
            .await
            .unwrap_or_else(|| panic!("socket {:?} tcp ingress channel closed", self.socket_id))
    }
}

#[derive(Clone)]
pub struct TcpSocketWriter {
    socket_id: TcpSocketId,
    socket_addr: SocketAddr,
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
                    socket_id = ?self.socket_id,
                    ?addr,
                    msg_length,
                    total_msgs_dropped = total,
                    "tcp egress channel full, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => {
                panic!("socket {:?} tcp egress channel closed", self.socket_id)
            }
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

    pub fn writer(&self) -> &TcpSocketWriter {
        &self.writer
    }

    pub fn id(&self) -> TcpSocketId {
        self.writer.socket_id
    }
}

impl Debug for TcpSocketHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocketHandle")
            .field("socket_id", &self.writer.socket_id)
            .field("socket_addr", &self.writer.socket_addr)
            .finish()
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
        socket_id: UdpSocketId,
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
        socket_id: UdpSocketId,
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
    pub(crate) socket_id: UdpSocketId,
    pub(crate) dst: SocketAddr,
    pub(crate) payload: Bytes,
    pub(crate) stride: u16,
    pub(crate) priority: UdpPriority,
}

const TCP_INGRESS_CHANNEL_SIZE: usize = 1024;
const TCP_EGRESS_CHANNEL_SIZE: usize = 1024;
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;
const UDP_EGRESS_CHANNEL_SIZE: usize = 12_800;

fn create_udp_socket_handle(
    socket_id: UdpSocketId,
    socket_addr: SocketAddr,
    egress_tx: mpsc::Sender<UdpMsg>,
) -> (
    UdpSocketHandle,
    (UdpSocketId, SocketAddr, mpsc::Sender<RecvUdpMsg>),
) {
    let (ingress_tx, ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
    let msgs_dropped = Arc::new(AtomicUsize::new(0));

    let reader = UdpSocketReader {
        socket_id,
        ingress_rx,
    };

    let writer = UdpSocketWriter {
        socket_id,
        socket_addr,
        egress_tx,
        msgs_dropped,
    };

    let handle = UdpSocketHandle { reader, writer };
    let config = (socket_id, socket_addr, ingress_tx);
    (handle, config)
}

fn create_tcp_socket_handle(
    socket_id: TcpSocketId,
    socket_addr: SocketAddr,
    egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
) -> (
    TcpSocketHandle,
    (TcpSocketId, SocketAddr, mpsc::Sender<RecvTcpMsg>),
) {
    let (ingress_tx, ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
    let msgs_dropped = Arc::new(AtomicUsize::new(0));

    let reader = TcpSocketReader {
        socket_id,
        ingress_rx,
    };

    let writer = TcpSocketWriter {
        socket_id,
        socket_addr,
        egress_tx,
        msgs_dropped,
    };

    let handle = TcpSocketHandle { reader, writer };
    let config = (socket_id, socket_addr, ingress_tx);
    (handle, config)
}

impl Dataplane {
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

fn validate_sockets<'a, I: Eq + std::hash::Hash + std::fmt::Debug + 'a>(
    sockets: impl Iterator<Item = &'a (I, SocketAddr)>,
    kind: impl AsRef<str>,
) {
    let kind = kind.as_ref();
    let mut seen_ids = std::collections::HashSet::new();
    let mut seen_ports = std::collections::HashSet::new();
    for (id, addr) in sockets {
        assert!(seen_ids.insert(id), "duplicate {kind} socket id {id:?}");
        assert!(
            seen_ports.insert(addr.port()),
            "duplicate {kind} port {} for socket {id:?}",
            addr.port()
        );
    }
}
