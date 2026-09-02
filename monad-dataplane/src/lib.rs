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
    num::{NonZeroU32, NonZeroU64},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::SyncSender,
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
use tokio::sync::mpsc;
use tracing::{debug, warn};

pub(crate) mod addrlist;
pub(crate) mod ban_expiry;
pub(crate) mod buffer_ext;
mod metrics;
pub mod pacing;
pub mod tcp;
pub mod udp;

pub use metrics::DataplaneMetrics;

pub(crate) const IPV4_HDR_SIZE: u16 = 20;
pub(crate) const UDP_HDR_SIZE: u16 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TcpSocketId {
    Raptorcast,
    AuthenticatedRaptorcast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UdpSocketId {
    Raptorcast,
    AuthenticatedRaptorcast,
    DirectUdp,
}

pub struct SocketHandles<I, H> {
    handles: Vec<(I, H)>,
}

impl<I, H> Default for SocketHandles<I, H> {
    fn default() -> Self {
        Self {
            handles: Vec::new(),
        }
    }
}

impl<I: PartialEq + Copy, H> SocketHandles<I, H> {
    fn push(&mut self, id: I, handle: H) {
        self.handles.push((id, handle));
    }

    pub fn get(&self, id: I) -> Option<&H> {
        self.handles
            .iter()
            .find(|(h_id, _)| *h_id == id)
            .map(|(_, h)| h)
    }

    pub fn take(&mut self, id: I) -> Option<H> {
        self.handles
            .iter()
            .position(|(h_id, _)| *h_id == id)
            .map(|idx| self.handles.swap_remove(idx).1)
    }
}

pub type TcpSocketHandles = SocketHandles<TcpSocketId, TcpSocketHandle>;
pub type UdpSocketHandles = SocketHandles<UdpSocketId, UdpSocketHandle>;

pub const DEFAULT_UDP_MAX_QUEUED_BYTES: usize = 100 * 1024 * 1024;
pub const DEFAULT_UDP_TX_WORKERS: usize = 1;
pub const MAX_UDP_TX_WORKERS: usize = 64;

/// Configuration for the UDP pacing queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpPacingConfig {
    /// Uniform per-destination bandwidth cap.
    pub peer_bandwidth_mbps: u64,
    /// Total payload byte budget shared by all queued UDP messages.
    pub max_queued_bytes: usize,
}

impl UdpPacingConfig {
    fn for_global_bandwidth(global_bandwidth_mbps: u64) -> Self {
        Self {
            peer_bandwidth_mbps: global_bandwidth_mbps,
            max_queued_bytes: DEFAULT_UDP_MAX_QUEUED_BYTES,
        }
    }

    fn validate(self) {
        assert!(
            self.peer_bandwidth_mbps > 0,
            "UDP peer bandwidth must be non-zero"
        );
        assert!(
            self.max_queued_bytes > 0,
            "UDP pacing queue byte limit must be non-zero"
        );
    }
}

pub struct DataplaneBuilder {
    trusted_addresses: Vec<IpAddr>,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    up_bandwidth_mbps: u64,
    udp_pacing_config: UdpPacingConfig,
    udp_buffer_size: Option<usize>,
    tcp_config: TcpConfig,
    ban_duration: Duration,
    udp_sockets: Vec<(UdpSocketId, SocketAddr)>,
    tcp_sockets: Vec<(TcpSocketId, SocketAddr)>,
    udp_multishot: bool,
    udp_tx_workers: usize,
}

impl DataplaneBuilder {
    pub fn new(up_bandwidth_mbps: u64) -> Self {
        assert!(
            up_bandwidth_mbps > 0,
            "global egress bandwidth must be non-zero"
        );
        Self {
            up_bandwidth_mbps,
            udp_pacing_config: UdpPacingConfig::for_global_bandwidth(up_bandwidth_mbps),
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
            udp_sockets: Vec::new(),
            tcp_sockets: Vec::new(),
            udp_multishot: true,
            udp_tx_workers: DEFAULT_UDP_TX_WORKERS,
        }
    }

    pub fn with_udp_buffer_size(mut self, buffer_size: usize) -> Self {
        self.udp_buffer_size = Some(buffer_size);
        self
    }

    pub fn with_udp_pacing_config(mut self, config: UdpPacingConfig) -> Self {
        config.validate();
        self.udp_pacing_config = config;
        self
    }

    pub fn with_udp_peer_bandwidth_mbps(mut self, peer_bandwidth_mbps: u64) -> Self {
        self.udp_pacing_config.peer_bandwidth_mbps = peer_bandwidth_mbps;
        self.udp_pacing_config.validate();
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

    /// sets the number of independent udp transmit workers.
    ///
    /// each destination is assigned to one worker after global pacing.
    pub fn with_udp_tx_workers(mut self, workers: usize) -> Self {
        assert!(
            (1..=MAX_UDP_TX_WORKERS).contains(&workers),
            "UDP TX workers must be between 1 and {MAX_UDP_TX_WORKERS}"
        );
        self.udp_tx_workers = workers;
        self
    }

    pub fn build(self) -> Dataplane {
        let DataplaneBuilder {
            up_bandwidth_mbps,
            udp_pacing_config,
            udp_buffer_size,
            trusted_addresses: trusted,
            tcp_config,
            ban_duration,
            udp_sockets,
            tcp_sockets,
            udp_multishot,
            udp_tx_workers,
        } = self;

        udp_pacing_config.validate();
        validate_sockets(udp_sockets.iter(), "udp");
        validate_sockets(tcp_sockets.iter(), "tcp");

        let metrics = DataplaneMetrics::new();
        let bandwidth_bytes_per_second = NonZeroU64::new(
            u64::try_from(u128::from(up_bandwidth_mbps) * 1_000_000 / 8)
                .expect("egress bandwidth overflows bytes per second"),
        )
        .expect("egress bandwidth must be non-zero");
        let (pacing, pacing_task, pacing_outputs) = pacing::PacingHandle::new(
            pacing::PacingTaskConfig {
                global_bytes_per_second: bandwidth_bytes_per_second,
                udp: udp_pacing_config,
                udp_workers: udp_tx_workers,
            },
            metrics.clone(),
        );
        thread::Builder::new()
            .name("monad-pacing".into())
            .spawn(move || {
                RuntimeBuilder::<IoUringDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("failed building pacing runtime")
                    .block_on(pacing_task.run());
            })
            .expect("failed to spawn pacing thread");

        let mut udp_socket_configs = Vec::new();
        let mut udp_pending_handles = Vec::new();
        for (id, addr) in udp_sockets {
            let (ingress_tx, ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
            udp_socket_configs.push((id, addr, ingress_tx));
            udp_pending_handles.push((id, ingress_rx));
        }

        let mut tcp_socket_configs = Vec::new();
        let mut tcp_pending_handles = Vec::new();
        for (id, addr) in tcp_sockets {
            let (ingress_tx, ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
            tcp_socket_configs.push((id, addr, ingress_tx));
            tcp_pending_handles.push((id, ingress_rx));
        }

        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();

        let (banned_ips_tx, banned_ips_rx) = mpsc::unbounded_channel();
        let addrlist = Arc::new(Addrlist::new_with_trusted(trusted.into_iter()));
        let tcp_control_map = TcpControl::new();

        let (tcp_bound_addrs_tx, tcp_bound_addrs_rx): (
            SyncSender<Vec<(TcpSocketId, SocketAddr)>>,
            _,
        ) = std::sync::mpsc::sync_channel(1);
        let (udp_bound_addrs_tx, udp_bound_addrs_rx): (
            SyncSender<Vec<(UdpSocketId, SocketAddr)>>,
            _,
        ) = std::sync::mpsc::sync_channel(1);

        thread::Builder::new()
            .name("monad-dataplane".into())
            .spawn({
                let tcp_control_map = tcp_control_map.clone();
                let addrlist = addrlist.clone();
                let metrics = metrics.clone();
                move || {
                    RuntimeBuilder::<IoUringDriver>::new()
                        .enable_timer()
                        .build()
                        .expect("Failed building the Runtime")
                        .block_on(async move {
                            let pacing::PacingOutputs {
                                tcp: tcp_egress_rx,
                                udp: udp_worker_rxs,
                            } = pacing_outputs;
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
                                tcp_bound_addrs_tx,
                                metrics.clone(),
                            );
                            udp::spawn_tasks(
                                udp_socket_configs,
                                udp::UdpTaskConfig {
                                    buffer_size: udp_buffer_size,
                                    use_multishot: udp_multishot,
                                    workers: udp_tx_workers,
                                },
                                udp_worker_rxs,
                                udp_bound_addrs_tx,
                                metrics,
                            );

                            ready_clone.store(true, Ordering::Release);

                            futures::future::pending::<()>().await;
                        });
                }
            })
            .expect("failed to spawn dataplane thread");

        let tcp_bound_addrs: std::collections::HashMap<TcpSocketId, SocketAddr> =
            tcp_bound_addrs_rx
                .recv()
                .expect("failed to receive tcp bound addresses")
                .into_iter()
                .collect();
        let udp_bound_addrs: std::collections::HashMap<UdpSocketId, SocketAddr> =
            udp_bound_addrs_rx
                .recv()
                .expect("failed to receive udp bound addresses")
                .into_iter()
                .collect();

        let mut udp_socket_handles = UdpSocketHandles::default();
        for (id, ingress_rx) in udp_pending_handles {
            let socket_addr = *udp_bound_addrs
                .get(&id)
                .unwrap_or_else(|| panic!("missing bound address for udp socket {:?}", id));
            let handle = UdpSocketHandle {
                reader: UdpSocketReader {
                    socket_id: id,
                    ingress_rx,
                },
                writer: UdpSocketWriter {
                    socket_id: id,
                    socket_addr,
                    pacing: pacing.clone(),
                    msgs_dropped: Arc::new(AtomicUsize::new(0)),
                    metrics: metrics.clone(),
                },
            };
            udp_socket_handles.push(id, handle);
        }

        let mut tcp_socket_handles = TcpSocketHandles::default();
        for (id, ingress_rx) in tcp_pending_handles {
            let socket_addr = *tcp_bound_addrs
                .get(&id)
                .unwrap_or_else(|| panic!("missing bound address for tcp socket {:?}", id));
            let handle = TcpSocketHandle {
                local_addr: socket_addr,
                reader: TcpSocketReader {
                    socket_id: id,
                    ingress_rx,
                },
                writer: TcpSocketWriter {
                    socket_id: id,
                    socket_addr,
                    pacing: pacing.clone(),
                    msgs_dropped: Arc::new(AtomicUsize::new(0)),
                    metrics: metrics.clone(),
                },
            };
            tcp_socket_handles.push(id, handle);
        }

        let control = DataplaneControl::new(tcp_control_map, banned_ips_tx, addrlist);

        Dataplane {
            tcp_sockets: tcp_socket_handles,
            udp_sockets: udp_socket_handles,
            control,
            ready,
            metrics,
        }
    }
}

pub struct Dataplane {
    pub tcp_sockets: TcpSocketHandles,
    pub udp_sockets: UdpSocketHandles,
    pub control: DataplaneControl,
    ready: Arc<AtomicBool>,
    metrics: DataplaneMetrics,
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
    pacing: pacing::PacingHandle,
    msgs_dropped: Arc<AtomicUsize>,
    metrics: DataplaneMetrics,
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

    pub fn local_addr(&self) -> SocketAddr {
        self.writer.local_addr()
    }
}

impl UdpSocketWriter {
    fn try_send(&self, msg: UdpMsg) -> Result<(), UdpMsg> {
        self.pacing.enqueue_udp(msg)
    }

    pub fn write(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
        let msg_length = payload.len();
        let result = self.try_send(UdpMsg {
            socket_id: self.socket_id,
            dst,
            payload,
            stride,
            priority: UdpPriority::Regular,
        });

        match result {
            Ok(()) => {}
            Err(_) => {
                self.metrics.udp_egress_messages_dropped.inc();
                let total = self.msgs_dropped.fetch_add(1, Ordering::Relaxed);
                warn!(
                    socket_id = ?self.socket_id,
                    ?dst,
                    msg_length,
                    total_msgs_dropped = total,
                    "udp pacing queue full, dropping message"
                );
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
            if self.try_send(udp_msg).is_ok() {
                pending_count -= 1;
            }
        }

        if pending_count > 0 {
            self.metrics
                .udp_egress_messages_dropped
                .add(pending_count as u64);
            let total = self
                .msgs_dropped
                .fetch_add(pending_count, Ordering::Relaxed);
            warn!(
                socket_id = ?self.socket_id,
                num_msgs_dropped = pending_count,
                total_msgs_dropped = total,
                msg_length = msg_len,
                ?priority,
                "udp pacing queue full, dropping broadcast messages"
            );
        }
    }

    pub fn write_unicast(&self, msg: UnicastMsg) {
        self.write_unicast_with_priority(msg, UdpPriority::Regular);
    }

    pub fn write_unicast_with_priority(&self, msg: UnicastMsg, priority: UdpPriority) {
        let mut pending_count = msg.msg_count();

        for udp_msg in msg.into_iter_with_priority(self.socket_id, priority) {
            if self.try_send(udp_msg).is_ok() {
                pending_count -= 1;
            }
        }

        if pending_count > 0 {
            self.metrics
                .udp_egress_messages_dropped
                .add(pending_count as u64);
            let total = self
                .msgs_dropped
                .fetch_add(pending_count, Ordering::Relaxed);
            warn!(
                socket_id = ?self.socket_id,
                num_msgs_dropped = pending_count,
                total_msgs_dropped = total,
                ?priority,
                "udp pacing queue full, dropping unicast messages"
            );
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.socket_addr
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
    pacing: pacing::PacingHandle,
    msgs_dropped: Arc<AtomicUsize>,
    metrics: DataplaneMetrics,
}

impl TcpSocketWriter {
    pub fn write(&self, addr: SocketAddr, msg: TcpMsg) {
        let msg_length = msg.msg.len();

        match self.pacing.enqueue_tcp(addr, msg) {
            Ok(()) => {}
            Err(_) => {
                self.metrics.tcp_egress_messages_dropped.inc();
                let total = self.msgs_dropped.fetch_add(1, Ordering::Relaxed);
                warn!(
                    socket_id = ?self.socket_id,
                    ?addr,
                    msg_length,
                    total_msgs_dropped = total,
                    "tcp pacing queue full, dropping message"
                );
            }
        }
    }
}

pub struct TcpSocketHandle {
    local_addr: SocketAddr,
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

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
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
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;

impl Dataplane {
    /// Returns the live dataplane metrics for Prometheus or OTel registration.
    pub fn metrics(&self) -> &monad_executor::ExecutorMetrics {
        self.metrics.executor_metrics()
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
        if addr.port() != 0 {
            assert!(
                seen_ports.insert(addr.port()),
                "duplicate {kind} port {} for socket {id:?}",
                addr.port()
            );
        }
    }
}
