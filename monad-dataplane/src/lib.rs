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

pub(crate) use udp::UdpMessageType;

pub struct DataplaneBuilder {
    local_addr: SocketAddr,
    trusted_addresses: Vec<IpAddr>,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    udp_up_bandwidth_mbps: u64,
    udp_buffer_size: Option<usize>,
    tcp_config: TcpConfig,
    ban_duration: Duration,
    direct_socket_port: Option<u16>,
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
            direct_socket_port: None,
        }
    }

    /// with_udp_buffer_size sets the buffer size for udp socket that is managed by dataplane
    /// to a requested value.
    pub fn with_udp_buffer_size(mut self, buffer_size: usize) -> Self {
        self.udp_buffer_size = Some(buffer_size);
        self
    }

    /// with_tcp_connections_limit sets total and per_ip connection limit. if per_ip is zero it will be
    /// equal to total.
    pub fn with_tcp_connections_limit(mut self, total: usize, per_ip: usize) -> Self {
        self.tcp_config.connections_limit = total;
        self.tcp_config.per_ip_connections_limit = if per_ip == 0 { total } else { per_ip };
        self
    }

    /// with_tcp_rps_burst sets the rate limit and burst for tcp connections.
    pub fn with_tcp_rps_burst(mut self, rps: u32, burst: u32) -> Self {
        self.tcp_config.rate_limit.rps = NonZeroU32::new(rps).expect("rps must be non-zero");
        self.tcp_config.rate_limit.rps_burst =
            NonZeroU32::new(burst).expect("burst must be non-zero");
        self
    }

    /// with trusted_ips sets the list of trusted ip addresses.
    pub fn with_trusted_ips(mut self, ips: Vec<IpAddr>) -> Self {
        self.trusted_addresses = ips;
        self
    }

    /// with_direct_socket configures an additional UDP socket for direct peer communication
    pub fn with_direct_socket(mut self, port: u16) -> Self {
        self.direct_socket_port = Some(port);
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
            direct_socket_port,
        } = self;

        let (tcp_ingress_tx, tcp_ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);
        let (udp_ingress_tx, udp_ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
        let (udp_direct_ingress_tx, udp_direct_ingress_rx) =
            mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);

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
                                local_addr,
                                direct_socket_port,
                                udp_ingress_tx,
                                udp_direct_ingress_tx,
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

        let writer = DataplaneWriter::new(
            tcp_egress_tx,
            udp_egress_tx,
            tcp_control_map,
            banned_ips_tx,
            addrlist,
        );
        let reader = DataplaneReader::new(tcp_ingress_rx, udp_ingress_rx, udp_direct_ingress_rx);

        Dataplane {
            writer,
            reader,
            ready,
        }
    }
}

pub struct Dataplane {
    writer: DataplaneWriter,
    reader: DataplaneReader,
    ready: Arc<AtomicBool>,
}

pub struct DataplaneReader {
    tcp_ingress_rx: mpsc::Receiver<RecvTcpMsg>,
    udp_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
    udp_direct_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
}

#[derive(Clone)]
pub struct DataplaneWriter {
    inner: Arc<DataplaneWriterInner>,
}

struct DataplaneWriterInner {
    tcp_egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
    udp_egress_tx: mpsc::Sender<UdpMsg>,

    tcp_control_map: TcpControl,
    notify_ban_expiry: mpsc::UnboundedSender<(IpAddr, Instant)>,
    addrlist: Arc<Addrlist>,

    tcp_msgs_dropped: AtomicUsize,
    udp_msgs_dropped: AtomicUsize,
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

    fn into_iter_with_priority(self, priority: UdpPriority) -> impl Iterator<Item = UdpMsg> {
        let Self {
            targets,
            payload,
            stride,
        } = self;
        targets.into_iter().map(move |dst| UdpMsg {
            dst,
            payload: payload.clone(),
            stride,
            msg_type: UdpMessageType::Broadcast,
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

    fn into_iter_with_priority(self, priority: UdpPriority) -> impl Iterator<Item = UdpMsg> {
        let Self { msgs, stride } = self;
        msgs.into_iter().map(move |(dst, payload)| UdpMsg {
            dst,
            payload,
            stride,
            msg_type: UdpMessageType::Broadcast,
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
    pub(crate) dst: SocketAddr,
    pub(crate) payload: Bytes,
    pub(crate) stride: u16,
    pub(crate) msg_type: UdpMessageType,
    pub(crate) priority: UdpPriority,
}

const TCP_INGRESS_CHANNEL_SIZE: usize = 1024;
const TCP_EGRESS_CHANNEL_SIZE: usize = 1024;
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;
const UDP_EGRESS_CHANNEL_SIZE: usize = 12_800;

impl Dataplane {
    pub fn split(self) -> (DataplaneReader, DataplaneWriter) {
        (self.reader, self.writer)
    }

    /// add_trusted marks ip address as trusted.
    /// connections limits are not applied to trusted ips.
    pub fn add_trusted(&self, addr: IpAddr) {
        self.writer.add_trusted(addr);
    }

    /// remove_trusted removes ip address from trusted list.
    pub fn remove_trusted(&self, addr: IpAddr) {
        self.writer.remove_trusted(addr);
    }

    pub fn update_trusted(&self, added: Vec<IpAddr>, removed: Vec<IpAddr>) {
        self.writer.update_trusted(added, removed);
    }

    /// ban ip address. ban duration is specified in dataplane config.
    pub fn ban(&self, ip: IpAddr) {
        self.writer.ban(ip);
    }

    /// disconnect all connections from specified ip address.
    pub fn disconnect_ip(&self, ip: IpAddr) {
        self.writer.disconnect_ip(ip);
    }

    /// disconnect single connection.
    pub fn disconnect(&self, addr: SocketAddr) {
        self.writer.disconnect(addr);
    }

    pub async fn tcp_read(&mut self) -> RecvTcpMsg {
        self.reader.tcp_read().await
    }

    pub fn tcp_write(&self, addr: SocketAddr, msg: TcpMsg) {
        self.writer.tcp_write(addr, msg)
    }

    pub async fn udp_read(&mut self) -> RecvUdpMsg {
        self.reader.udp_read().await
    }

    pub async fn udp_direct_read(&mut self) -> RecvUdpMsg {
        self.reader.udp_direct_read().await
    }

    pub fn udp_write_broadcast(&self, msg: BroadcastMsg) {
        self.writer.udp_write_broadcast(msg);
    }

    pub fn udp_write_broadcast_with_priority(&self, msg: BroadcastMsg, priority: UdpPriority) {
        self.writer.udp_write_broadcast_with_priority(msg, priority);
    }

    pub fn udp_write_unicast(&self, msg: UnicastMsg) {
        self.writer.udp_write_unicast(msg);
    }

    pub fn udp_write_direct(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
        self.writer.udp_write_direct(dst, payload, stride);
    }

    pub fn udp_write_unicast_with_priority(&self, msg: UnicastMsg, priority: UdpPriority) {
        self.writer.udp_write_unicast_with_priority(msg, priority);
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

impl DataplaneReader {
    fn new(
        tcp_ingress_rx: mpsc::Receiver<RecvTcpMsg>,
        udp_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
        udp_direct_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
    ) -> Self {
        Self {
            tcp_ingress_rx,
            udp_ingress_rx,
            udp_direct_ingress_rx,
        }
    }

    pub async fn tcp_read(&mut self) -> RecvTcpMsg {
        match self.tcp_ingress_rx.recv().await {
            Some(msg) => msg,
            None => panic!("tcp_ingress_rx channel closed"),
        }
    }

    pub async fn udp_read(&mut self) -> RecvUdpMsg {
        match self.udp_ingress_rx.recv().await {
            Some(msg) => msg,
            None => panic!("udp_ingress_rx channel closed"),
        }
    }

    pub async fn udp_direct_read(&mut self) -> RecvUdpMsg {
        match self.udp_direct_ingress_rx.recv().await {
            Some(msg) => msg,
            None => panic!("udp_direct_ingress_rx channel closed"),
        }
    }

    pub fn split(self) -> (TcpReader, UdpReader) {
        (
            TcpReader(self.tcp_ingress_rx),
            UdpReader {
                udp_ingress_rx: self.udp_ingress_rx,
                udp_direct_ingress_rx: self.udp_direct_ingress_rx,
            },
        )
    }
}

pub struct TcpReader(mpsc::Receiver<RecvTcpMsg>);
pub struct UdpReader {
    udp_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
    udp_direct_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
}

impl TcpReader {
    pub async fn read(&mut self) -> RecvTcpMsg {
        match self.0.recv().await {
            Some(msg) => msg,
            None => panic!("tcp_ingress_rx channel closed"),
        }
    }
}

impl UdpReader {
    pub async fn read(&mut self) -> RecvUdpMsg {
        match self.udp_ingress_rx.recv().await {
            Some(msg) => msg,
            None => panic!("udp_ingress_rx channel closed"),
        }
    }

    pub async fn direct_read(&mut self) -> RecvUdpMsg {
        match self.udp_direct_ingress_rx.recv().await {
            Some(msg) => msg,
            None => panic!("udp_direct_ingress_rx channel closed"),
        }
    }
}

impl DataplaneWriter {
    fn new(
        tcp_egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
        udp_egress_tx: mpsc::Sender<UdpMsg>,
        tcp_control_map: TcpControl,
        notify_ban_expiry: mpsc::UnboundedSender<(IpAddr, Instant)>,
        addrlist: Arc<Addrlist>,
    ) -> Self {
        let inner = DataplaneWriterInner {
            tcp_egress_tx,
            udp_egress_tx,
            tcp_control_map,
            notify_ban_expiry,
            addrlist,
            tcp_msgs_dropped: AtomicUsize::new(0),
            udp_msgs_dropped: AtomicUsize::new(0),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn tcp_write(&self, addr: SocketAddr, msg: TcpMsg) {
        let msg_length = msg.msg.len();

        match self.inner.tcp_egress_tx.try_send((addr, msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let tcp_msgs_dropped = self.inner.tcp_msgs_dropped.fetch_add(1, Ordering::Relaxed);

                warn!(
                    num_msgs_dropped = 1,
                    total_tcp_msgs_dropped = tcp_msgs_dropped,
                    ?addr,
                    msg_length,
                    "tcp_egress_tx channel full, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => panic!("tcp_egress_tx channel closed"),
        }
    }

    pub fn udp_write_broadcast(&self, msg: BroadcastMsg) {
        self.udp_write_broadcast_with_priority(msg, UdpPriority::Regular);
    }

    pub fn udp_write_unicast(&self, msg: UnicastMsg) {
        self.udp_write_unicast_with_priority(msg, UdpPriority::Regular);
    }

    #[tracing::instrument(
        level="trace",
        skip_all,
        fields(len = msg.payload.len(), targets = msg.targets.len(), priority = ?priority)
    )]
    pub fn udp_write_broadcast_with_priority(&self, msg: BroadcastMsg, priority: UdpPriority) {
        let mut pending_count = msg.msg_count();
        let msg_len = msg.payload.len();

        for udp_msg in msg.into_iter_with_priority(priority) {
            match self.inner.udp_egress_tx.try_send(udp_msg) {
                Ok(()) => {
                    pending_count -= 1;
                }
                Err(TrySendError::Full(_)) => {
                    break;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }

        if pending_count == 0 {
            return;
        }

        let udp_msgs_dropped = self
            .inner
            .udp_msgs_dropped
            .fetch_add(pending_count, Ordering::Relaxed);

        warn!(
            num_msgs_dropped = pending_count,
            total_udp_msgs_dropped = udp_msgs_dropped,
            msg_length = msg_len,
            ?priority,
            "udp_egress_tx channel full, dropping message"
        );
    }

    #[tracing::instrument(
        level="trace",
        skip_all,
        fields(msgs = msg.msgs.len(), priority = ?priority)
    )]
    pub fn udp_write_unicast_with_priority(&self, msg: UnicastMsg, priority: UdpPriority) {
        let mut pending_count = msg.msg_count();

        for udp_msg in msg.into_iter_with_priority(priority) {
            match self.inner.udp_egress_tx.try_send(udp_msg) {
                Ok(()) => {
                    pending_count -= 1;
                }
                Err(TrySendError::Full(_)) => {
                    break;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }

        if pending_count == 0 {
            return;
        }

        let udp_msgs_dropped = self
            .inner
            .udp_msgs_dropped
            .fetch_add(pending_count, Ordering::Relaxed);

        warn!(
            num_msgs_dropped = pending_count,
            total_udp_msgs_dropped = udp_msgs_dropped,
            ?priority,
            "udp_egress_tx channel full, dropping message"
        );
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

    pub fn udp_write_direct(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
        let msg_length = payload.len();
        let udp_msg = UdpMsg {
            dst,
            payload,
            stride,
            msg_type: UdpMessageType::Direct,
            priority: UdpPriority::Regular,
        };

        match self.inner.udp_egress_tx.try_send(udp_msg) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let udp_msgs_dropped = self.inner.udp_msgs_dropped.fetch_add(1, Ordering::Relaxed);

                warn!(
                    num_msgs_dropped = 1,
                    total_udp_msgs_dropped = udp_msgs_dropped,
                    ?dst,
                    msg_length,
                    "udp_egress_tx channel full, dropping direct message"
                );
            }
            Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
        }
    }
}
