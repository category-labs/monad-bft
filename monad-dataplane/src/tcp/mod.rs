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
    cell::Cell,
    collections::BTreeMap,
    io::{Error, ErrorKind},
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    os::fd::{AsRawFd, RawFd},
    sync::{Arc, Mutex},
    time::Duration,
};

use monoio::{
    io::{AsyncWriteRentExt, Splitable},
    net::{
        tcp::{TcpOwnedReadHalf, TcpOwnedWriteHalf},
        ListenerOpts, TcpListener, TcpStream,
    },
    select, spawn,
};
use tokio::sync::{mpsc, watch};
use tracing::{trace, warn};
use zerocopy::{
    byteorder::little_endian::{U32, U64},
    FromBytes, Immutable, IntoBytes,
};

use super::{RecvTcpMsg, TcpMsg, TcpSocketId};
use crate::{metrics::DataplaneMetrics, Addrlist};

mod conn;
pub mod rx;
pub mod tx;

#[derive(Clone)]
pub(crate) struct ConnectionContext {
    pub(crate) socket_id: TcpSocketId,
    pub(crate) rate_limit: TcpRateLimit,
    pub(crate) tcp_control_map: TcpControl,
    pub(crate) tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
    pub(crate) metrics: DataplaneMetrics,
}

const TCP_MESSAGE_LENGTH_LIMIT: usize = 3 * 1024 * 1024;

const HEADER_MAGIC: u32 = 0x434e5353; // "SSNC"
const HEADER_VERSION: u32 = 1;

#[derive(IntoBytes, Debug, FromBytes, Immutable)]
#[repr(C)]
struct TcpMsgHdr {
    magic: U32,
    version: U32,
    length: U64,
}

impl TcpMsgHdr {
    fn new(length: u64) -> TcpMsgHdr {
        TcpMsgHdr {
            magic: U32::new(HEADER_MAGIC),
            version: U32::new(HEADER_VERSION),
            length: U64::new(length),
        }
    }
}

pub(crate) type TcpReadHalf = TcpOwnedReadHalf;

pub(crate) struct TcpWriteHalf {
    inner: TcpOwnedWriteHalf,
    raw_fd: RawFd,
}

impl TcpWriteHalf {
    pub(crate) fn set_cork(&self, enabled: bool) {
        let r = unsafe {
            let cork_flag: libc::c_int = if enabled { 1 } else { 0 };
            libc::setsockopt(
                self.raw_fd,
                libc::SOL_TCP,
                libc::TCP_CORK,
                &cork_flag as *const _ as _,
                std::mem::size_of_val(&cork_flag) as _,
            )
        };
        if r != 0 {
            warn!(
                "setsockopt(TCP_CORK) failed with: {}",
                Error::last_os_error()
            );
        }
    }

    pub(crate) fn unacked_bytes(&self) -> usize {
        let mut outq: libc::c_int = 0;
        let r = unsafe { libc::ioctl(self.raw_fd, libc::TIOCOUTQ, &mut outq as *mut libc::c_int) };
        if r == 0 {
            outq as _
        } else {
            warn!("ioctl(TIOCOUTQ) failed with: {}", Error::last_os_error());
            0
        }
    }

    pub(crate) async fn write_all<T: monoio::buf::IoBuf>(
        &mut self,
        buf: T,
    ) -> monoio::BufResult<usize, T> {
        self.inner.write_all(buf).await
    }
}

pub(crate) fn split_stream(stream: TcpStream) -> (TcpReadHalf, TcpWriteHalf) {
    let raw_fd = stream.as_raw_fd();
    let (read, write) = stream.into_split();
    (
        read,
        TcpWriteHalf {
            inner: write,
            raw_fd,
        },
    )
}

// This tracks an explicit disconnect request, including while TCP is dialing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DisconnectState {
    NotRequested,
    Requested,
}

#[derive(Clone, Debug)]
pub(crate) struct TcpConnection(watch::Sender<DisconnectState>);

impl TcpConnection {
    pub(crate) fn new() -> Self {
        Self(watch::channel(DisconnectState::NotRequested).0)
    }

    pub(crate) fn disconnect(&self) {
        self.0.send_replace(DisconnectState::Requested);
    }

    pub(crate) async fn disconnected(&self) {
        let mut rx = self.0.subscribe();
        let state = *rx.borrow_and_update();
        if state == DisconnectState::NotRequested {
            let _ = rx.changed().await;
        }
    }

    #[cfg(test)]
    fn is_disconnected(&self) -> bool {
        *self.0.borrow() == DisconnectState::Requested
    }
}

const CONNECTION_IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IoState {
    Idle,
    Busy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IntervalActivity {
    None,
    Observed,
}

// Check both directions periodically while RX waits for a new frame and TX
// waits for queued data. Keep the connection if either side did work during
// the interval or is still busy; otherwise close it.
pub(crate) struct ConnectionIdle {
    tx_state: Cell<IoState>,
    rx_state: Cell<IoState>,
    // Remembers activity between idle checks even if the work has already finished.
    // A send starting at 3s and completing at 4s leaves TX idle at the 10s check,
    // but this marker keeps the connection open. The same applies to RX.
    interval_activity: Cell<IntervalActivity>,
}

impl ConnectionIdle {
    fn new() -> Self {
        Self {
            tx_state: Cell::new(IoState::Idle),
            rx_state: Cell::new(IoState::Idle),
            interval_activity: Cell::new(IntervalActivity::None),
        }
    }

    fn set_tx_state(&self, state: IoState) {
        if self.tx_state.replace(state) != state {
            self.interval_activity.set(IntervalActivity::Observed);
        }
    }

    fn set_rx_state(&self, state: IoState) {
        if self.rx_state.replace(state) != state {
            self.interval_activity.set(IntervalActivity::Observed);
        }
    }

    async fn both_idle(&self) {
        loop {
            monoio::time::sleep(CONNECTION_IDLE_CHECK_INTERVAL).await;
            let activity = self.interval_activity.replace(IntervalActivity::None);
            if activity == IntervalActivity::None
                && self.tx_state.get() == IoState::Idle
                && self.rx_state.get() == IoState::Idle
            {
                return;
            }
        }
    }
}

// Registers the connection task with TcpControl so disconnects and bans can
// stop it while dialing, doing I/O, or waiting in the failure cooldown.
// Dropping the guard removes the registration.
pub(crate) struct TcpConnectionGuard {
    control: TcpControl,
    id: TcpIdentifier,
    connection: TcpConnection,
}

impl TcpConnectionGuard {
    fn new(control: TcpControl, addr: SocketAddr, conn_id: u64) -> Self {
        let id = (addr.ip(), addr.port(), conn_id);
        let connection = TcpConnection::new();
        control.register(id, connection.clone());
        trace!(conn_id, ?addr, "starting tcp connection task");
        Self {
            control,
            id,
            connection,
        }
    }
}

impl Drop for TcpConnectionGuard {
    fn drop(&mut self) {
        self.control.unregister(&self.id);
        let conn_id = self.id.2;
        let addr = SocketAddr::new(self.id.0, self.id.1);
        trace!(conn_id, ?addr, "exiting tcp connection task");
    }
}

pub(crate) async fn task_connection(
    context: &ConnectionContext,
    addr: SocketAddr,
    stream: TcpStream,
    msg_receiver: &mut tx::BoundedQueueReceiver,
    connection: &TcpConnectionGuard,
) -> Result<(), Error> {
    let conn_id = connection.id.2;
    let metrics = &context.metrics;

    // 1. every 10 seconds, keep the connection open if either side did work
    //    during the interval or is still busy; otherwise close it.
    // 2. if either side closes or fails, close the whole connection.
    // 3. if a header or body read or a frame write exceeds 10 seconds, close
    //    the whole connection regardless of activity on the other side. the header
    //    deadline starts after its first byte arrives; the body deadline starts
    //    after the header is complete. progress does not reset these deadlines.
    select! {
        biased;
        _ = connection.connection.disconnected() => Ok(()),
        result = async {
            let (mut read_half, mut write_half) = split_stream(stream);
            let idle = ConnectionIdle::new();
            select! {
                _ = rx::read_messages(context, conn_id, addr, &mut read_half, &idle) => Err(ErrorKind::UnexpectedEof.into()),
                result = tx::send_messages(conn_id, &addr, &mut write_half, msg_receiver, metrics, &idle) => result,
                _ = idle.both_idle() => {
                    trace!(conn_id, ?addr, "closing idle TCP connection");
                    Ok(())
                },
            }
        } => result,
    }
}

pub(crate) fn spawn_tasks(
    cfg: TcpConfig,
    tcp_control_map: TcpControl,
    addrlist: Arc<Addrlist>,
    socket_configs: Vec<(TcpSocketId, SocketAddr, mpsc::Sender<RecvTcpMsg>)>,
    tcp_egress_rx: mpsc::Receiver<(TcpSocketId, SocketAddr, TcpMsg)>,
    bound_addrs_tx: std::sync::mpsc::SyncSender<Vec<(TcpSocketId, SocketAddr)>>,
    metrics: DataplaneMetrics,
) {
    let mut bound_addrs = Vec::with_capacity(socket_configs.len());
    let registry = conn::ConnectionRegistry::new();
    let outgoing_limit =
        tx::OutgoingLimit::new(addrlist.clone(), cfg.connections_limit, metrics.clone());
    let mut contexts = BTreeMap::new();

    let accept_limit = rx::AcceptLimit::new(
        addrlist,
        cfg.connections_limit,
        cfg.per_ip_connections_limit,
        metrics.clone(),
    );

    for (socket_id, socket_addr, ingress_tx) in socket_configs {
        let opts = ListenerOpts::new().reuse_addr(true);
        let tcp_listener = TcpListener::bind_with_config(socket_addr, &opts).unwrap();
        let actual_addr = tcp_listener.local_addr().unwrap();
        bound_addrs.push((socket_id, actual_addr));

        let context = ConnectionContext {
            socket_id,
            rate_limit: cfg.rate_limit,
            tcp_control_map: tcp_control_map.clone(),
            tcp_ingress_tx: ingress_tx,
            metrics: metrics.clone(),
        };
        contexts.insert(socket_id, context.clone());
        spawn(rx::task(
            context,
            accept_limit.clone(),
            registry.clone(),
            tcp_listener,
        ));
        trace!(?socket_id, ?socket_addr, actual_addr = ?actual_addr, "created tcp listener");
    }

    bound_addrs_tx.send(bound_addrs).unwrap();
    spawn(tx::task(outgoing_limit, registry, tcp_egress_rx, contexts));
}

// Minimum message receive/transmit speed in bytes per second.  Messages that are
// transferred slower than this are aborted.
const MINIMUM_TRANSFER_SPEED: u64 = 1_000_000;

// Allow for at least this transfer time, so that very small messages still have
// a chance to be transferred successfully.
const MINIMUM_TRANSFER_TIME: Duration = Duration::from_secs(10);

// RX applies this deadline to the body after its header completes; TX applies
// it to the whole frame. With the current message size limit, it is 10 seconds
// for every valid message. Partial progress never restarts either deadline.
fn message_timeout(len: usize) -> Duration {
    Duration::from_millis(u64::try_from(len).unwrap() / (MINIMUM_TRANSFER_SPEED / 1000))
        .max(MINIMUM_TRANSFER_TIME)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TcpConfig {
    pub(crate) rate_limit: TcpRateLimit,
    pub(crate) connections_limit: usize,
    pub(crate) per_ip_connections_limit: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TcpRateLimit {
    pub(crate) rps: NonZeroU32,
    pub(crate) rps_burst: NonZeroU32,
}

type RateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::QuantaClock,
    governor::middleware::NoOpMiddleware<governor::clock::QuantaInstant>,
>;

impl TcpRateLimit {
    pub(crate) fn new_rate_limiter(&self) -> RateLimiter {
        governor::RateLimiter::direct(
            governor::Quota::per_second(self.rps).allow_burst(self.rps_burst),
        )
    }
}

pub(crate) type TcpIdentifier = (IpAddr, u16, u64);

#[derive(Debug, Clone)]
pub(crate) struct TcpControl(Arc<Mutex<BTreeMap<TcpIdentifier, TcpConnection>>>);

impl TcpControl {
    pub(crate) fn new() -> TcpControl {
        TcpControl(Arc::new(Mutex::new(BTreeMap::new())))
    }

    pub(crate) fn register(&self, id: TcpIdentifier, connection: TcpConnection) {
        self.0.lock().unwrap().insert(id, connection);
    }

    pub(crate) fn unregister(&self, id: &TcpIdentifier) {
        self.0.lock().unwrap().remove(id);
    }

    #[allow(unused)]
    pub(crate) fn disconnect_ip(&self, ip: IpAddr) {
        let map = self.0.lock().unwrap();
        let mut count = 0;
        for (id, connection) in map.range((ip, u16::MIN, u64::MIN)..(ip, u16::MAX, u64::MAX)) {
            connection.disconnect();
            count += 1;
        }
        trace!(
            ?ip,
            connections_disconnected = count,
            "completed ip disconnect"
        );
    }

    #[allow(unused)]
    pub(crate) fn disconnect_socket(&self, ip: IpAddr, port: u16) {
        let map = self.0.lock().unwrap();
        let mut count = 0;
        for (_, connection) in map.range((ip, port, u64::MIN)..(ip, port, u64::MAX)) {
            connection.disconnect();
            count += 1;
        }
        trace!(
            ?ip,
            port,
            connections_disconnected = count,
            "completed socket disconnect"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::ErrorKind,
        net::{IpAddr, Ipv4Addr},
        num::NonZeroU32,
    };

    use bytes::BytesMut;
    use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt, Splitable};
    use rstest::*;
    use zerocopy::IntoBytes;

    use super::*;
    use crate::{addrlist::Addrlist, TcpSocketId};

    #[fixture]
    fn tcp_control() -> TcpControl {
        TcpControl::new()
    }

    #[fixture]
    fn tcp_id() -> TcpIdentifier {
        (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 12345)
    }

    #[rstest]
    fn test_register_and_unregister(tcp_control: TcpControl, tcp_id: TcpIdentifier) {
        let connection = TcpConnection::new();
        tcp_control.register(tcp_id, connection.clone());
        tcp_control.unregister(&tcp_id);
        tcp_control.disconnect_socket(tcp_id.0, tcp_id.1);
        assert!(!connection.is_disconnected());
    }

    #[rstest]
    fn test_multiple_registrations_same_id(tcp_control: TcpControl, tcp_id: TcpIdentifier) {
        let connection1 = TcpConnection::new();
        let connection2 = TcpConnection::new();
        tcp_control.register(tcp_id, connection1.clone());
        tcp_control.register(tcp_id, connection2.clone());

        tcp_control.disconnect_socket(tcp_id.0, tcp_id.1);
        assert!(!connection1.is_disconnected());
        assert!(connection2.is_disconnected());
    }

    #[rstest]
    #[case::same_ip_different_ports(
        vec![
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9090, 2),
            (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 3),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        vec![0, 1]
    )]
    #[case::edge_ports(
        vec![
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MIN, 1),
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MAX, 2),
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 3),
            (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 4),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        vec![0, 1, 2]
    )]
    #[case::no_matching_ip(
        vec![
            (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 1),
            (IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)), 9090, 2),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        vec![]
    )]
    fn test_disconnect_ip(
        tcp_control: TcpControl,
        #[case] sockets: Vec<TcpIdentifier>,
        #[case] disconnect_ip: IpAddr,
        #[case] expected_disconnected_indices: Vec<usize>,
    ) {
        let mut connections = HashMap::new();

        for (i, &socket) in sockets.iter().enumerate() {
            let connection = TcpConnection::new();
            tcp_control.register(socket, connection.clone());
            connections.insert(i, connection);
        }

        tcp_control.disconnect_ip(disconnect_ip);

        for (i, connection) in connections {
            assert_eq!(
                connection.is_disconnected(),
                expected_disconnected_indices.contains(&i)
            );
        }
    }

    #[rstest]
    #[case::same_ip_port_different_connections(
        vec![
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 2),
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9090, 3),
            (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 4),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        8080,
        vec![0, 1]
    )]
    #[case::edge_port_numbers(
        vec![
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MIN, 1),
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MAX, 2),
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 3),
            (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), u16::MIN, 4),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        u16::MIN,
        vec![0]
    )]
    #[case::no_matching_socket(
        vec![
            (IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9090, 2),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        9090,
        vec![]
    )]
    fn test_disconnect_socket(
        tcp_control: TcpControl,
        #[case] sockets: Vec<TcpIdentifier>,
        #[case] disconnect_ip: IpAddr,
        #[case] disconnect_port: u16,
        #[case] expected_disconnected_indices: Vec<usize>,
    ) {
        let mut connections = HashMap::new();

        for (i, &socket) in sockets.iter().enumerate() {
            let connection = TcpConnection::new();
            tcp_control.register(socket, connection.clone());
            connections.insert(i, connection);
        }

        tcp_control.disconnect_socket(disconnect_ip, disconnect_port);

        for (i, connection) in connections {
            assert_eq!(
                connection.is_disconnected(),
                expected_disconnected_indices.contains(&i)
            );
        }
    }

    const SOCKET: TcpSocketId = TcpSocketId::Raptorcast;
    const PEER_TIMEOUT: Duration = Duration::from_secs(2);

    async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
        monoio::time::timeout(PEER_TIMEOUT, future).await.unwrap()
    }

    struct Peer {
        addr: SocketAddr,
        read: TcpReadHalf,
        write: monoio::net::tcp::TcpOwnedWriteHalf,
    }

    impl Peer {
        fn new(stream: TcpStream, addr: SocketAddr) -> Self {
            let (read, write) = stream.into_split();
            Self { addr, read, write }
        }

        async fn connect(addr: SocketAddr) -> Self {
            let stream = within(TcpStream::connect(addr)).await.unwrap();
            let local_addr = stream.local_addr().unwrap();
            Self::new(stream, local_addr)
        }

        async fn send(&mut self, payload: &[u8]) {
            let frame = [TcpMsgHdr::new(payload.len() as u64).as_bytes(), payload].concat();
            let (result, _) = within(self.write.write_all(frame)).await;
            result.unwrap();
        }

        async fn received(&mut self, payload: &[u8]) {
            within(async {
                let (result, header) = self
                    .read
                    .read_exact(BytesMut::with_capacity(std::mem::size_of::<TcpMsgHdr>()))
                    .await;
                result.unwrap();
                let header = TcpMsgHdr::read_from_bytes(&header).unwrap();
                assert_eq!(header.magic.get(), HEADER_MAGIC);
                assert_eq!(header.version.get(), HEADER_VERSION);
                assert_eq!(header.length.get(), payload.len() as u64);
                let (result, body) = self
                    .read
                    .read_exact(BytesMut::with_capacity(payload.len()))
                    .await;
                result.unwrap();
                assert_eq!(&body[..], payload);
            })
            .await;
        }

        async fn closed(&mut self) {
            let (result, _) = self.read.read_exact(BytesMut::with_capacity(1)).await;
            assert_eq!(result.unwrap_err().kind(), ErrorKind::UnexpectedEof);
        }
    }

    struct TestDataplane {
        egress: mpsc::Sender<(TcpSocketId, SocketAddr, TcpMsg)>,
        ingress: HashMap<TcpSocketId, mpsc::Receiver<RecvTcpMsg>>,
        addrs: HashMap<TcpSocketId, SocketAddr>,
        control: TcpControl,
        metrics: DataplaneMetrics,
    }

    impl TestDataplane {
        fn new(sockets: &[TcpSocketId]) -> Self {
            let (egress, receiver) = mpsc::channel(16);
            let mut ingress = HashMap::new();
            let configs = sockets
                .iter()
                .map(|&id| {
                    let (sender, receiver) = mpsc::channel(16);
                    ingress.insert(id, receiver);
                    (id, "127.0.0.1:0".parse().unwrap(), sender)
                })
                .collect();
            let cfg = TcpConfig {
                rate_limit: TcpRateLimit {
                    rps: NonZeroU32::new(1000).unwrap(),
                    rps_burst: NonZeroU32::new(100).unwrap(),
                },
                connections_limit: sockets.len(),
                per_ip_connections_limit: sockets.len(),
            };
            let control = TcpControl::new();
            let metrics = DataplaneMetrics::new();
            let (bound_tx, bound_rx) = std::sync::mpsc::sync_channel(1);
            spawn_tasks(
                cfg,
                control.clone(),
                Arc::new(Addrlist::new()),
                configs,
                receiver,
                bound_tx,
                metrics.clone(),
            );
            let addrs = bound_rx.recv().unwrap().into_iter().collect();
            Self {
                egress,
                ingress,
                addrs,
                control,
                metrics,
            }
        }

        async fn send_msg(&self, socket: TcpSocketId, addr: SocketAddr, msg: TcpMsg) {
            within(self.egress.send((socket, addr, msg))).await.unwrap();
        }

        async fn send(&self, socket: TcpSocketId, addr: SocketAddr, payload: &'static [u8]) {
            self.send_msg(
                socket,
                addr,
                TcpMsg {
                    msg: bytes::Bytes::from_static(payload),
                    completion: None,
                },
            )
            .await;
        }

        async fn received(&mut self, socket: TcpSocketId, addr: SocketAddr, payload: &[u8]) {
            let message = within(self.ingress.get_mut(&socket).unwrap().recv())
                .await
                .unwrap();
            assert_eq!(message.src_addr, addr);
            assert_eq!(&message.payload[..], payload);
        }

        async fn open(
            &mut self,
            socket: TcpSocketId,
            outgoing: bool,
            listener: &TcpListener,
        ) -> Peer {
            if outgoing {
                let addr = listener.local_addr().unwrap();
                self.send(socket, addr, b"open").await;
                let (stream, _) = within(listener.accept()).await.unwrap();
                let mut peer = Peer::new(stream, addr);
                peer.received(b"open").await;
                peer
            } else {
                let mut peer = Peer::connect(self.addrs[&socket]).await;
                peer.send(b"open").await;
                self.received(socket, peer.addr, b"open").await;
                peer
            }
        }

        async fn disconnected(&self) {
            within(async {
                while !self.control.0.lock().unwrap().is_empty() {
                    monoio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await;
        }
    }

    #[rstest]
    #[case::accepted_receives(false, true)]
    #[case::accepted_sends(false, false)]
    #[case::outbound_receives(true, true)]
    #[case::outbound_sends(true, false)]
    #[monoio::test(enable_timer = true)]
    async fn test_one_way_traffic_keeps_both_halves_open(
        #[case] outgoing: bool,
        #[case] receiving: bool,
    ) {
        let mut dp = TestDataplane::new(&[SOCKET]);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut peer = dp.open(SOCKET, outgoing, &listener).await;
        // Traffic in only one direction must survive both former idle deadlines.
        for i in 0..27 {
            if i > 0 {
                monoio::time::sleep(Duration::from_millis(500)).await;
            }
            if receiving {
                peer.send(b"incoming").await;
                dp.received(SOCKET, peer.addr, b"incoming").await;
            } else {
                dp.send(SOCKET, peer.addr, b"outgoing").await;
                peer.received(b"outgoing").await;
            }
        }
        // Check that the inactive half still works on the same socket too.
        peer.send(b"request").await;
        dp.received(SOCKET, peer.addr, b"request").await;
        dp.send(SOCKET, peer.addr, b"response").await;
        peer.received(b"response").await;
        dp.control
            .disconnect_socket(peer.addr.ip(), peer.addr.port());
        within(peer.closed()).await;
        dp.disconnected().await;
    }

    #[rstest]
    #[case::accepted(false)]
    #[case::outbound(true)]
    #[monoio::test(enable_timer = true)]
    async fn test_idle_check_keeps_either_direction_alive(#[case] outgoing: bool) {
        let mut dp = TestDataplane::new(&[SOCKET]);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut peer = dp.open(SOCKET, outgoing, &listener).await;
        // TX-only traffic keeps the first two periodic checks alive.
        for _ in 0..27 {
            dp.send(SOCKET, peer.addr, b"outgoing").await;
            peer.received(b"outgoing").await;
            monoio::time::sleep(Duration::from_millis(500)).await;
        }
        dp.send(SOCKET, peer.addr, b"last").await;
        peer.received(b"last").await;
        let stopped = monoio::time::Instant::now();
        monoio::time::timeout(
            CONNECTION_IDLE_CHECK_INTERVAL * 2 + PEER_TIMEOUT,
            peer.closed(),
        )
        .await
        .unwrap();
        assert!(stopped.elapsed() >= CONNECTION_IDLE_CHECK_INTERVAL);
        dp.disconnected().await;
        assert_eq!(dp.metrics.tcp_receive_errors.get(), 0);
        assert_eq!(dp.metrics.tcp_send_errors.get(), 0);
    }

    #[monoio::test(enable_timer = true)]
    async fn test_partial_frame_is_not_idle() {
        let mut dp = TestDataplane::new(&[SOCKET]);
        let mut peer = Peer::connect(dp.addrs[&SOCKET]).await;
        let frame = [TcpMsgHdr::new(2).as_bytes(), b"ok"].concat();
        // TX becomes idle, but RX remains active while finishing this frame.
        // Each frame stage still has its own fixed deadline.
        for chunk in [&frame[..8], &frame[8..17], &frame[17..]] {
            monoio::time::sleep(Duration::from_secs(6)).await;
            let (result, _) = within(peer.write.write_all(chunk.to_vec())).await;
            result.unwrap();
        }
        dp.received(SOCKET, peer.addr, b"ok").await;
        dp.control
            .disconnect_socket(peer.addr.ip(), peer.addr.port());
        within(peer.closed()).await;
        dp.disconnected().await;
    }

    #[rstest]
    #[case::header(false)]
    #[case::body(true)]
    #[monoio::test(enable_timer = true)]
    async fn test_frame_deadline_is_not_extended_by_progress(#[case] body: bool) {
        let dp = TestDataplane::new(&[SOCKET]);
        let mut peer = Peer::connect(dp.addrs[&SOCKET]).await;
        let frame = [TcpMsgHdr::new(8).as_bytes(), &[0; 8]].concat();
        let prefix_len = if body { 17 } else { 1 };
        let (result, _) = within(peer.write.write_all(frame[..prefix_len].to_vec())).await;
        result.unwrap();
        for offset in 0..3 {
            monoio::time::sleep(Duration::from_secs(3)).await;
            let (result, _) = within(peer.write.write_all(vec![frame[prefix_len + offset]])).await;
            result.unwrap();
        }
        // RX is active while the frame is incomplete. Its fixed deadline
        // still closes the socket despite these intermediate bytes.
        within(peer.closed()).await;
        dp.disconnected().await;
        assert_eq!(dp.metrics.tcp_receive_errors.get(), 1);
        assert_eq!(dp.metrics.tcp_current_inbound_connections.get(), 0);
    }

    #[rstest]
    #[case::accepted(false)]
    #[case::outbound_same_endpoint(true)]
    #[monoio::test(enable_timer = true)]
    async fn test_socket_isolation(#[case] outgoing: bool) {
        let sockets = [SOCKET, TcpSocketId::AuthenticatedRaptorcast];
        let mut dp = TestDataplane::new(&sockets);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut peers = Vec::new();
        for socket in sockets {
            let mut peer = dp.open(socket, outgoing, &listener).await;
            peer.send(b"request").await;
            dp.received(socket, peer.addr, b"request").await;
            dp.send(socket, peer.addr, b"response").await;
            peer.received(b"response").await;
            peers.push(peer);
        }
        assert_eq!(dp.control.0.lock().unwrap().len(), 2);
        dp.control
            .disconnect_socket(peers[0].addr.ip(), peers[0].addr.port());
        within(peers[0].closed()).await;
        if !outgoing {
            // Closing one accepted peer must leave the other socket operational.
            peers[1].send(b"still connected").await;
            dp.received(sockets[1], peers[1].addr, b"still connected")
                .await;
            dp.control.disconnect_ip(peers[1].addr.ip());
        }
        within(peers[1].closed()).await;
        dp.disconnected().await;
    }

    #[monoio::test(enable_timer = true)]
    async fn test_disconnect_with_full_ingress_queue() {
        let mut dp = TestDataplane::new(&[SOCKET]);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut peer = dp.open(SOCKET, false, &listener).await;
        for _ in 0..17 {
            peer.send(b"fill ingress").await;
        }
        within(async {
            while dp.metrics.tcp_messages_received.get() != 18 {
                monoio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert_eq!(dp.ingress[&SOCKET].len(), 16);
        dp.control
            .disconnect_socket(peer.addr.ip(), peer.addr.port());
        within(peer.closed()).await;
        dp.disconnected().await;
        assert_eq!(dp.metrics.tcp_current_inbound_connections.get(), 0);
    }

    #[monoio::test(enable_timer = true)]
    async fn test_eof_cancels_writer_and_allows_reconnect() {
        let mut dp = TestDataplane::new(&[SOCKET]);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        for _ in 0..2 {
            let peer = dp.open(SOCKET, true, &listener).await;
            let close_time = monoio::time::Instant::now();
            drop(peer);
            dp.disconnected().await;
            assert!(close_time.elapsed() >= Duration::from_secs(1));
            assert_eq!(dp.metrics.tcp_current_outbound_connections.get(), 0);
        }
    }

    #[rstest]
    #[case::silent(Vec::new())]
    #[case::partial_header(TcpMsgHdr::new(1).as_bytes()[..8].to_vec())]
    #[case::partial_body([TcpMsgHdr::new(TCP_MESSAGE_LENGTH_LIMIT as u64).as_bytes(), &[1]].concat())]
    #[monoio::test(enable_timer = true)]
    async fn test_stalled_first_frame_releases_connection_slot(#[case] prefix: Vec<u8>) {
        let mut dp = TestDataplane::new(&[SOCKET]);
        let mut peer = Peer::connect(dp.addrs[&SOCKET]).await;
        if !prefix.is_empty() {
            let (result, _) = within(peer.write.write_all(prefix)).await;
            result.unwrap();
        }
        monoio::time::timeout(CONNECTION_IDLE_CHECK_INTERVAL + PEER_TIMEOUT, peer.closed())
            .await
            .unwrap();
        dp.disconnected().await;
        assert_eq!(dp.metrics.tcp_current_inbound_connections.get(), 0);
        // With a one-connection quota, a new peer proves the slot was released.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _replacement = dp.open(SOCKET, false, &listener).await;
    }
}
