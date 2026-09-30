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
    io::Error,
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
    spawn,
};
use tokio::sync::{mpsc, watch};
use tracing::{trace, warn};
use zerocopy::{
    byteorder::little_endian::{U32, U64},
    FromBytes, Immutable, IntoBytes,
};

use super::{RecvTcpMsg, TcpMsg, TcpSocketId};
use crate::{metrics::DataplaneMetrics, Addrlist};

pub mod rx;
pub mod tx;

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

#[derive(Clone, Debug)]
pub(crate) struct TcpConnection(watch::Sender<bool>);

impl TcpConnection {
    pub(crate) fn new() -> Self {
        Self(watch::channel(false).0)
    }

    pub(crate) fn disconnect(&self) {
        self.0.send_replace(true);
    }

    pub(crate) async fn disconnected(&self) {
        let mut rx = self.0.subscribe();
        let disconnected = *rx.borrow_and_update();
        if !disconnected {
            let _ = rx.changed().await;
        }
    }

    #[cfg(test)]
    fn is_disconnected(&self) -> bool {
        *self.0.borrow()
    }
}

const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

// Both loops update the same activity clock. A bounded frame transfer owns a
// guard so the idle timer cannot preempt its read or write deadline.
pub(crate) struct ConnectionActivity {
    last_activity: Cell<monoio::time::Instant>,
    transfers: Cell<usize>,
}

impl ConnectionActivity {
    fn new() -> Self {
        Self {
            last_activity: Cell::new(monoio::time::Instant::now()),
            transfers: Cell::new(0),
        }
    }

    fn transferring(&self) -> TransferGuard<'_> {
        self.transfers.set(self.transfers.get() + 1);
        TransferGuard(self)
    }

    async fn idle(&self) {
        loop {
            if self.transfers.get() > 0 {
                monoio::time::sleep(CONNECTION_IDLE_TIMEOUT).await;
                continue;
            }
            let deadline = self.last_activity.get() + CONNECTION_IDLE_TIMEOUT;
            monoio::time::sleep_until(deadline).await;
            if self.transfers.get() == 0
                && monoio::time::Instant::now()
                    >= self.last_activity.get() + CONNECTION_IDLE_TIMEOUT
            {
                return;
            }
        }
    }
}

struct TransferGuard<'a>(&'a ConnectionActivity);

impl Drop for TransferGuard<'_> {
    fn drop(&mut self) {
        self.0.last_activity.set(monoio::time::Instant::now());
        self.0.transfers.set(self.0.transfers.get() - 1);
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
    let tx_state = tx::TxState::new(addrlist.clone(), cfg.connections_limit, metrics.clone());
    let mut contexts = BTreeMap::new();

    let rx_state = rx::RxState::new(
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

        let context = rx::RxContext {
            socket_id,
            rate_limit: cfg.rate_limit,
            tcp_control_map: tcp_control_map.clone(),
            tcp_ingress_tx: ingress_tx,
            metrics: metrics.clone(),
        };
        contexts.insert(socket_id, context.clone());
        spawn(rx::task(
            context,
            rx_state.clone(),
            tx_state.clone(),
            tcp_listener,
        ));
        trace!(?socket_id, ?socket_addr, actual_addr = ?actual_addr, "created tcp listener");
    }

    bound_addrs_tx.send(bound_addrs).unwrap();
    spawn(tx::task(tx_state, tcp_egress_rx, contexts));
}

// Minimum message receive/transmit speed in bytes per second.  Messages that are
// transferred slower than this are aborted.
const MINIMUM_TRANSFER_SPEED: u64 = 1_000_000;

// Allow for at least this transfer time, so that very small messages still have
// a chance to be transferred successfully.
const MINIMUM_TRANSFER_TIME: Duration = Duration::from_secs(10);

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
        net::{IpAddr, Ipv4Addr},
    };

    use rstest::*;

    use super::*;

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
}
