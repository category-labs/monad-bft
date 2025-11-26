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
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    ops::RangeBounds,
    sync::{Arc, Mutex},
    time::Duration,
};

use monoio::{
    net::{ListenerOpts, TcpListener},
    spawn,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::trace;
use zerocopy::{
    byteorder::little_endian::{U32, U64},
    FromBytes, Immutable, IntoBytes,
};

use super::{RecvTcpMsg, TcpMsg};
use crate::Addrlist;

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

pub(crate) fn spawn_tasks(
    cfg: TcpConfig,
    tcp_control_map: TcpControl,
    addrlist: Arc<Addrlist>,
    socket_configs: Vec<(super::TcpSocketType, SocketAddr, mpsc::Sender<RecvTcpMsg>)>,
    tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>,
) {
    for (socket, socket_addr, ingress_tx) in socket_configs {
        let opts = ListenerOpts::new().reuse_addr(true);
        let tcp_listener = TcpListener::bind_with_config(socket_addr, &opts).unwrap();

        spawn(rx::task(
            socket,
            cfg,
            tcp_control_map.clone(),
            addrlist.clone(),
            tcp_listener,
            ingress_tx,
        ));
        trace!(socket = %socket, ?socket_addr, "created tcp listener");
    }
    spawn(tx::task(tcp_egress_rx));
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

#[derive(Debug, Clone)]
pub(crate) enum TcpControlMsg {
    Disconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TcpConnectionId {
    pub(crate) ip: IpAddr,
    pub(crate) port: u16,
    pub(crate) conn_id: u64,
    pub(crate) socket: super::TcpSocketType,
}

impl TcpConnectionId {
    fn range_for_ip(ip: IpAddr) -> impl RangeBounds<Self> {
        let start = Self {
            ip,
            port: u16::MIN,
            conn_id: u64::MIN,
            socket: super::TcpSocketType::Original,
        };
        let end = Self {
            ip,
            port: u16::MAX,
            conn_id: u64::MAX,
            socket: super::MAX_TCP_SOCKET_TYPE,
        };
        start..=end
    }

    fn range_for_socket(ip: IpAddr, port: u16) -> impl RangeBounds<Self> {
        let start = Self {
            ip,
            port,
            conn_id: u64::MIN,
            socket: super::TcpSocketType::Original,
        };
        let end = Self {
            ip,
            port,
            conn_id: u64::MAX,
            socket: super::MAX_TCP_SOCKET_TYPE,
        };
        start..=end
    }
}

pub(crate) type TcpControlSender = UnboundedSender<TcpControlMsg>;
pub(crate) type TcpControlReceiver = UnboundedReceiver<TcpControlMsg>;

#[derive(Debug, Clone)]
pub(crate) struct TcpControl(Arc<Mutex<BTreeMap<TcpConnectionId, TcpControlSender>>>);

impl TcpControl {
    pub(crate) fn new() -> TcpControl {
        TcpControl(Arc::new(Mutex::new(BTreeMap::new())))
    }

    pub(crate) fn register(&self, id: TcpConnectionId) -> TcpControlReceiver {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.lock().unwrap().insert(id, tx);
        rx
    }

    pub(crate) fn unregister(&self, id: &TcpConnectionId) {
        self.0.lock().unwrap().remove(id);
    }

    #[allow(unused)]
    pub(crate) fn send_lossy(&self, id: &TcpConnectionId, msg: TcpControlMsg) {
        if let Some(tx) = self.0.lock().unwrap().get(id) {
            let _ = tx.send(msg);
        }
    }

    #[allow(unused)]
    pub(crate) fn disconnect_ip(&self, ip: IpAddr) {
        let map = self.0.lock().unwrap();
        let mut count = 0;
        for (_, tx) in map.range(TcpConnectionId::range_for_ip(ip)) {
            let _ = tx.send(TcpControlMsg::Disconnect);
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
        for (_, tx) in map.range(TcpConnectionId::range_for_socket(ip, port)) {
            let _ = tx.send(TcpControlMsg::Disconnect);
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
    use std::net::{IpAddr, Ipv4Addr};

    use rstest::*;

    use super::*;
    use crate::TcpSocketType;

    fn id(socket: TcpSocketType, ip: IpAddr, port: u16, conn_id: u64) -> TcpConnectionId {
        TcpConnectionId {
            ip,
            port,
            conn_id,
            socket,
        }
    }

    #[fixture]
    fn tcp_control() -> TcpControl {
        TcpControl::new()
    }

    #[fixture]
    fn tcp_id() -> TcpConnectionId {
        id(
            TcpSocketType::Original,
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            8080,
            12345,
        )
    }

    #[rstest]
    fn test_register_and_unregister(tcp_control: TcpControl, tcp_id: TcpConnectionId) {
        let _rx = tcp_control.register(tcp_id);
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
        tcp_control.unregister(&tcp_id);
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
    }

    #[rstest]
    fn test_send_lossy_existing_and_nonexistent(tcp_control: TcpControl, tcp_id: TcpConnectionId) {
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);

        let mut rx = tcp_control.register(tcp_id);
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
        assert!(matches!(rx.try_recv().unwrap(), TcpControlMsg::Disconnect));
    }

    #[rstest]
    fn test_multiple_registrations_same_id(tcp_control: TcpControl, tcp_id: TcpConnectionId) {
        let _rx1 = tcp_control.register(tcp_id);
        let mut rx2 = tcp_control.register(tcp_id);

        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
        assert!(matches!(rx2.try_recv().unwrap(), TcpControlMsg::Disconnect));
    }

    #[rstest]
    #[case::same_ip_different_ports(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9090, 2),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 3),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        vec![0, 1]
    )]
    #[case::edge_ports(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MIN, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MAX, 2),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 3),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 4),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        vec![0, 1, 2]
    )]
    #[case::no_matching_ip(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)), 9090, 2),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        vec![]
    )]
    #[case::across_sockets(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 2),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 3),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        vec![0, 1]
    )]
    fn test_disconnect_ip(
        tcp_control: TcpControl,
        #[case] sockets: Vec<TcpConnectionId>,
        #[case] disconnect_ip: IpAddr,
        #[case] expected_disconnected_indices: Vec<usize>,
    ) {
        let mut socket_receivers = std::collections::HashMap::new();

        for (i, socket) in sockets.iter().enumerate() {
            let rx = tcp_control.register(*socket);
            socket_receivers.insert(i, rx);
        }

        tcp_control.disconnect_ip(disconnect_ip);

        for (i, mut rx) in socket_receivers {
            let should_disconnect = expected_disconnected_indices.contains(&i);
            if should_disconnect {
                assert!(matches!(rx.try_recv().unwrap(), TcpControlMsg::Disconnect));
            } else {
                assert!(rx.try_recv().is_err());
            }
        }
    }

    #[rstest]
    #[case::same_ip_port_different_connections(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 2),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9090, 3),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080, 4),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        8080,
        vec![0, 1]
    )]
    #[case::edge_port_numbers(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MIN, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), u16::MAX, 2),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 3),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), u16::MIN, 4),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        u16::MIN,
        vec![0]
    )]
    #[case::no_matching_socket(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9090, 2),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        9090,
        vec![]
    )]
    #[case::across_sockets(
        vec![
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 1),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080, 2),
            id(TcpSocketType::Original, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9090, 3),
        ],
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        8080,
        vec![0, 1]
    )]
    fn test_disconnect_socket(
        tcp_control: TcpControl,
        #[case] sockets: Vec<TcpConnectionId>,
        #[case] disconnect_ip: IpAddr,
        #[case] disconnect_port: u16,
        #[case] expected_disconnected_indices: Vec<usize>,
    ) {
        let mut socket_receivers = std::collections::HashMap::new();

        for (i, socket) in sockets.iter().enumerate() {
            let rx = tcp_control.register(*socket);
            socket_receivers.insert(i, rx);
        }

        tcp_control.disconnect_socket(disconnect_ip, disconnect_port);

        for (i, mut rx) in socket_receivers {
            let should_disconnect = expected_disconnected_indices.contains(&i);
            if should_disconnect {
                assert!(matches!(rx.try_recv().unwrap(), TcpControlMsg::Disconnect));
            } else {
                assert!(rx.try_recv().is_err());
            }
        }
    }
}
