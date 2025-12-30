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

use super::{RecvTcpMsg, TcpMsg, TcpSocketId};
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
    socket_configs: Vec<(TcpSocketId, SocketAddr, mpsc::Sender<RecvTcpMsg>)>,
    tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>,
    bound_addrs_tx: std::sync::mpsc::SyncSender<Vec<(TcpSocketId, SocketAddr)>>,
) {
    let mut bound_addrs = Vec::with_capacity(socket_configs.len());

    let rx_state = rx::RxState::new(
        addrlist,
        cfg.connections_limit,
        cfg.per_ip_connections_limit,
    );

    for (socket_id, socket_addr, ingress_tx) in socket_configs {
        let opts = ListenerOpts::new().reuse_addr(true);
        let tcp_listener = TcpListener::bind_with_config(socket_addr, &opts).unwrap();
        let actual_addr = tcp_listener.local_addr().unwrap();
        bound_addrs.push((socket_id, actual_addr));

        spawn(rx::task(
            cfg.rate_limit,
            tcp_control_map.clone(),
            rx_state.clone(),
            tcp_listener,
            ingress_tx,
        ));
        trace!(?socket_id, ?socket_addr, actual_addr = ?actual_addr, "created tcp listener");
    }

    bound_addrs_tx.send(bound_addrs).unwrap();
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

pub(crate) type TcpIdentifier = (IpAddr, u16, u64);
pub(crate) type TcpControlSender = UnboundedSender<TcpControlMsg>;
pub(crate) type TcpControlReceiver = UnboundedReceiver<TcpControlMsg>;

#[derive(Debug, Clone)]
pub(crate) struct TcpControl(Arc<Mutex<BTreeMap<TcpIdentifier, TcpControlSender>>>);

impl TcpControl {
    pub(crate) fn new() -> TcpControl {
        TcpControl(Arc::new(Mutex::new(BTreeMap::new())))
    }

    pub(crate) fn register(&self, id: TcpIdentifier) -> TcpControlReceiver {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.lock().unwrap().insert(id, tx);
        rx
    }

    pub(crate) fn unregister(&self, id: &TcpIdentifier) {
        self.0.lock().unwrap().remove(id);
    }

    #[allow(unused)]
    pub(crate) fn send_lossy(&self, id: &TcpIdentifier, msg: TcpControlMsg) {
        if let Some(tx) = self.0.lock().unwrap().get(id) {
            let _ = tx.send(msg);
        }
    }

    #[allow(unused)]
    pub(crate) fn disconnect_ip(&self, ip: IpAddr) {
        let map = self.0.lock().unwrap();
        let mut count = 0;
        for (id, tx) in map.range((ip, u16::MIN, u64::MIN)..(ip, u16::MAX, u64::MAX)) {
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
        for (_, tx) in map.range((ip, port, u64::MIN)..(ip, port, u64::MAX)) {
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
        let _rx = tcp_control.register(tcp_id);
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
        tcp_control.unregister(&tcp_id);
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
    }

    #[rstest]
    fn test_send_lossy_existing_and_nonexistent(tcp_control: TcpControl, tcp_id: TcpIdentifier) {
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);

        let mut rx = tcp_control.register(tcp_id);
        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
        assert!(matches!(rx.try_recv().unwrap(), TcpControlMsg::Disconnect));
    }

    #[rstest]
    fn test_multiple_registrations_same_id(tcp_control: TcpControl, tcp_id: TcpIdentifier) {
        let _rx1 = tcp_control.register(tcp_id);
        let mut rx2 = tcp_control.register(tcp_id);

        tcp_control.send_lossy(&tcp_id, TcpControlMsg::Disconnect);
        assert!(matches!(rx2.try_recv().unwrap(), TcpControlMsg::Disconnect));
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
        let mut socket_receivers = HashMap::new();

        for (i, &socket) in sockets.iter().enumerate() {
            let rx = tcp_control.register(socket);
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
        let mut socket_receivers = HashMap::new();

        for (i, &socket) in sockets.iter().enumerate() {
            let rx = tcp_control.register(socket);
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
