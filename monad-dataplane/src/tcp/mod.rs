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
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{trace, warn};
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

pub(crate) fn spawn_tasks(
    cfg: TcpConfig,
    tcp_control_map: TcpControl,
    addrlist: Arc<Addrlist>,
    socket_configs: Vec<(TcpSocketId, SocketAddr, mpsc::Sender<RecvTcpMsg>)>,
    tcp_egress_rx: mpsc::Receiver<(TcpSocketId, SocketAddr, TcpMsg)>,
    bound_addrs_tx: std::sync::mpsc::SyncSender<Vec<(TcpSocketId, SocketAddr)>>,
) {
    let mut bound_addrs = Vec::with_capacity(socket_configs.len());
    let (write_half_tx, write_half_rx) = mpsc::unbounded_channel();

    let mut read_half_txs = BTreeMap::new();

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

        let (read_half_tx, read_half_rx) = mpsc::unbounded_channel();
        read_half_txs.insert(socket_id, read_half_tx);

        spawn(rx::task(
            socket_id,
            cfg.rate_limit,
            tcp_control_map.clone(),
            rx_state.clone(),
            tcp_listener,
            ingress_tx,
            write_half_tx.clone(),
            read_half_rx,
        ));
        trace!(?socket_id, ?socket_addr, actual_addr = ?actual_addr, "created tcp listener");
    }

    bound_addrs_tx.send(bound_addrs).unwrap();
    spawn(tx::task(tcp_egress_rx, write_half_rx, read_half_txs));
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

    async fn write_tcp_message(
        write_half: &mut monoio::net::tcp::TcpOwnedWriteHalf,
        payload: &[u8],
    ) {
        let header = TcpMsgHdr::new(payload.len() as u64);
        let (ret, _) = write_half
            .write_all(Box::<[u8]>::from(header.as_bytes()))
            .await;
        ret.unwrap();
        let (ret, _) = write_half.write_all(payload.to_vec()).await;
        ret.unwrap();
    }

    async fn read_tcp_message(read_half: &mut monoio::net::tcp::TcpOwnedReadHalf) -> bytes::Bytes {
        let header_buf = BytesMut::with_capacity(std::mem::size_of::<TcpMsgHdr>());
        let (ret, header_buf) = read_half.read_exact(header_buf).await;
        ret.unwrap();
        let header = TcpMsgHdr::read_from_bytes(&header_buf[..]).unwrap();
        assert_eq!(header.magic.get(), HEADER_MAGIC);
        assert_eq!(header.version.get(), HEADER_VERSION);

        let msg_len = header.length.get() as usize;
        let msg_buf = BytesMut::with_capacity(msg_len);
        let (ret, msg_buf) = read_half.read_exact(msg_buf).await;
        ret.unwrap();
        msg_buf.freeze()
    }

    #[monoio::test(enable_timer = true)]
    async fn test_outbound_connection_receives_messages() {
        let server_listener = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let server_addr = server_listener.local_addr().unwrap();

        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(16);
        let (tcp_ingress_tx, mut tcp_ingress_rx) = mpsc::channel(16);

        let tcp_config = TcpConfig {
            rate_limit: TcpRateLimit {
                rps: NonZeroU32::new(1000).unwrap(),
                rps_burst: NonZeroU32::new(100).unwrap(),
            },
            connections_limit: 100,
            per_ip_connections_limit: 10,
        };

        let tcp_control = TcpControl::new();
        let addrlist = Arc::new(Addrlist::new());

        let listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket_configs = vec![(TcpSocketId::Raptorcast, listen_addr, tcp_ingress_tx)];
        let (bound_addrs_tx, _bound_addrs_rx) = std::sync::mpsc::sync_channel(1);

        spawn_tasks(
            tcp_config,
            tcp_control,
            addrlist,
            socket_configs,
            tcp_egress_rx,
            bound_addrs_tx,
        );

        let test_payload = b"hello from client";
        tcp_egress_tx
            .send((
                TcpSocketId::Raptorcast,
                server_addr,
                TcpMsg {
                    msg: bytes::Bytes::from_static(test_payload),
                    completion: None,
                },
            ))
            .await
            .unwrap();

        let (server_stream, _client_addr) = server_listener.accept().await.unwrap();
        let (mut server_read, mut server_write) = server_stream.into_split();

        let received = read_tcp_message(&mut server_read).await;
        assert_eq!(&received[..], test_payload);

        let response_payload = b"hello from server";
        write_tcp_message(&mut server_write, response_payload).await;

        let recv_msg =
            monoio::time::timeout(std::time::Duration::from_secs(5), tcp_ingress_rx.recv())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(&recv_msg.payload[..], response_payload);
        assert_eq!(recv_msg.src_addr, server_addr);
    }

    #[monoio::test(enable_timer = true)]
    async fn test_accepted_connection_can_send_messages() {
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(16);
        let (tcp_ingress_tx, mut tcp_ingress_rx) = mpsc::channel(16);

        let tcp_config = TcpConfig {
            rate_limit: TcpRateLimit {
                rps: NonZeroU32::new(1000).unwrap(),
                rps_burst: NonZeroU32::new(100).unwrap(),
            },
            connections_limit: 100,
            per_ip_connections_limit: 10,
        };

        let tcp_control = TcpControl::new();
        let addrlist = Arc::new(Addrlist::new());

        let socket_configs = vec![(
            TcpSocketId::Raptorcast,
            "127.0.0.1:0".parse().unwrap(),
            tcp_ingress_tx,
        )];
        let (bound_addrs_tx, bound_addrs_rx) = std::sync::mpsc::sync_channel(1);

        spawn_tasks(
            tcp_config,
            tcp_control,
            addrlist,
            socket_configs,
            tcp_egress_rx,
            bound_addrs_tx,
        );

        let bound_addrs = bound_addrs_rx.recv().unwrap();
        let listen_addr = bound_addrs[0].1;

        let client_stream = monoio::net::TcpStream::connect(listen_addr).await.unwrap();
        let client_addr = client_stream.local_addr().unwrap();
        let (mut client_read, mut client_write) = client_stream.into_split();

        let test_payload = b"hello from raw client";
        write_tcp_message(&mut client_write, test_payload).await;

        let recv_msg =
            monoio::time::timeout(std::time::Duration::from_secs(5), tcp_ingress_rx.recv())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(&recv_msg.payload[..], test_payload);
        assert_eq!(recv_msg.src_addr, client_addr);

        let response_payload = b"response to raw client";
        tcp_egress_tx
            .send((
                TcpSocketId::Raptorcast,
                client_addr,
                TcpMsg {
                    msg: bytes::Bytes::from_static(response_payload),
                    completion: None,
                },
            ))
            .await
            .unwrap();

        let received = monoio::time::timeout(
            std::time::Duration::from_secs(5),
            read_tcp_message(&mut client_read),
        )
        .await
        .unwrap();

        assert_eq!(&received[..], response_payload);
    }

    #[monoio::test(enable_timer = true)]
    async fn test_multi_socket_isolation() {
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(16);
        let (ingress_tx_a, mut ingress_rx_a) = mpsc::channel(16);
        let (ingress_tx_b, mut ingress_rx_b) = mpsc::channel(16);

        let tcp_config = TcpConfig {
            rate_limit: TcpRateLimit {
                rps: NonZeroU32::new(1000).unwrap(),
                rps_burst: NonZeroU32::new(100).unwrap(),
            },
            connections_limit: 100,
            per_ip_connections_limit: 10,
        };

        let addr_a: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let socket_configs = vec![
            (TcpSocketId::Raptorcast, addr_a, ingress_tx_a),
            (TcpSocketId::AuthenticatedRaptorcast, addr_b, ingress_tx_b),
        ];
        let (bound_addrs_tx, bound_addrs_rx) = std::sync::mpsc::sync_channel(1);

        spawn_tasks(
            tcp_config,
            TcpControl::new(),
            Arc::new(Addrlist::new()),
            socket_configs,
            tcp_egress_rx,
            bound_addrs_tx,
        );

        let bound_addrs = bound_addrs_rx.recv().unwrap();
        let listen_addr_a = bound_addrs[0].1;
        let listen_addr_b = bound_addrs[1].1;

        let client_a = monoio::net::TcpStream::connect(listen_addr_a)
            .await
            .unwrap();
        let client_a_addr = client_a.local_addr().unwrap();
        let (mut read_a, mut write_a) = client_a.into_split();

        let client_b = monoio::net::TcpStream::connect(listen_addr_b)
            .await
            .unwrap();
        let client_b_addr = client_b.local_addr().unwrap();
        let (mut read_b, mut write_b) = client_b.into_split();

        write_tcp_message(&mut write_a, b"msg_a").await;
        let recv = monoio::time::timeout(std::time::Duration::from_secs(2), ingress_rx_a.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&recv.payload[..], b"msg_a");

        write_tcp_message(&mut write_b, b"msg_b").await;
        let recv = monoio::time::timeout(std::time::Duration::from_secs(2), ingress_rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&recv.payload[..], b"msg_b");

        tcp_egress_tx
            .send((
                TcpSocketId::Raptorcast,
                client_a_addr,
                TcpMsg {
                    msg: bytes::Bytes::from_static(b"resp_a"),
                    completion: None,
                },
            ))
            .await
            .unwrap();

        tcp_egress_tx
            .send((
                TcpSocketId::AuthenticatedRaptorcast,
                client_b_addr,
                TcpMsg {
                    msg: bytes::Bytes::from_static(b"resp_b"),
                    completion: None,
                },
            ))
            .await
            .unwrap();

        let recv_a = monoio::time::timeout(
            std::time::Duration::from_secs(2),
            read_tcp_message(&mut read_a),
        )
        .await
        .unwrap();
        assert_eq!(&recv_a[..], b"resp_a");

        let recv_b = monoio::time::timeout(
            std::time::Duration::from_secs(2),
            read_tcp_message(&mut read_b),
        )
        .await
        .unwrap();
        assert_eq!(&recv_b[..], b"resp_b");
    }

    #[monoio::test(enable_timer = true)]
    async fn test_same_peer_multiple_sockets() {
        let server_a = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let server_b = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let server_a_addr = server_a.local_addr().unwrap();
        let server_b_addr = server_b.local_addr().unwrap();

        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(16);
        let (ingress_tx_a, mut ingress_rx_a) = mpsc::channel(16);
        let (ingress_tx_b, mut ingress_rx_b) = mpsc::channel(16);

        let tcp_config = TcpConfig {
            rate_limit: TcpRateLimit {
                rps: NonZeroU32::new(1000).unwrap(),
                rps_burst: NonZeroU32::new(100).unwrap(),
            },
            connections_limit: 100,
            per_ip_connections_limit: 10,
        };

        let socket_configs = vec![
            (
                TcpSocketId::Raptorcast,
                "127.0.0.1:0".parse().unwrap(),
                ingress_tx_a,
            ),
            (
                TcpSocketId::AuthenticatedRaptorcast,
                "127.0.0.1:0".parse().unwrap(),
                ingress_tx_b,
            ),
        ];
        let (bound_addrs_tx, bound_addrs_rx) = std::sync::mpsc::sync_channel(1);

        spawn_tasks(
            tcp_config,
            TcpControl::new(),
            Arc::new(Addrlist::new()),
            socket_configs,
            tcp_egress_rx,
            bound_addrs_tx,
        );
        let _ = bound_addrs_rx.recv().unwrap();

        tcp_egress_tx
            .send((
                TcpSocketId::Raptorcast,
                server_a_addr,
                TcpMsg {
                    msg: bytes::Bytes::from_static(b"via_socket_a"),
                    completion: None,
                },
            ))
            .await
            .unwrap();

        tcp_egress_tx
            .send((
                TcpSocketId::AuthenticatedRaptorcast,
                server_b_addr,
                TcpMsg {
                    msg: bytes::Bytes::from_static(b"via_socket_b"),
                    completion: None,
                },
            ))
            .await
            .unwrap();

        let (stream_a, _) = server_a.accept().await.unwrap();
        let (stream_b, _) = server_b.accept().await.unwrap();
        let (mut read_a, mut write_a) = stream_a.into_split();
        let (mut read_b, mut write_b) = stream_b.into_split();

        let msg_a = read_tcp_message(&mut read_a).await;
        assert_eq!(&msg_a[..], b"via_socket_a");

        let msg_b = read_tcp_message(&mut read_b).await;
        assert_eq!(&msg_b[..], b"via_socket_b");

        write_tcp_message(&mut write_a, b"resp_from_a").await;
        write_tcp_message(&mut write_b, b"resp_from_b").await;

        let recv_a = monoio::time::timeout(std::time::Duration::from_secs(2), ingress_rx_a.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&recv_a.payload[..], b"resp_from_a");

        let recv_b = monoio::time::timeout(std::time::Duration::from_secs(2), ingress_rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&recv_b.payload[..], b"resp_from_b");
    }
}
