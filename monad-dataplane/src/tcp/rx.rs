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
    cell::RefCell,
    collections::BTreeMap,
    io::ErrorKind,
    net::{IpAddr, SocketAddr},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use monoio::{
    io::AsyncReadRentExt,
    net::{TcpListener, TcpStream},
    select, spawn,
    time::timeout,
};
use tokio::sync::mpsc;
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::FromBytes;

use super::{
    message_timeout, split_stream, RecvTcpMsg, TcpControl, TcpControlMsg, TcpMsgHdr, TcpRateLimit,
    TcpReadHalf, TcpWriteHalf, HEADER_MAGIC, HEADER_VERSION, TCP_MESSAGE_LENGTH_LIMIT,
};
use crate::{
    addrlist::{Addrlist, Status},
    tcp::RateLimiter,
    TcpSocketId,
};

pub(crate) type WriteHalfSender = mpsc::UnboundedSender<(TcpSocketId, SocketAddr, TcpWriteHalf)>;
pub(crate) type ReadHalfReceiver = mpsc::UnboundedReceiver<(SocketAddr, TcpReadHalf)>;

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct RxState {
    inner: Rc<RefCell<RxStateInner>>,
    addrlist: Arc<Addrlist>,
}

impl RxState {
    pub(crate) fn new(
        addrlist: Arc<Addrlist>,
        tcp_connections_limit: usize,
        tcp_per_ip_connections_limit: usize,
    ) -> RxState {
        let inner = Rc::new(RefCell::new(RxStateInner {
            tcp_connections_limit,
            tcp_per_ip_connections_limit,
            num_connections: 0,
            num_connections_per_ip: BTreeMap::new(),
        }));

        RxState { addrlist, inner }
    }

    fn apply_limits(&self, ip: IpAddr) -> Result<ConnectionToken, ()> {
        let status = self.addrlist.status(&ip);
        match status {
            Status::Banned => {
                warn!(?ip, "banned address attempting connection, dropping");
                Err(())
            }
            Status::Trusted => {
                let inner_ref = self.inner.borrow();
                trace!(
                    ?ip,
                    total_connections = inner_ref.num_connections,
                    connection_limit = inner_ref.tcp_connections_limit,
                    "trusted peer connection accepted"
                );
                Ok(ConnectionToken::Trusted)
            }
            Status::Unknown => {
                let mut inner_ref = self.inner.borrow_mut();
                if inner_ref.num_connections >= inner_ref.tcp_connections_limit {
                    debug!(
                        ?ip,
                        total_connections = inner_ref.num_connections,
                        connection_limit = inner_ref.tcp_connections_limit,
                        "total connection limit reached, dropping"
                    );
                    return Err(());
                }
                {
                    let per_ip_limit = inner_ref.tcp_per_ip_connections_limit;
                    let count_ref = inner_ref.num_connections_per_ip.entry(ip).or_insert(0);
                    if *count_ref >= per_ip_limit {
                        debug!(
                            ?ip,
                            ip_connections = *count_ref,
                            per_ip_limit,
                            "per-ip connection limit reached, dropping"
                        );
                        return Err(());
                    }
                    *count_ref += 1;
                }
                inner_ref.num_connections += 1;
                trace!(
                    ?ip,
                    total_connections = inner_ref.num_connections,
                    ip_connections = inner_ref
                        .num_connections_per_ip
                        .get(&ip)
                        .copied()
                        .unwrap_or(0),
                    "unknown peer connection accepted"
                );
                Ok(ConnectionToken::Unknown {
                    inner: self.inner.clone(),
                    ip,
                })
            }
        }
    }
}

enum ConnectionToken {
    Trusted,
    Unknown {
        inner: Rc<RefCell<RxStateInner>>,
        ip: IpAddr,
    },
}

impl Drop for ConnectionToken {
    fn drop(&mut self) {
        match self {
            ConnectionToken::Trusted => {
                trace!("trusted connection dropped");
            }
            ConnectionToken::Unknown { inner, ip } => {
                let mut inner_ref = inner.borrow_mut();
                inner_ref.num_connections -= 1;
                if let Some(count_ref) = inner_ref.num_connections_per_ip.get_mut(ip) {
                    if *count_ref > 1 {
                        *count_ref -= 1;
                    } else {
                        inner_ref.num_connections_per_ip.remove(ip);
                    }
                } else {
                    warn!(%ip, "num_connections_per_ip should not be empty")
                }
            }
        }
    }
}

struct RxStateInner {
    tcp_connections_limit: usize,
    tcp_per_ip_connections_limit: usize,
    num_connections: usize,
    num_connections_per_ip: BTreeMap<IpAddr, usize>,
}

pub(crate) async fn task(
    socket_id: TcpSocketId,
    rate_limit: TcpRateLimit,
    tcp_control_map: TcpControl,
    rx_state: RxState,
    tcp_listener: TcpListener,
    tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
    write_half_tx: WriteHalfSender,
    mut read_half_rx: ReadHalfReceiver,
) {
    let mut conn_id: u64 = 0;
    loop {
        select! {
            accepted = tcp_listener.accept() => {
                match accepted {
                    Ok((tcp_stream, addr)) => match rx_state.apply_limits(addr.ip()) {
                        Ok(conn_state) => {
                            spawn(task_connection(
                                socket_id,
                                rate_limit.new_rate_limiter(),
                                tcp_control_map.clone(),
                                conn_state,
                                conn_id,
                                addr,
                                tcp_stream,
                                tcp_ingress_tx.clone(),
                                write_half_tx.clone(),
                            ));
                        }
                        Err(()) => {
                            debug!(
                                conn_id,
                                ?addr,
                                "connection limit reached, rejecting tcp connection"
                            );
                        }
                    },
                    Err(err) => {
                        warn!(conn_id, ?err, "error accepting tcp connection");
                    }
                }
                conn_id += 1;
            }
            read_half = read_half_rx.recv() => {
                let Some((addr, read_half)) = read_half else {
                    break;
                };
                trace!(conn_id, ?addr, "received read half from tx");

                spawn(task_read(
                    rate_limit.new_rate_limiter(),
                    tcp_control_map.clone(),
                    conn_id,
                    addr,
                    read_half,
                    tcp_ingress_tx.clone(),
                ));
                conn_id += 1;
            }
        }
    }
}

async fn task_connection(
    socket_id: TcpSocketId,
    rate_limiter: RateLimiter,
    tcp_control_map: TcpControl,
    _rx_state: ConnectionToken,
    conn_id: u64,
    addr: SocketAddr,
    tcp_stream: TcpStream,
    tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
    write_half_tx: WriteHalfSender,
) {
    let (read_half, write_half) = split_stream(tcp_stream);

    if let Err(err) = write_half_tx.send((socket_id, addr, write_half)) {
        warn!(conn_id, ?addr, ?err, "failed to send write half to tx task");
        return;
    }

    task_read_inner(
        rate_limiter,
        tcp_control_map,
        conn_id,
        addr,
        read_half,
        tcp_ingress_tx,
    )
    .await;
}

async fn task_read(
    rate_limiter: RateLimiter,
    tcp_control_map: TcpControl,
    conn_id: u64,
    addr: SocketAddr,
    read_half: TcpReadHalf,
    tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
) {
    task_read_inner(
        rate_limiter,
        tcp_control_map,
        conn_id,
        addr,
        read_half,
        tcp_ingress_tx,
    )
    .await;
}

async fn task_read_inner(
    rate_limiter: RateLimiter,
    tcp_control_map: TcpControl,
    conn_id: u64,
    addr: SocketAddr,
    mut read_half: TcpReadHalf,
    tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
) {
    let mut control_rx = tcp_control_map.register((addr.ip(), addr.port(), conn_id));
    let mut message_id: u64 = 0;
    loop {
        select! {
            biased;
            ctl = control_rx.recv() => {
                match ctl {
                    None => {
                        break;
                    }
                    Some(TcpControlMsg::Disconnect) => {
                        trace!(conn_id, ?addr, "received disconnect control message");
                        break;
                    }
                }
            },
            msg = read_message(conn_id, addr, message_id, &mut read_half) => {
                if rate_limiter.check().is_err() {
                    warn!(conn_id, ?addr, "rate limit exceeded");
                    break;
                }
                let Some(message) = msg else {
                    break;
                };
                let recv_msg = RecvTcpMsg {
                    src_addr: addr,
                    payload: message,
                };
                if let Err(err) = tcp_ingress_tx.send(recv_msg).await {
                    warn!(
                        conn_id,
                        ?addr,
                        message_id,
                        ?err,
                        "error queueing up received TCP message",
                    );
                    break;
                }
                message_id += 1;
            }
        }
    }
    tcp_control_map.unregister(&(addr.ip(), addr.port(), conn_id));
    trace!(
        conn_id,
        ?addr,
        "connection task ended, unregistered from control map"
    );
}

async fn read_message(
    conn_id: u64,
    addr: SocketAddr,
    message_id: u64,
    read_half: &mut TcpReadHalf,
) -> Option<Bytes> {
    let start_time = if enabled!(Level::DEBUG) {
        Some(Instant::now())
    } else {
        None
    };

    let header_bytes = BytesMut::with_capacity(std::mem::size_of::<TcpMsgHdr>());

    let header = match timeout(HEADER_TIMEOUT, read_half.read_exact(header_bytes)).await {
        Ok((ret, header_bytes)) => match ret {
            Ok(_len) => TcpMsgHdr::read_from_bytes(&header_bytes[..]).unwrap(),
            Err(err) => {
                if message_id == 0 || err.kind() != ErrorKind::UnexpectedEof {
                    debug!(
                        conn_id,
                        ?addr,
                        message_id,
                        ?err,
                        "error reading message header on TCP connection"
                    );
                } else {
                    trace!(conn_id, ?addr, "closing incoming TCP connection on EOF",);
                }
                return None;
            }
        },
        Err(_) => {
            warn!(
                conn_id,
                ?addr,
                message_id,
                "timeout while reading message header from TCP connection"
            );
            return None;
        }
    };

    let TcpMsgHdr {
        magic: header_magic,
        version: header_version,
        length: header_length,
    } = header;

    if header_magic.get() != HEADER_MAGIC {
        debug!(
            conn_id,
            ?addr,
            message_id,
            ?header,
            "received incorrect magic number on TCP connection"
        );
        return None;
    }
    if header_version.get() != HEADER_VERSION {
        debug!(
            conn_id,
            ?addr,
            message_id,
            ?header,
            "received incorrect version number on TCP connection"
        );
        return None;
    }

    let message_length: usize = header_length.get() as usize;

    if message_length > TCP_MESSAGE_LENGTH_LIMIT {
        debug!(
            conn_id,
            ?addr,
            message_id,
            ?header,
            "received header with oversized message length on TCP connection"
        );
        return None;
    }

    trace!(
        conn_id,
        ?addr,
        message_id,
        ?header,
        "received valid message header on TCP connection"
    );

    let message = BytesMut::with_capacity(message_length);

    let message = match timeout(
        message_timeout(message_length),
        read_half.read_exact(message),
    )
    .await
    {
        Ok((ret, message)) => match ret {
            Ok(_len) => message,
            Err(err) => {
                debug!(
                    conn_id,
                    ?addr,
                    message_id,
                    ?header,
                    ?err,
                    "error reading message body on TCP connection"
                );
                return None;
            }
        },
        Err(_) => {
            warn!(
                conn_id,
                ?addr,
                message_id,
                ?header,
                "timeout while reading message body from TCP connection"
            );
            return None;
        }
    };

    if let Some(start_time) = start_time {
        let duration = Instant::now() - start_time;

        let duration_ms = duration.as_millis();

        let bytes_per_second = {
            let bytes_received = std::mem::size_of::<TcpMsgHdr>() + message_length;
            let duration_f64 = duration.as_secs_f64();

            if duration_f64 >= 0.01 {
                (bytes_received as f64) / duration_f64
            } else {
                f64::NAN
            }
        };

        debug!(
            conn_id,
            ?addr,
            message_id,
            ?header,
            duration_ms,
            bytes_per_second,
            "received message on TCP connection"
        );
    }

    Some(message.freeze())
}
