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
    message_timeout, split_stream, RecvTcpMsg, TcpConnection, TcpControl, TcpMsgHdr, TcpRateLimit,
    TcpReadHalf, TcpWriteHalf, HEADER_MAGIC, HEADER_VERSION, TCP_MESSAGE_LENGTH_LIMIT,
};
use crate::{
    addrlist::{Addrlist, Status},
    metrics::{ActiveConnectionGuard, DataplaneMetrics},
    TcpSocketId,
};

pub(crate) type WriteHalfSender =
    mpsc::UnboundedSender<(TcpSocketId, SocketAddr, TcpWriteHalf, TcpConnection)>;
pub(crate) type ReadHalfReceiver =
    mpsc::UnboundedReceiver<(SocketAddr, TcpReadHalf, TcpConnection)>;

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct RxContext {
    pub(crate) socket_id: TcpSocketId,
    pub(crate) rate_limit: TcpRateLimit,
    pub(crate) tcp_control_map: TcpControl,
    pub(crate) tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
    pub(crate) tcp_disconnect_tx: mpsc::Sender<SocketAddr>,
    pub(crate) write_half_tx: WriteHalfSender,
    pub(crate) metrics: DataplaneMetrics,
}

#[derive(Clone)]
pub(crate) struct RxState {
    inner: Rc<RefCell<RxStateInner>>,
    addrlist: Arc<Addrlist>,
    metrics: DataplaneMetrics,
}

impl RxState {
    pub(crate) fn new(
        addrlist: Arc<Addrlist>,
        tcp_connections_limit: usize,
        tcp_per_ip_connections_limit: usize,
        metrics: DataplaneMetrics,
    ) -> RxState {
        let inner = Rc::new(RefCell::new(RxStateInner {
            tcp_connections_limit,
            tcp_per_ip_connections_limit,
            num_connections: 0,
            num_connections_per_ip: BTreeMap::new(),
        }));

        RxState {
            addrlist,
            inner,
            metrics,
        }
    }

    fn apply_limits(&self, ip: IpAddr) -> Result<ConnectionToken, ()> {
        let status = self.addrlist.status(&ip);
        match status {
            Status::Banned => {
                self.metrics.tcp_inbound_connections_rejected.inc();
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
                Ok(ConnectionToken::Trusted {
                    _active_connection: ActiveConnectionGuard::new(
                        &self.metrics.tcp_inbound_connections_accepted,
                        &self.metrics.tcp_current_inbound_connections,
                    ),
                })
            }
            Status::Unknown => {
                let mut inner_ref = self.inner.borrow_mut();
                if inner_ref.num_connections >= inner_ref.tcp_connections_limit {
                    self.metrics.tcp_inbound_connections_rejected.inc();
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
                        self.metrics.tcp_inbound_connections_rejected.inc();
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
                    _active_connection: ActiveConnectionGuard::new(
                        &self.metrics.tcp_inbound_connections_accepted,
                        &self.metrics.tcp_current_inbound_connections,
                    ),
                })
            }
        }
    }
}

enum ConnectionToken {
    Trusted {
        _active_connection: ActiveConnectionGuard,
    },
    Unknown {
        inner: Rc<RefCell<RxStateInner>>,
        ip: IpAddr,
        _active_connection: ActiveConnectionGuard,
    },
}

impl Drop for ConnectionToken {
    fn drop(&mut self) {
        match self {
            ConnectionToken::Trusted { .. } => {
                trace!("trusted connection dropped");
            }
            ConnectionToken::Unknown { inner, ip, .. } => {
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
    context: RxContext,
    rx_state: RxState,
    tcp_listener: TcpListener,
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
                                context.clone(),
                                conn_state,
                                conn_id,
                                addr,
                                tcp_stream,
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
                        context.metrics.tcp_receive_errors.inc();
                        warn!(conn_id, ?err, "error accepting tcp connection");
                    }
                }
                conn_id += 1;
            }
            read_half = read_half_rx.recv() => {
                let Some((addr, read_half, connection)) = read_half else {
                    break;
                };
                trace!(conn_id, ?addr, "received read half from tx");

                spawn(task_read(
                    context.clone(),
                    None,
                    conn_id,
                    addr,
                    read_half,
                    connection,
                ));
                conn_id += 1;
            }
        }
    }
}

async fn task_connection(
    context: RxContext,
    _rx_state: ConnectionToken,
    conn_id: u64,
    addr: SocketAddr,
    tcp_stream: TcpStream,
) {
    let (read_half, write_half) = split_stream(tcp_stream);
    let connection = TcpConnection::new();

    if let Err(err) =
        context
            .write_half_tx
            .send((context.socket_id, addr, write_half, connection.clone()))
    {
        warn!(conn_id, ?addr, ?err, "failed to send write half to tx task");
        return;
    }

    task_read(
        context,
        Some(_rx_state),
        conn_id,
        addr,
        read_half,
        connection,
    )
    .await;
}

async fn task_read(
    context: RxContext,
    _rx_state: Option<ConnectionToken>,
    conn_id: u64,
    addr: SocketAddr,
    mut read_half: TcpReadHalf,
    connection: TcpConnection,
) {
    let rate_limiter = context.rate_limit.new_rate_limiter();
    let tcp_id = (addr.ip(), addr.port(), context.socket_id, conn_id);
    context.tcp_control_map.register(tcp_id, connection.clone());
    let mut message_id: u64 = 0;
    loop {
        select! {
            biased;
            _ = connection.disconnected() => break,
            msg = read_message(conn_id, addr, message_id, &mut read_half, &context.metrics) => {
                let Some(message) = msg else {
                    break;
                };
                if rate_limiter.check().is_err() {
                    context.metrics.tcp_connections_rate_limited.inc();
                    warn!(conn_id, ?addr, "rate limit exceeded");
                    break;
                }
                let recv_msg = RecvTcpMsg {
                    src_addr: addr,
                    payload: message,
                };
                if let Err(err) = context.tcp_ingress_tx.send(recv_msg).await {
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
    connection.disconnect();
    context.tcp_control_map.unregister(&tcp_id);
    match context.tcp_disconnect_tx.try_send(addr) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            warn!(
                conn_id,
                ?addr,
                "tcp disconnect channel full, dropping event"
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
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
    metrics: &DataplaneMetrics,
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
                metrics.tcp_receive_errors.inc();
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
            metrics.tcp_receive_errors.inc();
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
        metrics.tcp_receive_errors.inc();
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
        metrics.tcp_receive_errors.inc();
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
        metrics.tcp_receive_errors.inc();
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
                metrics.tcp_receive_errors.inc();
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
            metrics.tcp_receive_errors.inc();
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

    metrics.tcp_messages_received.inc();
    metrics.tcp_bytes_received.add(message_length as u64);
    Some(message.freeze())
}
