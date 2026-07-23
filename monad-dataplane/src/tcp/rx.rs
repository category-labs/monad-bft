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
    buf::IoBufMut,
    io::{AsyncReadRent, AsyncReadRentExt},
    net::TcpListener,
    spawn,
    time::timeout,
};
use tokio::sync::mpsc;
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::FromBytes;

use super::{
    message_timeout,
    tx::{self, RegisterResult, TxState},
    ConnectionActivity, RecvTcpMsg, TcpControl, TcpMsgHdr, TcpRateLimit, TcpReadHalf, HEADER_MAGIC,
    HEADER_VERSION, TCP_MESSAGE_LENGTH_LIMIT,
};
use crate::{
    addrlist::{Addrlist, Status},
    metrics::{ActiveConnectionGuard, DataplaneMetrics},
    TcpSocketId,
};

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct RxContext {
    pub(crate) socket_id: TcpSocketId,
    pub(crate) rate_limit: TcpRateLimit,
    pub(crate) tcp_control_map: TcpControl,
    pub(crate) tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
    pub(crate) tcp_disconnect_tx: mpsc::Sender<SocketAddr>,
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
                Ok(ConnectionToken(ConnectionTokenInner::Trusted {
                    _active_connection: ActiveConnectionGuard::new(
                        &self.metrics.tcp_inbound_connections_accepted,
                        &self.metrics.tcp_current_inbound_connections,
                    ),
                }))
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
                Ok(ConnectionToken(ConnectionTokenInner::Unknown {
                    inner: self.inner.clone(),
                    ip,
                    _active_connection: ActiveConnectionGuard::new(
                        &self.metrics.tcp_inbound_connections_accepted,
                        &self.metrics.tcp_current_inbound_connections,
                    ),
                }))
            }
        }
    }
}

pub(crate) struct ConnectionToken(ConnectionTokenInner);

enum ConnectionTokenInner {
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
        match &self.0 {
            ConnectionTokenInner::Trusted { .. } => {
                trace!("trusted connection dropped");
            }
            ConnectionTokenInner::Unknown { inner, ip, .. } => {
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
    tx_state: TxState,
    tcp_listener: TcpListener,
) {
    loop {
        match tcp_listener.accept().await {
            Ok((stream, addr)) => match rx_state.apply_limits(addr.ip()) {
                Ok(conn_state) => {
                    // Register the send queue before receiving any messages, so
                    // replies immediately reuse this accepted connection.
                    if let RegisterResult::New(receiver, peer_handle) =
                        tx_state.register((context.socket_id, addr), false)
                    {
                        spawn(tx::task_connection(
                            context.clone(),
                            addr,
                            Some(stream),
                            receiver,
                            peer_handle,
                            Some(conn_state),
                        ));
                    } else {
                        warn!(?addr, "accepted connection for already registered peer");
                    }
                }
                Err(()) => debug!(?addr, "connection limit reached, rejecting tcp connection"),
            },
            Err(err) => {
                context.metrics.tcp_receive_errors.inc();
                warn!(?err, "error accepting tcp connection");
            }
        }
    }
}

pub(crate) async fn read_messages(
    context: &RxContext,
    conn_id: u64,
    addr: SocketAddr,
    read_half: &mut TcpReadHalf,
    activity: &ConnectionActivity,
) {
    let rate_limiter = context.rate_limit.new_rate_limiter();
    let mut message_id = 0;
    while let Some(message) = read_message(
        conn_id,
        addr,
        message_id,
        read_half,
        &context.metrics,
        activity,
    )
    .await
    {
        if rate_limiter.check().is_err() {
            context.metrics.tcp_connections_rate_limited.inc();
            warn!(conn_id, ?addr, "rate limit exceeded");
            break;
        }
        if let Err(err) = context
            .tcp_ingress_tx
            .send(RecvTcpMsg {
                src_addr: addr,
                payload: message,
            })
            .await
        {
            warn!(
                conn_id,
                ?addr,
                message_id,
                ?err,
                "error queueing up received TCP message"
            );
            break;
        }
        message_id += 1;
    }
}

async fn read_message(
    conn_id: u64,
    addr: SocketAddr,
    message_id: u64,
    read_half: &mut TcpReadHalf,
    metrics: &DataplaneMetrics,
    activity: &ConnectionActivity,
) -> Option<Bytes> {
    let start_time = if enabled!(Level::DEBUG) {
        Some(Instant::now())
    } else {
        None
    };

    let header_size = std::mem::size_of::<TcpMsgHdr>();
    let header_bytes = BytesMut::with_capacity(header_size);
    // Waiting for a new frame is governed by activity in either direction.
    // Once bytes arrive, completing this frame has its own bounded deadline.
    let (ret, header_bytes) = read_half.read(header_bytes).await;
    let header_len = match ret {
        Ok(len) if len > 0 => len,
        result => {
            let err = result
                .err()
                .unwrap_or_else(|| ErrorKind::UnexpectedEof.into());
            if message_id == 0 || err.kind() != ErrorKind::UnexpectedEof {
                metrics.tcp_receive_errors.inc();
                debug!(
                    conn_id,
                    ?addr,
                    message_id,
                    ?err,
                    "error reading message header on TCP connection"
                );
            } else {
                trace!(conn_id, ?addr, "closing incoming TCP connection on EOF");
            }
            return None;
        }
    };
    let _transfer = activity.transferring();
    let header = match timeout(
        HEADER_TIMEOUT,
        read_half.read_exact(header_bytes.slice_mut(header_len..header_size)),
    )
    .await
    {
        Ok((Ok(_), header_bytes)) => {
            TcpMsgHdr::read_from_bytes(&header_bytes.into_inner()[..]).unwrap()
        }
        Ok((Err(err), _)) => {
            metrics.tcp_receive_errors.inc();
            debug!(
                conn_id,
                ?addr,
                message_id,
                ?err,
                "error reading message header on TCP connection"
            );
            return None;
        }
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
