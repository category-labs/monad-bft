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
    net::{TcpListener, TcpStream},
    spawn,
    time::timeout,
};
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::FromBytes;

use super::{
    conn::{ConnectionRegistration, ConnectionRegistry},
    message_timeout, task_connection,
    tx::{drop_queued_messages, BoundedQueueReceiver},
    ConnectionContext, ConnectionIdle, IoState, RecvTcpMsg, TcpConnectionGuard, TcpMsgHdr,
    TcpReadHalf, HEADER_MAGIC, HEADER_VERSION, TCP_MESSAGE_LENGTH_LIMIT,
};
use crate::{
    addrlist::{Addrlist, Status},
    metrics::{ActiveConnectionGuard, DataplaneMetrics},
};

// Finish the remaining header within this deadline after its first byte arrives.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct AcceptLimit {
    inner: Rc<RefCell<AcceptLimitInner>>,
    addrlist: Arc<Addrlist>,
    metrics: DataplaneMetrics,
}

impl AcceptLimit {
    pub(crate) fn new(
        addrlist: Arc<Addrlist>,
        tcp_connections_limit: usize,
        tcp_per_ip_connections_limit: usize,
        metrics: DataplaneMetrics,
    ) -> Self {
        Self {
            inner: Rc::new(RefCell::new(AcceptLimitInner {
                tcp_connections_limit,
                tcp_per_ip_connections_limit,
                num_connections: 0,
                num_connections_per_ip: BTreeMap::new(),
            })),
            addrlist,
            metrics,
        }
    }

    fn try_acquire(&self, ip: IpAddr) -> Result<AcceptPermit, ()> {
        let quota = match self.addrlist.status(&ip) {
            Status::Banned => {
                self.metrics.tcp_inbound_connections_rejected.inc();
                warn!(?ip, "banned address attempting connection, dropping");
                return Err(());
            }
            Status::Trusted => {
                trace!(?ip, "trusted peer connection accepted");
                AcceptQuota::Exempt
            }
            Status::Unknown => {
                let mut inner = self.inner.borrow_mut();
                if inner.num_connections >= inner.tcp_connections_limit {
                    self.metrics.tcp_inbound_connections_rejected.inc();
                    debug!(
                        ?ip,
                        total_connections = inner.num_connections,
                        connection_limit = inner.tcp_connections_limit,
                        "total connection limit reached, dropping"
                    );
                    return Err(());
                }
                let per_ip_limit = inner.tcp_per_ip_connections_limit;
                let count = inner.num_connections_per_ip.entry(ip).or_insert(0);
                if *count >= per_ip_limit {
                    self.metrics.tcp_inbound_connections_rejected.inc();
                    debug!(
                        ?ip,
                        ip_connections = *count,
                        per_ip_limit,
                        "per-ip connection limit reached, dropping"
                    );
                    return Err(());
                }
                *count += 1;
                inner.num_connections += 1;
                trace!(
                    ?ip,
                    total_connections = inner.num_connections,
                    ip_connections = inner.num_connections_per_ip.get(&ip),
                    "unknown peer connection accepted"
                );
                AcceptQuota::Counted
            }
        };
        Ok(AcceptPermit {
            limit: self.clone(),
            ip,
            quota,
            _active_connection: ActiveConnectionGuard::new(
                &self.metrics.tcp_inbound_connections_accepted,
                &self.metrics.tcp_current_inbound_connections,
            ),
        })
    }
}

#[derive(Clone, Copy)]
enum AcceptQuota {
    Counted,
    Exempt,
}

struct AcceptPermit {
    limit: AcceptLimit,
    ip: IpAddr,
    quota: AcceptQuota,
    _active_connection: ActiveConnectionGuard,
}

impl AcceptPermit {
    fn is_banned(&self) -> bool {
        self.limit.addrlist.status(&self.ip) == Status::Banned
    }
}

impl Drop for AcceptPermit {
    fn drop(&mut self) {
        if matches!(self.quota, AcceptQuota::Exempt) {
            trace!("trusted connection dropped");
            return;
        }
        let mut inner = self.limit.inner.borrow_mut();
        inner.num_connections -= 1;
        if let Some(count) = inner.num_connections_per_ip.get_mut(&self.ip) {
            if *count > 1 {
                *count -= 1;
            } else {
                inner.num_connections_per_ip.remove(&self.ip);
            }
        } else {
            warn!(ip = ?self.ip, "num_connections_per_ip should not be empty");
        }
    }
}

struct AcceptLimitInner {
    tcp_connections_limit: usize,
    tcp_per_ip_connections_limit: usize,
    num_connections: usize,
    num_connections_per_ip: BTreeMap<IpAddr, usize>,
}

pub(crate) async fn task(
    context: ConnectionContext,
    accept_limit: AcceptLimit,
    registry: ConnectionRegistry,
    tcp_listener: TcpListener,
) {
    loop {
        match tcp_listener.accept().await {
            Ok((stream, addr)) => match accept_limit.try_acquire(addr.ip()) {
                Ok(permit) => {
                    // Register the send queue before receiving any messages, so
                    // replies immediately reuse this accepted connection.
                    if let Some((receiver, registration)) =
                        registry.register((context.socket_id, addr))
                    {
                        spawn(task_accepted_connection(
                            context.clone(),
                            addr,
                            stream,
                            receiver,
                            registration,
                            permit,
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

async fn task_accepted_connection(
    context: ConnectionContext,
    addr: SocketAddr,
    stream: TcpStream,
    mut msg_receiver: BoundedQueueReceiver,
    registration: ConnectionRegistration,
    permit: AcceptPermit,
) {
    let conn_id = registration.conn_id;
    let connection = TcpConnectionGuard::new(context.tcp_control_map.clone(), addr, conn_id);
    // A ban can arrive after admission but before this task registers for disconnects.
    if permit.is_banned() {
        context.metrics.tcp_inbound_connections_rejected.inc();
        debug!(
            conn_id,
            ?addr,
            "banned address, dropping accepted tcp connection"
        );
        drop_queued_messages(&mut msg_receiver, &context.metrics);
        return;
    }
    if let Err(err) = task_connection(&context, addr, stream, &mut msg_receiver, &connection).await
    {
        warn!(conn_id, ?addr, ?err, "error in tcp connection task");
    }
    drop_queued_messages(&mut msg_receiver, &context.metrics);
}

pub(crate) async fn read_messages(
    context: &ConnectionContext,
    conn_id: u64,
    addr: SocketAddr,
    read_half: &mut TcpReadHalf,
    idle: &ConnectionIdle,
) {
    let rate_limiter = context.rate_limit.new_rate_limiter();
    let mut message_id = 0;
    while let Some(message) =
        read_message(conn_id, addr, message_id, read_half, &context.metrics, idle).await
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
    idle: &ConnectionIdle,
) -> Option<Bytes> {
    let start_time = if enabled!(Level::DEBUG) {
        Some(Instant::now())
    } else {
        None
    };

    let header_size = std::mem::size_of::<TcpMsgHdr>();
    let header_bytes = BytesMut::with_capacity(header_size);
    // Waiting for the first byte uses the shared idle check. The remaining
    // header and body have separate fixed deadlines; the body deadline starts
    // after the header completes, regardless of TX activity.
    idle.set_rx_state(IoState::Idle);
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
    idle.set_rx_state(IoState::Busy);
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
