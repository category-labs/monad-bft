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
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use monoio::{
    io::AsyncReadRentExt,
    net::{TcpListener, TcpStream},
    spawn,
    time::timeout,
};
use tokio::sync::mpsc;
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::FromBytes;

use super::{
    message_timeout, RecvTcpMsg, TcpMsgHdr, HEADER_MAGIC, HEADER_VERSION, TCP_MESSAGE_LENGTH_LIMIT,
};

const PER_PEER_CONNECTION_LIMIT: usize = 100;

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct RxState {
    inner: Rc<RefCell<RxStateInner>>,
}

impl RxState {
    fn new() -> RxState {
        let inner = Rc::new(RefCell::new(RxStateInner {
            num_connections: BTreeMap::new(),
        }));

        RxState { inner }
    }

    fn check_new_connection(&self, ip: IpAddr) -> Result<(), ()> {
        let mut inner_ref = self.inner.borrow_mut();

        let count_ref = inner_ref.num_connections.entry(ip).or_insert(0);

        if *count_ref < PER_PEER_CONNECTION_LIMIT {
            *count_ref += 1;

            Ok(())
        } else {
            Err(())
        }
    }

    fn drop_connection(&self, ip: &IpAddr) {
        let mut inner_ref = self.inner.borrow_mut();

        let count_ref = inner_ref.num_connections.get_mut(ip).unwrap();

        if *count_ref > 1 {
            *count_ref -= 1;
        } else {
            inner_ref.num_connections.remove(ip);
        }
    }
}

struct RxStateInner {
    num_connections: BTreeMap<IpAddr, usize>,
}

pub async fn task(tcp_listener: TcpListener, tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>) {
    let rx_state = RxState::new();

    let mut conn_id: u64 = 0;

    loop {
        match tcp_listener.accept().await {
            Ok((tcp_stream, addr)) => match rx_state.check_new_connection(addr.ip()) {
                Ok(()) => {
                    trace!(conn_id, ?addr, "accepting incoming TCP connection");

                    spawn(task_connection(
                        rx_state.clone(),
                        conn_id,
                        addr,
                        tcp_stream,
                        tcp_ingress_tx.clone(),
                    ));
                }
                Err(()) => {
                    debug!(
                        conn_id,
                        ?addr,
                        "per-peer connection limit reached, rejecting TCP connection"
                    );
                }
            },
            Err(err) => {
                warn!(conn_id, ?err, "error accepting TCP connection");
            }
        }

        conn_id += 1;
    }
}

async fn task_connection(
    rx_state: RxState,
    conn_id: u64,
    addr: SocketAddr,
    mut tcp_stream: TcpStream,
    tcp_ingress_tx: mpsc::Sender<RecvTcpMsg>,
) {
    let mut message_id: u64 = 0;

    while let Some(message) = read_message(conn_id, addr, message_id, &mut tcp_stream).await {
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

    rx_state.drop_connection(&addr.ip());
}

async fn read_message(
    conn_id: u64,
    addr: SocketAddr,
    message_id: u64,
    tcp_stream: &mut TcpStream,
) -> Option<Bytes> {
    let start_time = if enabled!(Level::DEBUG) {
        Some(Instant::now())
    } else {
        None
    };

    let header_bytes = BytesMut::with_capacity(std::mem::size_of::<TcpMsgHdr>());

    let header = match timeout(HEADER_TIMEOUT, tcp_stream.read_exact(header_bytes)).await {
        Ok((ret, header_bytes)) => match ret {
            Ok(_len) => TcpMsgHdr::read_from(&header_bytes[..]).unwrap(),
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
        tcp_stream.read_exact(message),
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
