use std::{io::ErrorKind, net::SocketAddr, time::Instant};

use bytes::{Bytes, BytesMut};
use monoio::{
    io::AsyncReadRentExt,
    net::{TcpListener, TcpStream},
    spawn,
    time::timeout,
};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};
use zerocopy::FromBytes;

use super::{
    TcpMsgHdr, HEADER_MAGIC, HEADER_VERSION, TCP_HEADER_TIMEOUT, TCP_MESSAGE_LENGTH_LIMIT,
    TCP_MESSAGE_TIMEOUT,
};

pub async fn task(local_addr: SocketAddr, tcp_ingress_tx: mpsc::Sender<(SocketAddr, Bytes)>) {
    let tcp_listener = TcpListener::bind(local_addr).unwrap();

    let mut conn_id: u64 = 0;

    loop {
        match tcp_listener.accept().await {
            Ok((tcp_stream, addr)) => {
                spawn(task_connection(
                    conn_id,
                    addr,
                    tcp_stream,
                    tcp_ingress_tx.clone(),
                ));
            }
            Err(err) => {
                warn!(conn_id, ?err, "error accepting TCP connection");
            }
        }

        conn_id += 1;
    }
}

pub async fn task_connection(
    conn_id: u64,
    addr: SocketAddr,
    mut tcp_stream: TcpStream,
    tcp_ingress_tx: mpsc::Sender<(SocketAddr, Bytes)>,
) {
    trace!(conn_id, ?addr, "accepted incoming TCP connection");

    let mut message_id: u64 = 0;

    while let Some(message) = read_message(conn_id, addr, message_id, &mut tcp_stream).await {
        if let Err(err) = tcp_ingress_tx.send((addr, message)).await {
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

async fn read_message(
    conn_id: u64,
    addr: SocketAddr,
    message_id: u64,
    tcp_stream: &mut TcpStream,
) -> Option<Bytes> {
    let start_time = Instant::now();

    let header_bytes = BytesMut::with_capacity(std::mem::size_of::<TcpMsgHdr>());

    let header = match timeout(TCP_HEADER_TIMEOUT, tcp_stream.read_exact(header_bytes)).await {
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

    let message = match timeout(TCP_MESSAGE_TIMEOUT, tcp_stream.read_exact(message)).await {
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

    Some(message.freeze())
}
