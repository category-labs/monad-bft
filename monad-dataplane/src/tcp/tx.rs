use std::{
    io::Error,
    net::SocketAddr,
    os::fd::{AsRawFd, RawFd},
    time::{Duration, Instant},
};

use bytes::Bytes;
use monoio::{io::AsyncWriteRentExt, net::TcpStream, select, spawn, time::sleep};
use tokio::sync::mpsc;
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::AsBytes;

use super::{TcpMsgHdr, TCP_MESSAGE_TIMEOUT};

pub async fn task(mut tcp_egress_rx: mpsc::Receiver<(SocketAddr, Bytes)>) {
    let mut conn_id: u64 = 0;

    while let Some((addr, bytes)) = tcp_egress_rx.recv().await {
        spawn(send_message_with_timeout(conn_id, addr, bytes));

        conn_id += 1;
    }
}

async fn send_message_with_timeout(conn_id: u64, addr: SocketAddr, message: Bytes) {
    let message_len = message.len();

    select! {
        _ = send_message(conn_id, addr, message) => { },
        _ = sleep(TCP_MESSAGE_TIMEOUT) => {
            warn!(
                conn_id,
                ?addr,
                message_len,
                "timeout while writing message on TCP connection"
            );
        }
    }
}

async fn send_message(conn_id: u64, addr: SocketAddr, message: Bytes) {
    trace!(
        conn_id,
        ?addr,
        len = message.len(),
        "start transmission of TCP message"
    );

    match TcpStream::connect(addr).await {
        Err(err) => {
            debug!(
                conn_id,
                ?addr,
                ?err,
                "error connecting to remote host while sending TCP message"
            );
        }
        Ok(mut stream) => {
            trace!(conn_id, ?addr, "outbound TCP connection established");

            let connect_time = Instant::now();

            let raw_fd = stream.as_raw_fd();
            conn_cork(raw_fd, true);

            let message_len = message.len();
            let header = TcpMsgHdr::new(message_len as u64);

            let (ret, _header) = stream.write_all(Box::<[u8]>::from(header.as_bytes())).await;

            if let Err(err) = ret {
                debug!(
                    conn_id,
                    ?addr,
                    ?header,
                    ?err,
                    "error writing message header on TCP connection"
                );
                return;
            }

            let (ret, _message) = stream.write_all(message).await;

            if let Err(err) = ret {
                debug!(
                    conn_id,
                    ?addr,
                    ?header,
                    ?err,
                    "error writing message on TCP connection"
                );
                return;
            }

            conn_cork(raw_fd, false);

            wait_drain(raw_fd).await;

            if enabled!(Level::DEBUG) {
                let duration = Instant::now() - connect_time;

                let duration_ms = duration.as_millis();

                let bytes_per_second = {
                    let bytes_sent = std::mem::size_of::<TcpMsgHdr>() + message_len;
                    let duration_f64 = duration.as_secs_f64();

                    if duration_f64 >= 0.01 {
                        (bytes_sent as f64) / duration_f64
                    } else {
                        f64::NAN
                    }
                };

                debug!(
                    conn_id,
                    ?addr,
                    ?header,
                    duration_ms,
                    bytes_per_second,
                    "successfully transmitted TCP message"
                );
            }
        }
    }
}

fn conn_cork(raw_fd: RawFd, cork_flag: bool) {
    let r = unsafe {
        let cork_flag: libc::c_int = if cork_flag { 1 } else { 0 };

        libc::setsockopt(
            raw_fd,
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

async fn wait_drain(raw_fd: RawFd) {
    // This won't loop forever, as task_peer() runs a parallel TCP_MESSAGE_TIMEOUT timeout.
    loop {
        let mut outq: libc::c_int = 0;

        let r = unsafe { libc::ioctl(raw_fd, libc::TIOCOUTQ, &mut outq as *mut libc::c_int) };

        if r != 0 {
            warn!("ioctl(TIOCOUTQ) failed with: {}", Error::last_os_error());
            return;
        }

        if outq == 0 {
            return;
        }

        sleep(Duration::from_millis(1)).await;
    }
}
