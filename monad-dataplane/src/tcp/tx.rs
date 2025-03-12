use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, VecDeque},
    io::Error,
    net::SocketAddr,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    time::Instant,
};

use monoio::{io::AsyncWriteRentExt, net::TcpStream, spawn, time::timeout};
use tokio::sync::mpsc;
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::AsBytes;

use super::{message_timeout, TcpMsg, TcpMsgHdr};

const QUEUED_MESSAGE_WARN_LIMIT: usize = 10;

#[derive(Clone)]
struct TxState {
    inner: Rc<RefCell<TxStateInner>>,
}

impl TxState {
    fn new() -> TxState {
        let inner = Rc::new(RefCell::new(TxStateInner {
            conn_id: 0,
            messages: BTreeMap::new(),
        }));

        TxState { inner }
    }
}

struct TxStateInner {
    conn_id: u64,
    messages: BTreeMap<SocketAddr, VecDeque<TcpMsg>>,
}

pub async fn task(mut tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>) {
    let tx_state = TxState::new();

    while let Some((addr, msg)) = tcp_egress_rx.recv().await {
        debug!(?addr, len = msg.msg.len(), "queueing up TCP message");

        let peer_task_exists = {
            let mut inner_ref = tx_state.inner.borrow_mut();

            let entry = inner_ref.messages.entry(addr);

            let peer_task_exists = matches!(entry, Entry::Occupied(..));

            let queued_messages = entry.or_default();

            queued_messages.push_back(msg);

            if queued_messages.len() >= QUEUED_MESSAGE_WARN_LIMIT {
                warn!(
                    ?addr,
                    message_count = queued_messages.len(),
                    "excessive number of messages queued for peer"
                );
            }

            peer_task_exists
        };

        if !peer_task_exists {
            trace!(?addr, "spawning TCP tx task for peer");

            spawn(task_peer(tx_state.clone(), addr));
        }
    }
}

async fn task_peer(tx_state: TxState, addr: SocketAddr) {
    trace!(?addr, "starting TCP tx task for peer");

    loop {
        let (conn_id, message) = {
            let mut inner_ref = tx_state.inner.borrow_mut();

            let queued_messages = inner_ref.messages.get_mut(&addr).unwrap();

            if let Some(message) = queued_messages.pop_front() {
                if queued_messages.len() >= QUEUED_MESSAGE_WARN_LIMIT {
                    warn!(
                        ?addr,
                        message_count = queued_messages.len(),
                        "excessive number of messages queued for peer"
                    );
                }

                let conn_id = inner_ref.conn_id;
                inner_ref.conn_id += 1;

                (conn_id, message)
            } else {
                inner_ref.messages.remove(&addr);
                trace!(?addr, "exiting TCP tx task for peer");
                return;
            }
        };

        let len = message.msg.len();

        // TODO: When we experience a transmission failure, we should consider zapping
        // all outbound messages that are linked to this one (i.e. that are part of the
        // same (large, multi-message) blocksync or statesync response).
        if timeout(message_timeout(len), send_message(conn_id, addr, message))
            .await
            .is_err()
        {
            warn!(
                conn_id,
                ?addr,
                len,
                "timeout while writing message on TCP connection"
            );
        }
    }
}

async fn send_message(conn_id: u64, addr: SocketAddr, message: TcpMsg) {
    trace!(
        conn_id,
        ?addr,
        len = message.msg.len(),
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

            let connect_time = if enabled!(Level::DEBUG) {
                Some(Instant::now())
            } else {
                None
            };

            let raw_fd = stream.as_raw_fd();
            conn_cork(raw_fd, true);

            let message_len = message.msg.len();
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

            let (ret, _message) = stream.write_all(message.msg).await;

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

            if message
                .completion
                .is_some_and(|completion| completion.send(()).is_err())
            {
                warn!(
                    conn_id,
                    ?addr,
                    ?header,
                    "error sending completion for transmitted TCP message"
                );
            }

            if let Some(connect_time) = connect_time {
                let num_unacked_bytes = num_unacked_bytes(raw_fd);

                let duration = Instant::now() - connect_time;

                let duration_ms = duration.as_millis();

                let bytes_per_second = {
                    let bytes_acked =
                        std::mem::size_of::<TcpMsgHdr>() + message_len - num_unacked_bytes;
                    let duration_f64 = duration.as_secs_f64();

                    if duration_f64 >= 0.01 {
                        (bytes_acked as f64) / duration_f64
                    } else {
                        f64::NAN
                    }
                };

                debug!(
                    conn_id,
                    ?addr,
                    ?header,
                    num_unacked_bytes,
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

fn num_unacked_bytes(raw_fd: RawFd) -> usize {
    let mut outq: libc::c_int = 0;

    let r = unsafe { libc::ioctl(raw_fd, libc::TIOCOUTQ, &mut outq as *mut libc::c_int) };

    if r == 0 {
        outq as _
    } else {
        warn!("ioctl(TIOCOUTQ) failed with: {}", Error::last_os_error());
        0
    }
}
