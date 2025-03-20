use std::{
    cell::RefCell,
    collections::BTreeMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    time::{Duration, Instant},
};

use monoio::{
    io::AsyncWriteRentExt,
    net::TcpStream,
    spawn,
    time::{sleep, timeout},
};
use tokio::sync::{mpsc, mpsc::error::TrySendError};
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::AsBytes;

use super::{message_timeout, TcpMsg, TcpMsgHdr};

// These are per-peer limits.
pub const QUEUED_MESSAGE_WARN_LIMIT: usize = 10;
pub const QUEUED_MESSAGE_LIMIT: usize = 20;

const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_FAILURE_LINGER_WAIT: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct TxState {
    inner: Rc<RefCell<TxStateInner>>,
}

impl TxState {
    fn new() -> TxState {
        let inner = Rc::new(RefCell::new(TxStateInner {
            peer_channels: BTreeMap::new(),
        }));

        TxState { inner }
    }

    fn push(&self, addr: &SocketAddr, msg: TcpMsg) -> Option<mpsc::Receiver<TcpMsg>> {
        let mut ret = None;

        let mut inner_ref = self.inner.borrow_mut();

        let msg_sender = inner_ref.peer_channels.entry(*addr).or_insert_with(|| {
            let (sender, receiver) = mpsc::channel(QUEUED_MESSAGE_LIMIT);

            ret = Some(receiver);

            sender
        });

        match msg_sender.try_send(msg) {
            Ok(()) => {
                let message_count = msg_sender.max_capacity() - msg_sender.capacity();

                if message_count >= QUEUED_MESSAGE_WARN_LIMIT {
                    warn!(
                        ?addr,
                        message_count, "excessive number of messages queued for peer"
                    );
                }
            }
            Err(TrySendError::Full(_)) => {
                warn!(
                    ?addr,
                    message_count = msg_sender.max_capacity(),
                    "peer message limit reached, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => {
                warn!(?addr, "channel unexpectedly closed");
            }
        }

        ret
    }

    fn exit(&self, addr: &SocketAddr) {
        self.inner.borrow_mut().peer_channels.remove(addr);
    }
}

struct TxStateInner {
    // There is a transmit connection task running for a given peer iff there is an
    // entry for the peer address in this map.
    peer_channels: BTreeMap<SocketAddr, mpsc::Sender<TcpMsg>>,
}

pub async fn task(mut tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>) {
    let tx_state = TxState::new();

    let mut conn_id: u64 = 0;

    while let Some((addr, msg)) = tcp_egress_rx.recv().await {
        debug!(?addr, len = msg.msg.len(), "queueing up TCP message");

        if let Some(msg_receiver) = tx_state.push(&addr, msg) {
            trace!(
                conn_id,
                ?addr,
                "spawning TCP transmit connection task for peer"
            );

            spawn(task_connection(
                tx_state.clone(),
                conn_id,
                addr,
                msg_receiver,
            ));

            conn_id += 1;
        }
    }
}

async fn task_connection(
    tx_state: TxState,
    conn_id: u64,
    addr: SocketAddr,
    mut msg_receiver: mpsc::Receiver<TcpMsg>,
) {
    trace!(
        conn_id,
        ?addr,
        "starting TCP transmit connection task for peer"
    );

    if let Err(err) = connect_and_send_messages(conn_id, &addr, &mut msg_receiver).await {
        let mut additional_messages_dropped = 0;

        // Throw away (and fail) all remaining queued messages immediately.
        while let Ok(_msg) = msg_receiver.try_recv() {
            additional_messages_dropped += 1;
        }

        warn!(
            conn_id,
            ?addr,
            ?err,
            additional_messages_dropped,
            "error transmitting TCP message"
        );

        // Sleep to avoid reconnecting too soon.
        sleep(TCP_FAILURE_LINGER_WAIT).await;
    }

    tx_state.exit(&addr);

    trace!(
        conn_id,
        ?addr,
        "exiting TCP transmit connection task for peer"
    );
}

async fn connect_and_send_messages(
    conn_id: u64,
    addr: &SocketAddr,
    msg_receiver: &mut mpsc::Receiver<TcpMsg>,
) -> Result<(), Error> {
    let mut stream = timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
        .map_err(|err| {
            Error::new(
                ErrorKind::Other,
                format!("error connecting to remote host: {}", err),
            )
        })?;

    trace!(conn_id, ?addr, "outbound TCP connection established");

    conn_cork(stream.as_raw_fd(), true);

    let mut message_id: u64 = 0;

    while let Ok(message) = msg_receiver.try_recv() {
        let message_count = msg_receiver.max_capacity() - msg_receiver.capacity();

        if message_count >= QUEUED_MESSAGE_WARN_LIMIT {
            warn!(
                ?addr,
                message_count, "excessive number of messages queued for peer"
            );
        }

        let len = message.msg.len();

        timeout(
            message_timeout(len),
            send_message(conn_id, addr, &mut stream, message_id, message),
        )
        .await
        .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
        .map_err(|err| {
            Error::new(
                ErrorKind::Other,
                format!(
                    "error writing message {} on TCP connection: {}",
                    message_id, err
                ),
            )
        })?;

        message_id += 1;
    }

    Ok(())
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

async fn send_message(
    conn_id: u64,
    addr: &SocketAddr,
    stream: &mut TcpStream,
    message_id: u64,
    message: TcpMsg,
) -> Result<(), Error> {
    trace!(
        conn_id,
        ?addr,
        message_id,
        len = message.msg.len(),
        "start transmission of TCP message"
    );

    let start = if enabled!(Level::DEBUG) {
        Some((Instant::now(), num_unacked_bytes(stream.as_raw_fd())))
    } else {
        None
    };

    let message_len = message.msg.len();

    let header = TcpMsgHdr::new(message_len as u64);

    let (ret, _header) = stream.write_all(Box::<[u8]>::from(header.as_bytes())).await;
    ret?;

    let (ret, _message) = stream.write_all(message.msg).await;
    ret?;

    if let Some((start_time, start_unacked_bytes)) = start {
        let end_unacked_bytes = num_unacked_bytes(stream.as_raw_fd());

        let duration = Instant::now() - start_time;

        let duration_ms = duration.as_millis();

        let bytes_per_second = {
            let bytes_acked = start_unacked_bytes + std::mem::size_of::<TcpMsgHdr>() + message_len
                - end_unacked_bytes;
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
            message_id,
            ?header,
            start_unacked_bytes,
            end_unacked_bytes,
            duration_ms,
            bytes_per_second,
            "successfully transmitted TCP message"
        );
    }

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

    Ok(())
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
