use std::{
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::fd::AsRawFd,
    pin::Pin,
    sync::Arc,
    task::Poll,
    thread,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::{task::AtomicWaker, StreamExt};
use rtrb::{PopError, RingBuffer};
use socket2::{Domain, Protocol, Socket, Type};
use tracing::{debug, trace, warn};
use zerocopy::{
    byteorder::little_endian::{U32, U64},
    AsBytes, FromBytes,
};

use crate::{
    epoll::{EpollFd, EpollToken, EventFd, RawFd, TimerFd},
    network::NetworkSocket,
};

const NUM_EPOLL_EVENTS: usize = 20;

const UDP_RX_EVENT: EpollToken = EpollToken(0);
const UDP_TX_EVENT: EpollToken = EpollToken(1);

const TCP_CONNECTION_RECEIVED: EpollToken = EpollToken(2);

const TCP_INCOMING_MAX_CONNECTIONS: usize = 100;

const TCP_INCOMING_CONNECTION_DATA_FIRST_TOKEN: u64 = 3;
const TCP_INCOMING_CONNECTION_DATA_LAST_TOKEN: u64 =
    TCP_INCOMING_CONNECTION_DATA_FIRST_TOKEN + (TCP_INCOMING_MAX_CONNECTIONS as u64) - 1;

const TCP_INCOMING_CONNECTION_TIMER_FIRST_TOKEN: u64 = TCP_INCOMING_CONNECTION_DATA_LAST_TOKEN + 1;
const TCP_INCOMING_CONNECTION_TIMER_LAST_TOKEN: u64 =
    TCP_INCOMING_CONNECTION_TIMER_FIRST_TOKEN + (TCP_INCOMING_MAX_CONNECTIONS as u64) - 1;

const TCP_OUTGOING_MAX_CONNECTIONS: usize = 100;

const TCP_OUTGOING_CONNECTION_DATA_FIRST_TOKEN: u64 = TCP_INCOMING_CONNECTION_TIMER_LAST_TOKEN + 1;
const TCP_OUTGOING_CONNECTION_DATA_LAST_TOKEN: u64 =
    TCP_OUTGOING_CONNECTION_DATA_FIRST_TOKEN + (TCP_OUTGOING_MAX_CONNECTIONS as u64) - 1;

const TCP_OUTGOING_CONNECTION_TIMER_FIRST_TOKEN: u64 = TCP_OUTGOING_CONNECTION_DATA_LAST_TOKEN + 1;
const TCP_OUTGOING_CONNECTION_TIMER_LAST_TOKEN: u64 =
    TCP_OUTGOING_CONNECTION_TIMER_FIRST_TOKEN + (TCP_OUTGOING_MAX_CONNECTIONS as u64) - 1;

const TCP_MESSAGE_LENGTH_LIMIT: usize = 1 * 1024 * 1024 * 1024;

const TCP_HEADER_TIMEOUT: Duration = Duration::from_secs(2);
const TCP_MESSAGE_TIMEOUT: Duration = Duration::from_secs(60);

/// Send the same payload to multiple destinations
#[derive(Clone)]
pub struct BroadcastMsg {
    pub targets: Vec<SocketAddr>,
    pub payload: Bytes,
}

/// Send a list of unicast payloads
#[derive(Clone)]
pub struct UnicastMsg {
    pub msgs: Vec<(SocketAddr, Bytes)>,
}

#[derive(Clone)]
pub struct RecvMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub stride: usize,
}

pub struct Dataplane {
    efd: EventFd,

    /// Ingress refers to the direction of Network traffic (incoming network traffic)
    /// Ingress sender/receiver channel is used to deliver the ingress network traffic to the
    /// application
    udp_ingress_receiver: WakeableConsumer<RecvMsg>,
    tcp_ingress_receiver: WakeableConsumer<(SocketAddr, Bytes)>,

    /// Egress refers to the direction of Network traffic (outgoing network traffic)
    /// Egress sender/receiver channel is used to deliver data from the Application to this struct
    /// which will then send it out to the network
    egress_bcast_sender: rtrb::Producer<BroadcastMsg>,
    egress_ucast_sender: rtrb::Producer<UnicastMsg>,
    egress_tcp_sender: rtrb::Producer<(SocketAddr, Bytes)>,
}

struct IncomingConnection {
    stream: TcpStream,
    state: IncomingConnectionState,
}

enum IncomingConnectionState {
    ReadingHeader {
        header: TcpMsgHdr,
        bytes_received: usize,
    },
    ReadingMessage {
        message: BytesMut,
        bytes_received: usize,
    },
    TimedOut,
}

impl IncomingConnection {
    fn new(stream: TcpStream) -> IncomingConnection {
        IncomingConnection {
            stream,
            state: IncomingConnectionState::ReadingHeader {
                header: TcpMsgHdr::zeroed(),
                bytes_received: 0,
            },
        }
    }
}

struct OutgoingConnection {
    msg: Bytes,
    stream: TcpStream,
    state: OutgoingConnectionState,
}

enum OutgoingConnectionState {
    Connecting,
    WritingHeader { bytes_written: usize },
    WritingMessage { bytes_written: usize },
    TimedOut,
}

impl OutgoingConnection {
    fn new_connecting(msg: Bytes, stream: TcpStream) -> OutgoingConnection {
        OutgoingConnection {
            msg,
            stream,
            state: OutgoingConnectionState::Connecting,
        }
    }

    fn new_connected(msg: Bytes, stream: TcpStream) -> OutgoingConnection {
        OutgoingConnection {
            msg,
            stream,
            state: OutgoingConnectionState::WritingHeader { bytes_written: 0 },
        }
    }
}

pub const HEADER_MAGIC: u32 = 0x434e5353; // "SSNC"
pub const HEADER_VERSION: u32 = 1;

#[derive(AsBytes, Debug, FromBytes)]
#[repr(C)]
pub struct TcpMsgHdr {
    pub magic: U32,
    pub version: U32,
    pub length: U64,
}

impl TcpMsgHdr {
    pub fn zeroed() -> TcpMsgHdr {
        TcpMsgHdr {
            magic: U32::new(0),
            version: U32::new(0),
            length: U64::new(0),
        }
    }

    pub fn new(length: u64) -> TcpMsgHdr {
        TcpMsgHdr {
            magic: U32::new(HEADER_MAGIC),
            version: U32::new(HEADER_VERSION),
            length: U64::new(length),
        }
    }
}

fn make_producer_consumer<T>(size: usize) -> (WakeableProducer<T>, WakeableConsumer<T>) {
    let (ing_send, ing_recv) = RingBuffer::<T>::new(size);
    let n = Notifier::new();
    let ing_producer = WakeableProducer {
        producer: ing_send,
        notify: n.clone(),
    };
    let ing_consumer = WakeableConsumer {
        consumer: ing_recv,
        notify: n,
    };
    (ing_producer, ing_consumer)
}

impl Dataplane {
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    pub fn new(local_addr: &str, up_bandwidth_mbps: u64) -> Self {
        let (udp_ing_producer, udp_ing_consumer) = make_producer_consumer(12_800);
        let (tcp_ing_producer, tcp_ing_consumer) = make_producer_consumer(32);

        let (egr_bcast_send, egr_bcast_recv) = RingBuffer::<BroadcastMsg>::new(2048);
        let (egr_ucast_send, egr_ucast_recv) = RingBuffer::<UnicastMsg>::new(1024);
        let (egr_tcp_send, egr_tcp_recv) = RingBuffer::<(SocketAddr, Bytes)>::new(1024);

        let efd = EventFd::new().unwrap();

        let tcp_listening_socket = TcpListener::bind(local_addr).unwrap();

        let tcp_incoming_connection_timers: [TimerFd; TCP_INCOMING_MAX_CONNECTIONS] =
            core::array::from_fn(|_| TimerFd::new().unwrap());

        let tcp_outgoing_connection_timers: [TimerFd; TCP_INCOMING_MAX_CONNECTIONS] =
            core::array::from_fn(|_| TimerFd::new().unwrap());

        DataplaneEventLoop {
            efd,
            epoll: EpollFd::<NUM_EPOLL_EVENTS>::new().unwrap(),

            udp_ingress_sender: udp_ing_producer,
            tcp_ingress_sender: tcp_ing_producer,

            egress_bcast_receiver: egr_bcast_recv,
            egress_ucast_receiver: egr_ucast_recv,
            egress_tcp_receiver: egr_tcp_recv,
            udp_socket: NetworkSocket::new(local_addr, up_bandwidth_mbps),

            tcp_listening_socket,
            tcp_incoming_connections: [const { None }; TCP_INCOMING_MAX_CONNECTIONS],
            tcp_incoming_connection_timers,

            tcp_outgoing_connections: [const { None }; TCP_OUTGOING_MAX_CONNECTIONS],
            tcp_outgoing_connection_timers,
        }
        .start();

        Self {
            efd,
            udp_ingress_receiver: udp_ing_consumer,
            tcp_ingress_receiver: tcp_ing_consumer,
            egress_bcast_sender: egr_bcast_send,
            egress_ucast_sender: egr_ucast_send,
            egress_tcp_sender: egr_tcp_send,
        }
    }

    pub fn udp_write_broadcast(&mut self, msg: BroadcastMsg) {
        match self.egress_bcast_sender.push(msg) {
            Err(e) => {
                warn!(?e, "bcast egress channel sender error");
            }
            Ok(()) => {
                self.efd.trigger_event(1).unwrap();
            }
        }
    }

    pub fn udp_write_unicast(&mut self, msg: UnicastMsg) {
        match self.egress_ucast_sender.push(msg) {
            Err(e) => {
                warn!(?e, "ucast egress channel sender error");
            }
            Ok(()) => {
                self.efd.trigger_event(1).unwrap();
            }
        }
    }

    pub fn tcp_write(&mut self, addr: SocketAddr, msg: Bytes) {
        match self.egress_tcp_sender.push((addr, msg)) {
            Err(e) => {
                warn!(?e, "tcp egress channel sender error {e}");
            }
            Ok(()) => {
                self.efd.trigger_event(1).unwrap();
            }
        }
    }

    pub async fn udp_read(&mut self) -> RecvMsg {
        self.udp_ingress_receiver
            .next()
            .await
            .expect("udp_ingress_sender never dropped")
    }

    pub async fn tcp_read(&mut self) -> (SocketAddr, Bytes) {
        self.tcp_ingress_receiver
            .next()
            .await
            .expect("tcp_ingress_sender never dropped")
    }
}

struct DataplaneEventLoop {
    epoll: EpollFd<NUM_EPOLL_EVENTS>,
    efd: EventFd,

    udp_ingress_sender: WakeableProducer<RecvMsg>,
    tcp_ingress_sender: WakeableProducer<(SocketAddr, Bytes)>,

    egress_bcast_receiver: rtrb::Consumer<BroadcastMsg>,
    egress_ucast_receiver: rtrb::Consumer<UnicastMsg>,
    egress_tcp_receiver: rtrb::Consumer<(SocketAddr, Bytes)>,

    udp_socket: NetworkSocket<'static>,

    tcp_listening_socket: TcpListener,
    tcp_incoming_connections: [Option<IncomingConnection>; TCP_INCOMING_MAX_CONNECTIONS],
    tcp_incoming_connection_timers: [TimerFd; TCP_INCOMING_MAX_CONNECTIONS],

    tcp_outgoing_connections: [Option<OutgoingConnection>; TCP_OUTGOING_MAX_CONNECTIONS],
    tcp_outgoing_connection_timers: [TimerFd; TCP_OUTGOING_MAX_CONNECTIONS],
}

impl DataplaneEventLoop {
    fn start(mut self) {
        thread::spawn(move || {
            self.tcp_listening_socket
                .set_nonblocking(true)
                .expect("set nonblocking");

            self.epoll
                .register(UDP_RX_EVENT, RawFd(self.udp_socket.socket.as_raw_fd()))
                .expect("epoll register of udp must succeed");
            self.epoll
                .register(UDP_TX_EVENT, self.efd.get_fd())
                .expect("epoll register of eventfd must succeed");
            self.epoll
                .register(
                    TCP_CONNECTION_RECEIVED,
                    RawFd(self.tcp_listening_socket.as_raw_fd()),
                )
                .unwrap();

            for i in 0..TCP_INCOMING_MAX_CONNECTIONS {
                self.epoll
                    .register(
                        Self::tcp_incoming_slot_timer_epoll_token(i),
                        self.tcp_incoming_connection_timers[i].get_fd(),
                    )
                    .unwrap();
            }

            for i in 0..TCP_OUTGOING_MAX_CONNECTIONS {
                self.epoll
                    .register(
                        Self::tcp_outgoing_slot_timer_epoll_token(i),
                        self.tcp_outgoing_connection_timers[i].get_fd(),
                    )
                    .unwrap();
            }

            'epoll_loop: loop {
                // this blocks until something happens
                let polled_events = self.epoll.wait().unwrap();

                if polled_events.is_empty() {
                    // if a timeout was configured in the call to epoll_wait,
                    // then we can get here if there were no events ready at expiration
                    // if there was no timeout configured, its possible to reach here from spurious
                    // wakeups
                    continue 'epoll_loop;
                }

                for e in polled_events {
                    'handle_event: loop {
                        if e.token() == UDP_RX_EVENT {
                            match self.handle_rx() {
                                Some(_) => continue,
                                None => break 'handle_event,
                            }
                        }

                        if e.token() == UDP_TX_EVENT {
                            self.handle_tx();
                            break 'handle_event;
                        }

                        if e.token() == TCP_CONNECTION_RECEIVED {
                            self.tcp_incoming_new_connection();
                            break 'handle_event;
                        }

                        if e.token().0 >= TCP_INCOMING_CONNECTION_DATA_FIRST_TOKEN
                            && e.token().0 <= TCP_INCOMING_CONNECTION_DATA_LAST_TOKEN
                        {
                            let slot = e.token().0 - TCP_INCOMING_CONNECTION_DATA_FIRST_TOKEN;
                            self.tcp_incoming_slot_pollin(slot as _);

                            break 'handle_event;
                        }

                        if e.token().0 >= TCP_INCOMING_CONNECTION_TIMER_FIRST_TOKEN
                            && e.token().0 <= TCP_INCOMING_CONNECTION_TIMER_LAST_TOKEN
                        {
                            let slot = e.token().0 - TCP_INCOMING_CONNECTION_TIMER_FIRST_TOKEN;
                            self.tcp_incoming_slot_timer(slot as _);

                            break 'handle_event;
                        }

                        if e.token().0 >= TCP_OUTGOING_CONNECTION_DATA_FIRST_TOKEN
                            && e.token().0 <= TCP_OUTGOING_CONNECTION_DATA_LAST_TOKEN
                        {
                            let slot = e.token().0 - TCP_OUTGOING_CONNECTION_DATA_FIRST_TOKEN;
                            self.tcp_outgoing_slot_pollout(slot as _);

                            break 'handle_event;
                        }

                        if e.token().0 >= TCP_OUTGOING_CONNECTION_TIMER_FIRST_TOKEN
                            && e.token().0 <= TCP_OUTGOING_CONNECTION_TIMER_LAST_TOKEN
                        {
                            let slot = e.token().0 - TCP_OUTGOING_CONNECTION_TIMER_FIRST_TOKEN;
                            self.tcp_outgoing_slot_timer(slot as _);

                            break 'handle_event;
                        }
                    }
                }
            }
        });
    }

    fn handle_rx(&mut self) -> Option<usize> {
        let recvs = match self.udp_socket.recvmmsg() {
            Some(r) => r,
            None => {
                return None;
            }
        };

        let mut total_recv_bytes = 0;

        for (i, result) in recvs.into_iter().enumerate() {
            let len = result.len;
            total_recv_bytes += len;
            let src_sock_addr = result.src_addr;

            if len == 0 {
                continue;
            }

            let stride: usize = if result.stride == 0 {
                len
            } else {
                result.stride.into()
            };

            let b = Bytes::copy_from_slice(&self.udp_socket.recv_ctrl.buf_refs[i][0..len]);
            let rx = RecvMsg {
                src_addr: src_sock_addr,
                payload: b,
                stride,
            };
            match self.udp_ingress_sender.producer.push(rx) {
                Err(e) => {
                    warn!(?e, "send failed on ingress sender");
                }
                Ok(()) => {
                    self.udp_ingress_sender.notify.0.wake();
                }
            }
        }

        Some(total_recv_bytes)
    }

    fn handle_tx(&mut self) {
        let (_n, s) = self.efd.handle_event();
        if s == -1 {
            let r = std::io::Error::last_os_error();
            if r.kind() == std::io::ErrorKind::WouldBlock {
                return;
            } else {
                panic!();
            }
        }

        self.handle_broadcast();
        self.handle_unicast();
        self.handle_tcp();
    }

    fn handle_broadcast(&mut self) {
        loop {
            let (to, payload) = match self.egress_bcast_receiver.pop() {
                Err(PopError::Empty) => {
                    break;
                }
                Ok(msg) => (msg.targets, msg.payload),
            };

            self.udp_socket.broadcast_buffer(to, payload);
        }
    }

    fn handle_unicast(&mut self) {
        loop {
            let msgs = match self.egress_ucast_receiver.pop() {
                Err(PopError::Empty) => {
                    break;
                }
                Ok(m) => m.msgs,
            };

            self.udp_socket.unicast_buffer(msgs);
        }
    }

    fn handle_tcp(&mut self) {
        loop {
            let (addr, bytes) = match self.egress_tcp_receiver.pop() {
                Err(PopError::Empty) => {
                    break;
                }
                Ok((addr, bytes)) => (addr, bytes),
            };

            self.tcp_outgoing_start_message(&addr, bytes);
        }
    }
}

// TCP incoming connection related methods
impl DataplaneEventLoop {
    fn tcp_incoming_slot_data_epoll_token(slot: usize) -> EpollToken {
        EpollToken(TCP_INCOMING_CONNECTION_DATA_FIRST_TOKEN + (slot as u64))
    }

    fn tcp_incoming_slot_timer_epoll_token(slot: usize) -> EpollToken {
        EpollToken(TCP_INCOMING_CONNECTION_TIMER_FIRST_TOKEN + (slot as u64))
    }

    fn tcp_incoming_get_free_connection_slot(&self) -> Option<usize> {
        (0..TCP_INCOMING_MAX_CONNECTIONS).find(|&i| self.tcp_incoming_connections[i].is_none())
    }

    fn tcp_incoming_arm_timer(&self, slot: usize, duration: Duration) {
        self.tcp_incoming_connection_timers[slot]
            .arm_oneshot_timer(duration)
            .unwrap();
    }

    fn tcp_incoming_new_connection(&mut self) {
        match self.tcp_listening_socket.accept() {
            Err(err) => debug!(?err, "error accepting TCP connection"),
            Ok((stream, addr)) => {
                trace!(?addr, "accepted incoming connection");

                match self.tcp_incoming_get_free_connection_slot() {
                    None => debug!(
                        ?addr,
                        ?TCP_INCOMING_MAX_CONNECTIONS,
                        "no slot available for incoming connection, dropping"
                    ),
                    Some(slot) => {
                        trace!(?addr, ?slot, "assigning connection");

                        stream.set_nonblocking(true).expect("set nonblocking");

                        self.epoll
                            .register(
                                Self::tcp_incoming_slot_data_epoll_token(slot),
                                RawFd(stream.as_raw_fd()),
                            )
                            .unwrap();

                        self.tcp_incoming_connections[slot] = Some(IncomingConnection::new(stream));
                        self.tcp_incoming_arm_timer(slot, TCP_HEADER_TIMEOUT);
                    }
                }
            }
        }
    }

    fn tcp_incoming_disarm_timer(&self, slot: usize) {
        self.tcp_incoming_connection_timers[slot]
            .arm_oneshot_timer(Duration::from_secs(0))
            .unwrap();

        // The timer may have fired before we successfully disarmed it.
        self.tcp_incoming_connection_timers[slot].handle_event();
    }

    fn tcp_incoming_kill_slot(&mut self, slot: usize) {
        let Some(conn) = &mut self.tcp_incoming_connections[slot] else {
            warn!(?slot, "asked to kill unused slot");
            return;
        };

        self.epoll
            .unregister(RawFd(conn.stream.as_raw_fd()))
            .unwrap();

        match conn.state {
            IncomingConnectionState::ReadingHeader { .. }
            | IncomingConnectionState::ReadingMessage { .. } => {
                self.tcp_incoming_disarm_timer(slot)
            }
            IncomingConnectionState::TimedOut => {}
        }

        self.tcp_incoming_connections[slot] = None;
    }

    fn tcp_incoming_slot_pollin(&mut self, slot: usize) {
        let Some(conn) = &mut self.tcp_incoming_connections[slot] else {
            warn!(?slot, "TCP connection data arrived for unused slot");
            return;
        };

        match &mut conn.state {
            IncomingConnectionState::ReadingHeader {
                header,
                bytes_received,
            } => match conn
                .stream
                .read(&mut header.as_bytes_mut()[*bytes_received..])
            {
                Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                Err(err) => {
                    debug!(?err, ?slot, "got error on TCP connection, closing");
                    self.tcp_incoming_kill_slot(slot);
                }
                Ok(0) => {
                    debug!(?slot, "got EOF on TCP connection, closing");
                    self.tcp_incoming_kill_slot(slot);
                }
                Ok(ret) => {
                    *bytes_received += ret;
                    if *bytes_received < header.as_bytes_mut().len() {
                        return;
                    }

                    trace!(?slot, "received header on TCP connection");

                    if header.magic.get() != HEADER_MAGIC {
                        debug!(?slot, "received incorrect magic number on TCP connection");
                        self.tcp_incoming_kill_slot(slot);
                        return;
                    }

                    if header.version.get() != HEADER_VERSION {
                        debug!(?slot, "received incorrect version number on TCP connection");
                        self.tcp_incoming_kill_slot(slot);
                        return;
                    }

                    let message_length: usize = header.length.get() as usize;

                    if message_length > TCP_MESSAGE_LENGTH_LIMIT {
                        debug!(
                            ?slot,
                            ?message_length,
                            "received oversized message on TCP connection"
                        );
                        self.tcp_incoming_kill_slot(slot);
                        return;
                    }

                    // TODO: Try to avoid zero-initializing the message buffer.
                    conn.state = IncomingConnectionState::ReadingMessage {
                        message: BytesMut::zeroed(message_length),
                        bytes_received: 0,
                    };

                    self.tcp_incoming_arm_timer(slot, TCP_MESSAGE_TIMEOUT);
                }
            },
            IncomingConnectionState::ReadingMessage {
                message,
                bytes_received,
            } => match conn.stream.read(&mut message[*bytes_received..]) {
                Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                Err(err) => {
                    debug!(?err, ?slot, "got error on TCP connection, closing");
                    self.tcp_incoming_kill_slot(slot);
                }
                Ok(0) => {
                    debug!(?slot, "got EOF on TCP connection, closing");
                    self.tcp_incoming_kill_slot(slot);
                }
                Ok(ret) => {
                    *bytes_received += ret;
                    if *bytes_received < message.len() {
                        return;
                    }

                    match conn.stream.peer_addr() {
                        Ok(peer_addr) => {
                            trace!(
                                ?peer_addr,
                                ?slot,
                                ?bytes_received,
                                "received message on TCP connection"
                            );
                            let message = std::mem::take(message);
                            match self
                                .tcp_ingress_sender
                                .producer
                                .push((peer_addr, message.freeze()))
                            {
                                Err(e) => {
                                    warn!(?e, "send failed on ingress sender");
                                }
                                Ok(()) => {
                                    self.tcp_ingress_sender.notify.0.wake();
                                }
                            }
                        }
                        Err(e) => {
                            warn!(?e, "failed get peer_addr from TCP connection");
                        }
                    }

                    self.tcp_incoming_kill_slot(slot);
                }
            },
            IncomingConnectionState::TimedOut => {
                panic!("TCP connection data arrived for connection in TimedOut state");
            }
        }
    }

    fn tcp_incoming_slot_timer(&mut self, slot: usize) {
        let Some(conn) = &mut self.tcp_incoming_connections[slot] else {
            warn!(?slot, "TCP connection timer expired for unused slot");
            return;
        };

        match &conn.state {
            IncomingConnectionState::ReadingHeader {
                header,
                bytes_received,
            } => {
                trace!(
                    slot,
                    local_addr =? conn.stream.local_addr().unwrap(),
                    peer_addr =? conn.stream.peer_addr().unwrap(),
                    bytes_received,
                    ?header,
                    "incoming TCP connection timed out in state ReadingHeader"
                );
            }
            IncomingConnectionState::ReadingMessage {
                message,
                bytes_received,
            } => {
                debug!(
                    slot,
                    local_addr =? conn.stream.local_addr().unwrap(),
                    peer_addr =? conn.stream.peer_addr().unwrap(),
                    bytes_received,
                    message_len = message.len(),
                    "incoming TCP connection timed out in state ReadingMessage"
                );
            }
            _ => {}
        }

        self.tcp_incoming_connection_timers[slot].handle_event();

        conn.state = IncomingConnectionState::TimedOut;

        self.tcp_incoming_kill_slot(slot);
    }
}

// TCP outgoing connection related methods
impl DataplaneEventLoop {
    fn tcp_outgoing_slot_data_epoll_token(slot: usize) -> EpollToken {
        EpollToken(TCP_OUTGOING_CONNECTION_DATA_FIRST_TOKEN + (slot as u64))
    }

    fn tcp_outgoing_slot_timer_epoll_token(slot: usize) -> EpollToken {
        EpollToken(TCP_OUTGOING_CONNECTION_TIMER_FIRST_TOKEN + (slot as u64))
    }

    fn tcp_outgoing_disarm_timer(&self, slot: usize) {
        self.tcp_outgoing_connection_timers[slot]
            .arm_oneshot_timer(Duration::from_secs(0))
            .unwrap();

        // The timer may have fired before we successfully disarmed it.
        self.tcp_outgoing_connection_timers[slot].handle_event();
    }

    fn tcp_outgoing_kill_slot(&mut self, slot: usize) {
        let Some(conn) = &mut self.tcp_outgoing_connections[slot] else {
            warn!(?slot, "asked to kill unused slot");
            return;
        };

        self.epoll
            .unregister(RawFd(conn.stream.as_raw_fd()))
            .unwrap();

        match conn.state {
            OutgoingConnectionState::Connecting
            | OutgoingConnectionState::WritingHeader { .. }
            | OutgoingConnectionState::WritingMessage { .. } => {
                self.tcp_outgoing_disarm_timer(slot)
            }
            OutgoingConnectionState::TimedOut => {}
        }

        self.tcp_outgoing_connections[slot] = None;
    }

    fn tcp_outgoing_slot_pollout(&mut self, slot: usize) {
        let Some(conn) = &mut self.tcp_outgoing_connections[slot] else {
            warn!(?slot, "TCP connection data arrived for unused slot");
            return;
        };

        match &mut conn.state {
            OutgoingConnectionState::Connecting => {
                let ret = conn
                    .stream
                    .take_error()
                    .expect("take_error on outgoing TCP socket");

                match ret {
                    Some(err) if err.raw_os_error() == Some(libc::EINPROGRESS) => {}
                    Some(err) => {
                        debug!(?err, ?slot, "got error on connecting TCP, closing");
                        self.tcp_outgoing_kill_slot(slot);
                    }
                    None => {
                        conn.state = OutgoingConnectionState::WritingHeader { bytes_written: 0 }
                    }
                }
            }
            OutgoingConnectionState::WritingHeader { bytes_written } => {
                let header = TcpMsgHdr::new(conn.msg.len() as u64);

                match conn.stream.write(&header.as_bytes()[*bytes_written..]) {
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                    Err(err) => {
                        debug!(?err, ?slot, "got error on TCP connection, closing");
                        self.tcp_outgoing_kill_slot(slot);
                    }
                    Ok(ret) => {
                        *bytes_written += ret;
                        if *bytes_written < header.as_bytes().len() {
                            return;
                        }

                        conn.state = OutgoingConnectionState::WritingMessage { bytes_written: 0 };
                    }
                }
            }
            OutgoingConnectionState::WritingMessage { bytes_written } => {
                match conn.stream.write(&conn.msg[*bytes_written..]) {
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                    Err(err) => {
                        debug!(?err, ?slot, "got error on TCP connection, closing");
                        self.tcp_outgoing_kill_slot(slot);
                    }
                    Ok(ret) => {
                        *bytes_written += ret;
                        if *bytes_written < conn.msg.len() {
                            return;
                        }

                        self.tcp_outgoing_kill_slot(slot);
                    }
                }
            }
            OutgoingConnectionState::TimedOut => {
                panic!("TCP connection POLLOUT for connection in TimedOut state");
            }
        }
    }

    fn tcp_outgoing_slot_timer(&mut self, slot: usize) {
        let Some(conn) = &mut self.tcp_outgoing_connections[slot] else {
            warn!(?slot, "TCP connection timer expired for unused slot");
            return;
        };

        match &conn.state {
            OutgoingConnectionState::Connecting => {
                trace!(
                    slot,
                    message_len = conn.msg.len(),
                    "outgoing TCP connection timed out in state Connecting"
                );
            }
            OutgoingConnectionState::WritingHeader { bytes_written } => {
                debug!(
                    slot,
                    local_addr =? conn.stream.local_addr().unwrap(),
                    peer_addr =? conn.stream.peer_addr().unwrap(),
                    bytes_written,
                    message_len = conn.msg.len(),
                    "outgoing TCP connection timed out in state WritingHeader"
                );
            }
            OutgoingConnectionState::WritingMessage { bytes_written } => {
                debug!(
                    slot,
                    local_addr =? conn.stream.local_addr().unwrap(),
                    peer_addr =? conn.stream.peer_addr().unwrap(),
                    bytes_written,
                    message_len = conn.msg.len(),
                    "outgoing TCP connection timed out in state WritingMessage"
                );
            }
            _ => {}
        }

        self.tcp_outgoing_connection_timers[slot].handle_event();

        conn.state = OutgoingConnectionState::TimedOut;

        self.tcp_outgoing_kill_slot(slot);
    }

    fn tcp_outgoing_get_free_connection_slot(&self) -> Option<usize> {
        (0..TCP_OUTGOING_MAX_CONNECTIONS).find(|&i| self.tcp_outgoing_connections[i].is_none())
    }

    fn tcp_outgoing_arm_timer(&self, slot: usize, duration: Duration) {
        self.tcp_outgoing_connection_timers[slot]
            .arm_oneshot_timer(duration)
            .unwrap();
    }

    fn tcp_outgoing_start_message(&mut self, addr: &SocketAddr, msg: Bytes) {
        match self.tcp_outgoing_get_free_connection_slot() {
            None => warn!(
                ?addr,
                ?TCP_OUTGOING_MAX_CONNECTIONS,
                "no slot available for outgoing connection, dropping"
            ),
            Some(slot) => {
                trace!(?addr, ?slot, "assigning TCP connection");

                let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
                    .expect("outgoing TCP socket");
                socket.set_nonblocking(true).expect("set nonblocking");

                match socket.connect(&(*addr).into()) {
                    Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) => {
                        self.epoll
                            .register_pollout(
                                Self::tcp_outgoing_slot_data_epoll_token(slot),
                                RawFd(socket.as_raw_fd()),
                            )
                            .unwrap();

                        self.tcp_outgoing_connections[slot] = Some(
                            OutgoingConnection::new_connecting(msg, TcpStream::from(socket)),
                        );
                        self.tcp_outgoing_arm_timer(slot, TCP_MESSAGE_TIMEOUT);
                    }
                    Err(err) => {
                        warn!(?err, ?addr, "error on outgoing TCP connection, closing");
                    }
                    Ok(_) => {
                        self.epoll
                            .register_pollout(
                                Self::tcp_outgoing_slot_data_epoll_token(slot),
                                RawFd(socket.as_raw_fd()),
                            )
                            .unwrap();

                        self.tcp_outgoing_connections[slot] = Some(
                            OutgoingConnection::new_connected(msg, TcpStream::from(socket)),
                        );
                        self.tcp_outgoing_arm_timer(slot, TCP_MESSAGE_TIMEOUT);
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Notifier(pub Arc<AtomicWaker>);

#[allow(clippy::new_without_default)]
impl Notifier {
    pub fn new() -> Self {
        Self(Arc::new(AtomicWaker::new()))
    }
}

// wraps the spsc consumer with a notifier to implement stream
pub struct WakeableConsumer<T> {
    pub consumer: rtrb::Consumer<T>,
    pub notify: Notifier,
}

impl<T> Stream for WakeableConsumer<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.consumer.pop() {
            Ok(t) => {
                return Poll::Ready(Some(t));
            }
            Err(PopError::Empty) => {
                self.notify.0.register(cx.waker());
            }
        }
        // documentation for AtomicWaker shows example checking condition again after register
        // waker to "avoid race condition that would result in lost notification"
        match self.consumer.pop() {
            Ok(t) => Poll::Ready(Some(t)),
            Err(PopError::Empty) => Poll::Pending,
        }
    }
}

pub struct WakeableProducer<T> {
    pub producer: rtrb::Producer<T>,
    pub notify: Notifier,
}
