use std::{net::SocketAddr, os::fd::AsRawFd, pin::Pin, sync::Arc, task::Poll, thread};

use bytes::Bytes;
use futures::Stream;
use futures_util::{task::AtomicWaker, StreamExt};
use log::debug;
use rtrb::{PopError, RingBuffer};

use crate::{
    epoll::{EpollFd, EpollToken, EventFd, RawFd, TimerFd},
    network::NetworkSocket,
};

const NUM_EPOLL_EVENTS: usize = 10;

const RX_EVENT: EpollToken = EpollToken(0);
const TX_EVENT: EpollToken = EpollToken(1);
const TIMER: EpollToken = EpollToken(2);

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

    // this is not currently used
    tfd: TimerFd,

    /// Ingress refers to the direction of Network traffic (incoming network traffic)
    /// Ingress sender/receiver channel is used to deliver the ingress network traffic to the
    /// application
    ingress_receiver: WakeableConsumer<RecvMsg>,

    /// Egress refers to the direction of Network traffic (outgoing network traffic)
    /// Egress sender/receiver channel is used to deliver data from the Application to this struct
    /// which will then send it out to the network
    egress_bcast_sender: rtrb::Producer<BroadcastMsg>,
    egress_ucast_sender: rtrb::Producer<UnicastMsg>,
}

impl Dataplane {
    pub fn new(local_addr: &str) -> Self {
        let (ing_send, ing_recv) = RingBuffer::<RecvMsg>::new(1024);
        let n = Notifier::new();
        let ing_producer = WakeableProducer {
            producer: ing_send,
            notify: n.clone(),
        };
        let ing_consumer = WakeableConsumer {
            consumer: ing_recv,
            notify: n,
        };

        let (egr_bcast_send, egr_bcast_recv) = RingBuffer::<BroadcastMsg>::new(1024);
        let (egr_ucast_send, egr_ucast_recv) = RingBuffer::<UnicastMsg>::new(1024);

        let efd = EventFd::new().unwrap();
        let tfd = TimerFd::new().unwrap();

        DataplaneEventLoop {
            efd,
            tfd,
            epoll: EpollFd::<NUM_EPOLL_EVENTS>::new().unwrap(),
            ingress_sender: ing_producer,
            egress_bcast_receiver: egr_bcast_recv,
            egress_ucast_receiver: egr_ucast_recv,
            socket: NetworkSocket::new(local_addr),
        }
        .start();

        Self {
            efd,
            tfd,
            ingress_receiver: ing_consumer,
            egress_bcast_sender: egr_bcast_send,
            egress_ucast_sender: egr_ucast_send,
        }
    }

    pub fn broadcast(&mut self, msg: BroadcastMsg) {
        match self.egress_bcast_sender.push(msg) {
            Err(e) => {
                debug!("bcast egress channel sender error {e}");
                panic!("bcast egress sender queue is full, consider resizing");
            }
            Ok(()) => {
                debug!("sending into channel");
                self.efd.trigger_event(1).unwrap();
            }
        }
    }

    pub fn unicast(&mut self, msg: UnicastMsg) {
        match self.egress_ucast_sender.push(msg) {
            Err(e) => {
                debug!("ucast egress channel sender error {e}");
                panic!("ucast egress sender queue is full, consider resizing");
            }
            Ok(()) => {
                debug!("sending into channel");
                self.efd.trigger_event(1).unwrap();
            }
        }
    }
}

impl Stream for Dataplane {
    type Item = RecvMsg;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.ingress_receiver.poll_next_unpin(cx)
    }
}

struct DataplaneEventLoop {
    epoll: EpollFd<NUM_EPOLL_EVENTS>,
    efd: EventFd,
    tfd: TimerFd,

    ingress_sender: WakeableProducer<RecvMsg>,
    egress_bcast_receiver: rtrb::Consumer<BroadcastMsg>,
    egress_ucast_receiver: rtrb::Consumer<UnicastMsg>,

    socket: NetworkSocket<'static>,
}

impl DataplaneEventLoop {
    fn start(mut self) {
        thread::spawn(move || {
            self.epoll
                .register(RX_EVENT, RawFd(self.socket.socket.as_raw_fd()))
                .expect("epoll register of udp must succeed");
            self.epoll
                .register(TX_EVENT, self.efd.get_fd())
                .expect("epoll register of eventfd must succeed");
            self.epoll
                .register(TIMER, self.tfd.get_fd())
                .expect("epoll register of eventfd must succeed");

            'epoll_loop: loop {
                // this blocks until something happens
                let polled_events = self.epoll.wait().unwrap();

                debug!(
                    "event tokens: {:?}",
                    polled_events.iter().map(|e| e.token()).collect::<Vec<_>>()
                );

                if polled_events.is_empty() {
                    // if a timeout was configured in the call to epoll_wait,
                    // then we can get here if there were no events ready at expiration
                    // if there was no timeout configured, its possible to reach here from spurious
                    // wakeups
                    continue 'epoll_loop;
                }

                for e in polled_events {
                    debug!("event token: {:?}", e.token());

                    'handle_event: loop {
                        if e.token() == RX_EVENT {
                            match self.handle_rx() {
                                Some(_) => continue,
                                None => break 'handle_event,
                            }
                        }

                        if e.token() == TX_EVENT {
                            self.handle_tx();
                            break 'handle_event;
                        }

                        if e.token() == TIMER {
                            self.handle_timer();
                            break 'handle_event;
                        }
                    }
                }
            }
        });
    }

    fn handle_rx(&mut self) -> Option<usize> {
        let recvs = match self.socket.recvmmsg() {
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

            let stride: usize = if result.stride == 0 {
                len
            } else {
                result.stride.into()
            };

            let b = Bytes::copy_from_slice(&self.socket.recv_ctrl.buf_refs[i][0..len]);
            let rx = RecvMsg {
                src_addr: src_sock_addr,
                payload: b,
                stride,
            };
            match self.ingress_sender.producer.push(rx) {
                Err(e) => {
                    debug!("send failed on ingress sender {e}");
                    panic!("consider resizing queue for ingress");
                }
                Ok(()) => {
                    debug!("sent bytes on ingress sender");
                    self.ingress_sender.notify.0.wake();
                }
            }
        }

        Some(total_recv_bytes)
    }

    fn handle_tx(&mut self) {
        let (n, s) = self.efd.handle_event();
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
    }

    fn handle_broadcast(&mut self) {
        loop {
            let (to, payload) = match self.egress_bcast_receiver.pop() {
                Err(PopError::Empty) => {
                    break;
                }
                Ok(msg) => (msg.targets, msg.payload),
            };

            self.socket.broadcast_buffer(to, payload);
            // TODO: need pacing. SO_TXTIME
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

            self.socket.unicast_buffer(msgs);
            // TODO: need pacing. SO_TXTIME
        }
    }

    fn handle_timer(&mut self) {}
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
