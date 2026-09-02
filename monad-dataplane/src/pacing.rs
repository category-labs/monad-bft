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

//! Single-owner global and per-peer packet pacing.
//!
//! Flow state is ordered by its next eligible send time. Eligible UDP and TCP
//! messages move through one global priority queue.

use std::{
    cell::RefCell,
    cmp::{Ordering, Reverse},
    collections::{hash_map::DefaultHasher, BinaryHeap, HashMap},
    hash::{Hash, Hasher},
    net::{SocketAddr, SocketAddrV4},
    num::NonZeroU64,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::channel::oneshot;
use monad_types::UdpPriority;
use monoio::{select, time::sleep};
use tokio::sync::mpsc;
use zerocopy::IntoBytes;

use crate::{
    metrics::DataplaneMetrics,
    tcp::{
        tx::{QUEUED_MESSAGE_BYTE_LIMIT, QUEUED_MESSAGE_LIMIT},
        TcpMsgHdr, TCP_MESSAGE_LENGTH_LIMIT,
    },
    TcpMsg, UdpMsg, UdpPacingConfig, UdpSocketId, IPV4_HDR_SIZE, UDP_HDR_SIZE,
};

const PACER_MAX_CATCH_UP: Duration = Duration::from_millis(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PacingPriority {
    High,
    Regular,
    Background,
}

impl From<UdpPriority> for PacingPriority {
    fn from(priority: UdpPriority) -> Self {
        match priority {
            UdpPriority::High => Self::High,
            UdpPriority::Regular => Self::Regular,
        }
    }
}

impl PacingPriority {
    pub(crate) fn queued_requests(self, metrics: &DataplaneMetrics) -> &monad_executor::Gauge {
        match self {
            Self::High => &metrics.egress_pacing_high_queued_requests,
            Self::Regular => &metrics.egress_pacing_regular_queued_requests,
            Self::Background => &metrics.egress_pacing_background_queued_requests,
        }
    }

    pub(crate) fn record_grant(self, metrics: &DataplaneMetrics, bytes: usize, wait: Duration) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let wait_micros = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX);
        match self {
            Self::High => {
                metrics.egress_pacing_high_grants.inc();
                metrics.egress_pacing_high_granted_bytes.add(bytes);
                metrics.egress_pacing_high_wait_micros.add(wait_micros);
            }
            Self::Regular => {
                metrics.egress_pacing_regular_grants.inc();
                metrics.egress_pacing_regular_granted_bytes.add(bytes);
                metrics.egress_pacing_regular_wait_micros.add(wait_micros);
            }
            Self::Background => {
                metrics.egress_pacing_background_grants.inc();
                metrics.egress_pacing_background_granted_bytes.add(bytes);
                metrics
                    .egress_pacing_background_wait_micros
                    .add(wait_micros);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueError<T> {
    MemoryLimit(T),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PacingKey {
    Udp(SocketAddrV4),
    Tcp(SocketAddr),
}

impl From<SocketAddrV4> for PacingKey {
    fn from(addr: SocketAddrV4) -> Self {
        Self::Udp(addr)
    }
}

pub trait PacingItem {
    fn next_payload_bytes(&self) -> usize;

    fn next_pacing_bytes(&self) -> usize {
        self.next_payload_bytes()
            .saturating_add(usize::from(IPV4_HDR_SIZE + UDP_HDR_SIZE))
    }

    fn peer_bytes_per_second(&self, configured: NonZeroU64) -> NonZeroU64 {
        configured
    }

    fn uses_udp_memory(&self) -> bool {
        true
    }
}

impl PacingItem for Bytes {
    fn next_payload_bytes(&self) -> usize {
        self.len()
    }
}

impl PacingItem for Vec<u8> {
    fn next_payload_bytes(&self) -> usize {
        self.len()
    }
}

/// One item selected for transmission.
#[derive(Debug)]
pub struct Scheduled<T> {
    pub item: T,
    pub(crate) batch_bytes: usize,
    pub(crate) pacing_bytes: usize,
    pub(crate) priority: PacingPriority,
    pub(crate) order: u64,
}

struct Queued<T> {
    priority: PacingPriority,
    order: u64,
    item: T,
    queued_bytes: usize,
}

impl<T> PartialEq for Queued<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.order == other.order
    }
}

impl<T> Eq for Queued<T> {}

impl<T> PartialOrd for Queued<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Queued<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        Reverse(self.priority)
            .cmp(&Reverse(other.priority))
            .then_with(|| other.order.cmp(&self.order))
    }
}

struct PeerState<T> {
    messages: BinaryHeap<Queued<T>>,
    udp: bool,
}

type Peer<T> = Rc<RefCell<PeerState<T>>>;

struct PeerDeadline<T> {
    next_at: Duration,
    key: PacingKey,
    peer: Peer<T>,
}

impl<T> PartialEq for PeerDeadline<T> {
    fn eq(&self, other: &Self) -> bool {
        self.next_at == other.next_at && self.key == other.key
    }
}

impl<T> Eq for PeerDeadline<T> {}

impl<T> PartialOrd for PeerDeadline<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for PeerDeadline<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.next_at
            .cmp(&other.next_at)
            .then_with(|| self.key.cmp(&other.key))
    }
}

/// Socket-address keyed queue with global and uniform per-peer pacing.
pub struct PacingQueue<T> {
    peers: HashMap<PacingKey, Peer<T>>,
    next_peers: BinaryHeap<Reverse<PeerDeadline<T>>>,
    ready: BinaryHeap<Queued<T>>,
    global_bytes_per_second: NonZeroU64,
    peer_bytes_per_second: NonZeroU64,
    service_at: Duration,
    memory_limit: usize,
    memory_used: usize,
    pending: usize,
    next_order: u64,
    epoch: Instant,
    metrics: DataplaneMetrics,
}

impl<T: PacingItem> PacingQueue<T> {
    pub fn new(
        global_bytes_per_second: NonZeroU64,
        peer_bytes_per_second: NonZeroU64,
        memory_limit: usize,
        metrics: DataplaneMetrics,
    ) -> Self {
        metrics.udp_pacing_peers.set(0);
        metrics.udp_pacing_queued_bytes.set(0);
        metrics
            .udp_pacing_memory_limit_bytes
            .set(u64::try_from(memory_limit).unwrap_or(u64::MAX));
        Self {
            peers: HashMap::new(),
            next_peers: BinaryHeap::new(),
            ready: BinaryHeap::new(),
            global_bytes_per_second,
            peer_bytes_per_second,
            service_at: Duration::ZERO,
            memory_limit,
            memory_used: 0,
            pending: 0,
            next_order: 0,
            epoch: Instant::now(),
            metrics,
        }
    }

    pub const fn len(&self) -> usize {
        self.pending
    }

    pub const fn is_empty(&self) -> bool {
        self.pending == 0
    }

    pub fn elapsed(&self) -> Duration {
        self.epoch.elapsed()
    }

    pub fn next_wakeup(&self, now: Duration) -> Option<Duration> {
        let scheduled_at = if self.ready.is_empty() {
            self.service_at.max(self.next_peers.peek()?.0.next_at)
        } else {
            self.service_at
        };
        Some(scheduled_at.max(now))
    }

    pub(crate) fn ready_pacing_bytes(&mut self, now: Duration) -> Option<usize> {
        self.prepare(now);
        self.ready
            .peek()
            .map(|queued| queued.item.next_pacing_bytes())
    }

    pub(crate) const fn reserved_until(&self) -> Duration {
        self.service_at
    }

    pub fn enqueue(
        &mut self,
        key: impl Into<PacingKey>,
        priority: impl Into<PacingPriority>,
        item: T,
        queued_bytes: usize,
    ) -> Result<(), EnqueueError<T>> {
        self.enqueue_inner(key.into(), priority.into(), item, queued_bytes, None)
    }

    pub(crate) fn enqueue_egress(
        &mut self,
        key: PacingKey,
        priority: PacingPriority,
        item: T,
        queued_bytes: usize,
    ) -> Result<(), EnqueueError<T>> {
        self.enqueue_inner(key, priority, item, queued_bytes, None)
    }

    pub(crate) fn requeue(
        &mut self,
        key: PacingKey,
        priority: PacingPriority,
        item: T,
        queued_bytes: usize,
        order: u64,
    ) -> Result<(), EnqueueError<T>> {
        self.enqueue_inner(key, priority, item, queued_bytes, Some(order))
    }

    fn enqueue_inner(
        &mut self,
        key: PacingKey,
        priority: PacingPriority,
        item: T,
        queued_bytes: usize,
        order: Option<u64>,
    ) -> Result<(), EnqueueError<T>> {
        let udp = item.uses_udp_memory();
        let memory_bytes = if udp { queued_bytes } else { 0 };
        if memory_bytes > self.memory_limit.saturating_sub(self.memory_used) {
            self.metrics.udp_egress_messages_dropped.inc();
            self.metrics.udp_pacing_memory_limit_drops.inc();
            return Err(EnqueueError::MemoryLimit(item));
        }
        let queued = Queued {
            priority,
            order: order.unwrap_or(self.next_order),
            item,
            queued_bytes: memory_bytes,
        };

        if let Some(peer) = self.peers.get(&key) {
            debug_assert_eq!(peer.borrow().udp, udp);
            peer.borrow_mut().messages.push(queued);
        } else {
            let peer = Rc::new(RefCell::new(PeerState {
                messages: BinaryHeap::from([queued]),
                udp,
            }));
            self.peers.insert(key, Rc::clone(&peer));
            self.next_peers.push(Reverse(PeerDeadline {
                next_at: self.service_at,
                key,
                peer,
            }));
            if udp {
                self.metrics.udp_pacing_peers.inc();
            }
        }

        if order.is_none() {
            self.next_order = self.next_order.wrapping_add(1);
        }
        self.memory_used += memory_bytes;
        self.pending += 1;
        self.metrics
            .udp_pacing_queued_bytes
            .add(u64::try_from(memory_bytes).unwrap_or(u64::MAX));
        Ok(())
    }

    pub fn dequeue(&mut self, now: Duration, max_bytes: usize) -> Option<Scheduled<T>> {
        self.prepare(now);

        let queued = self.ready.peek()?;
        let next_bytes = queued.item.next_payload_bytes();
        if next_bytes > max_bytes {
            return None;
        }
        let pacing_bytes = queued.item.next_pacing_bytes();
        self.service_at =
            Self::advance(self.service_at, pacing_bytes, self.global_bytes_per_second);
        let queued = self.ready.pop().expect("peeked ready message");

        self.memory_used -= queued.queued_bytes;
        self.pending -= 1;
        self.metrics
            .udp_pacing_queued_bytes
            .sub(u64::try_from(queued.queued_bytes).unwrap_or(u64::MAX));
        Some(Scheduled {
            item: queued.item,
            batch_bytes: next_bytes,
            pacing_bytes,
            priority: queued.priority,
            order: queued.order,
        })
    }

    fn prepare(&mut self, now: Duration) {
        // account for time spent on cpu work or while this thread was scheduled off-cpu.
        // A temporarily empty ready heap can still have queued peers waiting on deadlines. Advance
        // to that deadline without discarding bounded catch-up time. After a long pause, reset
        // instead of accumulating an unbounded catch-up allowance.
        if now.saturating_sub(self.service_at) > PACER_MAX_CATCH_UP {
            self.service_at = self.service_at.max(now);
        } else if self.ready.is_empty() {
            self.service_at = self.service_at.max(
                self.next_peers
                    .peek()
                    .map_or(now, |entry| entry.0.next_at.min(now)),
            );
        }

        // promote every eligible peer before selection so the final heap can enforce priority across peers.
        while self
            .next_peers
            .peek()
            .is_some_and(|entry| entry.0.next_at <= self.service_at)
        {
            let deadline = self.next_peers.pop().expect("peeked peer").0;
            let message = deadline.peer.borrow_mut().messages.pop();
            // keep a tombstone for a drained peer until its deadline becomes eligible. a message
            // enqueued before then reuses this deadline instead of resetting the peer clock and
            // overrunning the per-peer rate.
            let Some(message) = message else {
                let peer = self
                    .peers
                    .remove(&deadline.key)
                    .expect("scheduled peer must exist");
                debug_assert!(Rc::ptr_eq(&peer, &deadline.peer));
                if peer.borrow().udp {
                    self.metrics.udp_pacing_peers.dec();
                }
                continue;
            };
            let pace_from = if self.service_at.saturating_sub(deadline.next_at) > PACER_MAX_CATCH_UP
            {
                self.service_at
            } else {
                deadline.next_at
            };
            let next_at = Self::advance(
                pace_from,
                message.item.next_pacing_bytes(),
                message
                    .item
                    .peer_bytes_per_second(self.peer_bytes_per_second),
            );
            self.next_peers.push(Reverse(PeerDeadline {
                next_at,
                key: deadline.key,
                peer: deadline.peer,
            }));
            self.ready.push(message);
        }
    }

    fn advance(at: Duration, bytes: usize, bytes_per_second: NonZeroU64) -> Duration {
        let nanos = (bytes as u128)
            .saturating_mul(Duration::from_secs(1).as_nanos())
            .div_ceil(bytes_per_second.get() as u128);
        at.saturating_add(Duration::from_nanos(
            u64::try_from(nanos).unwrap_or(u64::MAX),
        ))
    }
}

pub(crate) const TCP_DISPATCH_PAYLOAD_BYTES: usize = 256 * 1024;
const UDP_DISPATCH_PACING_BYTES: usize = 64 * 1024;
const UDP_WORKER_DISPATCH_CHANNEL_SIZE: usize = 128;
const PACING_COMMAND_CHANNEL_SIZE: usize = 12_800 + 256;
const MAX_COMMAND_DRAIN: usize = 256;

pub(crate) struct TcpDispatchEnd {
    pub(crate) message_len: usize,
    pub(crate) completion: Option<oneshot::Sender<()>>,
}

pub(crate) struct TcpDispatch {
    pub(crate) header: Option<Bytes>,
    pub(crate) payload: Bytes,
    pub(crate) end: Option<TcpDispatchEnd>,
}

pub(crate) struct UdpSend {
    pub(crate) socket_id: UdpSocketId,
    pub(crate) dst: SocketAddr,
    pub(crate) payload: Bytes,
}

pub(crate) struct UdpDispatch {
    pub(crate) sends: Vec<UdpSend>,
}

pub(crate) struct PacingTaskConfig {
    pub(crate) global_bytes_per_second: NonZeroU64,
    pub(crate) udp: UdpPacingConfig,
    pub(crate) udp_workers: usize,
}

#[derive(Clone)]
pub(crate) struct PacingHandle {
    commands: mpsc::Sender<Command>,
    udp_queued_bytes: Arc<AtomicUsize>,
    udp_memory_limit: usize,
    metrics: DataplaneMetrics,
}

pub(crate) struct PacingTask {
    config: PacingTaskConfig,
    commands: mpsc::Receiver<Command>,
    outputs: OutputSenders,
    metrics: DataplaneMetrics,
    udp_queued_bytes: Arc<AtomicUsize>,
}

pub(crate) struct PacingOutputs {
    pub(crate) tcp: mpsc::UnboundedReceiver<(SocketAddr, TcpDispatch)>,
    pub(crate) udp: Vec<mpsc::Receiver<UdpDispatch>>,
}

struct OutputSenders {
    tcp: mpsc::UnboundedSender<(SocketAddr, TcpDispatch)>,
    udp: Vec<mpsc::Sender<UdpDispatch>>,
}

enum Command {
    Tcp { addr: SocketAddr, message: TcpMsg },
    Udp { message: UdpMsg },
}

impl PacingHandle {
    pub(crate) fn new(
        config: PacingTaskConfig,
        metrics: DataplaneMetrics,
    ) -> (Self, PacingTask, PacingOutputs) {
        assert!(
            config.udp_workers != 0,
            "at least one UDP TX worker is required"
        );
        let (commands, command_rx) = mpsc::channel(PACING_COMMAND_CHANNEL_SIZE);
        let (tcp, tcp_rx) = mpsc::unbounded_channel();
        let mut udp = Vec::with_capacity(config.udp_workers);
        let mut udp_rx = Vec::with_capacity(config.udp_workers);
        for _ in 0..config.udp_workers {
            let (worker_tx, worker_rx) = mpsc::channel(UDP_WORKER_DISPATCH_CHANNEL_SIZE);
            udp.push(worker_tx);
            udp_rx.push(worker_rx);
        }
        let udp_queued_bytes = Arc::new(AtomicUsize::new(0));
        metrics
            .egress_pacing_bandwidth_limit_bytes_per_second
            .set(config.global_bytes_per_second.get());
        metrics
            .udp_pacing_memory_limit_bytes
            .set(u64::try_from(config.udp.max_queued_bytes).unwrap_or(u64::MAX));
        (
            Self {
                commands,
                udp_queued_bytes: udp_queued_bytes.clone(),
                udp_memory_limit: config.udp.max_queued_bytes,
                metrics: metrics.clone(),
            },
            PacingTask {
                config,
                commands: command_rx,
                outputs: OutputSenders { tcp, udp },
                metrics,
                udp_queued_bytes,
            },
            PacingOutputs {
                tcp: tcp_rx,
                udp: udp_rx,
            },
        )
    }

    pub(crate) fn enqueue_tcp(&self, addr: SocketAddr, message: TcpMsg) -> Result<(), TcpMsg> {
        match self.commands.try_send(Command::Tcp { addr, message }) {
            Ok(()) => Ok(()),
            Err(err) => match err.into_inner() {
                Command::Tcp { message, .. } => Err(message),
                Command::Udp { .. } => unreachable!("sent TCP pacing command"),
            },
        }
    }

    pub(crate) fn enqueue_udp(&self, message: UdpMsg) -> Result<(), UdpMsg> {
        let bytes = message.payload.len();
        if !reserve_bytes(&self.udp_queued_bytes, self.udp_memory_limit, bytes) {
            self.metrics.udp_pacing_memory_limit_drops.inc();
            return Err(message);
        }
        match self.commands.try_send(Command::Udp { message }) {
            Ok(()) => Ok(()),
            Err(err) => match err.into_inner() {
                Command::Udp { message } => {
                    self.udp_queued_bytes
                        .fetch_sub(bytes, AtomicOrdering::Relaxed);
                    Err(message)
                }
                Command::Tcp { .. } => unreachable!("sent UDP pacing command"),
            },
        }
    }
}

fn reserve_bytes(used: &AtomicUsize, limit: usize, bytes: usize) -> bool {
    let mut current = used.load(AtomicOrdering::Relaxed);
    loop {
        if bytes > limit.saturating_sub(current) {
            return false;
        }
        match used.compare_exchange_weak(
            current,
            current + bytes,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

fn udp_tx_worker(peer: SocketAddr, workers: usize) -> usize {
    debug_assert!(workers != 0);
    let mut hasher = DefaultHasher::new();
    peer.hash(&mut hasher);
    hasher.finish() as usize % workers
}

struct UdpWork {
    message: UdpMsg,
    requested_at: Instant,
}

struct TcpWork {
    addr: SocketAddr,
    header: Option<Bytes>,
    payload: Bytes,
    message_len: usize,
    completion: Option<oneshot::Sender<()>>,
    requested_at: Instant,
}

enum PacedMessage {
    Udp(UdpWork),
    Tcp(TcpWork),
}

impl PacedMessage {
    fn batch_limit(&self) -> usize {
        match self {
            Self::Udp(_) => UDP_DISPATCH_PACING_BYTES,
            Self::Tcp(_) => TCP_DISPATCH_PAYLOAD_BYTES,
        }
    }
}

impl PacingItem for PacedMessage {
    fn next_payload_bytes(&self) -> usize {
        match self {
            Self::Udp(work) => work.message.next_payload_bytes(),
            Self::Tcp(work) => work.payload.len().min(TCP_DISPATCH_PAYLOAD_BYTES),
        }
    }

    fn next_pacing_bytes(&self) -> usize {
        match self {
            Self::Udp(work) => work.message.next_pacing_bytes(),
            Self::Tcp(work) => work
                .header
                .as_ref()
                .map_or(0, Bytes::len)
                .saturating_add(work.payload.len().min(TCP_DISPATCH_PAYLOAD_BYTES)),
        }
    }

    fn peer_bytes_per_second(&self, configured: NonZeroU64) -> NonZeroU64 {
        match self {
            Self::Udp(_) => configured,
            Self::Tcp(_) => NonZeroU64::new(u64::MAX).unwrap(),
        }
    }

    fn uses_udp_memory(&self) -> bool {
        matches!(self, Self::Udp(_))
    }
}

#[derive(Default)]
struct TcpQueueUsage {
    bytes: usize,
    messages: usize,
}

struct PacingState {
    queue: PacingQueue<PacedMessage>,
    outputs: OutputSenders,
    tcp_usage: HashMap<SocketAddr, TcpQueueUsage>,
    metrics: DataplaneMetrics,
    udp_queued_bytes: Arc<AtomicUsize>,
}

impl PacingTask {
    pub(crate) async fn run(mut self) {
        let peer_bytes_per_second = NonZeroU64::new(
            u64::try_from(u128::from(self.config.udp.peer_bandwidth_mbps) * 1_000_000 / 8)
                .expect("UDP peer bandwidth overflows bytes per second"),
        )
        .expect("UDP peer bandwidth must be non-zero");
        let mut state = PacingState {
            queue: PacingQueue::new(
                self.config.global_bytes_per_second,
                peer_bytes_per_second,
                self.config.udp.max_queued_bytes,
                self.metrics.clone(),
            ),
            outputs: self.outputs,
            tcp_usage: HashMap::new(),
            metrics: self.metrics,
            udp_queued_bytes: self.udp_queued_bytes,
        };

        while let Some(first) = self.commands.recv().await {
            state.enqueue(first);
            for _ in 1..MAX_COMMAND_DRAIN {
                match self.commands.try_recv() {
                    Ok(command) => state.enqueue(command),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
            }

            loop {
                for _ in 0..MAX_COMMAND_DRAIN {
                    match self.commands.try_recv() {
                        Ok(command) => state.enqueue(command),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }

                let now = state.queue.elapsed();
                match state.dispatch_batch(now).await {
                    Ok(true) => continue,
                    Err(()) => return,
                    Ok(false) => {}
                }

                let Some(wake_at) = state.queue.next_wakeup(now) else {
                    break;
                };
                select! {
                    command = self.commands.recv() => {
                        let Some(command) = command else { return };
                        state.enqueue(command);
                    }
                    _ = sleep(wake_at.saturating_sub(now)) => {}
                }
            }
        }
    }
}

impl PacingState {
    fn enqueue(&mut self, command: Command) {
        match command {
            Command::Tcp { addr, message } => self.enqueue_tcp(addr, message),
            Command::Udp { message } => self.enqueue_udp(message),
        }
    }

    fn enqueue_tcp(&mut self, addr: SocketAddr, message: TcpMsg) {
        let message_len = message.msg.len();
        if message_len > TCP_MESSAGE_LENGTH_LIMIT {
            self.metrics.tcp_egress_messages_dropped.inc();
            return;
        }
        let usage = self.tcp_usage.entry(addr).or_default();
        if usage.messages >= QUEUED_MESSAGE_LIMIT
            || message_len > QUEUED_MESSAGE_BYTE_LIMIT.saturating_sub(usage.bytes)
        {
            self.metrics.tcp_egress_messages_dropped.inc();
            return;
        }
        usage.messages += 1;
        usage.bytes += message_len;

        let header = TcpMsgHdr::new(u64::try_from(message_len).unwrap_or(u64::MAX));
        let work = PacedMessage::Tcp(TcpWork {
            addr,
            header: Some(Bytes::copy_from_slice(header.as_bytes())),
            payload: message.msg,
            message_len,
            completion: message.completion,
            requested_at: Instant::now(),
        });
        PacingPriority::Background
            .queued_requests(&self.metrics)
            .inc();
        assert!(
            self.queue
                .enqueue_egress(
                    PacingKey::Tcp(addr),
                    PacingPriority::Background,
                    work,
                    message_len,
                )
                .is_ok(),
            "TCP messages do not consume UDP queue memory"
        );
    }

    fn enqueue_udp(&mut self, message: UdpMsg) {
        let bytes = message.payload.len();
        let priority = PacingPriority::from(message.priority);
        let SocketAddr::V4(destination) = message.dst else {
            self.udp_queued_bytes
                .fetch_sub(bytes, AtomicOrdering::Relaxed);
            self.metrics.udp_egress_messages_dropped.inc();
            return;
        };
        let work = PacedMessage::Udp(UdpWork {
            message,
            requested_at: Instant::now(),
        });
        if self
            .queue
            .enqueue_egress(PacingKey::Udp(destination), priority, work, bytes)
            .is_ok()
        {
            priority.queued_requests(&self.metrics).inc();
        } else {
            self.udp_queued_bytes
                .fetch_sub(bytes, AtomicOrdering::Relaxed);
        }
    }

    async fn dispatch_batch(&mut self, now: Duration) -> Result<bool, ()> {
        if self.queue.next_wakeup(now).is_none_or(|at| at > now) {
            return Ok(false);
        }
        let mut schedule_at = now;
        let mut pacing_bytes = 0_usize;
        let mut batch_limit = None;
        let mut udp_sends: Vec<Vec<UdpSend>> =
            (0..self.outputs.udp.len()).map(|_| Vec::new()).collect();

        loop {
            let Some(next_bytes) = self.queue.ready_pacing_bytes(schedule_at) else {
                break;
            };
            if pacing_bytes > 0
                && pacing_bytes.saturating_add(next_bytes) > batch_limit.expect("set by first item")
            {
                break;
            }
            let Some(scheduled) = self.queue.dequeue(schedule_at, usize::MAX) else {
                break;
            };
            if batch_limit.is_none() {
                batch_limit = Some(scheduled.item.batch_limit());
            }
            pacing_bytes = pacing_bytes.saturating_add(scheduled.pacing_bytes);
            self.dispatch(scheduled, &mut udp_sends)?;
            schedule_at = self.queue.reserved_until();
            if pacing_bytes >= batch_limit.expect("set by first item") {
                break;
            }
        }

        for (worker_tx, sends) in self.outputs.udp.iter().zip(udp_sends) {
            if sends.is_empty() {
                continue;
            }
            match worker_tx.try_send(UdpDispatch { sends }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dispatch)) => {
                    let messages = u64::try_from(dispatch.sends.len()).unwrap_or(u64::MAX);
                    let bytes = dispatch
                        .sends
                        .iter()
                        .map(|send| send.payload.len() as u64)
                        .sum();
                    self.metrics
                        .udp_tx_worker_channel_messages_dropped
                        .add(messages);
                    self.metrics.udp_tx_worker_channel_bytes_dropped.add(bytes);
                    self.metrics.udp_egress_messages_dropped.add(messages);
                }
                Err(mpsc::error::TrySendError::Closed(dispatch)) => {
                    self.metrics
                        .udp_egress_messages_dropped
                        .add(u64::try_from(dispatch.sends.len()).unwrap_or(u64::MAX));
                }
            }
        }
        Ok(pacing_bytes > 0)
    }

    fn dispatch(
        &mut self,
        scheduled: Scheduled<PacedMessage>,
        udp_sends: &mut [Vec<UdpSend>],
    ) -> Result<(), ()> {
        let Scheduled {
            item,
            batch_bytes,
            pacing_bytes,
            priority,
            order,
        } = scheduled;
        match item {
            PacedMessage::Udp(mut work) => {
                let requested_at = work.requested_at;
                let chunk = work.message.payload.split_to(batch_bytes);
                self.udp_queued_bytes
                    .fetch_sub(chunk.len(), AtomicOrdering::Relaxed);
                let worker = udp_tx_worker(work.message.dst, udp_sends.len());
                udp_sends[worker].push(UdpSend {
                    socket_id: work.message.socket_id,
                    dst: work.message.dst,
                    payload: chunk,
                });
                if work.message.payload.is_empty() {
                    priority.queued_requests(&self.metrics).dec();
                } else {
                    let SocketAddr::V4(destination) = work.message.dst else {
                        unreachable!("only IPv4 messages enter the pacing queue")
                    };
                    let remaining = work.message.payload.len();
                    assert!(
                        self.queue
                            .requeue(
                                PacingKey::Udp(destination),
                                priority,
                                PacedMessage::Udp(work),
                                remaining,
                                order,
                            )
                            .is_ok(),
                        "requeueing admitted UDP bytes cannot exceed memory"
                    );
                }
                priority.record_grant(&self.metrics, pacing_bytes, requested_at.elapsed());
            }
            PacedMessage::Tcp(mut work) => {
                let payload = work.payload.split_to(batch_bytes);
                let header = work.header.take();
                let complete = work.payload.is_empty();
                let end = complete.then(|| TcpDispatchEnd {
                    message_len: work.message_len,
                    completion: work.completion.take(),
                });
                let addr = work.addr;
                let requested_at = work.requested_at;
                if complete {
                    self.release_tcp(addr, work.message_len);
                    priority.queued_requests(&self.metrics).dec();
                } else {
                    let remaining = work.payload.len();
                    assert!(
                        self.queue
                            .requeue(
                                PacingKey::Tcp(addr),
                                priority,
                                PacedMessage::Tcp(work),
                                remaining,
                                order,
                            )
                            .is_ok(),
                        "TCP messages do not consume UDP queue memory"
                    );
                }
                let payload_bytes = payload.len();
                if self
                    .outputs
                    .tcp
                    .send((
                        addr,
                        TcpDispatch {
                            header,
                            payload,
                            end,
                        },
                    ))
                    .is_err()
                {
                    self.metrics.tcp_egress_messages_dropped.inc();
                    return Err(());
                }
                if payload_bytes > 0 {
                    self.metrics.tcp_pacing_chunks.inc();
                    self.metrics
                        .tcp_pacing_bytes
                        .add(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
                }
                priority.record_grant(&self.metrics, pacing_bytes, requested_at.elapsed());
            }
        }
        Ok(())
    }

    fn release_tcp(&mut self, addr: SocketAddr, bytes: usize) {
        let remove = if let Some(usage) = self.tcp_usage.get_mut(&addr) {
            usage.bytes -= bytes;
            usage.messages -= 1;
            usage.messages == 0
        } else {
            false
        };
        if remove {
            self.tcp_usage.remove(&addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNLIMITED: u64 = 1_000_000_000_000;

    #[derive(Debug, PartialEq, Eq)]
    struct Item {
        id: u64,
        bytes: usize,
    }

    impl PacingItem for Item {
        fn next_payload_bytes(&self) -> usize {
            self.bytes
        }
    }

    fn rate(bytes_per_second: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes_per_second).unwrap()
    }

    fn key(port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port)
    }

    fn item(id: u64, bytes: usize) -> Item {
        Item { id, bytes }
    }

    fn queue(global: u64, peer: u64) -> PacingQueue<Item> {
        PacingQueue::new(
            rate(global),
            rate(peer),
            usize::MAX,
            DataplaneMetrics::new(),
        )
    }

    #[test]
    fn one_peer_never_exceeds_its_rate() {
        let mut queue = queue(UNLIMITED, 1_000);
        for id in 0..3 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            0
        );
        assert!(queue
            .dequeue(Duration::from_millis(127), usize::MAX)
            .is_none());
        assert_eq!(
            queue
                .dequeue(Duration::from_millis(128), usize::MAX)
                .unwrap()
                .item
                .id,
            1
        );
        assert!(queue
            .dequeue(Duration::from_millis(255), usize::MAX)
            .is_none());
        assert_eq!(
            queue
                .dequeue(Duration::from_millis(256), usize::MAX)
                .unwrap()
                .item
                .id,
            2
        );
    }

    #[test]
    fn all_peers_share_the_global_rate() {
        let mut queue = queue(1_000, UNLIMITED);
        for id in 0..3 {
            queue
                .enqueue(key(id as u16), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        let service_at: Vec<_> = (0..3)
            .map(|_| {
                queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
                queue.service_at
            })
            .collect();
        assert_eq!(
            service_at,
            [
                Duration::from_millis(128),
                Duration::from_millis(256),
                Duration::from_millis(384),
            ]
        );
    }

    #[test]
    fn global_clock_catches_up_within_window() {
        let mut queue = queue(1_000, UNLIMITED);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
            .unwrap();
        queue
            .enqueue(key(2), UdpPriority::Regular, item(2, 100), 100)
            .unwrap();

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(128));
        assert_eq!(
            queue.next_wakeup(Duration::from_millis(133)),
            Some(Duration::from_millis(133))
        );
        queue
            .dequeue(Duration::from_millis(133), usize::MAX)
            .unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(256));
    }

    #[test]
    fn global_clock_clamps_beyond_catch_up_window() {
        let mut queue = queue(1_000, UNLIMITED);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
            .unwrap();
        queue
            .enqueue(key(2), UdpPriority::Regular, item(2, 100), 100)
            .unwrap();

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(128));
        assert_eq!(
            queue.next_wakeup(Duration::from_millis(134)),
            Some(Duration::from_millis(134))
        );
        queue
            .dequeue(Duration::from_millis(134), usize::MAX)
            .unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(262));
    }

    #[test]
    fn peer_clock_catches_up_within_window() {
        let mut queue = queue(UNLIMITED, 1_000);
        for id in 0..2 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        queue
            .dequeue(Duration::from_millis(133), usize::MAX)
            .unwrap();
        assert_eq!(
            queue.next_peers.peek().unwrap().0.next_at,
            Duration::from_millis(256)
        );
    }

    #[test]
    fn cooling_peer_preserves_global_catch_up_within_window() {
        let mut queue = queue(1_000, 1_000);
        for id in 0..2 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert!(queue.ready.is_empty());
        queue
            .dequeue(Duration::from_millis(133), usize::MAX)
            .unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(256));
    }

    #[test]
    fn peer_clock_clamps_beyond_catch_up_window() {
        let mut queue = queue(UNLIMITED, 1_000);
        for id in 0..2 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        queue
            .dequeue(Duration::from_millis(134), usize::MAX)
            .unwrap();
        assert_eq!(
            queue.next_peers.peek().unwrap().0.next_at,
            Duration::from_millis(262)
        );
    }

    #[test]
    fn next_wakeup_tracks_peer_deadline_and_tombstone() {
        let mut queue = queue(2_000, 1_000);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(0, 100), 100)
            .unwrap();
        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
            .unwrap();

        assert_eq!(queue.next_wakeup(Duration::ZERO), Some(Duration::ZERO));
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            0
        );

        let next_deadline = Duration::from_millis(128);
        assert_eq!(queue.next_wakeup(Duration::ZERO), Some(next_deadline));
        assert_eq!(queue.dequeue(next_deadline, usize::MAX).unwrap().item.id, 1);

        let expiry_deadline = Duration::from_millis(256);
        assert_eq!(queue.next_wakeup(next_deadline), Some(expiry_deadline));
        assert!(queue.dequeue(expiry_deadline, usize::MAX).is_none());
        assert_eq!(queue.next_wakeup(expiry_deadline), None);
        assert!(queue.peers.is_empty());
        assert!(queue.next_peers.is_empty());
    }

    #[test]
    fn new_peer_wakes_scheduler_before_cooling_peer() {
        let mut queue = queue(2_000, 1_000);
        for id in 0..2 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }
        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(
            queue.next_wakeup(Duration::from_millis(70)),
            Some(Duration::from_millis(128))
        );

        queue
            .enqueue(key(2), UdpPriority::High, item(2, 100), 100)
            .unwrap();
        assert_eq!(
            queue.next_wakeup(Duration::from_millis(70)),
            Some(Duration::from_millis(70))
        );
        assert_eq!(
            queue
                .dequeue(Duration::from_millis(70), usize::MAX)
                .unwrap()
                .item
                .id,
            2
        );
    }

    #[test]
    fn promoted_messages_remain_in_global_queue() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 1), 1)
            .unwrap();
        queue
            .enqueue(key(2), UdpPriority::Regular, item(2, 1), 1)
            .unwrap();
        assert!(queue.dequeue(Duration::ZERO, 0).is_none());
        queue
            .enqueue(key(1), UdpPriority::High, item(3, 1), 1)
            .unwrap();

        assert_eq!(queue.ready.len(), 2);
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            1
        );
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            3
        );
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            2
        );
    }

    #[test]
    fn equal_priority_is_fifo() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        for id in 0..10 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, 1), 1)
                .unwrap();
        }
        let items: Vec<_> = (0..10)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id)
            .collect();
        assert_eq!(items, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn high_priority_peer_drains_when_it_can_fill_global_capacity() {
        let mut queue = queue(1_000, 1_000);
        for id in 0..4 {
            queue
                .enqueue(key(1), UdpPriority::High, item(id, 100), 100)
                .unwrap();
        }
        for id in 100..104 {
            queue
                .enqueue(key(2), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        let observed: Vec<_> = (0..8)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id)
            .collect();
        assert_eq!(observed, [0, 1, 2, 3, 100, 101, 102, 103]);
    }

    #[test]
    fn regular_peer_fills_capacity_unused_by_high_priority_peer() {
        let mut queue = queue(2_000, 1_000);
        for id in 0..4 {
            queue
                .enqueue(key(1), UdpPriority::High, item(id, 100), 100)
                .unwrap();
        }
        for id in 100..104 {
            queue
                .enqueue(key(2), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        let observed: Vec<_> = (0..8)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id)
            .collect();
        assert_eq!(observed, [0, 100, 1, 101, 2, 102, 3, 103]);
    }

    #[test]
    fn equal_priority_peers_preserve_global_submission_order_when_eligible() {
        let mut queue = queue(1_000, 1_000);
        for id in 0..4 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }
        for id in 100..104 {
            queue
                .enqueue(key(2), UdpPriority::Regular, item(id, 100), 100)
                .unwrap();
        }

        let observed: Vec<_> = (0..8)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id)
            .collect();
        assert_eq!(observed, [0, 1, 2, 3, 100, 101, 102, 103]);
    }

    #[test]
    fn total_memory_limit_rejects_messages() {
        let mut queue =
            PacingQueue::new(rate(UNLIMITED), rate(UNLIMITED), 2, DataplaneMetrics::new());
        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 1), 1)
            .unwrap();
        queue
            .enqueue(key(1), UdpPriority::Regular, item(2, 1), 1)
            .unwrap();
        let error = queue
            .enqueue(key(2), UdpPriority::High, item(3, 1), 1)
            .unwrap_err();
        assert_eq!(error, EnqueueError::MemoryLimit(item(3, 1)));
        assert_eq!(queue.memory_used, 2);
        assert_eq!(queue.metrics.udp_pacing_queued_bytes.get(), 2);
        assert_eq!(queue.metrics.udp_pacing_memory_limit_bytes.get(), 2);
        assert_eq!(queue.metrics.udp_pacing_memory_limit_drops.get(), 1);
        assert_eq!(queue.metrics.udp_egress_messages_dropped.get(), 1);

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        queue
            .enqueue(key(2), UdpPriority::High, item(3, 1), 1)
            .unwrap();
    }

    #[test]
    fn peer_count_is_metered() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        for peer in 1..=3 {
            queue
                .enqueue(key(peer), UdpPriority::Regular, item(peer.into(), 1), 1)
                .unwrap();
        }
        assert_eq!(queue.metrics.udp_pacing_peers.get(), 3);

        for _ in 0..3 {
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        }
        let end = Duration::MAX;
        assert!(queue.dequeue(end, usize::MAX).is_none());
        assert_eq!(queue.metrics.udp_pacing_peers.get(), 0);
        assert!(queue.peers.is_empty());
        assert!(queue.next_peers.is_empty());
    }

    #[test]
    fn variable_payload_sizes_advance_both_clocks() {
        let mut queue = queue(1_000, 1_000);
        for (id, bytes) in [(1, 100), (2, 200), (3, 50)] {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, bytes), bytes)
                .unwrap();
        }
        let selected: Vec<_> = (0..3)
            .map(|_| {
                let item = queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
                (item.item.id, queue.service_at)
            })
            .collect();
        assert_eq!(
            selected,
            [
                (1, Duration::from_millis(128)),
                (2, Duration::from_millis(356)),
                (3, Duration::from_millis(434)),
            ]
        );
    }

    #[test]
    fn empty_peer_state_expires_under_address_churn() {
        let mut queue = queue(UNLIMITED, UNLIMITED);

        for port in 1..1_001 {
            queue
                .enqueue(key(port), UdpPriority::Regular, item(port.into(), 1), 1)
                .unwrap();
            assert_eq!(
                queue.dequeue(Duration::ZERO, 1).unwrap().item.id,
                u64::from(port)
            );
        }
        let now = queue.elapsed();
        assert!(queue.dequeue(now, usize::MAX).is_none());
        assert!(queue.peers.len() <= 2);
    }

    #[test]
    fn messages_exist_in_exactly_one_queue() {
        let mut queue = queue(UNLIMITED, 1_000);
        for port in 1..=100 {
            queue
                .enqueue(key(port), UdpPriority::Regular, item(port.into(), 100), 100)
                .unwrap();
        }
        for _ in 0..100 {
            let peer_messages: usize = queue
                .peers
                .values()
                .map(|peer| peer.borrow().messages.len())
                .sum();
            assert_eq!(queue.next_peers.len(), queue.peers.len());
            assert!(queue.peers.values().all(|peer| Rc::strong_count(peer) == 2));
            assert_eq!(peer_messages + queue.ready.len(), queue.len());
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        }
        let peer_messages: usize = queue
            .peers
            .values()
            .map(|peer| peer.borrow().messages.len())
            .sum();
        assert_eq!(queue.next_peers.len(), queue.peers.len());
        assert!(queue.peers.values().all(|peer| Rc::strong_count(peer) == 2));
        assert_eq!(peer_messages + queue.ready.len(), queue.len());
    }

    #[test]
    fn byte_limit_and_reenqueue() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(0, 100), 100)
            .unwrap();

        let first = queue.dequeue(Duration::ZERO, 120).unwrap();
        assert_eq!(first.item.id, 0);
        assert_eq!(queue.service_at, Duration::from_millis(128));
        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
            .unwrap();
        assert!(queue.dequeue(Duration::ZERO, 20).is_none());

        let second = queue.dequeue(Duration::ZERO, 120).unwrap();
        assert_eq!(second.item.id, 1);
        assert_eq!(queue.service_at, Duration::from_millis(256));
    }

    #[test]
    fn exhausted_ready_queue_keeps_peer_scheduled() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(0, 100), 100)
            .unwrap();
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            0
        );
        assert!(queue.dequeue(Duration::ZERO, usize::MAX).is_none());

        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
            .unwrap();
        let scheduled = queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(scheduled.item.id, 1);
        assert_eq!(queue.service_at, Duration::from_millis(256));
    }

    fn task_config(bytes_per_second: u64) -> PacingTaskConfig {
        PacingTaskConfig {
            global_bytes_per_second: NonZeroU64::new(bytes_per_second).unwrap(),
            udp: UdpPacingConfig {
                peer_bandwidth_mbps: 1_000,
                max_queued_bytes: 1024 * 1024,
            },
            udp_workers: 1,
        }
    }

    #[test]
    fn udp_peer_routing_is_balanced() {
        let peers = (0..10_000).map(|port| SocketAddr::from(([127, 0, 0, 1], port)));
        let mut counts = [0; 4];
        for peer in peers {
            let worker = udp_tx_worker(peer, counts.len());
            counts[worker] += 1;
        }
        assert!(counts
            .into_iter()
            .all(|count| (2_300..2_700).contains(&count)));
    }

    #[monoio::test(timer_enabled = true)]
    async fn udp_preempts_queued_tcp() {
        let (handle, task, mut outputs) =
            PacingHandle::new(task_config(1_000), DataplaneMetrics::new());
        let addr = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1).into();
        assert!(handle
            .enqueue_tcp(
                addr,
                TcpMsg {
                    msg: Bytes::from(vec![0_u8; UDP_DISPATCH_PACING_BYTES]),
                    completion: None,
                },
            )
            .is_ok());
        assert!(handle
            .enqueue_udp(UdpMsg {
                socket_id: UdpSocketId::Raptorcast,
                dst: addr,
                payload: Bytes::from(vec![0_u8; 100]),
                stride: 100,
                priority: UdpPriority::High,
            })
            .is_ok());
        monoio::spawn(task.run());

        let udp = monoio::time::timeout(Duration::from_millis(10), outputs.udp[0].recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(udp.sends.len(), 1);
        assert!(matches!(
            outputs.tcp.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[monoio::test(timer_enabled = true)]
    async fn full_udp_worker_drops_without_blocking_tcp() {
        let metrics = DataplaneMetrics::new();
        let mut config = task_config(1_000_000_000_000);
        config.udp.peer_bandwidth_mbps = 1_000_000_000;
        config.udp.max_queued_bytes =
            (UDP_WORKER_DISPATCH_CHANNEL_SIZE + 1) * UDP_DISPATCH_PACING_BYTES;
        let (handle, task, mut outputs) = PacingHandle::new(config, metrics.clone());
        for port in 1..=u16::try_from(UDP_WORKER_DISPATCH_CHANNEL_SIZE + 1).unwrap() {
            assert!(handle
                .enqueue_udp(UdpMsg {
                    socket_id: UdpSocketId::Raptorcast,
                    dst: SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port).into(),
                    payload: Bytes::from(vec![0_u8; UDP_DISPATCH_PACING_BYTES]),
                    stride: 1_200,
                    priority: UdpPriority::High,
                })
                .is_ok());
        }
        let tcp_addr = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 11).into();
        assert!(handle
            .enqueue_tcp(
                tcp_addr,
                TcpMsg {
                    msg: Bytes::from_static(&[1]),
                    completion: None,
                },
            )
            .is_ok());
        monoio::spawn(task.run());

        let (addr, _) = monoio::time::timeout(Duration::from_millis(10), outputs.tcp.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(addr, tcp_addr);
        assert!(metrics.udp_tx_worker_channel_messages_dropped.get() > 0);
        assert!(metrics.udp_tx_worker_channel_bytes_dropped.get() > 0);
    }

    #[monoio::test(timer_enabled = true)]
    async fn tcp_message_is_dispatched_in_ordered_chunks() {
        let (handle, task, mut outputs) =
            PacingHandle::new(task_config(1_000_000_000_000), DataplaneMetrics::new());
        let addr = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1).into();
        let message_len = TCP_DISPATCH_PAYLOAD_BYTES + 7;
        assert!(handle
            .enqueue_tcp(
                addr,
                TcpMsg {
                    msg: Bytes::from(vec![0_u8; message_len]),
                    completion: None,
                },
            )
            .is_ok());
        monoio::spawn(task.run());

        let (_, first) = monoio::time::timeout(Duration::from_millis(10), outputs.tcp.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(first.header.is_some());
        assert_eq!(first.payload.len(), TCP_DISPATCH_PAYLOAD_BYTES);
        assert!(first.end.is_none());

        let (_, second) = monoio::time::timeout(Duration::from_millis(10), outputs.tcp.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(second.header.is_none());
        assert_eq!(second.payload.len(), 7);
        assert_eq!(second.end.unwrap().message_len, message_len);
    }
}
