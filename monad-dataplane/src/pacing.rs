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
//! Peer state is keyed directly by [`SocketAddrV4`] and ordered by its next
//! eligible send time. Eligible messages move through one global priority queue.

use std::{
    cell::RefCell,
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    net::SocketAddrV4,
    num::NonZeroU64,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_types::UdpPriority;

use crate::{metrics::DataplaneMetrics, IPV4_HDR_SIZE, UDP_HDR_SIZE};

const PACER_MAX_CATCH_UP: Duration = Duration::from_millis(50);

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueError<T> {
    MemoryLimit(T),
}

pub trait PacingItem {
    fn next_payload_bytes(&self) -> usize;
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

/// Global byte timeline shared by otherwise independent pacing queues.
pub(crate) struct GlobalPacer {
    bytes_per_second: NonZeroU64,
    next_byte: AtomicU64,
    epoch: Instant,
}

struct Reservation {
    end_byte: u64,
    end_at: Duration,
}

impl GlobalPacer {
    pub(crate) fn new(bytes_per_second: NonZeroU64) -> Self {
        Self {
            bytes_per_second,
            next_byte: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.epoch.elapsed()
    }

    fn byte_at(&self, at: Duration) -> u64 {
        (at.as_nanos()
            .saturating_mul(self.bytes_per_second.get() as u128)
            / Duration::from_secs(1).as_nanos())
        .min(u64::MAX as u128) as u64
    }

    fn duration_at(&self, byte: u64) -> Duration {
        let nanos = (byte as u128)
            .saturating_mul(Duration::from_secs(1).as_nanos())
            .div_ceil(self.bytes_per_second.get() as u128);
        Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
    }

    fn start_byte(&self, next_byte: u64, now: Duration, reset: bool) -> u64 {
        let next_at = self.duration_at(next_byte);
        if reset || now.saturating_sub(next_at) > PACER_MAX_CATCH_UP {
            next_byte.max(self.byte_at(now))
        } else {
            next_byte
        }
    }

    fn next_at(&self, now: Duration, reset: bool) -> Duration {
        let next_byte = self.next_byte.load(AtomicOrdering::Relaxed);
        self.duration_at(self.start_byte(next_byte, now, reset))
    }

    fn reserve(
        &self,
        bytes: usize,
        now: Duration,
        reset: bool,
        owner: Option<u64>,
    ) -> Result<Reservation, Duration> {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let mut next_byte = self.next_byte.load(AtomicOrdering::Relaxed);
        loop {
            let start_byte = self.start_byte(next_byte, now, reset);
            let start_at = self.duration_at(start_byte);
            if start_at > now && owner != Some(next_byte) {
                return Err(start_at);
            }
            let end_byte = start_byte.saturating_add(bytes);
            match self.next_byte.compare_exchange_weak(
                next_byte,
                end_byte,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(Reservation {
                        end_byte,
                        end_at: self.duration_at(end_byte),
                    });
                }
                Err(current) => next_byte = current,
            }
        }
    }
}

/// One item selected for transmission.
#[derive(Debug)]
pub struct Scheduled<T> {
    pub item: T,
    pub(crate) batch_bytes: usize,
}

struct Queued<T> {
    priority: UdpPriority,
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
}

type Peer<T> = Rc<RefCell<PeerState<T>>>;

struct PeerDeadline<T> {
    next_at: Duration,
    key: SocketAddrV4,
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
    peers: HashMap<SocketAddrV4, Peer<T>>,
    next_peers: BinaryHeap<Reverse<PeerDeadline<T>>>,
    ready: BinaryHeap<Queued<T>>,
    global: Arc<GlobalPacer>,
    peer_bytes_per_second: NonZeroU64,
    service_at: Duration,
    global_reservation: Option<u64>,
    memory_limit: usize,
    memory_used: usize,
    pending: usize,
    next_order: u64,
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
        Self::with_global_pacer(
            Arc::new(GlobalPacer::new(global_bytes_per_second)),
            peer_bytes_per_second,
            memory_limit,
            metrics,
        )
    }

    pub(crate) fn with_global_pacer(
        global: Arc<GlobalPacer>,
        peer_bytes_per_second: NonZeroU64,
        memory_limit: usize,
        metrics: DataplaneMetrics,
    ) -> Self {
        Self {
            peers: HashMap::new(),
            next_peers: BinaryHeap::new(),
            ready: BinaryHeap::new(),
            global,
            peer_bytes_per_second,
            service_at: Duration::ZERO,
            global_reservation: None,
            memory_limit,
            memory_used: 0,
            pending: 0,
            next_order: 0,
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
        self.global.elapsed()
    }

    pub fn next_wakeup(&self, now: Duration) -> Option<Duration> {
        let reset_global = self.ready.is_empty();
        let scheduled_at = if reset_global {
            self.service_at.max(self.next_peers.peek()?.0.next_at)
        } else {
            self.service_at
        };
        Some(
            scheduled_at
                .max(self.global.next_at(now, reset_global))
                .max(now),
        )
    }

    pub fn enqueue(
        &mut self,
        key: SocketAddrV4,
        priority: UdpPriority,
        item: T,
        queued_bytes: usize,
    ) -> Result<(), EnqueueError<T>> {
        if queued_bytes > self.memory_limit.saturating_sub(self.memory_used) {
            self.metrics.udp_egress_messages_dropped.inc();
            self.metrics.udp_pacing_memory_limit_drops.inc();
            return Err(EnqueueError::MemoryLimit(item));
        }
        let queued = Queued {
            priority,
            order: self.next_order,
            item,
            queued_bytes,
        };

        if let Some(peer) = self.peers.get(&key) {
            peer.borrow_mut().messages.push(queued);
        } else {
            let peer = Rc::new(RefCell::new(PeerState {
                messages: BinaryHeap::from([queued]),
            }));
            self.peers.insert(key, Rc::clone(&peer));
            self.next_peers.push(Reverse(PeerDeadline {
                next_at: self.service_at,
                key,
                peer,
            }));
            self.metrics.udp_pacing_peers.inc();
        }

        self.next_order = self.next_order.wrapping_add(1);
        self.memory_used += queued_bytes;
        self.pending += 1;
        self.metrics
            .udp_pacing_queued_bytes
            .add(u64::try_from(queued_bytes).unwrap_or(u64::MAX));
        Ok(())
    }

    pub fn dequeue(&mut self, now: Duration, max_bytes: usize) -> Option<Scheduled<T>> {
        // account for time spent on cpu work or while this thread was scheduled off-cpu.
        // if the ready queue is empty, catch up to now because no global work remains scheduled.
        // after a long pause, reset to now instead of accumulating unbounded catch-up allowance.
        let reset_global = self.ready.is_empty();
        if reset_global || now.saturating_sub(self.service_at) > PACER_MAX_CATCH_UP {
            self.service_at = self.service_at.max(now);
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
                self.metrics.udp_pacing_peers.dec();
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
                Self::wire_bytes(message.item.next_payload_bytes()),
                self.peer_bytes_per_second,
            );
            self.next_peers.push(Reverse(PeerDeadline {
                next_at,
                key: deadline.key,
                peer: deadline.peer,
            }));
            self.ready.push(message);
        }

        let queued = self.ready.peek()?;
        let next_bytes = queued.item.next_payload_bytes();
        if next_bytes > max_bytes {
            return None;
        }
        let wire_bytes = Self::wire_bytes(next_bytes);
        let local_end = Self::advance(self.service_at, wire_bytes, self.global.bytes_per_second);
        let reservation = self
            .global
            .reserve(wire_bytes, now, reset_global, self.global_reservation)
            .ok()?;
        self.service_at = local_end.max(reservation.end_at);
        self.global_reservation = Some(reservation.end_byte);
        let queued = self.ready.pop().expect("peeked ready message");

        self.memory_used -= queued.queued_bytes;
        self.pending -= 1;
        self.metrics
            .udp_pacing_queued_bytes
            .sub(u64::try_from(queued.queued_bytes).unwrap_or(u64::MAX));
        Some(Scheduled {
            item: queued.item,
            batch_bytes: next_bytes,
        })
    }

    fn wire_bytes(payload_bytes: usize) -> usize {
        payload_bytes.saturating_add(usize::from(IPV4_HDR_SIZE + UDP_HDR_SIZE))
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
    fn all_queues_share_the_global_rate() {
        let global = Arc::new(GlobalPacer::new(rate(1_000)));
        let mut first = PacingQueue::with_global_pacer(
            Arc::clone(&global),
            rate(UNLIMITED),
            usize::MAX,
            DataplaneMetrics::new(),
        );
        let mut second = PacingQueue::with_global_pacer(
            global,
            rate(UNLIMITED),
            usize::MAX,
            DataplaneMetrics::new(),
        );
        first
            .enqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
            .unwrap();
        second
            .enqueue(key(2), UdpPriority::Regular, item(2, 100), 100)
            .unwrap();

        first.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(first.service_at, Duration::from_millis(128));
        assert_eq!(
            second.next_wakeup(Duration::ZERO),
            Some(Duration::from_millis(128))
        );
        assert!(second.dequeue(Duration::ZERO, usize::MAX).is_none());
        second
            .dequeue(Duration::from_millis(128), usize::MAX)
            .unwrap();
        assert_eq!(second.service_at, Duration::from_millis(256));
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
            queue.next_wakeup(Duration::from_millis(178)),
            Some(Duration::from_millis(178))
        );
        queue
            .dequeue(Duration::from_millis(178), usize::MAX)
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
            queue.next_wakeup(Duration::from_millis(179)),
            Some(Duration::from_millis(179))
        );
        queue
            .dequeue(Duration::from_millis(179), usize::MAX)
            .unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(307));
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
            .dequeue(Duration::from_millis(178), usize::MAX)
            .unwrap();
        assert_eq!(
            queue.next_peers.peek().unwrap().0.next_at,
            Duration::from_millis(256)
        );
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
            .dequeue(Duration::from_millis(179), usize::MAX)
            .unwrap();
        assert_eq!(
            queue.next_peers.peek().unwrap().0.next_at,
            Duration::from_millis(307)
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
}
