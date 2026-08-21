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

//! Shared-global and single-owner per-peer packet pacing.
//!
//! Peer state is keyed directly by [`SocketAddrV4`]. Each peer has exactly one
//! scheduler entry: a time-ordered entry while waiting or cooling down, or a
//! priority-ordered entry while ready.

use std::{
    cell::RefCell,
    cmp::{Ordering, Reverse},
    collections::{BTreeSet, BinaryHeap, HashMap},
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchLimits {
    pub max_bytes: usize,
    pub max_items: usize,
}

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
pub(crate) struct GlobalPacer<A = AtomicU64, C = Instant> {
    bytes_per_second: NonZeroU64,
    next_byte: A,
    clock: C,
}

#[doc(hidden)]
pub trait AtomicCounter {
    fn load_relaxed(&self) -> u64;
    fn compare_exchange_weak_relaxed(&self, current: u64, new: u64) -> Result<u64, u64>;
}

#[doc(hidden)]
pub trait PacerClock {
    fn elapsed(&self) -> Duration;
}

impl PacerClock for Instant {
    fn elapsed(&self) -> Duration {
        Instant::elapsed(self)
    }
}

impl AtomicCounter for AtomicU64 {
    fn load_relaxed(&self) -> u64 {
        self.load(AtomicOrdering::Relaxed)
    }

    fn compare_exchange_weak_relaxed(&self, current: u64, new: u64) -> Result<u64, u64> {
        self.compare_exchange_weak(
            current,
            new,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        )
    }
}

impl GlobalPacer {
    pub(crate) fn new(bytes_per_second: NonZeroU64) -> Self {
        Self::with_components(bytes_per_second, AtomicU64::new(0), Instant::now())
    }
}

impl<A: AtomicCounter, C: PacerClock> GlobalPacer<A, C> {
    pub(crate) fn with_components(bytes_per_second: NonZeroU64, next_byte: A, clock: C) -> Self {
        Self {
            bytes_per_second,
            next_byte,
            clock,
        }
    }

    fn reserve_at(&self, bytes: usize, now: Duration) -> Duration {
        let now_byte = (now
            .as_nanos()
            .saturating_mul(self.bytes_per_second.get() as u128)
            / Duration::from_secs(1).as_nanos())
        .min(u64::MAX as u128) as u64;
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let mut next = self.next_byte.load_relaxed();
        let start = loop {
            let start = next.max(now_byte);
            match self
                .next_byte
                .compare_exchange_weak_relaxed(next, start.saturating_add(bytes))
            {
                Ok(_) => break start,
                Err(current) => next = current,
            }
        };
        let nanos = (start as u128 * Duration::from_secs(1).as_nanos())
            .div_ceil(self.bytes_per_second.get() as u128);
        Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
    }
}

/// One item reserved for the current batch.
#[derive(Debug)]
pub struct Scheduled<T> {
    at: Duration,
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
    next_at: Duration,
    messages: BinaryHeap<Queued<T>>,
    ready_rank: Option<ReadyRank>,
}

impl<T> PeerState<T> {
    fn ready_rank(&self) -> ReadyRank {
        let message = self
            .messages
            .peek()
            .expect("ready peer must have a message");
        ReadyRank {
            priority: Reverse(message.priority),
            at: Reverse(self.next_at),
            order: Reverse(message.order),
        }
    }
}

type Peer<T> = Rc<RefCell<PeerState<T>>>;

struct SchedulerEntry<R, T> {
    rank: R,
    key: SocketAddrV4,
    peer: Peer<T>,
}

impl<R: PartialEq, T> PartialEq for SchedulerEntry<R, T> {
    fn eq(&self, other: &Self) -> bool {
        self.rank == other.rank && self.key == other.key
    }
}

impl<R: Eq, T> Eq for SchedulerEntry<R, T> {}

impl<R: Ord, T> PartialOrd for SchedulerEntry<R, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<R: Ord, T> Ord for SchedulerEntry<R, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank
            .cmp(&other.rank)
            .then_with(|| self.key.cmp(&other.key))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReadyRank {
    priority: Reverse<UdpPriority>,
    at: Reverse<Duration>,
    order: Reverse<u64>,
}

type WaitingEntry<T> = SchedulerEntry<Duration, T>;
type ReadyEntry<T> = SchedulerEntry<ReadyRank, T>;

/// Socket-address keyed queue with uniform per-peer and shared global pacing.
pub struct PacingQueue<T, A = AtomicU64, C = Instant> {
    peers: HashMap<SocketAddrV4, Peer<T>>,
    // separate indexes are required: waiting peers use time ordering, while ready peers use priority and fifo ordering.
    waiting: BinaryHeap<Reverse<WaitingEntry<T>>>,
    ready: BTreeSet<ReadyEntry<T>>,
    global: Arc<GlobalPacer<A, C>>,
    peer_bytes_per_second: NonZeroU64,
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
}

impl<T: PacingItem, A: AtomicCounter, C: PacerClock> PacingQueue<T, A, C> {
    pub(crate) fn with_global_pacer(
        global: Arc<GlobalPacer<A, C>>,
        peer_bytes_per_second: NonZeroU64,
        memory_limit: usize,
        metrics: DataplaneMetrics,
    ) -> Self {
        Self {
            peers: HashMap::new(),
            waiting: BinaryHeap::new(),
            ready: BTreeSet::new(),
            global,
            peer_bytes_per_second,
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

    pub(crate) fn elapsed(&self) -> Duration {
        self.global.clock.elapsed()
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

        if let Some(peer) = self.peers.get(&key).cloned() {
            let old_rank = peer.borrow().ready_rank;
            if let Some(rank) = old_rank {
                let removed = self.ready.remove(&ReadyEntry {
                    rank,
                    key,
                    peer: Rc::clone(&peer),
                });
                debug_assert!(removed);
            }
            let new_rank = {
                let mut state = peer.borrow_mut();
                state.messages.push(queued);
                old_rank.map(|_| {
                    let rank = state.ready_rank();
                    state.ready_rank = Some(rank);
                    rank
                })
            };
            if let Some(rank) = new_rank {
                let inserted = self.ready.insert(ReadyEntry { rank, key, peer });
                debug_assert!(inserted);
            }
        } else {
            let peer = Rc::new(RefCell::new(PeerState {
                next_at: Duration::ZERO,
                messages: BinaryHeap::from([queued]),
                ready_rank: None,
            }));
            self.peers.insert(key, Rc::clone(&peer));
            self.metrics.udp_pacing_peers.inc();
            self.waiting.push(Reverse(WaitingEntry {
                rank: Duration::ZERO,
                key,
                peer,
            }));
        }

        self.next_order = self.next_order.wrapping_add(1);
        self.memory_used += queued_bytes;
        self.pending += 1;
        self.metrics
            .udp_pacing_queued_bytes
            .add(u64::try_from(queued_bytes).unwrap_or(u64::MAX));
        Ok(())
    }

    pub fn batch(&mut self, limits: BatchLimits) -> Batch<'_, T, A, C> {
        let now = self.global.clock.elapsed();
        self.batch_at(now, limits)
    }

    fn batch_at(&mut self, now: Duration, limits: BatchLimits) -> Batch<'_, T, A, C> {
        Batch {
            queue: self,
            now,
            limits,
            bytes: 0,
            items: 0,
            deadline: now,
        }
    }

    fn dequeue(&mut self, now: Duration, max_batch_bytes: usize) -> Option<Scheduled<T>> {
        let reclaim_before = now;
        let mut slot = now;

        loop {
            let next_at = self.promote_ready(slot, reclaim_before);
            if !self.ready.is_empty() {
                break;
            }
            slot = slot.max(next_at?);
        }

        let next_bytes = {
            let peer = self.ready.last()?.peer.borrow();
            peer.messages.peek()?.item.next_payload_bytes()
        };
        if next_bytes > max_batch_bytes {
            return None;
        }
        let entry = self.ready.pop_last().expect("ready entry must exist");
        let mut peer = entry.peer.borrow_mut();
        let ready_rank = peer.ready_rank.take();
        debug_assert_eq!(ready_rank, Some(entry.rank));
        let queued = peer.messages.pop().expect("ready peer must have a message");
        let launch = slot.max(peer.next_at);
        let wire_bytes = Self::wire_bytes(next_bytes);
        peer.next_at = Self::advance(launch, wire_bytes, self.peer_bytes_per_second);
        let next_at = peer.next_at;
        drop(peer);
        self.waiting.push(Reverse(WaitingEntry {
            rank: next_at,
            key: entry.key,
            peer: entry.peer,
        }));

        self.memory_used -= queued.queued_bytes;
        self.pending -= 1;
        self.metrics
            .udp_pacing_queued_bytes
            .sub(u64::try_from(queued.queued_bytes).unwrap_or(u64::MAX));
        Some(Scheduled {
            at: launch,
            item: queued.item,
            batch_bytes: next_bytes,
        })
    }

    fn promote_ready(&mut self, slot: Duration, reclaim_before: Duration) -> Option<Duration> {
        let mut cooling = Vec::new();
        let mut reclaimed = 0_u64;
        let next_at = loop {
            let Some(entry) = self.waiting.peek() else {
                break None;
            };
            if entry.0.rank > slot && !self.ready.is_empty() {
                break None;
            }
            let entry = self.waiting.pop().expect("peeked waiting entry").0;
            let empty = {
                let peer = entry.peer.borrow();
                debug_assert!(peer.ready_rank.is_none());
                peer.messages.is_empty()
            };
            if empty {
                if entry.rank <= reclaim_before {
                    self.peers.remove(&entry.key);
                    reclaimed += 1;
                } else {
                    cooling.push(entry);
                }
            } else if entry.rank > slot {
                let next_at = entry.rank;
                self.waiting.push(Reverse(entry));
                break Some(next_at);
            } else {
                let rank = {
                    let mut peer = entry.peer.borrow_mut();
                    let rank = peer.ready_rank();
                    peer.ready_rank = Some(rank);
                    rank
                };
                let inserted = self.ready.insert(ReadyEntry {
                    rank,
                    key: entry.key,
                    peer: entry.peer,
                });
                debug_assert!(inserted);
            }
        };
        self.waiting.extend(cooling.into_iter().map(Reverse));
        if reclaimed != 0 {
            self.metrics.udp_pacing_peers.sub(reclaimed);
        }
        next_at
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

pub struct Batch<'a, T, A = AtomicU64, C = Instant> {
    queue: &'a mut PacingQueue<T, A, C>,
    now: Duration,
    limits: BatchLimits,
    bytes: usize,
    items: usize,
    deadline: Duration,
}

impl<T: PacingItem, A: AtomicCounter, C: PacerClock> Iterator for Batch<'_, T, A, C> {
    type Item = Scheduled<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.items >= self.limits.max_items || self.bytes >= self.limits.max_bytes {
            return None;
        }
        let scheduled = self
            .queue
            .dequeue(self.now, self.limits.max_bytes - self.bytes)?;
        if self.items == 0 {
            self.deadline = scheduled.at;
        }
        self.bytes += scheduled.batch_bytes;
        self.items += 1;
        Some(scheduled)
    }
}

impl<T: PacingItem, A: AtomicCounter, C: PacerClock> Batch<'_, T, A, C> {
    pub fn reenqueue(
        &mut self,
        key: SocketAddrV4,
        priority: UdpPriority,
        item: T,
        queued_bytes: usize,
    ) -> Result<(), EnqueueError<T>> {
        self.queue.enqueue(key, priority, item, queued_bytes)
    }

    pub const fn items(&self) -> usize {
        self.items
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn schedule(self) -> Duration {
        if self.items == 0 {
            return self.deadline;
        }
        let wire_bytes = self.bytes.saturating_add(
            self.items
                .saturating_mul(usize::from(IPV4_HDR_SIZE + UDP_HDR_SIZE)),
        );
        self.queue.global.reserve_at(wire_bytes, self.deadline)
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

        let launches: Vec<_> = (0..3)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().at)
            .collect();
        assert_eq!(
            launches,
            [
                Duration::ZERO,
                Duration::from_millis(128),
                Duration::from_millis(256),
            ]
        );
    }

    #[test]
    fn all_queues_share_the_global_rate() {
        let global = GlobalPacer::new(rate(1_000));
        let launches: Vec<_> = (0..3)
            .map(|_| global.reserve_at(128, Duration::ZERO))
            .collect();
        assert_eq!(
            launches,
            [
                Duration::ZERO,
                Duration::from_millis(128),
                Duration::from_millis(256),
            ]
        );
    }

    #[test]
    fn idle_global_reservation_starts_at_current_time() {
        let global = GlobalPacer::new(rate(1_000));
        assert_eq!(
            global.reserve_at(128, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        assert_eq!(
            global.reserve_at(128, Duration::ZERO),
            Duration::from_millis(1_128)
        );
    }

    #[test]
    fn priority_is_global_within_queue_and_updates_in_place() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 1), 1)
            .unwrap();
        queue
            .enqueue(key(2), UdpPriority::Regular, item(2, 1), 1)
            .unwrap();
        let next_at = queue.promote_ready(Duration::ZERO, Duration::ZERO);
        assert!(next_at.is_none());
        queue
            .enqueue(key(1), UdpPriority::High, item(3, 1), 1)
            .unwrap();

        assert_eq!(queue.ready.len(), 2);
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            3
        );
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            2
        );
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            1
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
        assert!(queue.promote_ready(end, end).is_none());
        assert_eq!(queue.metrics.udp_pacing_peers.get(), 0);
    }

    #[test]
    fn variable_wire_costs_advance_peer_clock() {
        let mut queue = queue(1_000, 1_000);
        for (id, bytes) in [(1, 100), (2, 200), (3, 50)] {
            queue
                .enqueue(key(1), UdpPriority::Regular, item(id, bytes), bytes)
                .unwrap();
        }
        let selected: Vec<_> = (0..3)
            .map(|_| {
                let item = queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
                (item.item.id, item.at)
            })
            .collect();
        assert_eq!(
            selected,
            [
                (1, Duration::ZERO),
                (2, Duration::from_millis(128)),
                (3, Duration::from_millis(356)),
            ]
        );
    }

    #[test]
    fn fractional_time_does_not_accumulate_rounding_drift() {
        const SENDS: usize = 100_000;
        let bytes_per_second = rate(375_000);
        let global = GlobalPacer::new(bytes_per_second);

        for _ in 0..SENDS {
            global.reserve_at(100, Duration::ZERO);
        }

        assert_eq!(
            global.next_byte.load(AtomicOrdering::Relaxed),
            100 * SENDS as u64
        );
        let expected = Duration::from_nanos(
            (100_u128 * Duration::from_secs(1).as_nanos() * SENDS as u128)
                .div_ceil(bytes_per_second.get() as u128) as u64,
        );
        assert_eq!(global.reserve_at(0, Duration::ZERO), expected);
    }

    #[test]
    fn concurrent_queues_reserve_disjoint_global_slots() {
        let global = Arc::new(GlobalPacer::new(rate(1_000)));
        let mut launches = std::thread::scope(|scope| {
            (0..8)
                .map(|_| {
                    let global = Arc::clone(&global);
                    scope.spawn(move || global.reserve_at(100, Duration::ZERO))
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        launches.sort_unstable();
        assert_eq!(
            launches,
            (0..8)
                .map(|slot| Duration::from_millis(slot * 100))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_peer_state_expires_under_address_churn() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        let limits = BatchLimits {
            max_bytes: 1,
            max_items: 1,
        };

        for port in 1..1_001 {
            queue
                .enqueue(key(port), UdpPriority::Regular, item(port.into(), 1), 1)
                .unwrap();
            assert_eq!(
                queue
                    .batch_at(Duration::ZERO, limits)
                    .next()
                    .unwrap()
                    .item
                    .id,
                u64::from(port)
            );
        }
        let now = queue.elapsed();
        assert!(queue.promote_ready(now, now).is_none());
        assert!(queue.peers.len() <= 2);
    }

    #[test]
    fn one_scheduler_entry_per_peer() {
        let mut queue = queue(UNLIMITED, 1_000);
        for port in 1..=100 {
            queue
                .enqueue(key(port), UdpPriority::Regular, item(port.into(), 100), 100)
                .unwrap();
        }
        for _ in 0..100 {
            assert_eq!(queue.waiting.len() + queue.ready.len(), queue.peers.len());
            assert!(queue.peers.values().all(|peer| Rc::strong_count(peer) == 2));
            assert!(queue
                .waiting
                .iter()
                .all(|entry| entry.0.peer.borrow().ready_rank.is_none()));
            assert!(queue
                .ready
                .iter()
                .all(|entry| entry.peer.borrow().ready_rank == Some(entry.rank)));
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        }
        assert_eq!(queue.waiting.len() + queue.ready.len(), queue.peers.len());
    }

    #[test]
    fn batch_bounds_and_reenqueue() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(0, 100), 100)
            .unwrap();

        {
            let mut batch = queue.batch_at(
                Duration::ZERO,
                BatchLimits {
                    max_bytes: 120,
                    max_items: 128,
                },
            );
            let first = batch.next().unwrap();
            assert_eq!((first.item.id, first.at), (0, Duration::ZERO));
            batch
                .reenqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
                .unwrap();
            assert!(batch.next().is_none());
            assert_eq!((batch.items(), batch.bytes()), (1, 100));
        }

        let second = queue
            .batch_at(
                Duration::ZERO,
                BatchLimits {
                    max_bytes: 120,
                    max_items: 128,
                },
            )
            .next()
            .unwrap();
        assert_eq!((second.item.id, second.at), (1, Duration::from_millis(128)));
    }

    #[test]
    fn exhausted_batch_keeps_cooling_peer_scheduled() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), UdpPriority::Regular, item(0, 100), 100)
            .unwrap();
        let limits = BatchLimits {
            max_bytes: usize::MAX,
            max_items: usize::MAX,
        };

        {
            let mut batch = queue.batch_at(Duration::ZERO, limits);
            assert_eq!(batch.next().unwrap().item.id, 0);
            assert!(batch.next().is_none());
        }

        queue
            .enqueue(key(1), UdpPriority::Regular, item(1, 100), 100)
            .unwrap();
        let scheduled = queue.batch_at(Duration::ZERO, limits).next().unwrap();
        assert_eq!(
            (scheduled.item.id, scheduled.at),
            (1, Duration::from_millis(128))
        );
    }
}

#[cfg(test)]
mod loom_tests {
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        num::NonZeroU64,
        sync::Arc as StdArc,
        time::Duration,
    };

    use loom::{
        sync::{
            atomic::{AtomicU64, Ordering},
            mpsc, Arc,
        },
        thread,
    };
    use monad_types::UdpPriority;

    use super::{
        AtomicCounter, BatchLimits, GlobalPacer, PacerClock, PacingQueue, IPV4_HDR_SIZE,
        UDP_HDR_SIZE,
    };
    use crate::DataplaneMetrics;

    const GLOBAL_MBPS: u64 = 1_000;
    const BITS_PER_BYTE: u64 = 8;
    const TICK: Duration = Duration::from_micros(100);
    const TICKS: u32 = 3;
    const MAX_DATAGRAM_WIRE_BYTES: usize = 1_250;

    #[derive(Clone)]
    struct ModelClock {
        nanos: Arc<AtomicU64>,
    }

    impl ModelClock {
        fn new() -> Self {
            Self {
                nanos: Arc::new(AtomicU64::new(0)),
            }
        }

        fn set(&self, now: Duration) {
            self.nanos.store(now.as_nanos() as u64, Ordering::SeqCst);
        }
    }

    impl PacerClock for ModelClock {
        fn elapsed(&self) -> Duration {
            Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
        }
    }

    impl AtomicCounter for AtomicU64 {
        fn load_relaxed(&self) -> u64 {
            self.load(Ordering::Relaxed)
        }

        fn compare_exchange_weak_relaxed(&self, current: u64, new: u64) -> Result<u64, u64> {
            self.compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed)
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct Transmission {
        at: Duration,
        wire_bytes: usize,
    }

    fn rate(bytes_per_second: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes_per_second).unwrap()
    }

    fn pacer(clock: ModelClock) -> StdArc<GlobalPacer<AtomicU64, ModelClock>> {
        let bytes_per_second = NonZeroU64::new(GLOBAL_MBPS * 1_000_000 / BITS_PER_BYTE).unwrap();
        StdArc::new(GlobalPacer::with_components(
            bytes_per_second,
            AtomicU64::new(0),
            clock,
        ))
    }

    fn wire_bytes_per_tick(mbps: u64) -> usize {
        (u128::from(mbps) * 1_000_000 * TICK.as_nanos()
            / u128::from(BITS_PER_BYTE)
            / Duration::from_secs(1).as_nanos()) as usize
    }

    fn transmission_time(wire_bytes: usize) -> Duration {
        let bytes_per_second = u128::from(GLOBAL_MBPS * 1_000_000 / BITS_PER_BYTE);
        let nanos =
            (wire_bytes as u128 * Duration::from_secs(1).as_nanos()).div_ceil(bytes_per_second);
        Duration::from_nanos(nanos as u64)
    }

    fn observed_mbps(wire_bytes: usize, elapsed: Duration) -> u64 {
        let bits = wire_bytes as u128 * u128::from(BITS_PER_BYTE);
        (bits * Duration::from_secs(1).as_nanos() / elapsed.as_nanos() / 1_000_000) as u64
    }

    fn enqueue_work(
        queue: &mut PacingQueue<Vec<u8>, AtomicU64, ModelClock>,
        peer: SocketAddrV4,
        mut wire_bytes: usize,
    ) -> (usize, usize) {
        let header_bytes = usize::from(IPV4_HDR_SIZE + UDP_HDR_SIZE);
        let mut items = 0;
        let mut payload_bytes = 0;
        while wire_bytes != 0 {
            let datagram_wire_bytes = wire_bytes.min(MAX_DATAGRAM_WIRE_BYTES);
            let datagram_payload_bytes = datagram_wire_bytes - header_bytes;
            queue
                .enqueue(
                    peer,
                    UdpPriority::Regular,
                    vec![0; datagram_payload_bytes],
                    datagram_payload_bytes,
                )
                .unwrap();
            items += 1;
            payload_bytes += datagram_payload_bytes;
            wire_bytes -= datagram_wire_bytes;
        }
        (items, payload_bytes)
    }

    fn run_worker(
        worker: u16,
        global: StdArc<GlobalPacer<AtomicU64, ModelClock>>,
        work_rx: mpsc::Receiver<Option<usize>>,
        transmission_tx: mpsc::Sender<Transmission>,
    ) {
        let mut queue = PacingQueue::with_global_pacer(
            global,
            rate(u64::MAX),
            usize::MAX,
            DataplaneMetrics::new(),
        );
        let peer = SocketAddrV4::new(Ipv4Addr::LOCALHOST, worker);
        while let Some(wire_bytes) = work_rx.recv().unwrap() {
            let (expected_items, expected_payload_bytes) =
                enqueue_work(&mut queue, peer, wire_bytes);
            let deadline = {
                let mut batch = queue.batch(BatchLimits {
                    max_bytes: usize::MAX,
                    max_items: usize::MAX,
                });
                while batch.next().is_some() {}
                assert_eq!(batch.items(), expected_items);
                assert_eq!(batch.bytes(), expected_payload_bytes);
                batch.schedule()
            };
            transmission_tx
                .send(Transmission {
                    at: deadline,
                    wire_bytes,
                })
                .unwrap();
        }
    }

    fn model_worker_rates(worker_mbps: [u64; 2], expected_mbps: u64) {
        loom::model(move || {
            let clock = ModelClock::new();
            let global = pacer(clock.clone());
            let (transmission_tx, transmission_rx) = mpsc::channel();
            let mut work_txs = Vec::new();
            let mut workers = Vec::new();
            for worker in 0..2 {
                let (work_tx, work_rx) = mpsc::channel();
                work_txs.push(work_tx);
                let global = StdArc::clone(&global);
                let transmission_tx = transmission_tx.clone();
                workers.push(thread::spawn(move || {
                    run_worker(worker + 1, global, work_rx, transmission_tx)
                }));
            }
            drop(transmission_tx);

            let mut transmissions = Vec::new();
            for tick in 0..TICKS {
                clock.set(TICK * tick);
                for (work_tx, mbps) in work_txs.iter().zip(worker_mbps) {
                    work_tx.send(Some(wire_bytes_per_tick(mbps))).unwrap();
                }
                for _ in 0..workers.len() {
                    transmissions.push(transmission_rx.recv().unwrap());
                }
            }
            for work_tx in work_txs {
                work_tx.send(None).unwrap();
            }
            for worker in workers {
                worker.join().unwrap();
            }

            transmissions.sort_unstable_by_key(|transmission| transmission.at);
            let observation = TICK * TICKS;
            let mut previous_end = Duration::ZERO;
            let mut drained_wire_bytes = 0;
            for transmission in transmissions {
                assert!(transmission.at >= previous_end);
                let end = transmission.at + transmission_time(transmission.wire_bytes);
                previous_end = end;
                let elapsed = end.min(observation).saturating_sub(transmission.at);
                drained_wire_bytes +=
                    GLOBAL_MBPS as usize * 1_000_000 * elapsed.as_nanos() as usize
                        / BITS_PER_BYTE as usize
                        / Duration::from_secs(1).as_nanos() as usize;
            }
            assert_eq!(
                observed_mbps(drained_wire_bytes, observation),
                expected_mbps
            );
        });
    }

    #[test]
    fn saturated_uneven_worker_load_reaches_global_rate() {
        model_worker_rates([400, 700], GLOBAL_MBPS);
    }

    #[test]
    fn unsaturated_uneven_worker_load_stays_below_global_rate() {
        model_worker_rates([400, 550], 950);
    }
}
