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
//! Peer state is keyed directly by [`SocketAddr`]. Each peer has exactly one
//! scheduler entry: a time-ordered entry while waiting or cooling down, or an
//! indexed priority-ordered entry while ready.

use std::{
    cell::RefCell,
    cmp::{Ordering, Reverse},
    collections::{hash_map::Entry, BinaryHeap, HashMap},
    mem::size_of,
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    rc::Rc,
    time::{Duration, Instant},
};

use monad_types::UdpPriority;

const FRACTION_BITS: u32 = 32;
const TICKS_PER_NANOSECOND: u128 = 1_u128 << FRACTION_BITS;
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Maximum number of destination peers retained by a pacing queue.
pub const MAX_PEERS: usize = 65_536;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct FixedTime(u128);

impl FixedTime {
    fn from_duration(duration: Duration) -> Self {
        Self(duration.as_nanos().saturating_mul(TICKS_PER_NANOSECOND))
    }

    fn to_duration_ceil(self) -> Duration {
        let nanoseconds = self.0.saturating_add(TICKS_PER_NANOSECOND - 1) / TICKS_PER_NANOSECOND;
        let seconds = nanoseconds / NANOS_PER_SECOND;
        if seconds > u64::MAX as u128 {
            return Duration::MAX;
        }
        Duration::new(seconds as u64, (nanoseconds % NANOS_PER_SECOND) as u32)
    }

    fn advance(self, bytes: NonZeroUsize, bytes_per_second: NonZeroU64) -> Self {
        let ticks = (bytes.get() as u128)
            .saturating_mul(NANOS_PER_SECOND)
            .saturating_mul(TICKS_PER_NANOSECOND)
            / bytes_per_second.get() as u128;
        Self(self.0.saturating_add(ticks.max(1)))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueCost {
    pub wire_bytes: NonZeroUsize,
    pub batch_bytes: usize,
    pub memory_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchLimits {
    pub max_bytes: usize,
    pub max_items: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueError<T> {
    MemoryLimit(T),
    PeerLimit(T),
}

/// One item reserved for the current batch.
#[derive(Debug)]
pub struct Scheduled<T> {
    at: Duration,
    pub item: T,
    batch_bytes: usize,
}

struct Queued<T> {
    priority: UdpPriority,
    order: u64,
    item: T,
    cost: QueueCost,
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
    next_at: FixedTime,
    messages: BinaryHeap<Queued<T>>,
    ready_index: Option<usize>,
}

type Peer<T> = Rc<RefCell<PeerState<T>>>;

struct SchedulerEntry<R, T> {
    rank: R,
    key: SocketAddr,
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ReadyRank {
    priority: Reverse<UdpPriority>,
    at: Reverse<FixedTime>,
    order: Reverse<u64>,
}

type WaitingEntry<T> = SchedulerEntry<FixedTime, T>;
type ReadyEntry<T> = SchedulerEntry<ReadyRank, T>;

/// Socket-address keyed queue with global and uniform per-peer pacing.
pub struct PacingQueue<T> {
    peers: HashMap<SocketAddr, Peer<T>>,
    waiting: BinaryHeap<Reverse<WaitingEntry<T>>>,
    ready: Vec<ReadyEntry<T>>,
    global_bytes_per_second: NonZeroU64,
    peer_bytes_per_second: NonZeroU64,
    global_next_at: FixedTime,
    memory_limit: usize,
    memory_used: usize,
    pending: usize,
    next_order: u64,
    epoch: Instant,
}

impl<T> PacingQueue<T> {
    pub fn new(
        global_bytes_per_second: NonZeroU64,
        peer_bytes_per_second: NonZeroU64,
        memory_limit: usize,
    ) -> Self {
        Self {
            peers: HashMap::new(),
            waiting: BinaryHeap::new(),
            ready: Vec::new(),
            global_bytes_per_second,
            peer_bytes_per_second,
            global_next_at: FixedTime::default(),
            memory_limit,
            memory_used: 0,
            pending: 0,
            next_order: 0,
            epoch: Instant::now(),
        }
    }

    pub const fn len(&self) -> usize {
        self.pending
    }

    pub const fn is_empty(&self) -> bool {
        self.pending == 0
    }

    pub fn enqueue(
        &mut self,
        key: SocketAddr,
        priority: UdpPriority,
        item: T,
        cost: QueueCost,
    ) -> Result<(), EnqueueError<T>> {
        let memory_charge = cost.memory_bytes.saturating_add(size_of::<Queued<T>>());
        if memory_charge > self.memory_limit.saturating_sub(self.memory_used) {
            return Err(EnqueueError::MemoryLimit(item));
        }
        if self.peers.len() >= MAX_PEERS && !self.peers.contains_key(&key) {
            let now = FixedTime::from_duration(self.epoch.elapsed());
            let _ = self.promote_ready(now, now);
            if self.peers.len() >= MAX_PEERS {
                return Err(EnqueueError::PeerLimit(item));
            }
        }
        let queued = Queued {
            priority,
            order: self.next_order,
            item,
            cost,
        };

        let ready_index = match self.peers.entry(key) {
            Entry::Occupied(entry) => {
                let mut peer = entry.get().borrow_mut();
                peer.messages.push(queued);
                peer.ready_index
            }
            Entry::Vacant(entry) => {
                let peer = Rc::new(RefCell::new(PeerState {
                    next_at: FixedTime::default(),
                    messages: BinaryHeap::from([queued]),
                    ready_index: None,
                }));
                entry.insert(Rc::clone(&peer));
                self.waiting.push(Reverse(WaitingEntry {
                    rank: FixedTime::default(),
                    key,
                    peer,
                }));
                None
            }
        };
        if let Some(index) = ready_index {
            self.update_ready(index);
        }

        self.next_order = self.next_order.wrapping_add(1);
        self.memory_used += memory_charge;
        self.pending += 1;
        Ok(())
    }

    pub fn batch(&mut self, limits: BatchLimits) -> Batch<'_, T> {
        let now = self.epoch.elapsed();
        self.batch_at(now, limits)
    }

    fn batch_at(&mut self, now: Duration, limits: BatchLimits) -> Batch<'_, T> {
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
        let reclaim_before = FixedTime::from_duration(now);
        let mut slot = reclaim_before.max(self.global_next_at);

        loop {
            let next_at = self.promote_ready(slot, reclaim_before);
            if !self.ready.is_empty() {
                break;
            }
            slot = slot.max(next_at?);
        }

        if self.ready[0]
            .peer
            .borrow()
            .messages
            .peek()?
            .cost
            .batch_bytes
            > max_batch_bytes
        {
            return None;
        }
        let entry = self.pop_ready();
        let mut peer = entry.peer.borrow_mut();
        let queued = peer.messages.pop().expect("ready peer must have a message");
        let launch = slot.max(peer.next_at);
        peer.next_at = launch.advance(queued.cost.wire_bytes, self.peer_bytes_per_second);
        let next_at = peer.next_at;
        drop(peer);
        self.global_next_at = launch.advance(queued.cost.wire_bytes, self.global_bytes_per_second);
        self.waiting.push(Reverse(WaitingEntry {
            rank: next_at,
            key: entry.key,
            peer: entry.peer,
        }));

        self.memory_used -= queued
            .cost
            .memory_bytes
            .saturating_add(size_of::<Queued<T>>());
        self.pending -= 1;
        Some(Scheduled {
            at: launch.to_duration_ceil(),
            item: queued.item,
            batch_bytes: queued.cost.batch_bytes,
        })
    }

    fn promote_ready(&mut self, slot: FixedTime, reclaim_before: FixedTime) -> Option<FixedTime> {
        let mut cooling = Vec::new();
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
                debug_assert!(peer.ready_index.is_none());
                peer.messages.is_empty()
            };
            if empty {
                if entry.rank <= reclaim_before {
                    self.peers.remove(&entry.key);
                } else {
                    cooling.push(entry);
                }
            } else if entry.rank > slot {
                let next_at = entry.rank;
                self.waiting.push(Reverse(entry));
                break Some(next_at);
            } else {
                self.push_ready(entry);
            }
        };
        self.waiting.extend(cooling.into_iter().map(Reverse));
        next_at
    }

    fn push_ready(&mut self, entry: WaitingEntry<T>) {
        let index = self.ready.len();
        let rank = {
            let peer = entry.peer.borrow();
            let message = peer
                .messages
                .peek()
                .expect("ready peer must have a message");
            ReadyRank {
                priority: Reverse(message.priority),
                at: Reverse(peer.next_at),
                order: Reverse(message.order),
            }
        };
        self.ready.push(ReadyEntry {
            rank,
            key: entry.key,
            peer: entry.peer,
        });
        self.sift_up(index);
    }

    fn pop_ready(&mut self) -> ReadyEntry<T> {
        let removed = self.ready.swap_remove(0);
        removed.peer.borrow_mut().ready_index = None;
        if !self.ready.is_empty() {
            self.sift_down(0);
        }
        removed
    }

    fn update_ready(&mut self, index: usize) {
        let old = self.ready[index].rank;
        let new = {
            let peer = self.ready[index].peer.borrow();
            let message = peer
                .messages
                .peek()
                .expect("ready peer must have a message");
            ReadyRank {
                priority: Reverse(message.priority),
                at: Reverse(peer.next_at),
                order: Reverse(message.order),
            }
        };
        self.ready[index].rank = new;
        if new > old {
            self.sift_up(index);
        } else if new < old {
            self.sift_down(index);
        }
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.ready[parent] >= self.ready[index] {
                break;
            }
            self.ready.swap(parent, index);
            self.ready[index].peer.borrow_mut().ready_index = Some(index);
            index = parent;
        }
        self.ready[index].peer.borrow_mut().ready_index = Some(index);
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.ready.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.ready.len() && self.ready[right] > self.ready[left] {
                right
            } else {
                left
            };
            if self.ready[index] >= self.ready[child] {
                break;
            }
            self.ready.swap(index, child);
            self.ready[index].peer.borrow_mut().ready_index = Some(index);
            index = child;
        }
        self.ready[index].peer.borrow_mut().ready_index = Some(index);
    }
}

pub struct Batch<'a, T> {
    queue: &'a mut PacingQueue<T>,
    now: Duration,
    limits: BatchLimits,
    bytes: usize,
    items: usize,
    deadline: Duration,
}

impl<T> Iterator for Batch<'_, T> {
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

impl<T> Batch<'_, T> {
    pub fn reenqueue(
        &mut self,
        key: SocketAddr,
        priority: UdpPriority,
        item: T,
        cost: QueueCost,
    ) -> Result<(), EnqueueError<T>> {
        self.queue.enqueue(key, priority, item, cost)
    }

    pub const fn items(&self) -> usize {
        self.items
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub async fn wait(&self) {
        const SLEEP_GUARD: Duration = Duration::from_millis(2);

        let wait = self.deadline.saturating_sub(self.queue.epoch.elapsed());
        if wait > SLEEP_GUARD {
            monoio::time::sleep(wait - SLEEP_GUARD).await;
        }
        while self.queue.epoch.elapsed() < self.deadline {
            std::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNLIMITED: u64 = 1_000_000_000_000;

    fn rate(bytes_per_second: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes_per_second).unwrap()
    }

    fn key(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn cost(bytes: usize) -> QueueCost {
        QueueCost {
            wire_bytes: NonZeroUsize::new(bytes).unwrap(),
            batch_bytes: bytes,
            memory_bytes: bytes,
        }
    }

    fn queue(global: u64, peer: u64) -> PacingQueue<u64> {
        PacingQueue::new(rate(global), rate(peer), usize::MAX)
    }

    #[test]
    fn one_peer_never_exceeds_its_rate() {
        let mut queue = queue(UNLIMITED, 1_000);
        for item in 0..3 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item, cost(100))
                .unwrap();
        }

        let launches: Vec<_> = (0..3)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().at)
            .collect();
        assert_eq!(
            launches,
            [
                Duration::ZERO,
                Duration::from_millis(100),
                Duration::from_millis(200),
            ]
        );
    }

    #[test]
    fn all_peers_share_the_global_rate() {
        let mut queue = queue(1_000, UNLIMITED);
        for item in 0..3 {
            queue
                .enqueue(key(item as u16), UdpPriority::Regular, item, cost(100))
                .unwrap();
        }

        let launches: Vec<_> = (0..3)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().at)
            .collect();
        assert_eq!(
            launches,
            [
                Duration::ZERO,
                Duration::from_millis(100),
                Duration::from_millis(200),
            ]
        );
    }

    #[test]
    fn priority_is_global_and_updates_in_place() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        queue
            .enqueue(key(1), UdpPriority::Regular, 1, cost(1))
            .unwrap();
        queue
            .enqueue(key(2), UdpPriority::Regular, 2, cost(1))
            .unwrap();
        let next_at = queue.promote_ready(FixedTime::default(), FixedTime::default());
        assert!(next_at.is_none());
        queue
            .enqueue(key(1), UdpPriority::High, 3, cost(1))
            .unwrap();

        assert_eq!(queue.ready.len(), 2);
        assert_eq!(queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item, 3);
        assert_eq!(queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item, 2);
        assert_eq!(queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item, 1);
    }

    #[test]
    fn equal_priority_is_fifo() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        for item in 0..10 {
            queue
                .enqueue(key(1), UdpPriority::Regular, item, cost(1))
                .unwrap();
        }
        let items: Vec<_> = (0..10)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item)
            .collect();
        assert_eq!(items, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn total_memory_limit_rejects_messages() {
        let charge = size_of::<Queued<u64>>() + 1;
        let mut queue = PacingQueue::new(rate(UNLIMITED), rate(UNLIMITED), charge * 2);
        queue
            .enqueue(key(1), UdpPriority::Regular, 1, cost(1))
            .unwrap();
        queue
            .enqueue(key(1), UdpPriority::Regular, 2, cost(1))
            .unwrap();
        let error = queue
            .enqueue(key(2), UdpPriority::High, 3, cost(1))
            .unwrap_err();
        assert_eq!(error, EnqueueError::MemoryLimit(3));
        assert_eq!(queue.memory_used, charge * 2);

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        queue
            .enqueue(key(2), UdpPriority::High, 3, cost(1))
            .unwrap();
    }

    #[test]
    fn peer_limit_rejects_only_new_peers() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        for peer in 0..MAX_PEERS {
            queue
                .enqueue(key(peer as u16), UdpPriority::Regular, peer as u64, cost(1))
                .unwrap();
        }

        let new_key = SocketAddr::from(([127, 0, 0, 2], 0));
        assert_eq!(
            queue.enqueue(new_key, UdpPriority::Regular, MAX_PEERS as u64, cost(1)),
            Err(EnqueueError::PeerLimit(MAX_PEERS as u64))
        );
        assert!(queue
            .enqueue(key(0), UdpPriority::Regular, MAX_PEERS as u64, cost(1))
            .is_ok());
        assert_eq!(queue.peers.len(), MAX_PEERS);
    }

    #[test]
    fn variable_wire_costs_advance_both_clocks() {
        let mut queue = queue(1_000, 1_000);
        for (item, bytes) in [(1, 100), (2, 200), (3, 50)] {
            queue
                .enqueue(key(1), UdpPriority::Regular, item, cost(bytes))
                .unwrap();
        }
        let selected: Vec<_> = (0..3)
            .map(|_| {
                let item = queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
                (item.item, item.at)
            })
            .collect();
        assert_eq!(
            selected,
            [
                (1, Duration::ZERO),
                (2, Duration::from_millis(100)),
                (3, Duration::from_millis(300)),
            ]
        );
    }

    #[test]
    fn fractional_time_does_not_accumulate_rounding_drift() {
        const SENDS: usize = 100_000;
        let bytes_per_second = rate(375_000);
        let mut queue = PacingQueue::new(bytes_per_second, bytes_per_second, usize::MAX);
        queue
            .enqueue(key(1), UdpPriority::Regular, (), cost(100))
            .unwrap();

        for _ in 0..SENDS {
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
            queue
                .enqueue(key(1), UdpPriority::Regular, (), cost(100))
                .unwrap();
        }

        let exact = 100_u128 * NANOS_PER_SECOND * TICKS_PER_NANOSECOND * SENDS as u128
            / bytes_per_second.get() as u128;
        assert!(exact - queue.global_next_at.0 < SENDS as u128);
        assert!(exact - queue.global_next_at.0 < TICKS_PER_NANOSECOND);
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
                .enqueue(key(port), UdpPriority::Regular, port.into(), cost(1))
                .unwrap();
            assert_eq!(
                queue.batch_at(Duration::ZERO, limits).next().unwrap().item,
                u64::from(port)
            );
        }
        let now = FixedTime::from_duration(queue.epoch.elapsed());
        assert!(queue.promote_ready(now, now).is_none());
        assert!(queue.peers.len() <= 2);
    }

    #[test]
    fn one_scheduler_entry_per_peer() {
        let mut queue = queue(UNLIMITED, 1_000);
        for port in 1..=100 {
            queue
                .enqueue(key(port), UdpPriority::Regular, port.into(), cost(100))
                .unwrap();
        }
        for _ in 0..100 {
            assert_eq!(queue.waiting.len() + queue.ready.len(), queue.peers.len());
            assert!(queue.peers.values().all(|peer| Rc::strong_count(peer) == 2));
            assert!(queue.ready.iter().enumerate().all(|(index, entry)| entry
                .peer
                .borrow()
                .ready_index
                == Some(index)));
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        }
        assert_eq!(queue.waiting.len() + queue.ready.len(), queue.peers.len());
    }

    #[test]
    fn batch_bounds_and_reenqueue() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), UdpPriority::Regular, 0, cost(100))
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
            assert_eq!((first.item, first.at), (0, Duration::ZERO));
            batch
                .reenqueue(key(1), UdpPriority::Regular, 1, cost(100))
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
        assert_eq!((second.item, second.at), (1, Duration::from_millis(100)));
    }

    #[test]
    fn exhausted_batch_keeps_cooling_peer_scheduled() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), UdpPriority::Regular, 0, cost(100))
            .unwrap();
        let limits = BatchLimits {
            max_bytes: usize::MAX,
            max_items: usize::MAX,
        };

        {
            let mut batch = queue.batch_at(Duration::ZERO, limits);
            assert_eq!(batch.next().unwrap().item, 0);
            assert!(batch.next().is_none());
        }

        queue
            .enqueue(key(1), UdpPriority::Regular, 1, cost(100))
            .unwrap();
        let scheduled = queue.batch_at(Duration::ZERO, limits).next().unwrap();
        assert_eq!(
            (scheduled.item, scheduled.at),
            (1, Duration::from_millis(100))
        );
    }
}
