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
//! Flow state is ordered by its next eligible send time. Eligible items move
//! through one global priority queue; callers supply flow keys and byte costs.

use std::{
    cell::{RefCell, RefMut},
    cmp::{Ordering, Reverse},
    collections::{binary_heap::PeekMut, BTreeMap, BinaryHeap, HashMap},
    hash::Hash,
    num::NonZeroU64,
    rc::Rc,
    time::{Duration, Instant},
};

const PACER_MAX_CATCH_UP: Duration = Duration::from_millis(5);
const MIN_PEER_QUEUE_CAPACITY: usize = 16;

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueError<T> {
    MemoryLimit(T),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PacingPriority {
    High,
    Regular,
    Background,
}

impl PacingPriority {
    const COUNT: usize = 3;

    const fn index(self) -> usize {
        match self {
            Self::High => 0,
            Self::Regular => 1,
            Self::Background => 2,
        }
    }
}

pub trait PacingItem {
    /// Remaining payload bytes charged against the queue's memory limit.
    fn queued_bytes(&self) -> usize;

    fn next_payload_bytes(&self) -> usize;

    fn next_pacing_bytes(&self) -> usize {
        self.next_payload_bytes()
    }

    /// None means this item uses only the global limit.
    fn peer_bytes_per_second(&self, configured: NonZeroU64) -> Option<NonZeroU64> {
        Some(configured)
    }
}

/// One item selected for transmission.
#[derive(Debug)]
pub struct Scheduled<T> {
    pub item: T,
    pub(crate) batch_bytes: usize,
    priority: PacingPriority,
    order: u64,
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

struct PeerState<K, T> {
    messages: BinaryHeap<Queued<T>>,
    ready: Option<ReadyKey<K>>,
}

/// Temporary access that keeps a peer's cached key and ready index in sync.
struct PeerMut<'a, K, T> {
    state: RefMut<'a, PeerState<K, T>>,
    ready: &'a mut BTreeMap<ReadyKey<K>, Peer<K, T>>,
}

impl<'a, K: Copy + Ord, T> PeerMut<'a, K, T> {
    fn new(peer: &'a Peer<K, T>, ready: &'a mut BTreeMap<ReadyKey<K>, Peer<K, T>>) -> Self {
        Self {
            state: peer.borrow_mut(),
            ready,
        }
    }

    fn push(&mut self, queued: Queued<T>) {
        self.state.messages.push(queued);
        if let Some(previous) = self.state.ready {
            let message = self.state.messages.peek().expect("just pushed a message");
            let key = ReadyKey::new(previous.key, message);
            if key != previous {
                // NOTE: Remove using the cached key, not the changed top message.
                // Update the map and cached key together, retaining the peer handle.
                let peer = self.ready.remove(&previous).expect("ready peer is indexed");
                self.ready.insert(key, peer);
                self.state.ready = Some(key);
            }
        }
    }

    /// Pop a ready message only if its next payload fits; otherwise change nothing.
    fn pop(&mut self, max_bytes: usize) -> Option<Queued<T>>
    where
        T: PacingItem,
    {
        let key = self.state.ready?;
        debug_assert!(
            !self.state.messages.is_empty(),
            "a cached ready key requires a queued message"
        );
        let message = self.state.messages.peek_mut()?;
        if message.item.next_payload_bytes() > max_bytes {
            return None;
        }
        let queued = PeekMut::pop(message);
        // Reclaim burst allocations even if a low-priority message keeps this peer alive.
        // Leave headroom so shrinking and growing do not alternate on each send.
        let messages = &mut self.state.messages;
        if messages.capacity() > MIN_PEER_QUEUE_CAPACITY
            && messages.len() <= messages.capacity() / 4
        {
            messages.shrink_to((messages.len() * 2).max(MIN_PEER_QUEUE_CAPACITY));
        }
        let indexed = self.ready.remove(&key);
        debug_assert!(indexed.is_some(), "a cached ready key must be indexed");
        self.state.ready = None;
        Some(queued)
    }

    fn drain(&mut self) -> impl Iterator<Item = Queued<T>> + '_ {
        if let Some(key) = self.state.ready.take() {
            self.ready.remove(&key);
        }
        self.state.messages.drain()
    }
}

/// Immutable ordering key for an eligible peer's top message. The ready map stores
/// the peer handle separately; rebuild this key when the top message changes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ReadyKey<K> {
    priority: PacingPriority,
    order: u64,
    key: K,
}

impl<K> ReadyKey<K> {
    fn new<T>(key: K, message: &Queued<T>) -> Self {
        Self {
            priority: message.priority,
            order: message.order,
            key,
        }
    }
}

// Ready and deadline entries retain direct peer access to avoid hash lookups when
// selecting or promoting a peer. Keep this indirection despite the single owner.
type Peer<K, T> = Rc<RefCell<PeerState<K, T>>>;

struct PeerDeadline<K, T> {
    next_at: Duration,
    key: K,
    peer: Peer<K, T>,
}

impl<K: Copy + Ord, T> PeerDeadline<K, T> {
    /// Consume an eligible deadline, or return the empty peer's deadline for expiry.
    fn into_ready(self, ready: &mut BTreeMap<ReadyKey<K>, Peer<K, T>>) -> Result<(), Self> {
        let key = {
            let mut state = self.peer.borrow_mut();
            debug_assert!(state.ready.is_none());
            state.ready = state
                .messages
                .peek()
                .map(|message| ReadyKey::new(self.key, message));
            state.ready
        };
        if let Some(key) = key {
            ready.insert(key, self.peer);
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl<K: PartialEq, T> PartialEq for PeerDeadline<K, T> {
    fn eq(&self, other: &Self) -> bool {
        self.next_at == other.next_at && self.key == other.key
    }
}

impl<K: Eq, T> Eq for PeerDeadline<K, T> {}

impl<K: Ord, T> PartialOrd for PeerDeadline<K, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, T> Ord for PeerDeadline<K, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.next_at
            .cmp(&other.next_at)
            .then_with(|| self.key.cmp(&other.key))
    }
}

/// Flow-keyed queue with global pacing and caller-supplied per-item costs.
pub struct PacingQueue<K, T> {
    peers: HashMap<K, Peer<K, T>>,
    next_peers: BinaryHeap<Reverse<PeerDeadline<K, T>>>,
    ready: BTreeMap<ReadyKey<K>, Peer<K, T>>,
    global_bytes_per_second: NonZeroU64,
    peer_bytes_per_second: NonZeroU64,
    service_at: Duration,
    memory: [QueueMemory; PacingPriority::COUNT],
    pending: usize,
    next_order: u64,
    epoch: Instant,
}

#[derive(Clone, Copy)]
struct QueueMemory {
    limit: usize,
    used: usize,
}

impl<K: Copy + Hash + Ord, T: PacingItem> PacingQueue<K, T> {
    pub fn new(
        global_bytes_per_second: NonZeroU64,
        peer_bytes_per_second: NonZeroU64,
        memory_limit: usize,
    ) -> Self {
        Self {
            peers: HashMap::new(),
            next_peers: BinaryHeap::new(),
            ready: BTreeMap::new(),
            global_bytes_per_second,
            peer_bytes_per_second,
            service_at: Duration::ZERO,
            memory: [QueueMemory {
                limit: memory_limit,
                used: 0,
            }; PacingPriority::COUNT],
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

    /// Admit an item, returning whether a new flow was created.
    pub fn enqueue(
        &mut self,
        key: K,
        priority: impl Into<PacingPriority>,
        item: T,
    ) -> Result<bool, EnqueueError<T>> {
        let order = self.next_order;
        self.next_order = self.next_order.wrapping_add(1);
        self.enqueue_inner(key, priority.into(), item, order)
    }

    /// Return a partially sent message without changing its priority or FIFO order.
    pub(crate) fn requeue(
        &mut self,
        key: K,
        scheduled: Scheduled<T>,
    ) -> Result<bool, EnqueueError<T>> {
        self.enqueue_inner(key, scheduled.priority, scheduled.item, scheduled.order)
    }

    fn enqueue_inner(
        &mut self,
        key: K,
        priority: PacingPriority,
        item: T,
        order: u64,
    ) -> Result<bool, EnqueueError<T>> {
        let queued_bytes = item.queued_bytes();
        let class = priority.index();
        let memory = &mut self.memory[class];
        if queued_bytes > memory.limit.saturating_sub(memory.used) {
            return Err(EnqueueError::MemoryLimit(item));
        }
        let queued = Queued {
            priority,
            order,
            item,
            queued_bytes,
        };

        let new_peer = if let Some(peer) = self.peers.get(&key) {
            PeerMut::new(peer, &mut self.ready).push(queued);
            false
        } else {
            let peer = Rc::new(RefCell::new(PeerState {
                messages: BinaryHeap::from([queued]),
                ready: None,
            }));
            self.peers.insert(key, Rc::clone(&peer));
            self.next_peers.push(Reverse(PeerDeadline {
                next_at: self.service_at,
                key,
                peer,
            }));
            true
        };

        self.memory[class].used += queued_bytes;
        self.pending += 1;
        Ok(new_peer)
    }

    pub fn dequeue(&mut self, now: Duration, max_bytes: usize) -> Option<Scheduled<T>> {
        self.dequeue_with_expiry(now, max_bytes, |_| {})
    }

    /// Like dequeue, notifying the caller when an empty flow's deadline expires.
    /// Notification happens inline, without collecting keys or scanning the map.
    pub fn dequeue_with_expiry(
        &mut self,
        now: Duration,
        max_bytes: usize,
        on_expired: impl FnMut(K),
    ) -> Option<Scheduled<T>> {
        self.prepare(now, on_expired);

        let (&ready, peer) = self.ready.first_key_value()?;
        let peer = Rc::clone(peer);
        let queued = PeerMut::new(&peer, &mut self.ready).pop(max_bytes)?;
        let next_bytes = queued.item.next_payload_bytes();
        let pacing_bytes = queued.item.next_pacing_bytes();
        let next_at = queued
            .item
            .peer_bytes_per_second(self.peer_bytes_per_second)
            .map_or(self.service_at, |rate| {
                Self::advance(self.service_at, pacing_bytes, rate)
            });
        self.service_at =
            Self::advance(self.service_at, pacing_bytes, self.global_bytes_per_second);
        self.next_peers.push(Reverse(PeerDeadline {
            next_at,
            key: ready.key,
            peer,
        }));

        let class = queued.priority.index();
        self.memory[class].used -= queued.queued_bytes;
        self.pending -= 1;
        Some(Scheduled {
            item: queued.item,
            batch_bytes: next_bytes,
            priority: queued.priority,
            order: queued.order,
        })
    }

    /// Discard all remaining items and deadlines for a flow.
    pub(crate) fn remove_peer(&mut self, key: K) -> usize {
        let Some(peer) = self.peers.remove(&key) else {
            return 0;
        };
        let mut peer = PeerMut::new(&peer, &mut self.ready);
        self.next_peers.retain(|entry| entry.0.key != key);
        let removed = peer.state.messages.len();
        self.pending -= removed;
        for queued in peer.drain() {
            self.memory[queued.priority.index()].used -= queued.queued_bytes;
        }
        removed
    }

    fn prepare(&mut self, now: Duration, mut on_expired: impl FnMut(K)) {
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
            // keep a tombstone for a drained peer until its deadline becomes eligible. a message
            // enqueued before then reuses this deadline instead of resetting the peer clock and
            // overrunning the per-peer rate.
            if let Err(deadline) = deadline.into_ready(&mut self.ready) {
                let peer = self
                    .peers
                    .remove(&deadline.key)
                    .expect("scheduled peer must exist");
                debug_assert!(Rc::ptr_eq(&peer, &deadline.peer));
                on_expired(deadline.key);
            }
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
        fn queued_bytes(&self) -> usize {
            self.bytes
        }

        fn next_payload_bytes(&self) -> usize {
            self.bytes
        }

        fn next_pacing_bytes(&self) -> usize {
            // Exercise a caller-defined cost distinct from payload and memory.
            self.bytes + 28
        }
    }

    fn rate(bytes_per_second: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes_per_second).unwrap()
    }

    fn key(id: u16) -> u16 {
        id
    }

    fn item(id: u64, bytes: usize) -> Item {
        Item { id, bytes }
    }

    fn queue(global: u64, peer: u64) -> PacingQueue<u16, Item> {
        PacingQueue::new(rate(global), rate(peer), usize::MAX)
    }

    fn assert_ready_consistent(queue: &PacingQueue<u16, Item>) {
        let mut count = 0;
        for (address, peer) in &queue.peers {
            let state = peer.borrow();
            if let Some(key) = state.ready {
                assert!(key == ReadyKey::new(*address, state.messages.peek().unwrap()));
                assert!(Rc::ptr_eq(queue.ready.get(&key).unwrap(), peer));
                count += 1;
            }
            assert_eq!(Rc::strong_count(peer), 2);
        }
        assert_eq!(count, queue.ready.len());
        for deadline in &queue.next_peers {
            assert!(deadline.0.peer.borrow().ready.is_none());
        }
    }

    #[test]
    fn drained_peer_heaps_shrink_with_headroom() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        for port in 1..=4 {
            queue
                .enqueue(key(port), PacingPriority::Regular, item(0, 64))
                .unwrap();
            for id in 1..128 {
                queue
                    .enqueue(key(port), PacingPriority::High, item(id, 64))
                    .unwrap();
            }
            assert_eq!(queue.peers[&key(port)].borrow().messages.capacity(), 128);

            for id in 1..128 {
                let capacity = queue.peers[&key(port)].borrow().messages.capacity();
                let now = queue.next_wakeup(queue.service_at).unwrap();
                assert_eq!(queue.dequeue(now, usize::MAX).unwrap().item.id, id);
                let state = queue.peers[&key(port)].borrow();
                let len = state.messages.len();
                let expected = if capacity > 16 && len <= capacity / 4 {
                    (len * 2).max(16)
                } else {
                    capacity
                };
                assert_eq!(state.messages.capacity(), expected);
                assert_ready_consistent(&queue);
            }
        }

        // Each regular message keeps its peer alive, but no longer pins its peak capacity.
        assert_eq!(queue.len(), 4);
        for peer in queue.peers.values() {
            let state = peer.borrow();
            assert_eq!(state.messages.len(), 1);
            assert_eq!(state.messages.capacity(), 16);
        }
    }

    #[test]
    fn one_peer_never_exceeds_its_rate() {
        let mut queue = queue(UNLIMITED, 1_000);
        for id in 0..3 {
            queue
                .enqueue(key(1), PacingPriority::Regular, item(id, 100))
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
    fn starved_peer_cannot_accumulate_ready_send_credit() {
        let mut queue = queue(10_000, 1_000);
        for port in 1..=10 {
            for id in 0..100 {
                queue
                    .enqueue(key(port), PacingPriority::High, item(id, 100))
                    .unwrap();
            }
        }
        for id in 0..100 {
            queue
                .enqueue(key(20), PacingPriority::Regular, item(10_000 + id, 100))
                .unwrap();
        }
        let mut regular_times = Vec::new();
        while !queue.is_empty() {
            let now = queue.next_wakeup(queue.service_at).unwrap();
            if let Some(scheduled) = queue.dequeue(now, usize::MAX) {
                if scheduled.item.id >= 10_000 {
                    regular_times.push(queue.service_at);
                }
            }
        }
        assert_eq!(regular_times.len(), 100);
        for pair in regular_times.windows(2) {
            assert!(pair[1] - pair[0] >= Duration::from_millis(128));
        }
    }

    #[test]
    fn all_peers_share_the_global_rate() {
        let mut queue = queue(1_000, UNLIMITED);
        for id in 0..3 {
            queue
                .enqueue(key(id as u16), PacingPriority::Regular, item(id, 100))
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
            .enqueue(key(1), PacingPriority::Regular, item(1, 100))
            .unwrap();
        queue
            .enqueue(key(2), PacingPriority::Regular, item(2, 100))
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
            .enqueue(key(1), PacingPriority::Regular, item(1, 100))
            .unwrap();
        queue
            .enqueue(key(2), PacingPriority::Regular, item(2, 100))
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
    fn peer_clock_is_charged_at_dispatch() {
        let mut queue = queue(UNLIMITED, 1_000);
        for id in 0..2 {
            queue
                .enqueue(key(1), PacingPriority::Regular, item(id, 100))
                .unwrap();
        }

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        queue
            .dequeue(Duration::from_millis(133), usize::MAX)
            .unwrap();
        assert_eq!(
            queue.next_peers.peek().unwrap().0.next_at,
            Duration::from_millis(261)
        );
    }

    #[test]
    fn peer_clock_clamps_beyond_catch_up_window() {
        let mut queue = queue(UNLIMITED, 1_000);
        for id in 0..2 {
            queue
                .enqueue(key(1), PacingPriority::Regular, item(id, 100))
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
            .enqueue(key(1), PacingPriority::Regular, item(0, 100))
            .unwrap();
        queue
            .enqueue(key(1), PacingPriority::Regular, item(1, 100))
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
                .enqueue(key(1), PacingPriority::Regular, item(id, 100))
                .unwrap();
        }
        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(
            queue.next_wakeup(Duration::from_millis(70)),
            Some(Duration::from_millis(128))
        );

        queue
            .enqueue(key(2), PacingPriority::High, item(2, 100))
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
    fn ready_peer_candidate_tracks_new_high_priority_messages() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        queue
            .enqueue(key(1), PacingPriority::Regular, item(1, 1))
            .unwrap();
        queue
            .enqueue(key(2), PacingPriority::Regular, item(2, 1))
            .unwrap();
        drop(PeerMut::new(&queue.peers[&key(1)], &mut queue.ready));
        assert!(queue.ready.is_empty());
        assert!(PeerMut::new(&queue.peers[&key(1)], &mut queue.ready)
            .pop(usize::MAX)
            .is_none());
        assert_eq!(queue.len(), 2);
        assert_ready_consistent(&queue);
        assert!(queue.dequeue(Duration::ZERO, 0).is_none());
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.service_at, Duration::ZERO);
        assert_eq!(queue.memory[PacingPriority::Regular.index()].used, 2);
        assert_ready_consistent(&queue);
        queue
            .enqueue(key(1), PacingPriority::High, item(3, 1))
            .unwrap();
        assert_ready_consistent(&queue);
        queue
            .enqueue(key(1), PacingPriority::Regular, item(4, 1))
            .unwrap();
        assert_ready_consistent(&queue);

        assert_eq!(queue.ready.len(), 2);
        for id in [3, 1, 2, 4] {
            assert_eq!(queue.dequeue(Duration::ZERO, 1).unwrap().item.id, id);
            assert_ready_consistent(&queue);
        }
        assert!(queue.dequeue(Duration::from_secs(1), usize::MAX).is_none());
        assert_ready_consistent(&queue);
        assert!(queue.peers.is_empty());
    }

    #[test]
    fn equal_priority_is_fifo() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        for id in 0..10 {
            queue
                .enqueue(key(1), PacingPriority::Regular, item(id, 1))
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
                .enqueue(key(1), PacingPriority::High, item(id, 100))
                .unwrap();
        }
        for id in 100..104 {
            queue
                .enqueue(key(2), PacingPriority::Regular, item(id, 100))
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
                .enqueue(key(1), PacingPriority::High, item(id, 100))
                .unwrap();
        }
        for id in 100..104 {
            queue
                .enqueue(key(2), PacingPriority::Regular, item(id, 100))
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
                .enqueue(key(1), PacingPriority::Regular, item(id, 100))
                .unwrap();
        }
        for id in 100..104 {
            queue
                .enqueue(key(2), PacingPriority::Regular, item(id, 100))
                .unwrap();
        }

        let observed: Vec<_> = (0..8)
            .map(|_| queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id)
            .collect();
        assert_eq!(observed, [0, 1, 2, 3, 100, 101, 102, 103]);
    }

    #[test]
    fn memory_limit_is_per_traffic_class() {
        let mut queue = PacingQueue::new(rate(UNLIMITED), rate(UNLIMITED), 2);
        queue
            .enqueue(key(1), PacingPriority::Regular, item(1, 1))
            .unwrap();
        queue
            .enqueue(key(1), PacingPriority::Regular, item(2, 1))
            .unwrap();
        queue
            .enqueue(key(2), PacingPriority::High, item(3, 1))
            .unwrap();
        queue
            .enqueue(key(2), PacingPriority::High, item(4, 1))
            .unwrap();
        queue
            .enqueue(key(3), PacingPriority::Background, item(6, 1))
            .unwrap();
        queue
            .enqueue(key(3), PacingPriority::Background, item(7, 1))
            .unwrap();
        let error = queue
            .enqueue(key(2), PacingPriority::High, item(5, 1))
            .unwrap_err();
        assert_eq!(error, EnqueueError::MemoryLimit(item(5, 1)));
        let memory_used: [usize; PacingPriority::COUNT] =
            std::array::from_fn(|class| queue.memory[class].used);
        assert_eq!(
            memory_used,
            [2, 2, 2],
            "all traffic classes must have independent budgets"
        );

        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        queue
            .enqueue(key(2), PacingPriority::High, item(5, 1))
            .unwrap();
    }

    #[test]
    fn drained_peers_expire() {
        let mut queue = queue(UNLIMITED, UNLIMITED);
        for peer in 1..=3 {
            assert!(queue
                .enqueue(key(peer), PacingPriority::Regular, item(peer.into(), 1))
                .unwrap());
        }

        let mut expired = Vec::new();
        for _ in 0..3 {
            queue
                .dequeue_with_expiry(Duration::ZERO, usize::MAX, |key| expired.push(key))
                .unwrap();
        }
        let end = Duration::MAX;
        assert!(queue
            .dequeue_with_expiry(end, usize::MAX, |key| expired.push(key))
            .is_none());
        assert_eq!(expired, [1, 2, 3]);
        assert!(queue
            .dequeue_with_expiry(end, usize::MAX, |_| panic!("flow expired twice"))
            .is_none());
        assert!(queue.peers.is_empty());
        assert!(queue.next_peers.is_empty());
    }

    #[test]
    fn opaque_keys_and_global_only_items_need_no_flow_limit_or_overhead() {
        #[derive(Debug)]
        struct GlobalOnly;
        impl PacingItem for GlobalOnly {
            fn queued_bytes(&self) -> usize {
                100
            }
            fn next_payload_bytes(&self) -> usize {
                100
            }
            fn peer_bytes_per_second(&self, _: NonZeroU64) -> Option<NonZeroU64> {
                None
            }
        }
        let mut queue = PacingQueue::new(rate(1_000), rate(1), 200);
        assert!(queue
            .enqueue("flow", PacingPriority::Regular, GlobalOnly)
            .unwrap());
        assert!(!queue
            .enqueue("flow", PacingPriority::Regular, GlobalOnly)
            .unwrap());
        queue.dequeue(Duration::ZERO, 100).unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(100));
        queue.dequeue(Duration::ZERO, 100).unwrap();
        assert_eq!(queue.service_at, Duration::from_millis(200));
    }

    #[test]
    fn variable_payload_sizes_advance_both_clocks() {
        let mut queue = queue(1_000, 1_000);
        for (id, bytes) in [(1, 100), (2, 200), (3, 50)] {
            queue
                .enqueue(key(1), PacingPriority::Regular, item(id, bytes))
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
                .enqueue(key(port), PacingPriority::Regular, item(port.into(), 1))
                .unwrap();
            assert_eq!(
                queue.dequeue(Duration::ZERO, 1).unwrap().item.id,
                u64::from(port)
            );
        }
        let now = queue.epoch.elapsed();
        assert!(queue.dequeue(now, usize::MAX).is_none());
        assert!(queue.peers.len() <= 2);
    }

    #[test]
    fn messages_exist_in_exactly_one_queue() {
        let mut queue = queue(UNLIMITED, 1_000);
        for port in 1..=100 {
            queue
                .enqueue(key(port), PacingPriority::Regular, item(port.into(), 100))
                .unwrap();
        }
        for _ in 0..100 {
            let peer_messages: usize = queue
                .peers
                .values()
                .map(|peer| peer.borrow().messages.len())
                .sum();
            assert_eq!(
                queue.next_peers.len() + queue.ready.len(),
                queue.peers.len()
            );
            assert_ready_consistent(&queue);
            assert_eq!(peer_messages, queue.len());
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        }
        let peer_messages: usize = queue
            .peers
            .values()
            .map(|peer| peer.borrow().messages.len())
            .sum();
        assert_eq!(
            queue.next_peers.len() + queue.ready.len(),
            queue.peers.len()
        );
        assert_ready_consistent(&queue);
        assert_eq!(peer_messages, queue.len());
    }

    #[test]
    fn byte_limit_and_reenqueue() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), PacingPriority::Regular, item(0, 100))
            .unwrap();

        let mut first = queue.dequeue(Duration::ZERO, 120).unwrap();
        assert_eq!(first.item.id, 0);
        assert_eq!(queue.service_at, Duration::from_millis(128));
        queue
            .enqueue(key(1), PacingPriority::Regular, item(1, 100))
            .unwrap();
        assert!(queue.dequeue(Duration::ZERO, 20).is_none());

        let original_order = first.order;
        first.item.bytes = 40;
        queue.requeue(key(1), first).unwrap();
        let remainder = queue.dequeue(Duration::ZERO, 120).unwrap();
        assert_eq!(remainder.item.id, 0);
        assert_eq!(remainder.order, original_order);
        assert_eq!(remainder.priority, PacingPriority::Regular);
        assert_eq!(remainder.batch_bytes, 40);

        let second = queue.dequeue(Duration::ZERO, 120).unwrap();
        assert_eq!(second.item.id, 1);
        assert_eq!(queue.service_at, Duration::from_millis(324));
    }

    #[test]
    fn exhausted_ready_queue_keeps_peer_scheduled() {
        let mut queue = queue(1_000, 1_000);
        queue
            .enqueue(key(1), PacingPriority::Regular, item(0, 100))
            .unwrap();
        assert_eq!(
            queue.dequeue(Duration::ZERO, usize::MAX).unwrap().item.id,
            0
        );
        assert!(queue.dequeue(Duration::ZERO, usize::MAX).is_none());

        queue
            .enqueue(key(1), PacingPriority::Regular, item(1, 100))
            .unwrap();
        let scheduled = queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        assert_eq!(scheduled.item.id, 1);
        assert_eq!(queue.service_at, Duration::from_millis(256));
    }

    #[test]
    fn removing_peer_discards_ready_and_cooling_messages() {
        let mut queue = queue(1_000, 1_000);
        for port in 1..=2 {
            for id in 0..3 {
                queue
                    .enqueue(key(port), PacingPriority::Regular, item(id, 100))
                    .unwrap();
            }
        }
        queue.dequeue(Duration::ZERO, usize::MAX).unwrap();
        // Peer 1 is cooling, peer 2 is eligible with a candidate in ready.
        assert_eq!(queue.remove_peer(key(1)), 2);
        assert_eq!(queue.remove_peer(key(2)), 3);
        assert!(queue.is_empty());
        assert!(queue.ready.is_empty());
        assert!(queue.next_peers.is_empty());
        assert!(queue.memory.iter().all(|class| class.used == 0));
    }
}
