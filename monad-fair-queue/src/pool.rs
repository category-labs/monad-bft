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

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::{Debug, Display},
    hash::Hash,
};

use tracing::warn;

use crate::{ensure, Len, PushError, Score};

const IDENTITY_QUEUE_SHRINK_RATIO: usize = 2;
const IDENTITY_QUEUE_SHRINK_FLOOR: usize = 4;

pub(crate) struct HeapEntry<Id> {
    pub(crate) finish_time: f64,
    pub(crate) id: Id,
}

impl<Id> PartialEq for HeapEntry<Id> {
    fn eq(&self, other: &Self) -> bool {
        self.finish_time == other.finish_time
    }
}

impl<Id> Eq for HeapEntry<Id> {}

impl<Id> PartialOrd for HeapEntry<Id> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Id> Ord for HeapEntry<Id> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.finish_time.total_cmp(&self.finish_time)
    }
}

struct IdentityState<T> {
    queue: VecDeque<T>,
    queued_bytes: usize,
    score: Score,
    finish_time: f64,
}

pub(crate) struct PopItem<Id, T> {
    pub(crate) id: Id,
    pub(crate) item: T,
}

pub(crate) struct Pool<Id, T> {
    heap: BinaryHeap<HeapEntry<Id>>,
    identities: HashMap<Id, IdentityState<T>>,
    virtual_time: f64,
    total_items: usize,
    total_bytes: usize,
    max_size: usize,
    max_bytes: usize,
    per_id_byte_limit: usize,
}

impl<Id, T> Pool<Id, T>
where
    Id: Eq + Hash + Clone + Debug + Display,
    T: Len,
{
    pub(crate) fn new(max_size: usize, per_id_byte_limit: usize, max_bytes: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            identities: HashMap::new(),
            virtual_time: 0.0,
            total_items: 0,
            total_bytes: 0,
            max_size,
            max_bytes,
            per_id_byte_limit,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.total_items
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.total_items == 0
    }

    pub(crate) fn has_item_capacity_for(&self, incoming_items: usize) -> bool {
        self.total_items
            .checked_add(incoming_items)
            .is_some_and(|next| next <= self.max_size)
    }

    pub(crate) fn has_byte_capacity_for(&self, incoming_bytes: usize) -> bool {
        self.total_bytes
            .checked_add(incoming_bytes)
            .is_some_and(|next| next <= self.max_bytes)
    }

    pub(crate) fn contains_identity(&self, id: &Id) -> bool {
        self.identities.contains_key(id)
    }

    #[cfg(test)]
    pub(crate) fn remove_items(&mut self, count: usize) {
        self.total_items = self.total_items.saturating_sub(count);
    }

    pub(crate) fn push_existing(&mut self, id: &Id, item: T) -> Result<(), PushError<Id>> {
        let incoming_bytes = item.len();
        let state = self
            .identities
            .get(id)
            .expect("identity must exist when routed to existing pool");
        let new_queued_bytes = state.queued_bytes.saturating_add(incoming_bytes);
        ensure!(
            new_queued_bytes <= self.per_id_byte_limit,
            PushError::PerIdByteLimitExceeded {
                id: id.clone(),
                limit: self.per_id_byte_limit,
            }
        );
        ensure!(
            self.has_item_capacity_for(1),
            PushError::Full {
                size: self.total_items,
                max_size: self.max_size,
            }
        );
        ensure!(
            self.has_byte_capacity_for(incoming_bytes),
            PushError::Full {
                size: self.total_bytes,
                max_size: self.max_bytes,
            }
        );
        let state = self
            .identities
            .get_mut(id)
            .expect("identity must exist when routed to existing pool");
        state.queue.push_back(item);
        state.queued_bytes = state.queued_bytes.saturating_add(incoming_bytes);
        self.total_items = self.total_items.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(incoming_bytes);
        Ok(())
    }

    pub(crate) fn push_new(
        &mut self,
        id: Id,
        item: T,
        score: Score,
    ) -> Result<(), (PushError<Id>, T)> {
        let incoming_bytes = item.len();
        ensure!(
            incoming_bytes <= self.per_id_byte_limit,
            (
                PushError::PerIdByteLimitExceeded {
                    id,
                    limit: self.per_id_byte_limit,
                },
                item,
            )
        );
        ensure!(
            self.has_item_capacity_for(1),
            (
                PushError::Full {
                    size: self.total_items,
                    max_size: self.max_size,
                },
                item,
            )
        );
        ensure!(
            self.has_byte_capacity_for(incoming_bytes),
            (
                PushError::Full {
                    size: self.total_bytes,
                    max_size: self.max_bytes,
                },
                item,
            )
        );

        let finish_time = self.virtual_time + score.reciprocal();
        self.identities.insert(
            id.clone(),
            IdentityState {
                queue: VecDeque::from([item]),
                queued_bytes: incoming_bytes,
                score,
                finish_time,
            },
        );
        self.total_items = self.total_items.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(incoming_bytes);
        self.heap.push(HeapEntry { finish_time, id });
        Ok(())
    }

    pub(crate) fn pop_next(&mut self) -> Option<PopItem<Id, T>> {
        loop {
            let entry = self.heap.pop()?;
            let id = entry.id.clone();

            let (item, item_bytes, next_finish) = {
                let Some(state) = self.identities.get_mut(&id) else {
                    warn!(?id, "heap entry missing identity state");
                    continue;
                };
                if entry.finish_time != state.finish_time {
                    warn!(
                        ?id,
                        entry_finish_time = entry.finish_time,
                        state_finish_time = state.finish_time,
                        "stale heap entry"
                    );
                    continue;
                }

                let item = match state.queue.pop_front() {
                    Some(item) => item,
                    None => {
                        warn!(?id, "heap entry points to empty identity queue");
                        continue;
                    }
                };
                let item_bytes = item.len();
                state.queued_bytes = state.queued_bytes.saturating_sub(item_bytes);

                let next_finish = if state.queue.is_empty() {
                    None
                } else {
                    if state.queue.capacity() > state.queue.len() * IDENTITY_QUEUE_SHRINK_RATIO {
                        state
                            .queue
                            .shrink_to(state.queue.len().max(IDENTITY_QUEUE_SHRINK_FLOOR));
                    }
                    let base = entry.finish_time.max(state.finish_time);
                    state.finish_time = base + state.score.reciprocal();
                    Some(state.finish_time)
                };
                (item, item_bytes, next_finish)
            };

            self.total_items = self.total_items.saturating_sub(1);
            self.total_bytes = self.total_bytes.saturating_sub(item_bytes);
            self.virtual_time = entry.finish_time;

            if let Some(finish_time) = next_finish {
                self.heap.push(HeapEntry {
                    finish_time,
                    id: id.clone(),
                });
            } else {
                self.identities.remove(&id);
            }

            return Some(PopItem { id, item });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_entry_eq_matches_ord_for_equal_finish_time() {
        let a = HeapEntry {
            finish_time: 1.0,
            id: 1u32,
        };
        let b = HeapEntry {
            finish_time: 1.0,
            id: 2u32,
        };

        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert!(a == b);
    }

    #[test]
    fn pop_next_does_not_panic_when_total_items_is_stale() {
        let item_len = std::mem::size_of::<u32>();
        let mut pool = Pool::new(100, 100 * item_len, 100 * item_len);
        let score = Score::try_from(1.0).unwrap();
        pool.push_new(0u32, 42u32, score).unwrap();
        pool.remove_items(usize::MAX);

        assert_eq!(pool.pop_next().map(|item| item.id), Some(0));
    }

    #[test]
    fn identity_byte_limit_is_enforced() {
        let item_len = std::mem::size_of::<u32>();
        let mut pool = Pool::new(10, item_len, 10 * item_len);
        let score = Score::try_from(1.0).unwrap();

        pool.push_new(0u32, 0u32, score).unwrap();

        let err = pool.push_existing(&0, 99).unwrap_err();
        assert!(matches!(err, PushError::PerIdByteLimitExceeded { .. }));
    }

    #[test]
    fn partially_drained_identity_shrinks_queue_capacity() {
        let item_len = std::mem::size_of::<u32>();
        let mut pool = Pool::new(4_096, 2_048 * item_len, 4_096 * item_len);
        let score = Score::try_from(1.0).unwrap();

        pool.push_new(0u32, 0u32, score).unwrap();
        for item in 1..1_024u32 {
            pool.push_existing(&0, item).unwrap();
        }

        let peak_capacity = pool.identities[&0].queue.capacity();
        assert!(peak_capacity >= 1_024);

        for _ in 0..1_019 {
            let popped = pool.pop_next().unwrap();
            assert_eq!(popped.id, 0);
        }

        assert_eq!(pool.identities[&0].queue.len(), 5);
        assert!(pool.identities[&0].queue.capacity() <= 10);

        for _ in 0..4 {
            let popped = pool.pop_next().unwrap();
            assert_eq!(popped.id, 0);
        }

        assert_eq!(pool.identities[&0].queue.len(), 1);
        assert_eq!(
            pool.identities[&0].queue.capacity(),
            IDENTITY_QUEUE_SHRINK_FLOOR
        );
    }
}
