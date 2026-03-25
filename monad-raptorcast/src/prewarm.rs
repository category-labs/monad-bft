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
    collections::{HashSet, VecDeque},
    hash::Hash,
};

pub(crate) trait SessionPrewarmBackend<ID> {
    fn has_session(&self, id: &ID) -> bool;
    fn connect(&mut self, id: &ID) -> bool;
    fn flush(&mut self);
}

pub(crate) struct RoundRobinSessionPrewarmer<ID> {
    max_connects_per_tick: usize,
    pending: VecDeque<ID>,
    existing: HashSet<ID>,
}

impl<ID> RoundRobinSessionPrewarmer<ID>
where
    ID: Clone + Eq + Hash,
{
    pub(crate) fn new(max_connects_per_tick: usize) -> Self {
        Self {
            max_connects_per_tick,
            pending: VecDeque::new(),
            existing: HashSet::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.pending.clear();
        self.existing.clear();
    }

    pub(crate) fn reset(&mut self, ids: impl IntoIterator<Item = ID>) {
        self.clear();

        let mut seen = HashSet::new();
        self.pending
            .extend(ids.into_iter().filter(|id| seen.insert(id.clone())));
    }

    pub(crate) fn tick<B>(&mut self, backend: &mut B)
    where
        B: SessionPrewarmBackend<ID>,
    {
        self.requeue_missing_existing(backend);
        self.promote_pending_with_sessions(backend);

        let mut connect_calls = 0;
        let mut needs_flush = false;
        let pending_len = self.pending.len();

        for _ in 0..pending_len {
            if connect_calls == self.max_connects_per_tick {
                break;
            }

            let id = self
                .pending
                .pop_front()
                .expect("pending length computed from the queue");

            if backend.has_session(&id) {
                self.existing.insert(id);
                continue;
            }

            connect_calls += 1;
            needs_flush |= backend.connect(&id);
            self.pending.push_back(id);
        }

        if needs_flush {
            backend.flush();
        }
    }

    fn requeue_missing_existing<B>(&mut self, backend: &mut B)
    where
        B: SessionPrewarmBackend<ID>,
    {
        let mut missing = Vec::new();
        self.existing.retain(|id| {
            let has_session = backend.has_session(id);
            if !has_session {
                missing.push(id.clone());
            }
            has_session
        });
        self.pending.extend(missing);
    }

    fn promote_pending_with_sessions<B>(&mut self, backend: &mut B)
    where
        B: SessionPrewarmBackend<ID>,
    {
        let pending_len = self.pending.len();
        for _ in 0..pending_len {
            let id = self
                .pending
                .pop_front()
                .expect("pending length computed from the queue");

            if backend.has_session(&id) {
                self.existing.insert(id);
            } else {
                self.pending.push_back(id);
            }
        }
    }

    #[cfg(test)]
    fn pending_ids(&self) -> Vec<ID> {
        self.pending.iter().cloned().collect()
    }

    #[cfg(test)]
    fn existing_ids(&self) -> HashSet<ID> {
        self.existing.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::{HashMap, HashSet, VecDeque},
    };

    use super::{RoundRobinSessionPrewarmer, SessionPrewarmBackend};

    #[derive(Default)]
    struct FakeBackend {
        sessions: HashSet<u8>,
        scripted_has_session: RefCell<HashMap<u8, VecDeque<bool>>>,
        connect_results: HashMap<u8, bool>,
        connect_calls: Vec<u8>,
        flushes: usize,
    }

    impl SessionPrewarmBackend<u8> for FakeBackend {
        fn has_session(&self, id: &u8) -> bool {
            self.scripted_has_session
                .borrow_mut()
                .get_mut(id)
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| self.sessions.contains(id))
        }

        fn connect(&mut self, id: &u8) -> bool {
            self.connect_calls.push(*id);
            self.connect_results.get(id).copied().unwrap_or(false)
        }

        fn flush(&mut self) {
            self.flushes += 1;
        }
    }

    #[test]
    fn reset_replaces_queue_and_deduplicates_ids() {
        let mut prewarmer = RoundRobinSessionPrewarmer::new(10);
        prewarmer.reset([1, 2, 1]);
        prewarmer.reset([3, 3, 4]);

        assert_eq!(prewarmer.pending_ids(), vec![3, 4]);
        assert!(prewarmer.existing_ids().is_empty());
    }

    #[test]
    fn tick_round_robins_without_starvation_when_connects_fail() {
        let mut prewarmer = RoundRobinSessionPrewarmer::new(2);
        prewarmer.reset(0..12);

        let mut backend = FakeBackend::default();
        for _ in 0..6 {
            prewarmer.tick(&mut backend);
        }

        assert_eq!(backend.connect_calls, (0..12).collect::<Vec<_>>());
        assert_eq!(backend.flushes, 0);
        assert_eq!(prewarmer.pending_ids(), (0..12).collect::<Vec<_>>());
    }

    #[test]
    fn tick_removes_pending_id_when_session_already_exists() {
        let mut prewarmer = RoundRobinSessionPrewarmer::new(10);
        prewarmer.reset([1, 2]);

        let mut backend = FakeBackend::default();
        backend.sessions.insert(1);

        prewarmer.tick(&mut backend);

        assert_eq!(backend.connect_calls, vec![2]);
        assert_eq!(prewarmer.pending_ids(), vec![2]);
        assert_eq!(prewarmer.existing_ids(), HashSet::from([1]));
    }

    #[test]
    fn tick_removes_pending_id_when_session_appears_during_tick() {
        let mut prewarmer = RoundRobinSessionPrewarmer::new(10);
        prewarmer.reset([1]);

        let mut backend = FakeBackend::default();
        backend
            .scripted_has_session
            .borrow_mut()
            .insert(1, VecDeque::from([false, true]));

        prewarmer.tick(&mut backend);

        assert!(backend.connect_calls.is_empty());
        assert!(prewarmer.pending_ids().is_empty());
        assert_eq!(prewarmer.existing_ids(), HashSet::from([1]));
    }

    #[test]
    fn tick_moves_existing_id_back_to_pending_when_session_is_lost() {
        let mut prewarmer = RoundRobinSessionPrewarmer::new(0);
        prewarmer.reset([1]);

        let mut backend = FakeBackend::default();
        backend.sessions.insert(1);
        prewarmer.tick(&mut backend);

        backend.sessions.clear();
        prewarmer.tick(&mut backend);

        assert!(backend.connect_calls.is_empty());
        assert_eq!(prewarmer.pending_ids(), vec![1]);
        assert!(prewarmer.existing_ids().is_empty());
    }

    #[test]
    fn tick_keeps_existing_id_out_of_pending_when_session_still_exists() {
        let mut prewarmer = RoundRobinSessionPrewarmer::new(0);
        prewarmer.reset([1]);

        let mut backend = FakeBackend::default();
        backend.sessions.insert(1);
        prewarmer.tick(&mut backend);
        prewarmer.tick(&mut backend);

        assert!(backend.connect_calls.is_empty());
        assert!(prewarmer.pending_ids().is_empty());
        assert_eq!(prewarmer.existing_ids(), HashSet::from([1]));
    }

    #[test]
    fn tick_flushes_once_after_successful_connects() {
        let mut prewarmer = RoundRobinSessionPrewarmer::new(10);
        prewarmer.reset([1, 2, 3]);

        let mut backend = FakeBackend {
            connect_results: HashMap::from([(1, true), (2, false), (3, true)]),
            ..Default::default()
        };

        prewarmer.tick(&mut backend);

        assert_eq!(backend.connect_calls, vec![1, 2, 3]);
        assert_eq!(backend.flushes, 1);
        assert_eq!(prewarmer.pending_ids(), vec![1, 2, 3]);
        assert!(prewarmer.existing_ids().is_empty());
    }
}
