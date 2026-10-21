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

use std::collections::{BTreeMap, BTreeSet};

use super::CodaVal;
use crate::types::{CodaMetablock, CodaValue, NodeId, OrdMap};

pub(super) type Key<V> = OrdMap<CodaValue<V>>;
pub(super) type EchoKey<V> = (u16, CodaValue<V>);

/// The view-specific variables of `ln:core:vs_header`.
pub(super) struct OwnView<V> {
    // ln:core:pv_guard; none for a previous view entered only for echoes
    pub guard: Option<CodaMetablock<V>>,
    // ln:core:pv_lock
    pub lock: Option<CodaMetablock<V>>,
    // ln:core:pv_votes
    pub vote1: Option<CodaMetablock<V>>,
    pub vote2: Option<(Option<CodaMetablock<V>>, CodaMetablock<V>)>,
    // ln:core:pv_echo_set
    pub echoes: BTreeSet<EchoKey<V>>,
}

impl<V> OwnView<V> {
    pub fn new(guard: Option<CodaMetablock<V>>) -> Self {
        Self {
            guard,
            lock: None,
            vote1: None,
            vote2: None,
            echoes: BTreeSet::new(),
        }
    }
}

/// Messages of one view received from senders whose window covers it, at
/// most one Prepare, Vote1 and Vote2 per sender.
pub(super) struct ReceivedView<V: CodaVal> {
    pub prepare: Option<CodaMetablock<V>>,
    pub vote1: BTreeMap<NodeId, CodaMetablock<V>>,
    pub vote1_count: BTreeMap<Key<V>, usize>,
    // ln:vc:rule_one_vote2_per_sender: the first Vote2 per sender
    pub vote2: BTreeMap<NodeId, (Option<CodaMetablock<V>>, CodaMetablock<V>)>,
    pub vote2_lock_count: BTreeMap<Key<V>, usize>,
    // senders whose Vote2 is accepted (ln:vc:rule_accept_vote2_*)
    pub accepted: BTreeSet<NodeId>,
    pub echo: BTreeMap<EchoKey<V>, BTreeSet<NodeId>>,
    // distinct values kept per (sender, j), capped by `add_echo`
    echoed: BTreeMap<(NodeId, u16), usize>,
}

impl<V: CodaVal> Default for ReceivedView<V> {
    fn default() -> Self {
        Self {
            prepare: None,
            vote1: BTreeMap::new(),
            vote1_count: BTreeMap::new(),
            vote2: BTreeMap::new(),
            vote2_lock_count: BTreeMap::new(),
            accepted: BTreeSet::new(),
            echo: BTreeMap::new(),
            echoed: BTreeMap::new(),
        }
    }
}

fn bump<V: CodaVal>(counts: &mut BTreeMap<Key<V>, usize>, key: &CodaMetablock<V>) {
    *counts.entry(OrdMap(key.clone())).or_default() += 1;
}

fn unbump<V: CodaVal>(counts: &mut BTreeMap<Key<V>, usize>, key: &CodaMetablock<V>) {
    let key = OrdMap(key.clone());
    if let Some(count) = counts.get_mut(&key) {
        *count -= 1;
        if *count == 0 {
            counts.remove(&key);
        }
    }
}

impl<V: CodaVal> ReceivedView<V> {
    pub fn is_empty(&self) -> bool {
        self.prepare.is_none()
            && self.vote1.is_empty()
            && self.vote2.is_empty()
            && self.echo.is_empty()
    }

    #[cfg(test)]
    pub fn holds(&self, sender: &NodeId) -> bool {
        self.vote1.contains_key(sender)
            || self.vote2.contains_key(sender)
            || self.echo.values().any(|senders| senders.contains(sender))
    }

    #[cfg(test)]
    pub fn echoes_of(&self, sender: &NodeId) -> usize {
        self.echo
            .values()
            .filter(|senders| senders.contains(sender))
            .count()
    }

    pub fn add_vote1(&mut self, sender: NodeId, x: CodaMetablock<V>) {
        if self.vote1.contains_key(&sender) {
            return;
        }
        bump(&mut self.vote1_count, &x);
        self.vote1.insert(sender, x);
    }

    pub fn add_vote2(
        &mut self,
        sender: NodeId,
        lock: Option<CodaMetablock<V>>,
        guard: CodaMetablock<V>,
    ) {
        if self.vote2.contains_key(&sender) {
            return;
        }
        if let Some(lock) = &lock {
            bump(&mut self.vote2_lock_count, lock);
        }
        self.vote2.insert(sender, (lock, guard));
    }

    /// Keeps at most `cap` distinct values per (sender, j); a correct sender
    /// echoes at most n+1 (clm:coda:echoes_per_view).
    pub fn add_echo(&mut self, sender: NodeId, j: u16, v: CodaValue<V>, cap: usize) {
        let echoed = self.echoed.entry((sender, j)).or_default();
        if *echoed >= cap {
            return;
        }
        if self.echo.entry((j, v)).or_default().insert(sender) {
            *echoed += 1;
        }
    }

    /// Drops every message of `sender`, with its contributions to counts.
    /// `is_leader` says whether the kept Prepare came from it.
    pub fn evict(&mut self, sender: &NodeId, is_leader: bool) {
        if is_leader {
            self.prepare = None;
        }
        if let Some(x) = self.vote1.remove(sender) {
            unbump(&mut self.vote1_count, &x);
        }
        if let Some((lock, _)) = self.vote2.remove(sender)
            && let Some(lock) = lock
        {
            unbump(&mut self.vote2_lock_count, &lock);
        }
        self.accepted.remove(sender);
        self.echo.retain(|_, senders| {
            senders.remove(sender);
            !senders.is_empty()
        });
        self.echoed.retain(|(echoer, _), _| echoer != sender);
    }

    pub fn vote1_count(&self, x: &CodaMetablock<V>) -> usize {
        self.vote1_count
            .get(&OrdMap(x.clone()))
            .copied()
            .unwrap_or(0)
    }

    pub fn echo_count(&self, j: u16, v: &CodaValue<V>) -> usize {
        self.echo.get(&(j, v.clone())).map_or(0, BTreeSet::len)
    }

    /// Accepts every pending Vote2 whose condition now holds; true if any.
    pub fn update_acceptance(&mut self, weak: usize) -> bool {
        let newly: Vec<NodeId> = self
            .vote2
            .iter()
            .filter(|(sender, _)| !self.accepted.contains(sender))
            .filter(|(_, (lock, _))| match lock {
                // ln:vc:rule_accept_vote2_bot
                None => true,
                // ln:vc:rule_accept_vote2_lock
                Some(lock) => self.vote1_count(lock) >= weak,
            })
            .map(|(sender, _)| *sender)
            .collect();
        self.accepted.extend(newly.iter().copied());
        !newly.is_empty()
    }

    pub fn accepted_vote2s(
        &self,
    ) -> impl Iterator<Item = (&Option<CodaMetablock<V>>, &CodaMetablock<V>)> {
        self.accepted.iter().map(|sender| {
            let (lock, guard) = &self.vote2[sender];
            (lock, guard)
        })
    }

    /// Per proposer, the value echoed by the most senders among those
    /// echoed by at least `threshold`, ties to the smallest value.
    pub fn certificate(&self, k: usize, threshold: usize) -> Option<CodaMetablock<V>> {
        let mut best: Vec<Option<(usize, &CodaValue<V>)>> = vec![None; k];
        for ((j, v), senders) in &self.echo {
            let count = senders.len();
            let Some(slot) = best.get_mut(*j as usize) else {
                continue;
            };
            if count >= threshold && slot.is_none_or(|(best_count, _)| count > best_count) {
                *slot = Some((count, v));
            }
        }
        let values: Option<Vec<CodaValue<V>>> = best
            .into_iter()
            .map(|choice| choice.map(|(_, v)| v.clone()))
            .collect();
        let values = values?;
        Some(CodaMetablock::new(k, |j| values[j].clone()))
    }
}
