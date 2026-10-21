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

//! Steps 4 and 5 of Alg. 1: accepting second-round entries, clean sets and
//! the adoption rule; plus the fast-entry tally of the fast path.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    quorum::Quorum,
    types::{Entry, NodeId, PathEntry, ProposalId, ProposalIndex, ProposalMap},
};

pub(crate) struct SecondRound {
    q: Quorum,
    // per j, accepted entries in acceptance order
    accepted: Vec<Vec<(NodeId, PathEntry)>>,
    // fast entries awaiting corroboration (ln:qv:accept)
    pending: BTreeMap<(NodeId, ProposalIndex), Entry>,
    // ln:qv:clean: the first clean set obtained, per j
    clean: Vec<Option<Vec<(NodeId, PathEntry)>>>,
    // per j, the senders of ⟨fast, v⟩ per v
    fast: Vec<BTreeMap<Entry, BTreeSet<NodeId>>>,
}

impl SecondRound {
    pub fn new(q: Quorum, k: usize) -> Self {
        Self {
            q,
            accepted: vec![Vec::new(); k],
            pending: BTreeMap::new(),
            clean: vec![None; k],
            fast: vec![BTreeMap::new(); k],
        }
    }

    /// Adds the entries of a path vote: fallback entries are accepted upon
    /// receipt, fast entries once corroborated.
    pub fn add(&mut self, sender: NodeId, entries: &ProposalMap<PathEntry>) {
        for j in 0..entries.size() {
            match entries[j] {
                PathEntry::Fallback(_) => self.accept(j, sender, entries[j]),
                PathEntry::Fast(value) => {
                    self.fast[j].entry(value).or_default().insert(sender);
                    self.pending.insert((sender, j), value);
                }
            }
        }
    }

    /// Accepts every pending fast entry `corroborated(j, v)` now holds for;
    /// true if any.
    pub fn try_accept(&mut self, corroborated: impl Fn(ProposalIndex, Entry) -> bool) -> bool {
        let ready: Vec<(NodeId, ProposalIndex, Entry)> = self
            .pending
            .iter()
            .filter(|((_, j), value)| corroborated(*j, **value))
            .map(|((sender, j), value)| (*sender, *j, *value))
            .collect();
        for (sender, j, value) in &ready {
            self.pending.remove(&(*sender, *j));
            self.accept(*j, *sender, PathEntry::Fast(*value));
        }
        !ready.is_empty()
    }

    fn accept(&mut self, j: ProposalIndex, sender: NodeId, entry: PathEntry) {
        self.accepted[j].push((sender, entry));
        if self.clean[j].is_none() {
            self.clean[j] = self.clean_set(j, entry);
        }
    }

    // the clean set the newly accepted `trigger` completes: strong accepted
    // entries whose fast entries agree, the first ones by acceptance order
    fn clean_set(&self, j: ProposalIndex, trigger: PathEntry) -> Option<Vec<(NodeId, PathEntry)>> {
        let accepted = &self.accepted[j];
        let fallbacks = accepted.iter().filter(|(_, e)| !e.is_fast()).count();
        let fast_count = |v: Entry| {
            accepted
                .iter()
                .filter(|(_, e)| *e == PathEntry::Fast(v))
                .count()
        };
        let fast_value = match trigger {
            PathEntry::Fast(v) => (fast_count(v) + fallbacks >= self.q.strong).then_some(v)?,
            PathEntry::Fallback(_) => {
                let candidates: BTreeSet<Entry> = accepted
                    .iter()
                    .filter(|(_, e)| e.is_fast())
                    .map(|(_, e)| e.value())
                    .collect();
                let best = candidates
                    .into_iter()
                    .map(|v| (fast_count(v), v))
                    .filter(|(count, _)| count + fallbacks >= self.q.strong)
                    .max_by(|(a, va), (b, vb)| a.cmp(b).then(vb.cmp(va)));
                match best {
                    Some((_, v)) => v,
                    None if fallbacks >= self.q.strong => {
                        return Some(self.first(j, |e| !e.is_fast()));
                    }
                    None => return None,
                }
            }
        };
        Some(self.first(j, |e| !e.is_fast() || e == PathEntry::Fast(fast_value)))
    }

    fn first(
        &self,
        j: ProposalIndex,
        member: impl Fn(PathEntry) -> bool,
    ) -> Vec<(NodeId, PathEntry)> {
        self.accepted[j]
            .iter()
            .filter(|(_, e)| member(*e))
            .take(self.q.strong)
            .copied()
            .collect()
    }

    pub fn all_clean(&self) -> bool {
        self.clean.iter().all(Option::is_some)
    }

    #[cfg(test)]
    pub fn clean(&self, j: ProposalIndex) -> Option<&[(NodeId, PathEntry)]> {
        self.clean[j].as_deref()
    }

    /// `adopt(clean(j))` (ln:qv:adopt to ln:qv:adoptbot); None until the
    /// set is obtained or while a fast root's check is unresolved.
    pub fn adopt(
        &self,
        j: ProposalIndex,
        checked: impl Fn(&ProposalId) -> Option<Entry>,
    ) -> Option<Entry> {
        let set = self.clean[j].as_ref()?;
        if let Some(fast) = set.iter().map(|(_, e)| *e).find(PathEntry::is_fast) {
            return match fast.value() {
                Entry::Bot => Some(Entry::Bot),
                // ln:qv:adoptcheck
                Entry::Root(pid) => checked(&pid),
            };
        }
        let mut roots: Vec<(usize, ProposalId)> = Vec::new();
        for (_, entry) in set {
            if let Entry::Root(pid) = entry.value() {
                match roots.iter_mut().find(|(_, r)| *r == pid) {
                    Some((count, _)) => *count += 1,
                    None => roots.push((1, pid)),
                }
            }
        }
        // two roots can each reach f+1 when n ≠ 3f+1: most, then smallest
        let root = roots
            .into_iter()
            .filter(|(count, _)| *count >= self.q.weak)
            .max_by(|(a, pa), (b, pb)| a.cmp(b).then(pb.as_bytes().cmp(pa.as_bytes())));
        Some(root.map_or(Entry::Bot, |(_, pid)| Entry::Root(pid)))
    }

    /// The value `strong` path votes carry as a fast entry for j, if any.
    pub fn fast_quorum(&self, j: ProposalIndex) -> Option<(Entry, &BTreeSet<NodeId>)> {
        self.fast[j]
            .iter()
            .find(|(_, senders)| senders.len() >= self.q.strong)
            .map(|(value, senders)| (*value, senders))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::test_util::pid;

    fn round(n: usize) -> SecondRound {
        SecondRound::new(Quorum::new(n), 1)
    }

    fn add(round: &mut SecondRound, sender: u64, entry: PathEntry) {
        round.add(NodeId::dummy(sender), &ProposalMap::new(1, |_| entry));
    }

    fn members(round: &SecondRound) -> Vec<u64> {
        let set = round.clean(0).expect("clean set");
        set.iter().map(|(id, _)| u64::from(*id)).collect()
    }

    #[test]
    fn fallback_entries_alone_are_clean() {
        let mut round = round(4);
        for sender in 0..3 {
            add(&mut round, sender, PathEntry::Fallback(Entry::Bot));
        }
        assert_eq!(members(&round), vec![0, 1, 2]);
        assert_eq!(round.adopt(0, |_| None), Some(Entry::Bot));
    }

    #[test]
    fn fast_entries_wait_for_corroboration() {
        let mut round = round(4);
        let r = Entry::Root(pid(1));
        for sender in 0..3 {
            add(&mut round, sender, PathEntry::Fast(r));
        }
        assert!(!round.all_clean());
        assert!(!round.try_accept(|_, _| false));
        assert!(round.try_accept(|_, v| v == r));
        assert_eq!(members(&round), vec![0, 1, 2]);
        // the fast root is adopted only once its check resolves
        assert_eq!(round.adopt(0, |_| None), None);
        assert_eq!(round.adopt(0, |p| Some(Entry::Root(*p))), Some(r));
        assert_eq!(round.adopt(0, |_| Some(Entry::Bot)), Some(Entry::Bot));
    }

    #[test]
    fn disagreeing_fast_entries_never_share_a_set() {
        let mut round = round(4);
        let r = Entry::Root(pid(1));
        add(&mut round, 0, PathEntry::Fast(r));
        add(&mut round, 1, PathEntry::Fast(Entry::Bot));
        round.try_accept(|_, _| true);
        add(&mut round, 2, PathEntry::Fallback(r));
        assert!(!round.all_clean());
        add(&mut round, 3, PathEntry::Fallback(Entry::Bot));
        // both fast values complete at once: the larger fast count, then
        // the smaller value (roots before ⊥)
        assert_eq!(members(&round), vec![0, 2, 3]);
        assert_eq!(round.adopt(0, |p| Some(Entry::Root(*p))), Some(r));
    }

    #[test]
    fn fast_bot_in_the_set_adopts_bot() {
        let mut round = round(4);
        add(&mut round, 0, PathEntry::Fast(Entry::Bot));
        round.try_accept(|_, _| true);
        add(&mut round, 1, PathEntry::Fallback(Entry::Root(pid(2))));
        add(&mut round, 2, PathEntry::Fallback(Entry::Root(pid(2))));
        assert_eq!(round.adopt(0, |_| None), Some(Entry::Bot));
    }

    #[test]
    fn fallback_roots_need_weak_support() {
        let mut round = round(4);
        add(&mut round, 0, PathEntry::Fallback(Entry::Root(pid(2))));
        add(&mut round, 1, PathEntry::Fallback(Entry::Root(pid(2))));
        add(&mut round, 2, PathEntry::Fallback(Entry::Bot));
        assert_eq!(round.adopt(0, |_| None), Some(Entry::Root(pid(2))));

        let mut round = super::tests::round(4);
        add(&mut round, 0, PathEntry::Fallback(Entry::Root(pid(2))));
        add(&mut round, 1, PathEntry::Fallback(Entry::Root(pid(3))));
        add(&mut round, 2, PathEntry::Fallback(Entry::Bot));
        assert_eq!(round.adopt(0, |_| None), Some(Entry::Bot));
    }

    // n = 8: weak 3, strong 6, so two fallback roots can each reach weak
    #[test]
    fn two_weak_fallback_roots_pick_the_larger_count() {
        let mut round = round(8);
        for (sender, byte) in [(0, 5), (1, 5), (2, 5), (3, 4), (4, 4), (5, 4), (6, 4)] {
            add(
                &mut round,
                sender,
                PathEntry::Fallback(Entry::Root(pid(byte))),
            );
        }
        // the first six: three of each, ties to the smaller pid
        assert_eq!(round.adopt(0, |_| None), Some(Entry::Root(pid(4))));
    }

    #[test]
    fn fast_quorum_needs_strong_matching_fast_entries() {
        let mut round = round(4);
        let r = Entry::Root(pid(1));
        add(&mut round, 0, PathEntry::Fast(r));
        add(&mut round, 1, PathEntry::Fast(r));
        add(&mut round, 2, PathEntry::Fallback(r));
        assert!(round.fast_quorum(0).is_none());
        add(&mut round, 3, PathEntry::Fast(r));
        assert_eq!(
            round.fast_quorum(0).map(|(v, s)| (v, s.len())),
            Some((r, 3))
        );
    }
}
