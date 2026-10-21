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

//! The view-change rule (`ln:vc:on_vote2_quorum_compute_approved`)
//! evaluated by counts, per Claim `clm:coda:count_rule`.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    CodaVal,
    view::{EchoKey, Key},
};
use crate::{
    quorum::Quorum,
    types::{CodaMetablock, CodaValue, OrdMap},
};

/// Every `(j, v)` the view-change rule echoes for some set `S` of `strong`
/// accepted Vote2 messages (`accepted`: one `(lock, guard)` per sender).
pub(super) fn view_change_echoes<'a, V: CodaVal + 'a>(
    q: Quorum,
    k: usize,
    accepted: impl IntoIterator<Item = (&'a Option<CodaMetablock<V>>, &'a CodaMetablock<V>)>,
) -> BTreeSet<EchoKey<V>> {
    // |U|, |L_ℓ| and c_j(v) of the claim
    let mut unlocked = 0;
    let mut locks: BTreeMap<Key<V>, usize> = BTreeMap::new();
    let mut guards: Vec<BTreeMap<&CodaValue<V>, usize>> = vec![BTreeMap::new(); k];
    for (lock, guard) in accepted {
        match lock {
            Some(lock) => *locks.entry(OrdMap(lock.clone())).or_default() += 1,
            None => {
                unlocked += 1;
                for (j, counts) in guards.iter_mut().enumerate() {
                    *counts.entry(&guard[j]).or_default() += 1;
                }
            }
        }
    }

    let mut echoes = BTreeSet::new();
    // condition 1: a lock ℓ completed to a set by unlocked messages
    for (OrdMap(lock), count) in &locks {
        if *count + unlocked >= q.strong {
            echoes.extend((0..k).map(|j| (j as u16, lock[j].clone())));
        }
    }
    for (j, counts) in guards.iter().enumerate() {
        // condition 2: an entry f+1 guards share in an unlocked set
        if unlocked >= q.strong {
            echoes.extend(
                counts
                    .iter()
                    .filter(|(_, count)| **count >= q.weak)
                    .map(|(v, _)| (j as u16, (*v).clone())),
            );
        }
        // condition 3: an unlocked set in which no entry reaches f+1
        let capped: usize = counts.values().map(|count| (*count).min(q.f)).sum();
        if capped >= q.strong {
            echoes.insert((j as u16, CodaValue::Empty));
        }
    }
    echoes
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::types::ProposalMap;

    type Vote2 = (Option<CodaMetablock<u8>>, CodaMetablock<u8>);

    // the rule as written: every set S of `strong` accepted Vote2s whose
    // non-none locks are identical
    fn brute_force(q: Quorum, k: usize, accepted: &[Vote2]) -> BTreeSet<EchoKey<u8>> {
        let mut echoes = BTreeSet::new();
        let m = accepted.len();
        for mask in 0u32..(1 << m) {
            if mask.count_ones() as usize != q.strong {
                continue;
            }
            let set: Vec<&Vote2> = (0..m)
                .filter(|i| mask & (1 << i) != 0)
                .map(|i| &accepted[i])
                .collect();
            let locks: BTreeSet<OrdMap<CodaValue<u8>>> = set
                .iter()
                .filter_map(|(lock, _)| lock.clone().map(OrdMap))
                .collect();
            match locks.len() {
                0 => {
                    for j in 0..k {
                        let mut counts: BTreeMap<&CodaValue<u8>, usize> = BTreeMap::new();
                        for (_, guard) in &set {
                            *counts.entry(&guard[j]).or_default() += 1;
                        }
                        let shared: Vec<_> = counts
                            .iter()
                            .filter(|(_, count)| **count >= q.weak)
                            .map(|(v, _)| (j as u16, **v))
                            .collect();
                        if shared.is_empty() {
                            echoes.insert((j as u16, CodaValue::Empty));
                        }
                        echoes.extend(shared);
                    }
                }
                1 => {
                    let OrdMap(lock) = locks.into_iter().next().unwrap();
                    echoes.extend((0..k).map(|j| (j as u16, lock[j])));
                }
                _ => {}
            }
        }
        echoes
    }

    fn value(choice: u8) -> CodaValue<u8> {
        match choice {
            0 => CodaValue::Empty,
            v => CodaValue::Value(v),
        }
    }

    fn vote2_strategy(k: usize) -> impl Strategy<Value = Vote2> {
        let guard = proptest::collection::vec(0u8..3, k);
        let lock = proptest::option::weighted(0.4, 1u8..3);
        (lock, guard).prop_map(move |(lock, guard)| {
            let lock = lock.map(|l| ProposalMap::new(k, |j| value((l + j as u8) % 3)));
            (lock, ProposalMap::new(k, |j| value(guard[j])))
        })
    }

    fn case() -> impl Strategy<Value = (usize, usize, Vec<Vote2>)> {
        (1usize..=10, 1usize..=2).prop_flat_map(|(n, k)| {
            let votes = proptest::collection::vec(vote2_strategy(k), 0..=n);
            (Just(n), Just(k), votes)
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn count_rule_matches_brute_force((n, k, accepted) in case()) {
            let q = Quorum::new(n);
            let by_count = view_change_echoes(q, k, accepted.iter().map(|(l, g)| (l, g)));
            prop_assert_eq!(by_count, brute_force(q, k, &accepted));
        }
    }

    #[test]
    fn unanimous_unlocked_guards_echo_their_value() {
        let q = Quorum::new(4);
        let guard = ProposalMap::new(1, |_| CodaValue::Value(5u8));
        let accepted: Vec<Vote2> = vec![(None, guard); 3];
        let echoes = view_change_echoes(q, 1, accepted.iter().map(|(l, g)| (l, g)));
        assert_eq!(echoes, BTreeSet::from([(0, CodaValue::Value(5))]));
    }

    #[test]
    fn diverging_guards_echo_empty() {
        let q = Quorum::new(4);
        let accepted: Vec<Vote2> = (1..=3u8)
            .map(|v| (None, ProposalMap::new(1, |_| CodaValue::Value(v))))
            .collect();
        let echoes = view_change_echoes(q, 1, accepted.iter().map(|(l, g)| (l, g)));
        assert_eq!(echoes, BTreeSet::from([(0, CodaValue::Empty)]));
    }

    #[test]
    fn a_lock_completed_by_unlocked_messages_is_echoed() {
        let q = Quorum::new(4);
        let lock = ProposalMap::new(2, |j| CodaValue::Value(j as u8 + 1));
        let guard = ProposalMap::new(2, |_| CodaValue::Value(9u8));
        let accepted: Vec<Vote2> = vec![
            (Some(lock), guard.clone()),
            (None, guard.clone()),
            (None, guard),
        ];
        let echoes = view_change_echoes(q, 2, accepted.iter().map(|(l, g)| (l, g)));
        // the lock-free set {2, 3} is too small; the set with the lock
        // echoes the lock
        assert_eq!(
            echoes,
            BTreeSet::from([(0, CodaValue::Value(1)), (1, CodaValue::Value(2))])
        );
    }
}
