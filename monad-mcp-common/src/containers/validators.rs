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

//! Containers over the validators of one epoch, addressed by
//! [`ValidatorIndex`]. Iteration is in index order.

use std::fmt;

use super::index_vec::Idx;

/// The most validators an epoch may have. Fixes the size of a
/// [`ValidatorSet`].
pub const MAX_VALIDATORS: usize = WORDS * 64;

const WORDS: usize = 24;

/// A validator's position in one epoch's validator set, handed out by that
/// epoch's validator data. Meaningless outside that epoch, and not the same
/// thing as a node's position in a chunk assignment.
///
/// Invariant: below [`MAX_VALIDATORS`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValidatorIndex(u16);

impl ValidatorIndex {
    /// For the validator data that defines the indices; `None` from
    /// [`MAX_VALIDATORS`] on.
    pub const fn new(index: usize) -> Option<Self> {
        if index < MAX_VALIDATORS {
            Some(Self(index as u16))
        } else {
            None
        }
    }

    pub const fn get(self) -> usize {
        self.0 as usize
    }
}

impl fmt::Debug for ValidatorIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl Idx for ValidatorIndex {
    fn new(index: usize) -> Self {
        Self::new(index).expect("validator index in range")
    }

    fn index(self) -> usize {
        self.get()
    }
}

/// A set of validators of one epoch: a fixed-size bitset, so it never
/// allocates, and union, intersection and test-and-set are a few word
/// operations. Iterates in index order.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ValidatorSet {
    words: [u64; WORDS],
}

impl ValidatorSet {
    pub const fn new() -> Self {
        Self { words: [0; WORDS] }
    }

    /// Whether the validator was not in the set before.
    pub fn insert(&mut self, validator: ValidatorIndex) -> bool {
        let (word, bit) = Self::position(validator);
        let absent = self.words[word] & bit == 0;
        self.words[word] |= bit;
        absent
    }

    /// Whether the validator was in the set.
    pub fn remove(&mut self, validator: ValidatorIndex) -> bool {
        let (word, bit) = Self::position(validator);
        let present = self.words[word] & bit != 0;
        self.words[word] &= !bit;
        present
    }

    pub fn contains(&self, validator: ValidatorIndex) -> bool {
        let (word, bit) = Self::position(validator);
        self.words[word] & bit != 0
    }

    pub fn len(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }

    pub fn clear(&mut self) {
        self.words = [0; WORDS];
    }

    pub fn union(&self, other: &Self) -> Self {
        let mut union = *self;
        union.union_with(other);
        union
    }

    pub fn union_with(&mut self, other: &Self) {
        for (word, other) in self.words.iter_mut().zip(&other.words) {
            *word |= other;
        }
    }

    pub fn intersection(&self, other: &Self) -> Self {
        let mut intersection = *self;
        for (word, other) in intersection.words.iter_mut().zip(&other.words) {
            *word &= other;
        }
        intersection
    }

    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(&other.words)
            .all(|(word, other)| word & other == 0)
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(&other.words)
            .all(|(word, other)| word & !other == 0)
    }

    /// In index order.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            words: &self.words,
            next_word: 0,
            current: 0,
        }
    }

    fn position(validator: ValidatorIndex) -> (usize, u64) {
        let index = validator.get();
        (index / 64, 1 << (index % 64))
    }
}

impl fmt::Debug for ValidatorSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

impl FromIterator<ValidatorIndex> for ValidatorSet {
    fn from_iter<I: IntoIterator<Item = ValidatorIndex>>(iter: I) -> Self {
        let mut set = Self::new();
        set.extend(iter);
        set
    }
}

impl Extend<ValidatorIndex> for ValidatorSet {
    fn extend<I: IntoIterator<Item = ValidatorIndex>>(&mut self, iter: I) {
        for validator in iter {
            self.insert(validator);
        }
    }
}

impl<'a> IntoIterator for &'a ValidatorSet {
    type Item = ValidatorIndex;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// The members of a [`ValidatorSet`], in index order.
#[derive(Clone)]
pub struct Iter<'a> {
    words: &'a [u64; WORDS],
    // the index of the word after `current`
    next_word: usize,
    // the bits of word `next_word - 1` not yet returned
    current: u64,
}

impl Iterator for Iter<'_> {
    type Item = ValidatorIndex;

    fn next(&mut self) -> Option<ValidatorIndex> {
        while self.current == 0 {
            self.current = *self.words.get(self.next_word)?;
            self.next_word += 1;
        }
        let bit = self.current.trailing_zeros() as usize;
        self.current &= self.current - 1;
        Some(ValidatorIndex(((self.next_word - 1) * 64 + bit) as u16))
    }
}

/// A value per validator of one epoch, for some of them. Iterates in index
/// order. Sized by the highest index inserted so far, so it suits values
/// held for most validators, such as votes.
// todo: box the values, or keep them dense behind a u16 slot index, once a
// value type is large (a BLS signature is 96 bytes)
#[derive(Clone)]
pub struct ValidatorMap<T> {
    slots: Vec<Option<T>>,
    len: usize,
}

impl<T> ValidatorMap<T> {
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            len: 0,
        }
    }

    /// Room for validators below `validators` without reallocating.
    pub fn with_capacity(validators: usize) -> Self {
        Self {
            slots: Vec::with_capacity(validators),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, validator: ValidatorIndex) -> Option<&T> {
        self.slots.get(validator.get())?.as_ref()
    }

    pub fn get_mut(&mut self, validator: ValidatorIndex) -> Option<&mut T> {
        self.slots.get_mut(validator.get())?.as_mut()
    }

    pub fn contains_key(&self, validator: ValidatorIndex) -> bool {
        self.get(validator).is_some()
    }

    /// The value it replaces, if any.
    pub fn insert(&mut self, validator: ValidatorIndex, value: T) -> Option<T> {
        let index = validator.get();
        if index >= self.slots.len() {
            self.slots.resize_with(index + 1, || None);
        }
        let previous = self.slots[index].replace(value);
        if previous.is_none() {
            self.len += 1;
        }
        previous
    }

    pub fn remove(&mut self, validator: ValidatorIndex) -> Option<T> {
        let removed = self.slots.get_mut(validator.get())?.take();
        if removed.is_some() {
            self.len -= 1;
        }
        removed
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.len = 0;
    }

    /// The validators with a value.
    pub fn keys(&self) -> ValidatorSet {
        self.iter().map(|(validator, _)| validator).collect()
    }

    /// In index order.
    pub fn iter(&self) -> impl Iterator<Item = (ValidatorIndex, &T)> {
        self.slots.iter().enumerate().filter_map(|(index, slot)| {
            let value = slot.as_ref()?;
            Some((ValidatorIndex(index as u16), value))
        })
    }

    /// In index order.
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().flatten()
    }
}

impl<T> Default for ValidatorMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

// by content: maps with the same entries are equal however far their slots
// were grown
impl<T: PartialEq> PartialEq for ValidatorMap<T> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl<T: Eq> Eq for ValidatorMap<T> {}

impl<T: fmt::Debug> fmt::Debug for ValidatorMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// Panics if the validator has no value.
impl<T> std::ops::Index<ValidatorIndex> for ValidatorMap<T> {
    type Output = T;

    fn index(&self, validator: ValidatorIndex) -> &T {
        self.get(validator).expect("validator has a value")
    }
}

impl<T> FromIterator<(ValidatorIndex, T)> for ValidatorMap<T> {
    fn from_iter<I: IntoIterator<Item = (ValidatorIndex, T)>>(iter: I) -> Self {
        let mut map = Self::new();
        for (validator, value) in iter {
            map.insert(validator, value);
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;

    use super::*;

    fn index(i: u16) -> ValidatorIndex {
        ValidatorIndex::new(usize::from(i)).unwrap()
    }

    #[test]
    fn indices_stop_below_the_maximum() {
        assert_eq!(MAX_VALIDATORS, 1536);
        assert!(ValidatorIndex::new(MAX_VALIDATORS - 1).is_some());
        assert!(ValidatorIndex::new(MAX_VALIDATORS).is_none());
    }

    #[test]
    fn a_set_holds_the_first_and_last_index() {
        let last = index(MAX_VALIDATORS as u16 - 1);
        let mut set = ValidatorSet::new();
        assert!(set.insert(last));
        assert!(set.insert(index(0)));
        assert!(!set.insert(last));
        assert_eq!(set.iter().collect::<Vec<_>>(), [index(0), last]);
        assert_eq!(set.len(), 2);
    }

    #[derive(Debug, Clone)]
    enum SetOp {
        Insert(u16),
        Remove(u16),
    }

    fn validator() -> impl Strategy<Value = u16> {
        // dense at the low end, where word boundaries are, and across the range
        prop_oneof![0..130u16, 0..MAX_VALIDATORS as u16]
    }

    fn set_ops() -> impl Strategy<Value = Vec<SetOp>> {
        prop::collection::vec(
            prop_oneof![
                3 => validator().prop_map(SetOp::Insert),
                1 => validator().prop_map(SetOp::Remove),
            ],
            0..200,
        )
    }

    // applies the ops to the set and to a BTreeSet reference, checking
    // every answer on the way
    fn apply(ops: &[SetOp]) -> Result<(ValidatorSet, BTreeSet<u16>), TestCaseError> {
        let mut set = ValidatorSet::new();
        let mut reference = BTreeSet::new();
        for op in ops {
            match *op {
                SetOp::Insert(i) => prop_assert_eq!(set.insert(index(i)), reference.insert(i)),
                SetOp::Remove(i) => prop_assert_eq!(set.remove(index(i)), reference.remove(&i)),
            }
        }
        Ok((set, reference))
    }

    fn members(reference: &BTreeSet<u16>) -> Vec<ValidatorIndex> {
        reference.iter().map(|i| index(*i)).collect()
    }

    proptest! {
        #[test]
        fn a_set_behaves_like_an_ordered_set(ops in set_ops(), probe in validator()) {
            let (set, reference) = apply(&ops)?;
            prop_assert_eq!(set.iter().collect::<Vec<_>>(), members(&reference));
            prop_assert_eq!(set.len(), reference.len());
            prop_assert_eq!(set.is_empty(), reference.is_empty());
            prop_assert_eq!(set.contains(index(probe)), reference.contains(&probe));
            prop_assert_eq!(set, members(&reference).into_iter().collect::<ValidatorSet>());
        }

        #[test]
        fn set_algebra_matches_ordered_sets(a in set_ops(), b in set_ops()) {
            let (a, a_ref) = apply(&a)?;
            let (b, b_ref) = apply(&b)?;
            let union: BTreeSet<u16> = a_ref.union(&b_ref).copied().collect();
            let intersection: BTreeSet<u16> = a_ref.intersection(&b_ref).copied().collect();
            prop_assert_eq!(a.union(&b).iter().collect::<Vec<_>>(), members(&union));
            prop_assert_eq!(a.intersection(&b).iter().collect::<Vec<_>>(), members(&intersection));
            prop_assert_eq!(a.is_disjoint(&b), a_ref.is_disjoint(&b_ref));
            prop_assert_eq!(a.is_subset(&b), a_ref.is_subset(&b_ref));
            prop_assert!(a.is_subset(&a.union(&b)));
        }

        #[test]
        fn a_map_behaves_like_an_ordered_map(
            ops in prop::collection::vec((validator(), any::<Option<u8>>()), 0..200),
            probe in validator(),
        ) {
            let mut map = ValidatorMap::new();
            let mut reference = BTreeMap::new();
            for (i, value) in ops {
                match value {
                    Some(value) => {
                        prop_assert_eq!(map.insert(index(i), value), reference.insert(i, value));
                    }
                    None => prop_assert_eq!(map.remove(index(i)), reference.remove(&i)),
                }
            }
            let entries: Vec<_> = reference.iter().map(|(i, v)| (index(*i), v)).collect();
            prop_assert_eq!(map.iter().collect::<Vec<_>>(), entries.clone());
            prop_assert_eq!(map.values().collect::<Vec<_>>(), reference.values().collect::<Vec<_>>());
            prop_assert_eq!(map.len(), reference.len());
            prop_assert_eq!(map.is_empty(), reference.is_empty());
            prop_assert_eq!(map.get(index(probe)), reference.get(&probe));
            prop_assert_eq!(map.keys(), reference.keys().map(|i| index(*i)).collect());
            let rebuilt: ValidatorMap<u8> = entries.iter().map(|(i, v)| (*i, **v)).collect();
            prop_assert!(map == rebuilt);
        }
    }
}
