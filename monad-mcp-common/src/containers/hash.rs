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

//! Hash maps and sets for lookups, named by their hasher:
//!
//! - [`FastHashMap`], [`FastHashSet`]: rapidhash, for keys an adversary does
//!   not choose (proposal ids, headers that passed authentication, node ids).
//! - [`SipHashMap`], [`SipHashSet`]: std's SipHash with random keys, where
//!   keys may be adversarial (transaction hashes from the mempool).
//!
//! Their iteration order is unspecified and must not reach an output: it
//! differs between processes (the seed), and even under one seed between
//! x86 and arm (hashbrown's probe groups) and across versions. Iteration is
//! therefore only offered under `*_unordered` names, for order-independent
//! uses such as a sum or a membership test of all elements.
//! `clippy::iter_over_hash_type` still flags a `for` loop over them.

// the wrappers are the one place the std types are named
#![allow(clippy::disallowed_types)]

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet, hash_map},
    fmt,
    hash::{BuildHasher, Hash},
    sync::OnceLock,
};

use rapidhash::v3::RapidSecrets;

/// A [`LookupMap`] with the fast hasher.
pub type FastHashMap<K, V> = LookupMap<K, V, FastHashState>;
/// A [`LookupSet`] with the fast hasher.
pub type FastHashSet<T> = LookupSet<T, FastHashState>;
/// A [`LookupMap`] with the DoS-resistant hasher.
pub type SipHashMap<K, V> = LookupMap<K, V, SipHashState>;
/// A [`LookupSet`] with the DoS-resistant hasher.
pub type SipHashSet<T> = LookupSet<T, SipHashState>;

/// std's SipHash-1-3 with random keys per instance.
pub type SipHashState = std::hash::RandomState;

// fixed, so that a seed alone reproduces a table; the fast hasher makes no
// claim of DoS resistance either way
static FAST_SECRETS: RapidSecrets = RapidSecrets::seed(0x6d6f_6e61_645f_6d63);

/// rapidhash (its `fast` variant), seeded per process by default.
///
/// A random seed per process makes the cross-process determinism test catch
/// any output that depends on a table's order; [`FastHashState::with_seed`]
/// reproduces a given table.
#[derive(Clone, Copy)]
pub struct FastHashState(rapidhash::fast::SeedableState<'static>);

impl FastHashState {
    pub fn with_seed(seed: u64) -> Self {
        Self(rapidhash::fast::SeedableState::custom(
            RapidSecrets::seed(seed).seed,
            &FAST_SECRETS.secrets,
        ))
    }

    /// The seed every default-constructed state in this process uses.
    pub fn process_seed() -> u64 {
        static SEED: OnceLock<u64> = OnceLock::new();
        *SEED.get_or_init(|| std::hash::RandomState::new().hash_one(0u64))
    }
}

impl Default for FastHashState {
    fn default() -> Self {
        Self::with_seed(Self::process_seed())
    }
}

impl fmt::Debug for FastHashState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FastHashState")
    }
}

impl BuildHasher for FastHashState {
    type Hasher = rapidhash::fast::RapidHasher<'static>;

    fn build_hasher(&self) -> Self::Hasher {
        self.0.build_hasher()
    }
}

/// A hash map for lookups; see the module docs for which alias to use.
#[derive(Clone)]
pub struct LookupMap<K, V, S>(HashMap<K, V, S>);

impl<K, V, S: Default> LookupMap<K, V, S> {
    pub fn new() -> Self {
        Self(HashMap::default())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(HashMap::with_capacity_and_hasher(capacity, S::default()))
    }
}

impl<K, V, S> LookupMap<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self(HashMap::with_hasher(hasher))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn retain(&mut self, f: impl FnMut(&K, &mut V) -> bool) {
        self.0.retain(f)
    }

    /// In unspecified order.
    pub fn iter_unordered(&self) -> hash_map::Iter<'_, K, V> {
        self.0.iter()
    }

    /// In unspecified order.
    pub fn keys_unordered(&self) -> hash_map::Keys<'_, K, V> {
        self.0.keys()
    }

    /// In unspecified order.
    pub fn values_unordered(&self) -> hash_map::Values<'_, K, V> {
        self.0.values()
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> LookupMap<K, V, S> {
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.get(key)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.get_mut(key)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.contains_key(key)
    }

    /// The value it replaces, if any.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.0.insert(key, value)
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.remove(key)
    }

    pub fn entry(&mut self, key: K) -> hash_map::Entry<'_, K, V> {
        self.0.entry(key)
    }
}

impl<K, V, S: Default> Default for LookupMap<K, V, S> {
    fn default() -> Self {
        Self(HashMap::default())
    }
}

impl<K: Eq + Hash, V: PartialEq, S: BuildHasher> PartialEq for LookupMap<K, V, S> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K: Eq + Hash, V: Eq, S: BuildHasher> Eq for LookupMap<K, V, S> {}

/// In unspecified order: for logs only.
impl<K: fmt::Debug, V: fmt::Debug, S> fmt::Debug for LookupMap<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<K: Eq + Hash, V, S: BuildHasher + Default> FromIterator<(K, V)> for LookupMap<K, V, S> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(HashMap::from_iter(iter))
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> Extend<(K, V)> for LookupMap<K, V, S> {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}

impl<K: Eq + Hash, V, S: BuildHasher + Default, const N: usize> From<[(K, V); N]>
    for LookupMap<K, V, S>
{
    fn from(entries: [(K, V); N]) -> Self {
        entries.into_iter().collect()
    }
}

/// Panics if the key is absent.
impl<K, Q, V, S> std::ops::Index<&Q> for LookupMap<K, V, S>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + ?Sized,
    S: BuildHasher,
{
    type Output = V;

    fn index(&self, key: &Q) -> &V {
        &self.0[key]
    }
}

/// A hash set for membership tests; see the module docs for which alias to
/// use.
#[derive(Clone)]
pub struct LookupSet<T, S>(HashSet<T, S>);

impl<T, S: Default> LookupSet<T, S> {
    pub fn new() -> Self {
        Self(HashSet::default())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(HashSet::with_capacity_and_hasher(capacity, S::default()))
    }
}

impl<T, S> LookupSet<T, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self(HashSet::with_hasher(hasher))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn retain(&mut self, f: impl FnMut(&T) -> bool) {
        self.0.retain(f)
    }

    /// In unspecified order.
    pub fn iter_unordered(&self) -> std::collections::hash_set::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T: Eq + Hash, S: BuildHasher> LookupSet<T, S> {
    /// Whether the value was not in the set before.
    pub fn insert(&mut self, value: T) -> bool {
        self.0.insert(value)
    }

    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.contains(value)
    }

    /// Whether the value was in the set.
    pub fn remove<Q>(&mut self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.remove(value)
    }
}

impl<T, S: Default> Default for LookupSet<T, S> {
    fn default() -> Self {
        Self(HashSet::default())
    }
}

impl<T: Eq + Hash, S: BuildHasher> PartialEq for LookupSet<T, S> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Eq + Hash, S: BuildHasher> Eq for LookupSet<T, S> {}

/// In unspecified order: for logs only.
impl<T: fmt::Debug, S> fmt::Debug for LookupSet<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: Eq + Hash, S: BuildHasher + Default> FromIterator<T> for LookupSet<T, S> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(HashSet::from_iter(iter))
    }
}

impl<T: Eq + Hash, S: BuildHasher> Extend<T> for LookupSet<T, S> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}

impl<T: Eq + Hash, S: BuildHasher + Default, const N: usize> From<[T; N]> for LookupSet<T, S> {
    fn from(values: [T; N]) -> Self {
        values.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn a_seed_fixes_the_hash() {
        let a = FastHashState::with_seed(1);
        let b = FastHashState::with_seed(1);
        let c = FastHashState::with_seed(2);
        assert_eq!(a.hash_one(42u64), b.hash_one(42u64));
        assert_ne!(a.hash_one(42u64), c.hash_one(42u64));
        assert_eq!(
            FastHashState::default().hash_one(42u64),
            FastHashState::with_seed(FastHashState::process_seed()).hash_one(42u64)
        );
    }

    proptest! {
        #[test]
        fn a_fast_map_behaves_like_an_ordered_map(
            seed in any::<u64>(),
            ops in prop::collection::vec((0..64u32, any::<Option<u8>>()), 0..200),
            probe in 0..64u32,
        ) {
            let mut map = LookupMap::with_hasher(FastHashState::with_seed(seed));
            let mut reference = BTreeMap::new();
            for (key, value) in ops {
                match value {
                    Some(value) => prop_assert_eq!(map.insert(key, value), reference.insert(key, value)),
                    None => prop_assert_eq!(map.remove(&key), reference.remove(&key)),
                }
            }
            prop_assert_eq!(map.len(), reference.len());
            prop_assert_eq!(map.get(&probe), reference.get(&probe));
            prop_assert_eq!(map.contains_key(&probe), reference.contains_key(&probe));
            let mut entries: Vec<_> = map.iter_unordered().map(|(k, v)| (*k, *v)).collect();
            entries.sort();
            prop_assert_eq!(entries, reference.into_iter().collect::<Vec<_>>());
        }

        #[test]
        fn a_sip_set_behaves_like_an_ordered_set(
            ops in prop::collection::vec((0..64u32, any::<bool>()), 0..200),
            probe in 0..64u32,
        ) {
            let mut set = SipHashSet::new();
            let mut reference = BTreeSet::new();
            for (value, insert) in ops {
                if insert {
                    prop_assert_eq!(set.insert(value), reference.insert(value));
                } else {
                    prop_assert_eq!(set.remove(&value), reference.remove(&value));
                }
            }
            prop_assert_eq!(set.len(), reference.len());
            prop_assert_eq!(set.contains(&probe), reference.contains(&probe));
            let mut values: Vec<_> = set.iter_unordered().copied().collect();
            values.sort();
            prop_assert_eq!(values, reference.into_iter().collect::<Vec<_>>());
        }
    }
}
