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

//! Dense containers over a small index domain, such as the lanes of a slot
//! or the validators of an epoch: a value for each index ([`DenseVec`]) or
//! for some of them ([`DenseMap`]). The size of the domain is set at
//! construction, and iteration is in index order.

use std::{fmt, marker::PhantomData};

/// An index of a [`DenseVec`] or a [`DenseMap`]. Implemented by the crate
/// that owns the index type; `new` is only for the container, which hands
/// out indices it holds.
pub trait Idx: Copy {
    /// The limit of a well-formed index, and so of the size of a container.
    const MAX: usize;

    fn new(index: usize) -> Self;
    fn index(self) -> usize;
}

/// One value for each index below its size, in index order. The size is set
/// at construction.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DenseVec<I, V> {
    values: Box<[V]>,
    _index: PhantomData<fn(I) -> I>,
}

impl<I, V> DenseVec<I, V> {
    // the caller must ensure `size` is contextually valid.
    pub fn new(size: usize, init_fn: impl FnMut(I) -> V) -> Self
    where
        I: Idx,
    {
        assert!(size <= I::MAX, "too many values");
        Self::from_values((0..size).map(I::new).map(init_fn).collect())
    }

    // the caller must ensure `size` is contextually valid.
    pub fn new_default(size: usize) -> Self
    where
        I: Idx,
        V: Default,
    {
        Self::new(size, |_| V::default())
    }

    // the values for indices 0, 1, .. in order, or None if they are more than I::MAX
    pub fn from_vec(values: Vec<V>) -> Option<Self>
    where
        I: Idx,
    {
        let fits = values.len() <= I::MAX;
        fits.then(|| Self::from_values(values.into_boxed_slice()))
    }

    fn from_values(values: Box<[V]>) -> Self {
        Self {
            values,
            _index: PhantomData,
        }
    }

    pub fn as_slice(&self) -> &[V] {
        &self.values
    }

    pub const fn size(&self) -> usize {
        self.values.len()
    }

    pub fn indices(&self) -> impl Iterator<Item = I> + use<I, V>
    where
        I: Idx,
    {
        (0..self.size()).map(I::new)
    }

    // None if `index` is not an index of this container
    pub fn checked_index(&self, index: usize) -> Option<I>
    where
        I: Idx,
    {
        let in_range = index < self.size();
        in_range.then(|| I::new(index))
    }

    pub fn get(&self, index: I) -> Option<&V>
    where
        I: Idx,
    {
        self.values.get(index.index())
    }

    pub fn get_mut(&mut self, index: I) -> Option<&mut V>
    where
        I: Idx,
    {
        self.values.get_mut(index.index())
    }

    pub fn as_ref(&self) -> DenseVec<I, &V> {
        DenseVec::from_values(self.values.iter().collect())
    }

    pub fn iter(&self) -> std::slice::Iter<'_, V> {
        self.values.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, V> {
        self.values.iter_mut()
    }

    pub fn indexed_iter(&self) -> impl Iterator<Item = (I, &V)>
    where
        I: Idx,
    {
        self.indices().zip(&self.values)
    }

    pub fn into_indexed_iter(self) -> impl Iterator<Item = (I, V)>
    where
        I: Idx,
    {
        self.indices().zip(self.values)
    }

    pub fn map<F, U>(self, f: F) -> DenseVec<I, U>
    where
        F: FnMut(V) -> U,
    {
        DenseVec::from_values(self.values.into_iter().map(f).collect())
    }

    pub fn map_indexed<F, U>(self, mut f: F) -> DenseVec<I, U>
    where
        I: Idx,
        F: FnMut(I, V) -> U,
    {
        let values = self
            .indices()
            .zip(self.values)
            .map(|(index, v)| f(index, v))
            .collect();
        DenseVec::from_values(values)
    }
}

impl<I, V> DenseVec<I, Option<V>> {
    /// Panics if the index is out of bounds. The caller must ensure the index is valid.
    pub fn set(&mut self, index: I, value: V)
    where
        I: Idx,
    {
        self.values[index.index()] = Some(value);
    }

    // A partial container comes back untouched.
    pub fn try_into_total<S>(self) -> Result<DenseVec<I, S>, Self>
    where
        S: From<V>,
    {
        let is_partial = self.values.iter().any(|v| v.is_none());
        if is_partial {
            return Err(self);
        }

        let values = self
            .values
            .into_iter()
            // SAFETY: we just checked that all values are Some
            .map(|opt| S::from(opt.unwrap()))
            .collect();

        Ok(DenseVec::from_values(values))
    }

    pub fn into_total<S>(self) -> DenseVec<I, S>
    where
        S: From<Option<V>>,
    {
        self.map(|opt| S::from(opt))
    }
}

impl<I, V> DenseVec<I, &V> {
    pub fn into_owned(self) -> DenseVec<I, V>
    where
        V: Clone,
    {
        self.map(|v| v.clone())
    }
}

impl<I, V> IntoIterator for DenseVec<I, V> {
    type Item = V;
    type IntoIter = std::vec::IntoIter<V>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<I, V: fmt::Debug> fmt::Debug for DenseVec<I, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.values.fmt(f)
    }
}

/// Panics if the index is out of bounds. The caller must ensure the index is valid.
impl<I: Idx, V> std::ops::Index<I> for DenseVec<I, V> {
    type Output = V;

    fn index(&self, index: I) -> &V {
        &self.values[index.index()]
    }
}

/// Panics if the index is out of bounds. The caller must ensure the index is valid.
impl<I: Idx, V> std::ops::IndexMut<I> for DenseVec<I, V> {
    fn index_mut(&mut self, index: I) -> &mut V {
        &mut self.values[index.index()]
    }
}

/// A value for some of the indices below its cap, in index order. The cap is
/// set at construction, so it suits values held for most indices, such as
/// votes. A map with no value owns no memory.
// todo: box the values, or keep them dense behind a u16 slot index, once a
// value type is large (a BLS signature is 96 bytes)
#[derive(Clone, PartialEq, Eq)]
pub struct DenseMap<I, V>(Slots<I, V>);

#[derive(Clone, PartialEq, Eq)]
enum Slots<I, V> {
    Empty {
        cap: usize,
    },
    // invariant: at least one slot has a value, and `len` counts them
    Occupied {
        slots: DenseVec<I, Option<V>>,
        len: usize,
    },
}

impl<I, V> DenseMap<I, V> {
    // the caller must ensure `cap` is contextually valid.
    pub fn new(cap: usize) -> Self
    where
        I: Idx,
    {
        assert!(cap <= I::MAX, "too many values");
        Self(Slots::Empty { cap })
    }

    pub fn len(&self) -> usize {
        match &self.0 {
            Slots::Empty { .. } => 0,
            Slots::Occupied { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.0, Slots::Empty { .. })
    }

    /// The number of indices the map has room for.
    pub fn cap(&self) -> usize {
        match &self.0 {
            Slots::Empty { cap } => *cap,
            Slots::Occupied { slots, .. } => slots.size(),
        }
    }

    fn slots(&self) -> Option<&DenseVec<I, Option<V>>> {
        match &self.0 {
            Slots::Empty { .. } => None,
            Slots::Occupied { slots, .. } => Some(slots),
        }
    }

    pub fn get(&self, index: I) -> Option<&V>
    where
        I: Idx,
    {
        self.slots()?.get(index)?.as_ref()
    }

    pub fn get_mut(&mut self, index: I) -> Option<&mut V>
    where
        I: Idx,
    {
        let Slots::Occupied { slots, .. } = &mut self.0 else {
            return None;
        };
        slots.get_mut(index)?.as_mut()
    }

    pub fn contains_key(&self, index: I) -> bool
    where
        I: Idx,
    {
        self.get(index).is_some()
    }

    /// The value it replaces, if any. Panics if the index is not below the
    /// cap.
    pub fn insert(&mut self, index: I, value: V) -> Option<V>
    where
        I: Idx,
    {
        let cap = self.cap();
        assert!(index.index() < cap, "index beyond the cap");
        match &mut self.0 {
            Slots::Empty { .. } => {
                let mut slots = DenseVec::new_default(cap);
                slots[index] = Some(value);
                self.0 = Slots::Occupied { slots, len: 1 };
                None
            }
            Slots::Occupied { slots, len } => {
                let previous = slots[index].replace(value);
                if previous.is_none() {
                    *len += 1;
                }
                previous
            }
        }
    }

    pub fn remove(&mut self, index: I) -> Option<V>
    where
        I: Idx,
    {
        let Slots::Occupied { slots, len } = &mut self.0 else {
            return None;
        };
        let removed = slots.get_mut(index)?.take()?;
        *len -= 1;
        if *len == 0 {
            self.clear();
        }
        Some(removed)
    }

    pub fn clear(&mut self) {
        self.0 = Slots::Empty { cap: self.cap() };
    }

    /// In index order.
    pub fn iter(&self) -> impl Iterator<Item = (I, &V)>
    where
        I: Idx,
    {
        let slots = self.slots().into_iter().flat_map(DenseVec::indexed_iter);
        slots.filter_map(|(index, slot)| Some((index, slot.as_ref()?)))
    }

    /// In index order.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        let slots = self.slots().into_iter().flat_map(DenseVec::iter);
        slots.flatten()
    }
}

impl<I: Idx + fmt::Debug, V: fmt::Debug> fmt::Debug for DenseMap<I, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// Panics if the index has no value.
impl<I: Idx, V> std::ops::Index<I> for DenseMap<I, V> {
    type Output = V;

    fn index(&self, index: I) -> &V {
        self.get(index).expect("index has a value")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    struct Pos(usize);

    impl Idx for Pos {
        const MAX: usize = 4;

        fn new(index: usize) -> Self {
            Self(index)
        }

        fn index(self) -> usize {
            self.0
        }
    }

    #[test]
    fn a_dense_vec_is_addressed_and_enumerated_by_its_index() {
        let mut v: DenseVec<Pos, usize> = DenseVec::new(3, |pos: Pos| pos.0 * 10);
        v[Pos(1)] += 1;
        assert_eq!(v.size(), 3);
        assert_eq!(v.get(Pos(3)), None);
        assert_eq!(v.checked_index(2), Some(Pos(2)));
        assert_eq!(v.checked_index(3), None);
        assert_eq!(
            v.indexed_iter().collect::<Vec<_>>(),
            [(Pos(0), &0), (Pos(1), &11), (Pos(2), &20)]
        );
        assert_eq!(Some(v), DenseVec::from_vec(vec![0, 11, 20]));
        assert!(DenseVec::<Pos, usize>::from_vec(vec![0; Pos::MAX + 1]).is_none());
    }

    #[test]
    fn a_dense_map_holds_a_value_for_some_indices() {
        let mut map: DenseMap<Pos, char> = DenseMap::new(3);
        assert_eq!(map.get(Pos(2)), None);
        assert_eq!(map.insert(Pos(2), 'a'), None);
        assert_eq!(map.insert(Pos(2), 'b'), Some('a'));
        assert_eq!(map.insert(Pos(0), 'c'), None);
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.iter().collect::<Vec<_>>(),
            [(Pos(0), &'c'), (Pos(2), &'b')]
        );
        assert_eq!(map.values().collect::<Vec<_>>(), [&'c', &'b']);

        // a map that lost its last value is again a new map
        assert_eq!(map.remove(Pos(0)), Some('c'));
        assert_eq!(map.remove(Pos(2)), Some('b'));
        assert_eq!(map.remove(Pos(2)), None);
        assert!(map.is_empty());
        assert!(map == DenseMap::new(3));
        assert!(map != DenseMap::new(2));
        assert_eq!(map.insert(Pos(1), 'd'), None);
        assert_eq!(map.iter().collect::<Vec<_>>(), [(Pos(1), &'d')]);
    }
}
