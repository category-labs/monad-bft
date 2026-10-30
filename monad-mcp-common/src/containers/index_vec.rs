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

//! A `Vec` indexed by one index type only, for dense state over a small
//! fixed domain such as the nodes of a chunk assignment.

use std::{fmt, marker::PhantomData};

/// An index into an [`IndexVec`]. Implemented by the crate that owns the
/// index type; `new` is only for the container, which hands out indices it
/// holds.
pub trait Idx: Copy {
    fn new(index: usize) -> Self;
    fn index(self) -> usize;
}

/// One value per index in `0..len`, in index order.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct IndexVec<I, T> {
    raw: Vec<T>,
    _index: PhantomData<fn(I) -> I>,
}

impl<I: Idx, T> IndexVec<I, T> {
    pub fn from_fn(len: usize, mut f: impl FnMut(I) -> T) -> Self {
        Self::from_raw((0..len).map(|index| f(I::new(index))).collect())
    }

    pub fn from_elem(len: usize, value: T) -> Self
    where
        T: Clone,
    {
        Self::from_raw(vec![value; len])
    }

    pub fn len(&self) -> usize {
        self.raw.len()
    }

    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    pub fn get(&self, index: I) -> Option<&T> {
        self.raw.get(index.index())
    }

    pub fn get_mut(&mut self, index: I) -> Option<&mut T> {
        self.raw.get_mut(index.index())
    }

    /// In index order.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.raw.iter()
    }

    /// In index order.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.raw.iter_mut()
    }

    /// In index order.
    pub fn iter_enumerated(&self) -> impl Iterator<Item = (I, &T)> {
        self.raw
            .iter()
            .enumerate()
            .map(|(index, value)| (I::new(index), value))
    }

    /// In index order.
    pub fn into_iter_enumerated(self) -> impl Iterator<Item = (I, T)> {
        self.raw
            .into_iter()
            .enumerate()
            .map(|(index, value)| (I::new(index), value))
    }

    fn from_raw(raw: Vec<T>) -> Self {
        Self {
            raw,
            _index: PhantomData,
        }
    }
}

impl<I: Idx, T> FromIterator<T> for IndexVec<I, T> {
    /// The values for indices 0, 1, ... in order.
    fn from_iter<It: IntoIterator<Item = T>>(iter: It) -> Self {
        Self::from_raw(iter.into_iter().collect())
    }
}

impl<I: Idx, T> IntoIterator for IndexVec<I, T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.raw.into_iter()
    }
}

impl<I, T: fmt::Debug> fmt::Debug for IndexVec<I, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.fmt(f)
    }
}

/// Panics if the index is out of bounds.
impl<I: Idx, T> std::ops::Index<I> for IndexVec<I, T> {
    type Output = T;

    fn index(&self, index: I) -> &T {
        &self.raw[index.index()]
    }
}

/// Panics if the index is out of bounds.
impl<I: Idx, T> std::ops::IndexMut<I> for IndexVec<I, T> {
    fn index_mut(&mut self, index: I) -> &mut T {
        &mut self.raw[index.index()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    struct Pos(usize);

    impl Idx for Pos {
        fn new(index: usize) -> Self {
            Self(index)
        }

        fn index(self) -> usize {
            self.0
        }
    }

    #[test]
    fn an_index_vec_is_addressed_and_enumerated_by_its_index() {
        let mut v: IndexVec<Pos, usize> = IndexVec::from_fn(3, |pos: Pos| pos.0 * 10);
        v[Pos(1)] += 1;
        assert_eq!(v.get(Pos(3)), None);
        assert_eq!(
            v.iter_enumerated().collect::<Vec<_>>(),
            [(Pos(0), &0), (Pos(1), &11), (Pos(2), &20)]
        );
        assert_eq!(IndexVec::<Pos, _>::from_elem(2, 'x').len(), 2);
    }
}
