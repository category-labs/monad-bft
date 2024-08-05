use std::{iter::IntoIterator, slice};

use crate::binary_search::smallest_integer_satisfying;

// Empirically determined threshold for binary search.
const BINARY_SEARCH_THRESHOLD: usize = 250;

#[derive(Clone, Debug)]
pub struct OrderedSet {
    data: Vec<u16>,
}

impl OrderedSet {
    pub fn new() -> OrderedSet {
        OrderedSet { data: Vec::new() }
    }

    pub fn append(&mut self, value: u16) {
        self.data.push(value);
    }

    // Find the array index where element 'value' is located or where it would be inserted.
    // This involves finding the lowest-numbered entry that is higher than or equal to `value`.
    fn placement_index(&self, value: &u16) -> usize {
        if self.data.len() < BINARY_SEARCH_THRESHOLD {
            for i in 0..self.data.len() {
                if self.data[i] >= *value {
                    return i;
                }
            }

            self.data.len()
        } else {
            match smallest_integer_satisfying(0, self.data.len(), |pivot| {
                self.data[pivot] >= *value
            }) {
                None => self.data.len(),
                Some(index) => index,
            }
        }
    }

    pub fn contains(&self, value: &u16) -> bool {
        let index = self.placement_index(value);

        index < self.data.len() && self.data[index] == *value
    }

    pub fn first(&self) -> Option<&u16> {
        if !self.data.is_empty() {
            Some(&self.data[0])
        } else {
            None
        }
    }

    pub fn insert(&mut self, value: u16) -> bool {
        let len = self.data.len();
        let index = self.placement_index(&value);

        if index == len || self.data[index] != value {
            self.data.push(0);
            self.data.copy_within(index..len, index + 1);
            self.data[index] = value;

            true
        } else {
            false
        }
    }

    pub fn insert_or_remove(&mut self, value: u16) -> bool {
        let len = self.data.len();
        let index = self.placement_index(&value);

        if index == len || self.data[index] != value {
            self.data.push(0);
            self.data.copy_within(index..len, index + 1);
            self.data[index] = value;

            true
        } else {
            self.data.copy_within(index + 1..len, index);
            self.data.truncate(len - 1);

            false
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &u16> {
        self.data.iter()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn remove(&mut self, value: &u16) -> bool {
        let len = self.data.len();
        let index = self.placement_index(value);

        if index < len && self.data[index] == *value {
            self.data.copy_within(index + 1..len, index);
            self.data.truncate(len - 1);

            true
        } else {
            false
        }
    }
}

impl Default for OrderedSet {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for OrderedSet {
    type Item = u16;
    type IntoIter = <Vec<u16> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<'a> IntoIterator for &'a OrderedSet {
    type Item = &'a u16;
    type IntoIter = slice::Iter<'a, u16>;

    fn into_iter(self) -> Self::IntoIter {
        self.data[..].iter()
    }
}
