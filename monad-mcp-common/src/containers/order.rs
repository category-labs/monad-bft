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

//! Pseudo-random orders that are the same on every run, process, platform
//! and release: an explicit keyed permutation, never a hash table's order.

use alloy_rlp::Encodable;
use rand::{SeedableRng as _, seq::SliceRandom as _};
use rand_chacha::ChaCha20Rng;

/// The key of a pseudo-random order: Blake3 over a domain tag followed by
/// the RLP encoding of each part, seeding ChaCha20. The order is uniform
/// over keys and fixed by blake3, rlp, rand and rand_chacha, which all pin
/// their output.
///
/// The domain tag names the use, so two uses with the same parts draw
/// unrelated orders; no tag may be a prefix of another. The parts are
/// self-delimiting, being RLP.
#[derive(Clone)]
pub struct OrderKey(blake3::Hasher);

impl OrderKey {
    pub fn new(domain: &'static [u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        Self(hasher)
    }

    pub fn with(mut self, part: &impl Encodable) -> Self {
        self.0.update(&alloy_rlp::encode(part));
        self
    }

    /// Permutes `items` in place: one pass, no allocation.
    pub fn shuffle<T>(&self, items: &mut [T]) {
        let seed = *self.0.finalize().as_bytes();
        items.shuffle(&mut ChaCha20Rng::from_seed(seed));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(domain: &'static [u8], part: u64) -> Vec<u8> {
        let mut items: Vec<u8> = (0..16).collect();
        OrderKey::new(domain).with(&part).shuffle(&mut items);
        items
    }

    #[test]
    fn an_order_is_a_permutation_fixed_by_its_key() {
        let mut sorted = order(b"test", 1);
        sorted.sort();
        assert_eq!(sorted, (0..16).collect::<Vec<_>>());
        assert_eq!(order(b"test", 1), order(b"test", 1));
        assert_ne!(order(b"test", 1), order(b"test", 2));
        assert_ne!(order(b"test", 1), order(b"other", 1));
        // pinned: the same across processes and releases
        assert_eq!(order(b"test", 1), PINNED);
    }

    const PINNED: [u8; 16] = [12, 5, 9, 0, 15, 6, 14, 7, 4, 1, 3, 11, 10, 8, 13, 2];
}
