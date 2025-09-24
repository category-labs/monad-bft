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

use crate::prelude::*;

const MAX_CACHED: usize = 1_000_000;

pub enum KeyPool {
    Cached(SeededKeyPoolCached),
    Generator(SeededKeyPoolGenerator),
}

impl KeyPool {
    pub fn new(num_keys: usize, seed: u64) -> KeyPool {
        if num_keys > MAX_CACHED {
            KeyPool::Generator(SeededKeyPoolGenerator::new(num_keys, seed))
        } else {
            KeyPool::Cached(SeededKeyPoolCached::new(num_keys, seed))
        }
    }

    pub fn next_addr(&mut self) -> Address {
        match self {
            KeyPool::Cached(pool) => pool.next_addr(),
            KeyPool::Generator(pool) => pool.next_addr(),
        }
    }
}

pub struct SeededKeyPoolCached {
    pub rng: SmallRng,
    pub buf: Vec<(Address, PrivateKey)>,
    pub cursor: usize,
}

pub struct SeededKeyPoolGenerator {
    pub rng: SmallRng,
    pub original_seed: u64,
    pub cursor: usize,
    pub num_keys: usize,
}

impl SeededKeyPoolGenerator {
    fn new(num_keys: usize, seed: u64) -> SeededKeyPoolGenerator {
        SeededKeyPoolGenerator {
            rng: SmallRng::seed_from_u64(seed),
            original_seed: seed,
            num_keys,
            cursor: 0,
        }
    }

    fn next_addr(&mut self) -> Address {
        if self.cursor == self.num_keys {
            self.rng = SmallRng::seed_from_u64(self.original_seed);
            self.cursor = 0;
        }

        let key = PrivateKey::new_with_random(&mut self.rng);
        self.cursor += 1;
        key.0
    }
}

impl SeededKeyPoolCached {
    fn new(num_keys: usize, seed: u64) -> SeededKeyPoolCached {
        let capacity = if num_keys > MAX_CACHED { 1 } else { num_keys };
        let mut buf = Vec::with_capacity(capacity);

        let mut rng = SmallRng::seed_from_u64(seed);
        buf.push(PrivateKey::new_with_random(&mut rng));

        SeededKeyPoolCached {
            rng,
            buf,
            cursor: 0,
        }
    }

    fn next_idx(&mut self) -> usize {
        if self.buf.len() < self.buf.capacity() {
            self.buf.push(PrivateKey::new_with_random(&mut self.rng));
        }
        let idx = self.cursor;
        self.cursor = (self.cursor + 1) % self.buf.capacity();
        idx
    }

    fn next_addr(&mut self) -> Address {
        let idx = self.next_idx();
        self.buf[idx].0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_pool() {
        let mut pool = KeyPool::new(5, 42);

        // Get 5 addresses
        let mut addresses = Vec::with_capacity(5);
        for _ in 0..5 {
            addresses.push(pool.next_addr());
        }

        // After reaching the limit, it should reset and generate the same addresses
        let mut second_round = Vec::with_capacity(5);
        for _ in 0..5 {
            second_round.push(pool.next_addr());
        }

        // The addresses should be the same since we reset the RNG with the same seed
        assert_eq!(addresses, second_round);
    }

    #[test]
    fn test_seeded_key_pool_generator() {
        let mut pool = SeededKeyPoolGenerator::new(5, 42);

        // Get 5 addresses
        let mut addresses = Vec::with_capacity(5);
        for _ in 0..5 {
            addresses.push(pool.next_addr());
        }

        // After reaching the limit, it should reset and generate the same addresses
        let mut second_round = Vec::with_capacity(5);
        for _ in 0..5 {
            second_round.push(pool.next_addr());
        }

        // The addresses should be the same since we reset the RNG with the same seed
        assert_eq!(addresses, second_round);
    }

    #[test]
    fn test_sync_pool() {
        let mut idxs = Vec::with_capacity(100);
        let mut gen = SeededKeyPoolCached::new(11, 1);

        for _ in 0..12 {
            idxs.push(gen.next_idx());
        }

        assert_eq!(idxs, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0]);

        idxs.clear();
        for _ in 0..12 {
            idxs.push(gen.next_idx());
        }

        assert_eq!(idxs, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1]);
    }
}
