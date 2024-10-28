use crate::prelude::*;

pub struct SeededKeyPool {
    pub rng: SmallRng,
    pub buf: Vec<(Address, PrivateKey)>,
    pub cursor: usize,
}

impl SeededKeyPool {
    pub fn new(num_keys: usize, seed: u64) -> SeededKeyPool {
        let mut buf = Vec::with_capacity(num_keys);
        let mut rng = SmallRng::seed_from_u64(seed);
        buf.push(PrivateKey::new_with_random(&mut rng));
        SeededKeyPool {
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

    pub fn next_addr(&mut self) -> Address {
        let idx = self.next_idx();
        self.buf[idx].0
    }

    pub fn next_key(&mut self) -> (Address, PrivateKey) {
        let idx = self.next_idx();
        self.buf[idx].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_pool() {
        let mut idxs = Vec::with_capacity(100);
        let mut gen = SeededKeyPool::new(11, 1);

        for i in 0..12 {
            idxs.push(gen.next_idx());
        }

        assert_eq!(idxs, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0]);

        idxs.clear();
        for i in 0..12 {
            idxs.push(gen.next_idx());
        }

        assert_eq!(idxs, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1]);
    }
}
