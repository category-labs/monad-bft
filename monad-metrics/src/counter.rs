use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Clone, Default, Debug)]
pub struct Counter {
    value: Arc<AtomicU64>,
}

impl Counter {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, val: u64) {
        self.value.fetch_add(val, Ordering::Release);
    }

    pub fn read(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }
}
