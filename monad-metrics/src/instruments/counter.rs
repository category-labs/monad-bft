use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub trait Counter: Clone + std::fmt::Debug + Unpin + Send + Sync + 'static {
    fn add(&self, val: u64);

    fn inc(&self) {
        self.add(1);
    }

    fn read(&self) -> u64;
}

// Note: Do NOT implement Default for this type to force downstream metrics users to explicitly
// build static counters using StaticCounter::from_static.
#[derive(Clone, Debug)]
pub struct StaticCounter {
    value: &'static AtomicU64,
}

impl StaticCounter {
    pub fn from_static(value: &'static AtomicU64) -> Self {
        Self { value }
    }
}

impl Counter for StaticCounter {
    fn add(&self, val: u64) {
        self.value.fetch_add(val, Ordering::Relaxed);
    }

    fn read(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Default, Debug)]
pub struct LocalCounter {
    value: Arc<AtomicU64>,
}

impl Counter for LocalCounter {
    fn add(&self, val: u64) {
        self.value.fetch_add(val, Ordering::Relaxed);
    }

    fn read(&self) -> u64 {
        self.value.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Default, Debug)]
pub struct NoopCounter;

impl Counter for NoopCounter {
    fn add(&self, _: u64) {}

    fn read(&self) -> u64 {
        panic!("Never read noop counter");
    }
}
