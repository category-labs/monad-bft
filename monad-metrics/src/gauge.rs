use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Clone, Default, Debug)]
pub struct Gauge {
    value: Arc<AtomicU64>,
}

impl Gauge {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, val: u64) {
        self.value.fetch_add(val, Ordering::Release);
    }

    pub fn dec_saturating(&self) {
        self.sub_saturating(1);
    }

    pub fn sub_saturating(&self, val: u64) {
        let _ = self
            .value
            .fetch_update(Ordering::Release, Ordering::Acquire, |value| {
                Some(value.saturating_sub(val))
            });
    }

    pub fn set(&self, val: u64) {
        self.value.store(val, Ordering::Release);
    }

    pub fn read(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }
}
