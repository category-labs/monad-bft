use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub trait Gauge: Clone + std::fmt::Debug + Unpin + Send + Sync + 'static {
    fn add(&self, val: u64);

    fn inc(&self) {
        self.add(1);
    }

    fn sub_saturating(&self, val: u64);

    fn dec_saturating(&self) {
        self.sub_saturating(1);
    }

    fn set(&self, val: u64);

    fn read(&self) -> u64;
}

// Note: Do NOT implement Default for this type to force downstream metrics users to explicitly
// build static gauges using StaticGauge::from_static.
#[derive(Clone, Debug)]
pub struct StaticGauge {
    value: &'static AtomicU64,
}

impl StaticGauge {
    pub fn from_static(value: &'static AtomicU64) -> Self {
        Self { value }
    }
}

impl Gauge for StaticGauge {
    fn add(&self, val: u64) {
        self.value.fetch_add(val, Ordering::Relaxed);
    }

    fn sub_saturating(&self, val: u64) {
        self.value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(val))
            })
            .expect("atomics never panic");
    }

    fn set(&self, val: u64) {
        self.value.swap(val, Ordering::Relaxed);
    }

    fn read(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Default, Debug)]
pub struct LocalGauge {
    value: Arc<AtomicU64>,
}

impl Gauge for LocalGauge {
    fn add(&self, val: u64) {
        self.value.fetch_add(val, Ordering::Relaxed);
    }

    fn sub_saturating(&self, val: u64) {
        self.value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(val))
            })
            .expect("atomics never panic");
    }

    fn set(&self, val: u64) {
        self.value.swap(val, Ordering::Relaxed);
    }

    fn read(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Default, Debug)]
pub struct NoopGauge;

impl Gauge for NoopGauge {
    fn add(&self, _: u64) {}

    fn sub_saturating(&self, _: u64) {}

    fn set(&self, _: u64) {}

    fn read(&self) -> u64 {
        0
    }
}
