use opentelemetry::{
    metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter},
    KeyValue,
};

pub struct VirtualPoolMetrics {
    pub pending_pool: Gauge<u64>,
    pub queued_pool: Gauge<u64>,
    pub pending_evicted: Counter<u64>,
    pub queued_evicted: Counter<u64>,
}

impl VirtualPoolMetrics {
    pub fn new(meter: Meter) -> Self {
        let pending_pool = meter
            .u64_gauge("monad.vpool.pending_pool")
            .with_description("Number of transactions in the pending pool")
            .init();

        let queued_pool = meter
            .u64_gauge("monad.vpool.queued_pool")
            .with_description("Number of transactions in the queued pool")
            .init();

        let pending_evicted = meter
            .u64_counter("monad.vpool.pending_evicted")
            .with_description("Number of transactions evicted from the pending pool")
            .init();
        let queued_evicted = meter
            .u64_counter("monad.vpool.queued_evicted")
            .with_description("Number of transactions evicted from the queued pool")
            .init();

        Self {
            pending_pool,
            queued_pool,
            pending_evicted,
            queued_evicted,
        }
    }
}