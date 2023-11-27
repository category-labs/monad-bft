use std::time::Duration;

#[derive(Clone)]
pub struct PoolConfig {
    pub ttl_duration: Duration,
    pub pending_duration: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            ttl_duration: Duration::from_secs(120),
            pending_duration: Duration::from_secs(1),
        }
    }
}

impl PoolConfig {
    pub fn new(ttl_duration: Duration, pending_duration: Duration) -> Self {
        Self {
            ttl_duration,
            pending_duration,
        }
    }

    pub fn with_ttl_duration(mut self, ttl_duration: Duration) -> Self {
        self.ttl_duration = ttl_duration;

        self
    }

    pub fn with_pending_duration(mut self, pending_duration: Duration) -> Self {
        self.pending_duration = pending_duration;

        self
    }
}
