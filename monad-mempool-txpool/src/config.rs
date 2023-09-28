use std::time::Duration;

#[derive(Clone)]
pub struct PoolConfig {
    pub ttl_duration: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            ttl_duration: Duration::from_secs(120),
        }
    }
}

impl PoolConfig {
    pub fn new(ttl_duration: Duration) -> Self {
        Self { ttl_duration }
    }
}
