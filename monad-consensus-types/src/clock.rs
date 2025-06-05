use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub enum TimestampAdjusterConfig {
    Disabled,
    Enabled {
        max_delta_ns: u128,
        adjustment_period: usize,
    },
}

pub trait Clock {
    fn new() -> Self;

    fn get(&self) -> Duration;

    fn update(&mut self, time: Duration);
}

#[derive(Debug)]
pub struct SystemClock {}

impl Clock for SystemClock {
    fn new() -> Self {
        Self {}
    }

    fn get(&self) -> Duration {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
    }

    fn update(&mut self, _time: Duration) {}
}

#[derive(Debug)]
pub struct TestClock {
    time: Duration,
}

impl Clock for TestClock {
    fn new() -> Self {
        Self {
            time: Duration::from_nanos(0),
        }
    }
    fn get(&self) -> Duration {
        self.time
    }

    fn update(&mut self, time: Duration) {
        self.time = time;
    }
}
