use std::time::SystemTime;

#[derive(Debug)]
pub enum TimestampAdjusterConfig {
    Disabled,
    Enabled {
        max_delta: u128,
        adjustment_period: usize,
    },
}

pub trait Clock {
    fn new() -> Self;

    fn get(&self) -> u128;

    fn update(&mut self, time: u128);
}

#[derive(Debug)]
pub struct SystemClock {}

impl Clock for SystemClock {
    fn new() -> Self {
        Self {}
    }

    fn get(&self) -> u128 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    fn update(&mut self, _time: u128) {}
}

#[derive(Debug)]
pub struct TestClock {
    time: u128,
}

impl Clock for TestClock {
    fn new() -> Self {
        Self { time: 0 }
    }
    fn get(&self) -> u128 {
        self.time
    }

    fn update(&mut self, time: u128) {
        self.time = time;
    }
}
