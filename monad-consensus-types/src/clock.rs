use std::time::SystemTime;

#[derive(Debug)]
pub enum AdjusterConfig {
    Disabled,
    Enabled {
        max_delta: u64,
        adjustment_period: usize,
    },
}

pub trait Clock {
    fn new() -> Self;

    fn get(&self) -> u64;

    fn update(&mut self, time: u64);
}

#[derive(Debug)]
pub struct SystemClock {}

impl Clock for SystemClock {
    fn new() -> Self {
        Self {}
    }

    fn get(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap()
    }

    fn update(&mut self, _time: u64) {}
}

#[derive(Debug)]
pub struct TestClock {
    time: u64,
}

impl Clock for TestClock {
    fn new() -> Self {
        Self { time: 0 }
    }
    fn get(&self) -> u64 {
        self.time
    }

    fn update(&mut self, time: u64) {
        self.time = time;
    }
}
