use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant, SystemTime},
};

use secp256k1::rand::{rng, rngs::ThreadRng};

pub trait Context {
    type Rng: secp256k1::rand::Rng + secp256k1::rand::CryptoRng;

    fn system_time(&self) -> SystemTime;
    fn duration_since_start(&self) -> Duration;
    fn rng(&mut self) -> &mut Self::Rng;
}

pub struct StdContext {
    rng: ThreadRng,
    start_instant: Instant,
}

impl StdContext {
    pub fn new() -> Self {
        Self {
            rng: rng(),
            start_instant: Instant::now(),
        }
    }
}

impl Context for StdContext {
    type Rng = ThreadRng;

    fn system_time(&self) -> SystemTime {
        SystemTime::now()
    }

    fn duration_since_start(&self) -> Duration {
        self.start_instant.elapsed()
    }

    fn rng(&mut self) -> &mut Self::Rng {
        &mut self.rng
    }
}

impl Default for StdContext {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct TestContext {
    shared: Rc<RefCell<TestContextShared>>,
}

struct TestContextShared {
    rng: ThreadRng,
    time_offset: Duration,
    start_time: SystemTime,
}

impl TestContext {
    pub fn new() -> Self {
        Self {
            shared: Rc::new(RefCell::new(TestContextShared {
                rng: rng(),
                time_offset: Duration::ZERO,
                start_time: SystemTime::UNIX_EPOCH,
            })),
        }
    }

    pub fn advance_time(&self, duration: Duration) {
        let mut shared = self.shared.borrow_mut();
        shared.time_offset += duration;
    }

    pub fn rewind_time(&self, duration: Duration) {
        let mut shared = self.shared.borrow_mut();
        shared.time_offset = shared.time_offset.saturating_sub(duration);
    }
}

impl Context for TestContext {
    type Rng = ThreadRng;

    fn system_time(&self) -> SystemTime {
        let shared = self.shared.borrow();
        shared.start_time + shared.time_offset
    }

    fn duration_since_start(&self) -> Duration {
        let shared = self.shared.borrow();
        shared.time_offset
    }

    fn rng(&mut self) -> &mut Self::Rng {
        unsafe { &mut (*self.shared.as_ptr()).rng }
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}
