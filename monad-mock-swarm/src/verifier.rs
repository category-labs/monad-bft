use std::time::Duration;

use crate::{mock_swarm::Nodes, swarm_relation::SwarmRelation};

#[derive(Debug, PartialEq, Eq)]
enum ExpectedTick {
    /// Exact tick timestamp
    Exact(Duration),
    /// Range(tick, delta)
    /// Expects swarm tick in the range [tick - delta, tick + delta]
    Range(Duration, Duration),
    /// No expected
    None,
}

#[derive(Debug)]
pub struct MockSwarmVerifier {
    tick: ExpectedTick,
}

impl Default for MockSwarmVerifier {
    fn default() -> Self {
        Self {
            tick: ExpectedTick::None,
        }
    }
}

impl MockSwarmVerifier {
    pub fn tick_exact(mut self, tick: Duration) -> Self {
        if self.tick != ExpectedTick::None {
            panic!("Tick verify already set");
        }
        self.tick = ExpectedTick::Exact(tick);
        self
    }

    pub fn tick_range(mut self, mean_tick: Duration, delta: Duration) -> Self {
        if self.tick != ExpectedTick::None {
            panic!("Tick verify already set");
        }
        self.tick = ExpectedTick::Range(mean_tick, delta);
        self
    }
}

impl MockSwarmVerifier {
    pub fn verify<S: SwarmRelation>(&self, swarm: &Nodes<S>) -> bool {
        let actual_tick = swarm.tick;
        match self.tick {
            ExpectedTick::Exact(tick) => {
                if actual_tick != tick {
                    eprintln!(
                        "Tick verify error expected={:?} actual={:?}",
                        tick, swarm.tick
                    );
                    return false;
                }
            }
            ExpectedTick::Range(mean, delta) => {
                let lower = mean.saturating_sub(delta);
                let upper = mean.saturating_add(delta);
                if actual_tick < lower || actual_tick > upper {
                    eprintln!(
                        "Tick verify error expected=[{:?},{:?}] actual={:?}",
                        lower, upper, actual_tick
                    );
                    return false;
                }
            }
            ExpectedTick::None => {}
        }

        true
    }
}
