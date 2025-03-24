use sorted_vec::SortedVec;
use tracing::info;

use crate::timestamp::{TimestampAdjustment, TimestampAdjustmentDirection};

#[derive(Debug)]
pub struct TimestampAdjuster {
    /// track adjustments to make to the local time
    adjustment: i128,
    /// number of timestamp_adjustment commands before updating adjustment
    adjustment_period: usize,
    /// list of deltas received from consensus to use towards updating adjustment
    deltas: SortedVec<i128>,
    /// maximum abs value of a delta we can use
    max_delta_ns: u128,
}

impl TimestampAdjuster {
    pub fn new(max_delta_ns: u128, adjustment_period: usize) -> Self {
        assert!(max_delta_ns < i128::MAX as u128);
        assert!(
            adjustment_period % 2 == 1,
            "median accuracy expects odd period"
        );
        Self {
            adjustment: 0,
            adjustment_period,
            deltas: SortedVec::new(),
            max_delta_ns,
        }
    }

    fn add_delta(&mut self, delta: i128) {
        info!(delta, period = self.adjustment_period, "add delta");
        self.deltas.insert(delta);
        if self.deltas.len() == self.adjustment_period {
            let i = self.deltas.len() / 2;
            self.adjustment += self.deltas[i];

            info!(
                median_delta = self.deltas[i],
                adjustment = self.adjustment,
                "local timestamper adjustment"
            );

            self.deltas.clear();
        }
    }

    fn determine_signed_delta(&self, t: TimestampAdjustment) -> i128 {
        let delta = if t.delta > self.max_delta_ns {
            self.max_delta_ns
        } else {
            t.delta
        };
        let delta: i128 = delta.try_into().unwrap_or(0);
        match t.direction {
            TimestampAdjustmentDirection::Forward => delta,
            TimestampAdjustmentDirection::Backward => -delta,
        }
    }

    pub fn handle_adjustment(&mut self, t: TimestampAdjustment) {
        self.add_delta(self.determine_signed_delta(t));
    }

    pub fn get_adjustment(&self) -> i128 {
        self.adjustment
    }
}
