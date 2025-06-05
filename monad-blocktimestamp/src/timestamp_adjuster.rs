use std::ops::Neg;

use monad_consensus_types::quorum_certificate::{
    TimestampAdjustment, TimestampAdjustmentDirection,
};
use rand::Rng;
use sorted_vec::SortedVec;
use tracing::{debug, info};

#[derive(Debug)]
pub struct TimestampAdjuster {
    /// track adjustments to make to the local time
    adjustment: i64,
    /// number of timestamp_adjustment commands before updating adjustment
    adjustment_period: usize,
    /// list of deltas received from consensus to use towards updating adjustment
    deltas: SortedVec<i64>,
    /// maximum abs value of a delta we can use
    max_delta_ns: u128,
}

impl TimestampAdjuster {
    pub fn new(max_delta_ns: u128, adjustment_period: usize, max_drift: Option<i64>) -> Self {
        assert!(max_delta_ns < i128::MAX as u128);
        assert!(
            adjustment_period % 2 == 1,
            "median accuracy expects odd period"
        );
        let mut init_adjustment = 0;
        let mut rng = rand::thread_rng();
        if let Some(max_drift) = max_drift {
            init_adjustment = rng.gen_range(max_drift.neg()..max_drift);
            debug!(?init_adjustment, "Set initial clock adjustment");
        }
        Self {
            adjustment: init_adjustment,
            adjustment_period,
            deltas: SortedVec::new(),
            max_delta_ns,
        }
    }

    pub fn add_delta(&mut self, delta: i64) {
        debug!(delta, "add delta");
        self.deltas.insert(delta);
        if self.deltas.len() == self.adjustment_period {
            let i = self.deltas.len() / 2;
            let adjustment = self.deltas[i];

            info!(
                median_idx = i,
                median_delta = self.deltas[i],
                old_adjustment = self.adjustment,
                new_adjustment = adjustment,
                "Set block timestamper adjustment"
            );

            self.adjustment = adjustment;
            self.deltas.clear();
        }
    }

    pub fn determine_signed_delta(&self, t: TimestampAdjustment) -> i64 {
        let delta = if t.delta > self.max_delta_ns {
            self.max_delta_ns
        } else {
            t.delta
        };
        let delta: i64 = delta.try_into().unwrap_or(0);
        match t.direction {
            TimestampAdjustmentDirection::Forward => delta,
            TimestampAdjustmentDirection::Backward => -delta,
        }
    }

    pub fn handle_adjustment(&mut self, t: TimestampAdjustment) {
        self.add_delta(self.determine_signed_delta(t));
    }

    pub fn get_adjustment(&self) -> i64 {
        self.adjustment
    }
}
