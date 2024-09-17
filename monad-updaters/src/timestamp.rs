use monad_consensus_types::quorum_certificate::{
    TimestampAdjustment, TimestampAdjustmentDirection,
};
use sorted_vec::SortedVec;
use tracing::{info, trace};

pub struct TimestampAdjuster {
    /// track adjustments to make to the local time
    adjustment: i64,
    /// number of timestamp_adjustment commands before updating adjustment
    adjustment_period: usize,
    /// list of deltas received from consensus to use towards updating adjustment
    deltas: SortedVec<i64>,
    /// maximum abs value of a delta we can use
    max_delta: u64,
}

impl TimestampAdjuster {
    pub fn new(max_delta: u64, adjustment_period: usize) -> Self {
        assert!(max_delta < i64::MAX as u64);
        assert!(
            adjustment_period % 2 == 1,
            "median accuracy expects odd period"
        );
        Self {
            adjustment: 0,
            adjustment_period,
            deltas: SortedVec::new(),
            max_delta,
        }
    }

    pub fn add_delta(&mut self, delta: i64) {
        trace!(delta, "add delta");
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

    pub fn determine_signed_delta(&self, t: TimestampAdjustment) -> i64 {
        let delta = if t.delta > self.max_delta {
            self.max_delta
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
