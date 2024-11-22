use std::cmp::max;

use monad_consensus_types::{
    clock::{AdjusterConfig, Clock},
    quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
};

use crate::timestamp_adjuster::TimestampAdjuster;

#[derive(Debug)]
pub struct BlockTimestamp<T: Clock> {
    clock: T,

    max_delta: u64,

    /// TODO: this needs an upper-bound
    latency_estimate_ms: u64,

    adjuster: Option<TimestampAdjuster>,
}

impl<T: Clock> BlockTimestamp<T> {
    pub fn new(max_delta: u64, latency_estimate_ms: u64, adjuster_config: AdjusterConfig) -> Self {
        assert!(latency_estimate_ms > 0);
        println!("adjuster_config: {:?}", adjuster_config);
        Self {
            clock: T::new(),
            max_delta,
            latency_estimate_ms,
            adjuster: match adjuster_config {
                AdjusterConfig::Disabled => None,
                AdjusterConfig::Enabled {
                    max_delta,
                    adjustment_period,
                } => Some(TimestampAdjuster::new(max_delta, adjustment_period)),
            },
        }
    }

    pub fn update_time(&mut self, time: u64) {
        self.clock.update(time);
    }

    pub fn handle_adjustment(&mut self, delta: TimestampAdjustment) {
        if let Some(adjuster) = &mut self.adjuster {
            adjuster.handle_adjustment(delta);
        }
    }

    pub fn get_current_time(&self) -> u64 {
        let now = self.clock.get();
        if let Some(adjuster) = &self.adjuster {
            let adjustment = adjuster.get_adjustment();
            if adjustment >= 0 {
                now.checked_add(adjustment as u64).unwrap_or(now)
            } else {
                now.saturating_sub(adjustment.unsigned_abs())
            }
        } else {
            now
        }
    }

    pub fn get_valid_block_timestamp(&self, prev_block_ts: u64) -> u64 {
        max(prev_block_ts + 1, self.get_current_time())
    }

    fn valid_bounds(&self, timestamp: u64) -> bool {
        let now = self.get_current_time();
        let lower_bound = now.saturating_sub(self.max_delta);
        let upper_bound = now.saturating_add(self.max_delta);

        lower_bound <= timestamp && timestamp <= upper_bound
    }

    pub fn valid_block_timestamp(
        &self,
        prev_block_ts: u64,
        curr_block_ts: u64,
    ) -> Option<TimestampAdjustment> {
        let delta = curr_block_ts.checked_sub(prev_block_ts);
        match delta {
            // block timestamp must be strictly monotonically increasing
            None => None,
            Some(0) => None,
            // check that its not higher than the valid upper bound
            Some(_) => {
                if !self.valid_bounds(curr_block_ts) {
                    None
                } else {
                    // return the delta between local time and block time for adjustment
                    let now = self.get_current_time();
                    let mut adjustment = now.abs_diff(curr_block_ts);
                    // adjust for estimated latency
                    adjustment = adjustment.saturating_sub(self.latency_estimate_ms);
                    if curr_block_ts > now {
                        Some(TimestampAdjustment {
                            delta: adjustment,
                            direction: TimestampAdjustmentDirection::Forward,
                        })
                    } else {
                        Some(TimestampAdjustment {
                            delta: adjustment,
                            direction: TimestampAdjustmentDirection::Backward,
                        })
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        clock::TestClock,
        quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
    };

    use crate::{timestamp::AdjusterConfig, BlockTimestamp};

    #[test]
    fn test_block_timestamp_validate() {
        let mut b = BlockTimestamp::<TestClock>::new(10, 1, AdjusterConfig::Disabled);
        b.update_time(0);

        assert!(b.valid_block_timestamp(1, 1).is_none());
        assert!(b.valid_block_timestamp(2, 1).is_none());
        assert!(b.valid_block_timestamp(0, 11).is_none());

        assert!(matches!(
            b.valid_block_timestamp(1, 2),
            Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Forward
            })
        ));

        b.update_time(10);

        assert!(matches!(
            b.valid_block_timestamp(5, 8),
            Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Backward
            })
        ));
    }
}
