use monad_consensus_types::quorum_certificate::{
    TimestampAdjustment, TimestampAdjustmentDirection,
};

#[derive(Debug)]
pub struct BlockTimestamp {
    local_time: u64,

    max_delta: u64,

    /// TODO: this needs an upper-bound
    latency_estimate_ms: u64,
}

impl BlockTimestamp {
    pub fn new(max_delta: u64, latency_estimate_ms: u64) -> Self {
        assert!(latency_estimate_ms > 0);
        Self {
            local_time: 0,
            max_delta,
            latency_estimate_ms,
        }
    }

    pub fn update_time(&mut self, time: u64) {
        self.local_time = time;
    }

    pub fn get_current_time(&self) -> u64 {
        self.local_time
    }

    pub fn get_valid_block_timestamp(&self, prev_block_ts: u64) -> u64 {
        if self.local_time <= prev_block_ts {
            prev_block_ts + 1
        } else {
            self.local_time
        }
    }

    fn valid_bounds(&self, timestamp: u64) -> bool {
        let lower_bound = self.local_time.saturating_sub(self.max_delta);
        let upper_bound = self.local_time.saturating_add(self.max_delta);

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
                    let mut adjustment = self.local_time.abs_diff(curr_block_ts);
                    // adjust for estimated latency
                    adjustment = adjustment.saturating_sub(self.latency_estimate_ms);
                    if curr_block_ts > self.local_time {
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
    use monad_consensus_types::quorum_certificate::{
        TimestampAdjustment, TimestampAdjustmentDirection,
    };

    use crate::BlockTimestamp;

    #[test]
    fn test_block_timestamp_validate() {
        let mut b = BlockTimestamp::new(10, 1);
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
