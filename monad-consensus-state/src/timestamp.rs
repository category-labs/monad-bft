// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use monad_consensus_types::quorum_certificate::{
    TimestampAdjustment, TimestampAdjustmentDirection,
};

#[derive(Debug, Clone)]
pub struct BlockTimestamp {
    local_time_ns: u128,

    max_delta_ns: u128,

    /// TODO: this needs an upper-bound
    latency_estimate_ns: u128,
}

impl BlockTimestamp {
    pub fn new(max_delta_ns: u128, latency_estimate_ns: u128) -> Self {
        assert!(latency_estimate_ns > 0);
        Self {
            local_time_ns: 0,
            max_delta_ns,
            latency_estimate_ns,
        }
    }

    pub fn update_time(&mut self, time: u128) {
        self.local_time_ns = time;
    }

    pub fn get_current_time(&self) -> u128 {
        self.local_time_ns
    }

    pub fn get_valid_block_timestamp(&self, prev_block_ts: u128) -> u128 {
        if self.local_time_ns <= prev_block_ts {
            prev_block_ts + 1
        } else {
            self.local_time_ns
        }
    }

    fn valid_bounds(&self, timestamp: u128, vote_delay_ns: u128) -> bool {
        let max_delta_ns = self.max_delta_ns.saturating_add(vote_delay_ns);

        let lower_bound = self.local_time_ns.saturating_sub(max_delta_ns);
        let upper_bound = self.local_time_ns.saturating_add(max_delta_ns);

        lower_bound <= timestamp && timestamp <= upper_bound
    }

    pub fn valid_block_timestamp(
        &self,
        prev_block_ts: u128,
        curr_block_ts: u128,
        vote_delay_ns: u128,
        is_reproposal: bool,
    ) -> Option<TimestampAdjustment> {
        if is_reproposal {
            // we can't validate precise bounds of reproposal
            // just make sure that it's monotonically increasing and less than current time
            return if curr_block_ts > prev_block_ts && curr_block_ts < self.local_time_ns {
                Some(TimestampAdjustment {
                    delta: 0,
                    direction: TimestampAdjustmentDirection::Forward,
                })
            } else {
                None
            };
        }
        let delta = curr_block_ts.checked_sub(prev_block_ts);
        match delta {
            // block timestamp must be strictly monotonically increasing
            None => None,
            Some(0) => None,
            // check that its not higher than the valid upper bound
            Some(_) => {
                if !self.valid_bounds(curr_block_ts, vote_delay_ns) {
                    None
                } else {
                    // return the delta between local time and block time for adjustment
                    let mut adjustment = self.local_time_ns.abs_diff(curr_block_ts);
                    // adjust for estimated latency
                    adjustment = adjustment.saturating_sub(self.latency_estimate_ns);
                    if curr_block_ts > self.local_time_ns {
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

        assert!(b.valid_block_timestamp(1, 1, 0, false).is_none());
        assert!(b.valid_block_timestamp(2, 1, 0, false).is_none());
        assert!(b.valid_block_timestamp(0, 11, 0, false).is_none());

        assert!(matches!(
            b.valid_block_timestamp(0, 11, 20, false),
            Some(TimestampAdjustment {
                delta: 10,
                direction: TimestampAdjustmentDirection::Forward
            })
        ));

        assert!(matches!(
            b.valid_block_timestamp(1, 2, 0, false),
            Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Forward
            })
        ));

        b.update_time(10);

        assert!(matches!(
            b.valid_block_timestamp(5, 8, 0, false),
            Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Backward
            })
        ));
    }
}
