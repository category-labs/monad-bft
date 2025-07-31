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
