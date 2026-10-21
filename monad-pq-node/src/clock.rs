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

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::chorus::types::Timestamp;

// monotonic since start, anchored to unix time at start
#[derive(Clone, Copy)]
pub struct Clock {
    unix_at_start: Timestamp,
    start: Instant,
}

impl Clock {
    pub fn start() -> Self {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is past 1970");
        Self {
            unix_at_start: Timestamp::from_nanos(since_epoch.as_nanos()),
            start: Instant::now(),
        }
    }

    pub fn now(&self) -> Timestamp {
        let elapsed = self.start.elapsed().as_nanos();
        Timestamp::from_nanos(self.unix_at_start.as_nanos() + elapsed)
    }

    // the instant of a timestamp, now if already past
    pub fn instant_of(&self, at: Timestamp) -> Instant {
        let Some(since_start) = at.duration_since(self.unix_at_start) else {
            return Instant::now();
        };
        self.start + since_start.as_duration()
    }
}
