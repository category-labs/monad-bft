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

//! Simulation time and clock differences.

// `Duration::from_nanos_u128` is stable only since Rust 1.93, but this workspace
// builds on 1.91.1, so it is provided locally by the `DurationFromNanosU128`
// extension trait below. This `allow` silences the future-ambiguity lint raised by
// calling it through the trait while std's (still-unstable) inherent method
// already exists. Remove the trait and this `allow` once the toolchain reaches 1.93.
#![allow(unstable_name_collisions)]

use std::{
    ops::{Add, AddAssign, Sub, SubAssign},
    time::Duration,
};

/// Local stand-in for [`Duration::from_nanos_u128`] (stable since Rust 1.93; the
/// workspace toolchain is 1.91.1). Behaviour matches std, including the panic
/// when the nanosecond count exceeds [`Duration::MAX`].
///
/// Remove once the toolchain reaches 1.93: std's inherent method then shadows
/// this, and the `use crate::time::DurationFromNanosU128;` imports in sibling
/// modules surface as `unused_imports` warnings pointing back here.
pub(crate) trait DurationFromNanosU128 {
    fn from_nanos_u128(nanos: u128) -> Duration;
}

impl DurationFromNanosU128 for Duration {
    #[inline]
    #[track_caller]
    fn from_nanos_u128(nanos: u128) -> Duration {
        const NANOS_PER_SEC: u128 = 1_000_000_000;
        let secs =
            u64::try_from(nanos / NANOS_PER_SEC).expect("overflow in `Duration::from_nanos_u128`");
        // `nanos % NANOS_PER_SEC < 1_000_000_000`, so `Duration::new` takes its
        // no-carry fast path and the result equals std's `new_unchecked` form.
        Duration::new(secs, (nanos % NANOS_PER_SEC) as u32)
    }
}

/// Discrete simulation time: the affine line over clock differences measured in
/// nanoseconds. It has no defined epoch; time `0` is the conventional origin of
/// the global clock. Represented as `i128` so it can hold the full `Duration`
/// range and negative points (used by local clocks with a negative offset).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Time(pub i128);

impl std::fmt::Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = Duration::from_nanos_u128(self.0.unsigned_abs());
        if self.0 < 0 {
            write!(f, "Time(-{:?})", d)
        } else {
            write!(f, "Time({:?})", d)
        }
    }
}

/// Signed difference between two [`Time`] points, in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct ClockDiff(pub i128);

impl std::fmt::Debug for ClockDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = Duration::from_nanos_u128(self.0.unsigned_abs());
        if self.0 < 0 {
            write!(f, "-{:?}", d)
        } else {
            write!(f, "{:?}", d)
        }
    }
}

impl Time {
    /// Unsigned magnitude of the difference between two times. Use `self - other`
    /// (a [`ClockDiff`]) when you need the sign.
    pub fn abs_diff(self, other: Time) -> Duration {
        Duration::from_nanos_u128(self.0.abs_diff(other.0))
    }
}

/// `Time` is a torsor over [`ClockDiff`]: subtracting two times yields their
/// signed difference.
impl Sub<Time> for Time {
    type Output = ClockDiff;
    fn sub(self, rhs: Time) -> Self::Output {
        ClockDiff(self.0 - rhs.0)
    }
}

impl Add<Duration> for Time {
    type Output = Self;
    fn add(self, d: Duration) -> Self::Output {
        Time(self.0 + d.as_nanos() as i128)
    }
}

impl AddAssign<Duration> for Time {
    fn add_assign(&mut self, d: Duration) {
        *self = *self + d;
    }
}

impl Sub<Duration> for Time {
    type Output = Self;
    fn sub(self, d: Duration) -> Self::Output {
        Time(self.0 - d.as_nanos() as i128)
    }
}

impl SubAssign<Duration> for Time {
    fn sub_assign(&mut self, d: Duration) {
        *self = *self - d;
    }
}

impl Add<ClockDiff> for Time {
    type Output = Self;
    fn add(self, d: ClockDiff) -> Self::Output {
        Time(self.0 + d.0)
    }
}

impl AddAssign<ClockDiff> for Time {
    fn add_assign(&mut self, d: ClockDiff) {
        *self = *self + d;
    }
}

impl Sub<ClockDiff> for Time {
    type Output = Self;
    fn sub(self, d: ClockDiff) -> Self::Output {
        Time(self.0 - d.0)
    }
}

impl SubAssign<ClockDiff> for Time {
    fn sub_assign(&mut self, d: ClockDiff) {
        *self = *self - d;
    }
}

impl Add<ClockDiff> for ClockDiff {
    type Output = Self;
    fn add(self, d: ClockDiff) -> Self::Output {
        ClockDiff(self.0 + d.0)
    }
}

impl Sub<ClockDiff> for ClockDiff {
    type Output = Self;
    fn sub(self, d: ClockDiff) -> Self::Output {
        ClockDiff(self.0 - d.0)
    }
}

impl ClockDiff {
    pub fn as_nanos(self) -> i128 {
        self.0
    }

    pub fn from_nanos(ns: i128) -> Self {
        ClockDiff(ns)
    }

    pub fn from_duration(d: Duration) -> Self {
        ClockDiff(d.as_nanos() as i128)
    }

    /// Convert to a floating-point number of nanoseconds. Beware of precision
    /// loss for magnitudes beyond `2^53` nanoseconds.
    pub fn as_f64(self) -> f64 {
        self.0 as f64
    }

    /// Build from a floating-point number of nanoseconds, rounding to the nearest
    /// integer. Beware of precision loss for large magnitudes.
    pub fn from_nanos_f64(s: f64) -> Self {
        ClockDiff(s.round() as i128)
    }
}

/// Shorthand for a [`Duration`] of `ns` nanoseconds.
pub const fn nanos(ns: u64) -> Duration {
    Duration::from_nanos(ns)
}

/// Shorthand for a [`Duration`] of `us` microseconds.
pub const fn micros(us: u64) -> Duration {
    Duration::from_micros(us)
}

/// Shorthand for a [`Duration`] of `ms` milliseconds.
pub const fn millis(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

/// Shorthand for a [`Duration`] of `s` seconds.
pub const fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

/// Shorthand for a [`Duration`] of `m` minutes.
pub fn mins(m: u64) -> Duration {
    Duration::from_mins(m)
}

/// Shorthand for a [`Duration`] of `h` hours.
pub fn hours(h: u64) -> Duration {
    Duration::from_hours(h)
}

/// Shorthand for a [`Duration`] of `d` days.
pub fn days(d: u64) -> Duration {
    Duration::from_hours(d * 24)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_duration_arithmetic() {
        let t = Time(0) + millis(100);
        assert_eq!(t, Time(100_000_000));
        assert_eq!(t - millis(40), Time(60_000_000));

        let mut t = Time(0);
        t += secs(1);
        assert_eq!(t, Time(1_000_000_000));
    }

    #[test]
    fn time_difference_is_signed() {
        assert_eq!(
            Time(0) + millis(100) - (Time(0) + millis(30)),
            ClockDiff(70_000_000)
        );
        assert_eq!(Time(0) - (Time(0) + millis(30)), ClockDiff(-30_000_000));
        assert_eq!((Time(0) + millis(30)).abs_diff(Time(0)), millis(30));
    }

    #[test]
    fn clock_diff_applies_to_time() {
        assert_eq!(Time(5) + ClockDiff(10), Time(15));
        assert_eq!(Time(5) - ClockDiff(10), Time(-5));
        assert_eq!(ClockDiff::from_duration(millis(2)).as_nanos(), 2_000_000);
    }
}
