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

//! Probability distributions for modelling clock and network behaviour.
//!
//! These return `impl Distribution<_>` values intended to be sampled through
//! [`crate::Simulation::sample`] or [`crate::Ctx::sample`] so that all randomness
//! is driven by the simulation's seeded generator.

// See `crate::time::DurationFromNanosU128`: local shim for the 1.93-only
// `Duration::from_nanos_u128` while the workspace builds on 1.91.1.
#![allow(unstable_name_collisions)]

use std::time::Duration;

use rand::{distributions::Distribution, Rng};
use rand_distr::{LogNormal, Normal};

use crate::time::{ClockDiff, DurationFromNanosU128};

/// The pair of two independent distributions, sampled jointly.
#[derive(Clone)]
struct Pair<D0, D1> {
    d0: D0,
    d1: D1,
}

impl<D0, T0, D1, T1> Distribution<(T0, T1)> for Pair<D0, D1>
where
    D0: Distribution<T0>,
    D1: Distribution<T1>,
{
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (T0, T1) {
        (self.d0.sample(rng), self.d1.sample(rng))
    }
}

/// Normally distributed duration. Negative samples are clipped to zero; use
/// [`normal_clock_diff`] when negative values are meaningful.
pub fn normal_duration(mean: Duration, stddev: Duration) -> impl Distribution<Duration> + 'static {
    Normal::new(mean.as_nanos() as f64, stddev.as_nanos() as f64)
        .unwrap()
        .map(|ns| Duration::from_nanos_u128(ns.max(0.0) as u128))
}

/// Normally distributed signed clock difference.
pub fn normal_clock_diff(
    mean: ClockDiff,
    stddev: Duration,
) -> impl Distribution<ClockDiff> + 'static {
    Normal::new(mean.0 as f64, stddev.as_nanos() as f64)
        .unwrap()
        .map(|ns| ClockDiff(ns as i128))
}

/// Log-normal duration parameterised by the mean and standard deviation of the
/// underlying normal distribution (i.e. of `ln` of the variable). See
/// [`lognormal_duration2`] for the more intuitive parameterisation by the mean
/// and standard deviation of the duration itself.
///
/// Panics if a sample exceeds the representable [`Duration`] range.
pub fn lognormal_duration(
    normal_mean: f64,
    normal_stddev: f64,
) -> impl Distribution<Duration> + 'static {
    LogNormal::new(normal_mean, normal_stddev)
        .unwrap()
        .map(|ns| {
            assert!(
                ns <= Duration::MAX.as_nanos() as f64,
                "lognormal sample too large to represent as a Duration"
            );
            Duration::from_nanos_u128(ns as u128)
        })
}

/// Log-normal duration parameterised directly by its `mean` and `stddev`.
pub fn lognormal_duration2(
    mean: Duration,
    stddev: Duration,
) -> impl Distribution<Duration> + 'static {
    let mean_f64 = mean.as_nanos() as f64;
    let var = (stddev.as_nanos() as f64).powi(2);
    let normal_var = (1.0 + var / mean_f64.powi(2)).ln();
    let normal_mean = mean_f64.ln() - 0.5 * normal_var;
    lognormal_duration(normal_mean, normal_var.sqrt())
}

/// Uniformly distributed duration in the inclusive range `[min, max]`.
pub fn uniform_duration(min: Duration, max: Duration) -> impl Distribution<Duration> + 'static {
    rand::distributions::Uniform::new_inclusive(min.as_nanos(), max.as_nanos())
        .map(Duration::from_nanos_u128)
}

/// Lower bound estimate on global UDP latency.
const MINIMUM_NETWORK_DELAY: Duration = Duration::from_micros(100);
/// Upper bound estimate on global UDP latency.
const MAXIMUM_NETWORK_DELAY: Duration = Duration::from_millis(100);

/// Latency model for a globally distributed UDP network: a uniform baseline
/// (geographic distance) plus log-normal jitter (network conditions). Reasonable
/// jitter parameters for inter-datacenter links are mean 5ms, stddev 500us.
pub fn global_udp_duration(
    mean_jitter: Duration,
    stddev_jitter: Duration,
) -> impl Distribution<Duration> + 'static {
    Pair {
        d0: uniform_duration(MINIMUM_NETWORK_DELAY, MAXIMUM_NETWORK_DELAY),
        d1: lognormal_duration2(mean_jitter, stddev_jitter),
    }
    .map(|(baseline, jitter)| baseline + jitter)
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;
    use crate::time::{micros, millis};

    #[test]
    fn uniform_duration_respects_bounds() {
        let mut rng = StdRng::seed_from_u64(1);
        let d = uniform_duration(millis(10), millis(20));
        for _ in 0..1000 {
            let s = d.sample(&mut rng);
            assert!(s >= millis(10) && s <= millis(20));
        }
    }

    #[test]
    fn global_udp_duration_at_least_baseline() {
        let mut rng = StdRng::seed_from_u64(2);
        let d = global_udp_duration(millis(5), micros(500));
        for _ in 0..1000 {
            assert!(d.sample(&mut rng) >= MINIMUM_NETWORK_DELAY);
        }
    }

    #[test]
    fn same_seed_same_samples() {
        let sample = |seed| {
            let mut rng = StdRng::seed_from_u64(seed);
            let d = normal_duration(millis(5), millis(1));
            (0..16).map(|_| d.sample(&mut rng)).collect::<Vec<_>>()
        };
        assert_eq!(sample(7), sample(7));
        assert_ne!(sample(7), sample(8));
    }
}
