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

use std::{
    collections::HashMap,
    ops::{Index, IndexMut},
};

use hdrhistogram::Histogram as HdrHistogram;

#[derive(Default, Debug, Clone)]
pub struct ExecutorMetrics(HashMap<&'static str, u64>);

impl Index<&'static str> for ExecutorMetrics {
    type Output = u64;

    fn index(&self, index: &'static str) -> &Self::Output {
        self.0.get(index).unwrap_or(&0)
    }
}

impl IndexMut<&'static str> for ExecutorMetrics {
    fn index_mut(&mut self, index: &'static str) -> &mut Self::Output {
        self.0.entry(index).or_default()
    }
}

impl AsRef<Self> for ExecutorMetrics {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<'a> From<&'a ExecutorMetrics> for ExecutorMetricsChain<'a> {
    fn from(metrics: &'a ExecutorMetrics) -> Self {
        ExecutorMetricsChain(vec![metrics])
    }
}

#[derive(Default)]
pub struct ExecutorMetricsChain<'a>(Vec<&'a ExecutorMetrics>);

impl<'a> ExecutorMetricsChain<'a> {
    pub fn push(mut self, metrics: &'a ExecutorMetrics) -> Self {
        self.0.push(metrics);
        self
    }

    pub fn chain(mut self, metrics: ExecutorMetricsChain<'a>) -> Self {
        self.0.extend(metrics.0);
        self
    }

    pub fn into_inner(self) -> Vec<(&'static str, u64)> {
        self.0
            .into_iter()
            .flat_map(|metrics| metrics.0.clone().into_iter())
            .collect()
    }
}

/// A wrapper around hdrhistogram for computing latency percentiles.
///
/// Percentiles method take on order of 1us and nearly constant time even for larger histograms.
pub struct Histogram {
    histogram: HdrHistogram<u64>,
}

impl Histogram {
    pub fn new(high: u64, sigfig: u8) -> Result<Self, hdrhistogram::CreationError> {
        Ok(Self {
            histogram: HdrHistogram::new_with_bounds(1, high, sigfig)?,
        })
    }

    pub fn record(&mut self, value: u64) -> Result<(), hdrhistogram::RecordError> {
        self.histogram.record(value)
    }

    pub fn p50(&self) -> u64 {
        self.histogram.value_at_quantile(0.5)
    }

    pub fn p90(&self) -> u64 {
        self.histogram.value_at_quantile(0.9)
    }

    pub fn p99(&self) -> u64 {
        self.histogram.value_at_quantile(0.99)
    }

    pub fn count(&self) -> u64 {
        self.histogram.len()
    }

    pub fn clear(&mut self) {
        self.histogram.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram() {
        let mut hist = Histogram::new(1_000_000, 3).unwrap();

        for i in 1..=100 {
            hist.record(i * 100).unwrap();
        }

        assert_eq!(hist.count(), 100);
        assert!(hist.p50() >= 5000 && hist.p50() <= 5100);
        assert!(hist.p90() >= 9000 && hist.p90() <= 9100);
        assert!(hist.p99() >= 9900 && hist.p99() <= 10000);
    }
}
