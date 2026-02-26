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

#[derive(Copy, Clone, Debug)]
pub struct MetricDef {
    pub name: &'static str,
    pub help: &'static str,
}

impl MetricDef {
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self { name, help }
    }
}

impl std::hash::Hash for MetricDef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for MetricDef {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for MetricDef {}

/// Defines one or more `MetricDef` constants with co-located name and help text.
///
/// # Example
///
/// ```ignore
/// monad_executor::metric_consts! {
///     pub GAUGE_TOTAL_EXEC_US {
///         name: "monad.executor.total_exec_us",
///         help: "Total executor execution time in microseconds",
///     }
///     GAUGE_POLL_US {
///         name: "monad.executor.poll_us",
///         help: "Total executor poll time in microseconds",
///     }
/// }
/// ```
#[macro_export]
macro_rules! metric_consts {
    ($( $vis:vis $ident:ident { name: $name:expr, help: $help:expr $(,)? } )+) => {
        $(
            $vis const $ident: &'static $crate::MetricDef = &$crate::MetricDef::new($name, $help);
        )+
    };
}

#[derive(Default, Debug, Clone)]
pub struct ExecutorMetrics {
    values: HashMap<&'static MetricDef, u64>,
}

impl ExecutorMetrics {
    pub fn set(&mut self, metric: &'static MetricDef, value: u64) {
        self.values.insert(metric, value);
    }

    pub fn iter_with_descriptions(
        &self,
    ) -> impl Iterator<Item = (&'static str, u64, &'static str)> + '_ {
        self.values.iter().map(|(k, &v)| (k.name, v, k.help))
    }
}

impl Index<&'static MetricDef> for ExecutorMetrics {
    type Output = u64;

    fn index(&self, metric: &'static MetricDef) -> &Self::Output {
        self.values.get(metric).unwrap_or(&0)
    }
}

impl IndexMut<&'static MetricDef> for ExecutorMetrics {
    fn index_mut(&mut self, metric: &'static MetricDef) -> &mut Self::Output {
        self.values.entry(metric).or_default()
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

    pub fn into_inner(self) -> Vec<(&'static str, u64, &'static str)> {
        self.0
            .into_iter()
            .flat_map(|metrics| metrics.values.iter().map(|(k, &v)| (k.name, v, k.help)))
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
