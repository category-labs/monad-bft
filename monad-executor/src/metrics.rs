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
    collections::{HashMap, HashSet},
    ops::{Index, IndexMut},
};

use hdrhistogram::Histogram as HdrHistogram;
use prometheus::{
    core::{AtomicU64, GenericGauge},
    Opts, Registry,
};

type UIntGauge = GenericGauge<AtomicU64>;

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

fn prometheus_metric_name(metric: &MetricDef) -> String {
    let mut name = String::with_capacity(metric.name.len());
    for ch in metric.name.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | ':' => name.push(ch),
            _ => name.push('_'),
        }
    }

    if matches!(name.chars().next(), Some('0'..='9')) {
        name.insert(0, '_');
    }

    name
}

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

#[derive(Clone, Debug)]
pub struct ExecutorMetricHandle {
    gauge: UIntGauge,
}

impl ExecutorMetricHandle {
    pub fn set(&self, value: u64) {
        self.gauge.set(value);
    }

    pub fn inc(&self) {
        self.gauge.inc();
    }

    pub fn add(&self, value: u64) {
        self.gauge.add(value);
    }

    pub fn get(&self) -> u64 {
        self.gauge.get()
    }
}

pub struct ExecutorMetrics {
    values: HashMap<&'static MetricDef, u64>,
    registered: HashMap<&'static MetricDef, UIntGauge>,
    registry: Registry,
}

impl Default for ExecutorMetrics {
    fn default() -> Self {
        Self {
            values: HashMap::new(),
            registered: HashMap::new(),
            registry: Registry::new(),
        }
    }
}

impl Clone for ExecutorMetrics {
    fn clone(&self) -> Self {
        let mut metrics = Self::default();
        for (metric, value) in self.snapshot() {
            metrics.set(metric, value);
        }
        metrics
    }
}

impl std::fmt::Debug for ExecutorMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorMetrics")
            .field("values", &self.snapshot())
            .finish()
    }
}

impl ExecutorMetrics {
    fn ensure_registered(&mut self, metric: &'static MetricDef) -> UIntGauge {
        if let Some(gauge) = self.registered.get(metric) {
            return gauge.clone();
        }

        let gauge = UIntGauge::with_opts(Opts::new(prometheus_metric_name(metric), metric.help))
            .expect("executor metric definition is valid");
        if let Some(&value) = self.values.get(metric) {
            gauge.set(value);
        }

        self.registry
            .register(Box::new(gauge.clone()))
            .expect("executor metric can be registered once");
        self.registered.insert(metric, gauge.clone());
        gauge
    }

    fn snapshot(&self) -> Vec<(&'static MetricDef, u64)> {
        let mut metrics = Vec::with_capacity(self.values.len().max(self.registered.len()));
        let mut seen = HashSet::with_capacity(self.values.len() + self.registered.len());

        for (&metric, &value) in &self.values {
            if let Some(gauge) = self.registered.get(metric) {
                gauge.set(value);
            }
            metrics.push((metric, value));
            seen.insert(metric);
        }

        for (&metric, gauge) in &self.registered {
            if seen.insert(metric) {
                metrics.push((metric, gauge.get()));
            }
        }

        metrics
    }

    pub fn register(&mut self, metric: &'static MetricDef) -> ExecutorMetricHandle {
        ExecutorMetricHandle {
            gauge: self.ensure_registered(metric),
        }
    }

    pub fn set(&mut self, metric: &'static MetricDef, value: u64) {
        self.values.insert(metric, value);
        self.ensure_registered(metric).set(value);
    }

    pub fn inc(&mut self, metric: &'static MetricDef) {
        let value = self.get(metric) + 1;
        self.values.insert(metric, value);
        self.ensure_registered(metric).set(value);
    }

    pub fn add(&mut self, metric: &'static MetricDef, value: u64) {
        let updated = self.get(metric) + value;
        self.values.insert(metric, updated);
        self.ensure_registered(metric).set(updated);
    }

    pub fn get(&self, metric: &'static MetricDef) -> u64 {
        self.values
            .get(metric)
            .copied()
            .or_else(|| self.registered.get(metric).map(UIntGauge::get))
            .unwrap_or_default()
    }

    pub fn iter_with_descriptions(
        &self,
    ) -> impl Iterator<Item = (&'static str, u64, &'static str)> + '_ {
        self.snapshot()
            .into_iter()
            .map(|(metric, value)| (metric.name, value, metric.help))
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
        self.ensure_registered(metric);
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
            .flat_map(|metrics| metrics.iter_with_descriptions())
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

    metric_consts! {
        TEST_LEGACY_METRIC {
            name: "monad.executor.test.legacy",
            help: "Test legacy metric",
        }
        TEST_REGISTERED_METRIC {
            name: "monad.executor.test.registered",
            help: "Test registered metric",
        }
    }

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

    #[test]
    fn test_legacy_metrics_are_exported() {
        let mut metrics = ExecutorMetrics::default();

        metrics[TEST_LEGACY_METRIC] += 2;
        metrics[TEST_LEGACY_METRIC] = 5;

        assert_eq!(metrics[TEST_LEGACY_METRIC], 5);
        assert_eq!(metrics.get(TEST_LEGACY_METRIC), 5);

        let exported = metrics.iter_with_descriptions().collect::<Vec<_>>();
        assert_eq!(
            exported,
            vec![(TEST_LEGACY_METRIC.name, 5, TEST_LEGACY_METRIC.help,)]
        );
    }

    #[test]
    fn test_registered_metrics_are_readable_and_clone_as_snapshot() {
        let mut metrics = ExecutorMetrics::default();
        let registered = metrics.register(TEST_REGISTERED_METRIC);

        registered.add(3);
        registered.inc();

        assert_eq!(registered.get(), 4);
        assert_eq!(metrics.get(TEST_REGISTERED_METRIC), 4);

        let snapshot = metrics.clone();
        registered.inc();

        assert_eq!(metrics.get(TEST_REGISTERED_METRIC), 5);
        assert_eq!(snapshot.get(TEST_REGISTERED_METRIC), 4);
    }
}
