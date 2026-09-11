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
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use prometheus::{
    core::{AtomicU64, GenericGauge},
    Opts, Registry,
};
use tracing::error;

pub type Gauge = GenericGauge<AtomicU64>;

mod counter_labels;
pub use counter_labels::LabeledCounterFamily;
pub use prometheus::IntCounter as Counter;

mod native_histogram;
use native_histogram::NativeHistogramFamily;
pub use native_histogram::{NativeHistogram, NativeHistogramRegistry};

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

impl Hash for MetricDef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for MetricDef {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for MetricDef {}

fn is_valid_prometheus_metric_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || matches!(ch, '_' | ':')
}

fn is_valid_prometheus_metric_char(ch: char) -> bool {
    is_valid_prometheus_metric_start(ch) || ch.is_ascii_digit()
}

pub fn prometheus_metric_name(metric: &str) -> prometheus::Result<String> {
    let mut name = String::with_capacity(metric.len());
    for (idx, ch) in metric.char_indices() {
        match ch {
            '.' => name.push('_'),
            ch if is_valid_prometheus_metric_char(ch) => name.push(ch),
            _ => {
                return Err(prometheus::Error::Msg(format!(
                    "invalid prometheus metric name {metric:?}: character {ch:?} at byte {idx} is not allowed"
                )));
            }
        }
    }

    let Some(first) = name.chars().next() else {
        return Err(prometheus::Error::Msg(
            "prometheus metric name must not be empty".to_owned(),
        ));
    };
    if !is_valid_prometheus_metric_start(first) {
        return Err(prometheus::Error::Msg(format!(
            "invalid prometheus metric name {metric:?}: first character {first:?} is not allowed"
        )));
    }

    Ok(name)
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

#[derive(Clone, Default)]
pub struct ExecutorMetrics {
    gauges: HashMap<&'static MetricDef, Gauge>,
    counter_families: HashMap<&'static MetricDef, Arc<LabeledCounterFamily>>,
    histogram_families: HashMap<&'static MetricDef, Arc<NativeHistogramFamily>>,
}

impl fmt::Debug for ExecutorMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutorMetrics")
            .field("values", &self.snapshot())
            .field("counter_families", &self.counter_families)
            .field("histogram_families", &self.histogram_families)
            .finish()
    }
}

impl ExecutorMetrics {
    fn ensure_gauge(&mut self, metric: &'static MetricDef) -> &Gauge {
        assert!(
            !self.counter_families.contains_key(metric)
                && !self.histogram_families.contains_key(metric),
            "metric is already a counter or histogram: {}",
            metric.name
        );
        match self.gauges.entry(metric) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(
                Gauge::with_opts(Opts::new(
                    prometheus_metric_name(metric.name)
                        .expect("executor metric definition is valid"),
                    metric.help,
                ))
                .expect("executor metric definition is valid"),
            ),
        }
    }

    fn snapshot(&self) -> Vec<(&'static MetricDef, u64)> {
        let mut metrics = Vec::with_capacity(self.gauges.len());

        for (&metric, gauge) in &self.gauges {
            metrics.push((metric, gauge.get()));
        }

        metrics
    }

    pub fn with_metric_defs(metric_defs: &[&'static MetricDef]) -> Self {
        let mut metrics = Self::default();
        for &metric in metric_defs {
            metrics.ensure_gauge(metric);
        }
        metrics
    }

    pub fn gauge(&mut self, metric: &'static MetricDef) -> &Gauge {
        match self.gauges.entry(metric) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                assert!(
                    !self.counter_families.contains_key(metric)
                        && !self.histogram_families.contains_key(metric),
                    "metric is already a counter or histogram: {}",
                    metric.name
                );
                error!(
                    metric = metric.name,
                    "executor metric gauge was not registered before access"
                );
                debug_assert!(
                    false,
                    "executor metric must be initialized before taking a gauge: {}",
                    metric.name
                );
                entry.insert(
                    Gauge::with_opts(Opts::new(
                        prometheus_metric_name(metric.name)
                            .expect("executor metric definition is valid"),
                        metric.help,
                    ))
                    .expect("executor metric definition is valid"),
                )
            }
        }
    }

    /// Creates a counter family, or returns handles to the existing family.
    /// Prefer [`crate::counter_labels!`] for typed, named counter fields.
    /// Initialize all families before registering metrics with an exporter.
    pub fn counter_family<const N: usize>(
        &mut self,
        metric: &'static MetricDef,
        label_name: &'static str,
        label_values: &'static [&'static str; N],
    ) -> [Counter; N] {
        assert!(
            !self.gauges.contains_key(metric) && !self.histogram_families.contains_key(metric),
            "metric is already a gauge or histogram: {}",
            metric.name
        );
        let family = self.counter_families.entry(metric).or_insert_with(|| {
            Arc::new(LabeledCounterFamily::new(metric, label_name, label_values))
        });
        family.assert_schema(metric, label_name, label_values);
        family.handles()
    }

    /// Creates an unlabeled histogram using the default latency configuration.
    pub fn native_histogram(&mut self, metric: &'static MetricDef) -> NativeHistogram {
        let [histogram] = self.histogram_family_inner(metric, None, &[""]);
        histogram
    }

    /// Creates cached labeled histograms. Prefer [`crate::histogram_labels!`].
    /// Initialize before registering with exporters. Values are recorded in seconds.
    pub fn histogram_family<const N: usize>(
        &mut self,
        metric: &'static MetricDef,
        label_name: &'static str,
        label_values: &'static [&'static str; N],
    ) -> [NativeHistogram; N] {
        self.histogram_family_inner(metric, Some(label_name), label_values)
    }

    fn histogram_family_inner<const N: usize>(
        &mut self,
        metric: &'static MetricDef,
        label_name: Option<&'static str>,
        label_values: &'static [&'static str; N],
    ) -> [NativeHistogram; N] {
        assert!(
            !self.gauges.contains_key(metric) && !self.counter_families.contains_key(metric),
            "metric is already a gauge or counter: {}",
            metric.name
        );
        let family = self.histogram_families.entry(metric).or_insert_with(|| {
            Arc::new(NativeHistogramFamily::new(metric, label_name, label_values))
        });
        family.handles(metric, label_name, label_values)
    }

    /// Scalar gauge handles. Labeled counters are exported as families separately.
    pub fn metric_handles(&self) -> Vec<(&'static str, Gauge, &'static str)> {
        self.gauges
            .iter()
            .map(|(&metric, gauge)| (metric.name, gauge.clone(), metric.help))
            .collect()
    }

    pub fn register(&self, registry: &Registry) -> prometheus::Result<()> {
        for (_, gauge, _) in self.metric_handles() {
            registry.register(Box::new(gauge))?;
        }
        for family in self.counter_families.values() {
            family.register(registry)?;
        }
        Ok(())
    }

    /// Scalar gauge snapshots. Use counter families to retain counter labels.
    pub fn iter_with_descriptions(
        &self,
    ) -> impl Iterator<Item = (&'static str, u64, &'static str)> + '_ {
        self.snapshot()
            .into_iter()
            .map(|(metric, value)| (metric.name, value, metric.help))
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

    /// Scalar gauge snapshots. Use [`Self::counter_families`] for labeled counters.
    pub fn into_inner(self) -> Vec<(&'static str, u64, &'static str)> {
        self.metric_handles()
            .into_iter()
            .map(|(name, gauge, help)| (name, gauge.get(), help))
            .collect()
    }

    /// Deduplicates shared families, preserving every label value within a family.
    /// Independent families with conflicting names are rejected during registration.
    pub fn counter_families(&self) -> Vec<Arc<LabeledCounterFamily>> {
        let mut seen = HashSet::new();
        self.0
            .iter()
            .flat_map(|metrics| metrics.counter_families.values())
            .filter(|family| seen.insert(Arc::as_ptr(family)))
            .cloned()
            .collect()
    }

    /// Builds the native histogram registry after scalar registration so metric
    /// names and global labels can be checked for collisions.
    pub fn native_histogram_registry(
        &self,
        labels: HashMap<String, String>,
        scalar_registry: &Registry,
    ) -> prometheus::Result<NativeHistogramRegistry> {
        let mut seen = HashSet::new();
        let families = self
            .0
            .iter()
            .flat_map(|metrics| metrics.histogram_families.values())
            .filter(|family| seen.insert(Arc::as_ptr(family)))
            .cloned()
            .collect();
        NativeHistogramRegistry::new(labels, families, scalar_registry)
    }

    pub fn register(&self, registry: &Registry) -> prometheus::Result<()> {
        for (_, gauge, _) in self.metric_handles() {
            registry.register(Box::new(gauge))?;
        }
        for family in self.counter_families() {
            family.register(registry)?;
        }
        Ok(())
    }

    pub fn metric_handles(&self) -> Vec<(&'static str, Gauge, &'static str)> {
        let mut seen = HashSet::new();
        let mut gauges = Vec::new();

        for metrics in &self.0 {
            for (name, gauge, help) in metrics.metric_handles() {
                if seen.insert(name) {
                    gauges.push((name, gauge, help));
                }
            }
        }

        gauges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    metric_consts! {
        TEST_REGISTERED_METRIC {
            name: "monad.executor.test.registered",
            help: "Test registered metric",
        }
        TEST_INVALID_METRIC {
            name: "monad.executor.test-invalid",
            help: "Test invalid metric",
        }
        TEST_LEADING_DIGIT_METRIC {
            name: "1monad.executor.test.invalid",
            help: "Test leading digit metric",
        }
    }

    #[test]
    fn prometheus_metric_name_replaces_dots_only() {
        assert_eq!(
            prometheus_metric_name(TEST_REGISTERED_METRIC.name).expect("valid name"),
            "monad_executor_test_registered"
        );
    }

    #[test]
    fn prometheus_metric_name_rejects_invalid_names() {
        assert!(prometheus_metric_name(TEST_INVALID_METRIC.name).is_err());
        assert!(prometheus_metric_name(TEST_LEADING_DIGIT_METRIC.name).is_err());
    }
}
