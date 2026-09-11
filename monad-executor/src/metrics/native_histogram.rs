// Copyright (C) 2026 Category Labs, Inc.
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
    sync::Arc,
    time::Duration,
};

use prometheus::{proto, Registry};
use prometheus_client::{
    encoding::prometheus_protobuf,
    metrics::{
        family::Family,
        histogram::{Histogram as ClientHistogram, NativeHistogramConfig},
    },
    registry::Registry as NativeRegistry,
};

use super::{prometheus_metric_name, MetricDef};

/// A cumulative native histogram. Durations are recorded in seconds.
/// Clones share observations. The client synchronizes recording and scraping.
#[derive(Clone, Debug)]
pub struct NativeHistogram(ClientHistogram);

impl NativeHistogram {
    pub fn observe(&self, value: f64) {
        self.0.observe(value);
    }

    pub fn observe_duration(&self, value: Duration) {
        self.observe(value.as_secs_f64());
    }
}

/// Defines cached, named histogram fields for a bounded label dimension.
///
/// ```
/// use monad_executor::{histogram_labels, metric_consts, ExecutorMetrics};
/// histogram_labels! {
///     struct Latencies { primary => "primary", secondary => "secondary" }
/// }
/// metric_consts! {
///     LATENCY { name: "example.latency_seconds", help: "Latency in seconds" }
/// }
/// let mut metrics = ExecutorMetrics::default();
/// let latency = Latencies::new(&mut metrics, LATENCY, "mode");
/// latency.primary.observe_duration(std::time::Duration::from_millis(12));
/// ```
#[macro_export]
macro_rules! histogram_labels {
    ($vis:vis struct $name:ident { $($field:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Debug)]
        $vis struct $name {
            $($vis $field: $crate::NativeHistogram),+
        }
        impl $name {
            $vis fn new(
                metrics: &mut $crate::ExecutorMetrics,
                metric: &'static $crate::MetricDef,
                label_name: &'static str,
            ) -> Self {
                let [$($field),+] = metrics.histogram_family(metric, label_name, &[$($value),+]);
                Self { $($field),+ }
            }
        }
    };
}

// Native buckets carry the distribution in protobuf scrapes. Explicit buckets
// provide a classic representation for clients using the existing text endpoint.
fn new_histogram() -> ClientHistogram {
    ClientHistogram::new_classic_and_native(
        [
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ],
        NativeHistogramConfig::new(1.1).max_buckets(256),
    )
}

type HistogramFamily =
    Family<Vec<(&'static str, &'static str)>, ClientHistogram, fn() -> ClientHistogram>;

#[derive(Debug)]
pub(super) struct NativeHistogramFamily {
    metric: &'static MetricDef,
    label_name: Option<&'static str>,
    label_values: &'static [&'static str],
    handles: Box<[NativeHistogram]>,
    collector: HistogramFamily,
}

impl NativeHistogramFamily {
    pub(super) fn new(
        metric: &'static MetricDef,
        label_name: Option<&'static str>,
        label_values: &'static [&'static str],
    ) -> Self {
        // Reuse the existing client's metric and label-name validation.
        let opts = prometheus::Opts::new(
            prometheus_metric_name(metric.name).expect("valid histogram name"),
            metric.help,
        );
        prometheus::HistogramVec::new(
            prometheus::HistogramOpts::from(opts),
            &label_name.into_iter().collect::<Vec<_>>(),
        )
        .expect("valid histogram definition");
        assert!(
            !label_values.is_empty(),
            "histogram must have at least one series"
        );
        let mut seen = HashSet::new();
        assert!(
            label_values
                .iter()
                .all(|value| (label_name.is_none() || !value.is_empty()) && seen.insert(value)),
            "histogram label values must be nonempty and unique"
        );
        let collector = Family::new_with_constructor(new_histogram as fn() -> ClientHistogram);
        let handles = label_values
            .iter()
            .map(|value| {
                let labels = label_name
                    .map(|name| vec![(name, *value)])
                    .unwrap_or_default();
                NativeHistogram(collector.get_or_create(&labels).clone())
            })
            .collect();
        Self {
            metric,
            label_name,
            label_values,
            handles,
            collector,
        }
    }

    pub(super) fn handles<const N: usize>(
        &self,
        metric: &'static MetricDef,
        label_name: Option<&str>,
        label_values: &[&str],
    ) -> [NativeHistogram; N] {
        assert_eq!(self.metric.help, metric.help, "histogram help mismatch");
        assert_eq!(self.label_name, label_name, "histogram label name mismatch");
        assert_eq!(
            self.label_values, label_values,
            "histogram label values mismatch"
        );
        std::array::from_fn(|index| self.handles[index].clone())
    }
}

/// Histograms exported alongside the existing scalar Prometheus registry.
/// Keep both registries alive after initializing all metric families.
#[derive(Debug, Default)]
pub struct NativeHistogramRegistry {
    registry: NativeRegistry,
}

impl NativeHistogramRegistry {
    pub(super) fn new(
        labels: HashMap<String, String>,
        families: Vec<Arc<NativeHistogramFamily>>,
        scalar_registry: &Registry,
    ) -> prometheus::Result<Self> {
        let mut names: HashSet<_> = scalar_registry
            .gather()
            .into_iter()
            .map(|family| family.name().to_owned())
            .collect();
        let mut registry = NativeRegistry::with_labels(
            labels
                .iter()
                .map(|(name, value)| (name.clone().into(), value.clone().into())),
        );
        for family in families {
            let name = prometheus_metric_name(family.metric.name)?;
            // Reserve native family and classic sample names, across both registries.
            for suffix in ["", "_bucket", "_sum", "_count"] {
                if !names.insert(format!("{name}{suffix}")) {
                    return Err(prometheus::Error::Msg(format!(
                        "duplicate histogram metric: {name}{suffix}"
                    )));
                }
            }
            if family
                .label_name
                .is_some_and(|name| labels.contains_key(name))
            {
                return Err(prometheus::Error::Msg(format!(
                    "histogram label conflicts with global label: {name}"
                )));
            }
            if labels.contains_key("le") {
                return Err(prometheus::Error::Msg(
                    "global label le conflicts with classic histogram buckets".into(),
                ));
            }
            registry.register(name, family.metric.help, family.collector.clone());
        }
        Ok(Self { registry })
    }

    /// Length-delimited Prometheus MetricFamily messages, including native buckets.
    pub fn encode_protobuf(&self) -> Result<Vec<u8>, prometheus_protobuf::EncodeError> {
        prometheus_protobuf::encode_to_vec(&self.registry)
    }

    /// Classic buckets for the existing Prometheus text encoder. Native buckets
    /// deliberately remain in the separate protobuf representation.
    pub fn classic_metric_families(&self) -> Result<Vec<proto::MetricFamily>, std::fmt::Error> {
        Ok(prometheus_protobuf::encode(&self.registry)?
            .into_iter()
            .map(|family| {
                let mut result = proto::MetricFamily::default();
                result.set_name(family.name);
                result.set_help(family.help);
                result.set_field_type(proto::MetricType::HISTOGRAM);
                result.set_metric(
                    family
                        .metric
                        .into_iter()
                        .map(|metric| {
                            let mut result = proto::Metric::default();
                            result.set_label(
                                metric
                                    .label
                                    .into_iter()
                                    .map(|label| {
                                        let mut result = proto::LabelPair::default();
                                        result.set_name(label.name);
                                        result.set_value(label.value);
                                        result
                                    })
                                    .collect(),
                            );
                            let histogram = metric
                                .histogram
                                .expect("only histogram families registered");
                            let mut classic = proto::Histogram::default();
                            classic.set_sample_count(histogram.sample_count);
                            classic.set_sample_sum(histogram.sample_sum);
                            classic.set_bucket(
                                histogram
                                    .bucket
                                    .into_iter()
                                    .map(|bucket| {
                                        let mut result = proto::Bucket::default();
                                        result.set_upper_bound(bucket.upper_bound);
                                        result.set_cumulative_count(bucket.cumulative_count);
                                        result
                                    })
                                    .collect(),
                            );
                            result.set_histogram(classic);
                            result
                        })
                        .collect(),
                );
                result
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutorMetrics, ExecutorMetricsChain};

    crate::histogram_labels! { struct Latencies { fast => "fast", slow => "slow" } }
    crate::metric_consts! {
        LATENCY { name: "test.latency_seconds", help: "Latency" }
        COLLISION { name: "test_latency_seconds_count", help: "Conflicting scalar" }
    }

    #[test]
    fn native_buckets_are_cumulative_and_shared_after_registration() {
        let mut metrics = ExecutorMetrics::default();
        let latency = Latencies::new(&mut metrics, LATENCY, "mode");
        let cloned = metrics.clone();
        let registry = ExecutorMetricsChain::from(&metrics)
            .push(&cloned)
            .native_histogram_registry(HashMap::new(), &Registry::new())
            .unwrap();
        let before = prometheus_protobuf::encode(&registry.registry).unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].metric.len(), 2);
        assert!(before[0]
            .metric
            .iter()
            .all(|m| m.histogram.as_ref().unwrap().sample_count == 0));
        latency.fast.observe_duration(Duration::ZERO);
        latency
            .fast
            .clone()
            .observe_duration(Duration::from_millis(12));
        Latencies::new(&mut metrics, LATENCY, "mode")
            .slow
            .observe_duration(Duration::from_secs(60));
        for _ in 0..2 {
            let families = prometheus_protobuf::encode(&registry.registry).unwrap();
            let fast = families[0]
                .metric
                .iter()
                .find(|m| m.label[0].value == "fast")
                .unwrap()
                .histogram
                .as_ref()
                .unwrap();
            assert_eq!(fast.sample_count, 2);
            assert!((fast.sample_sum - 0.012).abs() < 1e-12);
            assert_eq!(fast.zero_count, 1);
            assert!(!fast.positive_span.is_empty());
            assert_eq!(fast.positive_delta, [1]);
            let slow = families[0]
                .metric
                .iter()
                .find(|m| m.label[0].value == "slow")
                .unwrap()
                .histogram
                .as_ref()
                .unwrap();
            assert_eq!(slow.sample_count, 1);
            assert_eq!(slow.sample_sum, 60.0);
            assert!(!slow.positive_span.is_empty());
        }
    }

    #[test]
    fn rejects_collisions_with_classic_sample_names() {
        let mut metrics = ExecutorMetrics::with_metric_defs(&[COLLISION]);
        metrics.native_histogram(LATENCY);
        let scalars = Registry::new();
        metrics.register(&scalars).unwrap();
        assert!(ExecutorMetricsChain::from(&metrics)
            .native_histogram_registry(HashMap::new(), &scalars)
            .is_err());
    }

    #[test]
    fn rejects_independent_histograms_with_the_same_name() {
        let mut first = ExecutorMetrics::default();
        let mut second = ExecutorMetrics::default();
        first.native_histogram(LATENCY);
        second.native_histogram(LATENCY);
        assert!(ExecutorMetricsChain::from(&first)
            .push(&second)
            .native_histogram_registry(HashMap::new(), &Registry::new())
            .is_err());
    }

    #[test]
    fn rejects_conflicting_global_labels() {
        let mut metrics = ExecutorMetrics::default();
        let latency = Latencies::new(&mut metrics, LATENCY, "mode");
        latency.fast.observe(1.0);
        latency.slow.observe(2.0);
        assert!(ExecutorMetricsChain::from(&metrics)
            .native_histogram_registry(
                HashMap::from([("mode".into(), "global".into())]),
                &Registry::new()
            )
            .is_err());
    }
}
