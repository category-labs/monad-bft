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

use std::collections::HashSet;

use prometheus::{IntCounter, IntCounterVec, Opts, Registry};

use super::{prometheus_metric_name, MetricDef};

/// Defines named counter fields with explicit, stable label values.
/// Each field caches a child of the same Prometheus counter vector. Clones share
/// counters; recording requires no label lookup or enum-to-index mapping.
///
/// ```
/// use monad_executor::{counter_labels, metric_consts, ExecutorMetrics};
///
/// counter_labels! {
///     struct ReceiveErrors {
///         header_io => "header_io",
///         header_timeout => "header_timeout",
///     }
/// }
/// metric_consts! {
///     RECEIVE_ERRORS {
///         name: "example.receive_errors_total",
///         help: "Receive errors",
///     }
/// }
/// let mut metrics = ExecutorMetrics::default();
/// let errors = ReceiveErrors::new(&mut metrics, RECEIVE_ERRORS, "reason");
/// errors.header_timeout.inc();
/// assert_eq!(errors.header_timeout.get(), 1);
/// ```
///
/// Only declared fields can be recorded:
///
/// ```compile_fail,E0609
/// monad_executor::counter_labels! {
///     struct ReceiveErrors { header_timeout => "header_timeout" }
/// }
/// fn record(errors: &ReceiveErrors) {
///     errors.unknown_error.inc();
/// }
/// ```
#[macro_export]
macro_rules! counter_labels {
    (
        $vis:vis struct $name:ident {
            $($field:ident => $value:literal),+ $(,)?
        }
    ) => {
        #[derive(Clone, Debug)]
        $vis struct $name {
            $($vis $field: $crate::Counter),+
        }

        impl $name {
            $vis fn new(
                metrics: &mut $crate::ExecutorMetrics,
                metric: &'static $crate::MetricDef,
                label_name: &'static str,
            ) -> Self {
                let [$($field),+] = metrics.counter_family(metric, label_name, &[$($value),+]);
                Self { $($field),+ }
            }
        }
    };
}

/// A type-erased family for exporters. Child counters are read-only through this API.
#[derive(Debug)]
pub struct LabeledCounterFamily {
    metric: &'static MetricDef,
    label_name: &'static str,
    label_values: &'static [&'static str],
    counters: Box<[IntCounter]>,
    collector: IntCounterVec,
}

impl LabeledCounterFamily {
    pub(super) fn new(
        metric: &'static MetricDef,
        label_name: &'static str,
        label_values: &'static [&'static str],
    ) -> Self {
        assert!(
            !label_values.is_empty(),
            "counter label must have at least one value"
        );
        let mut seen = HashSet::new();
        assert!(
            label_values
                .iter()
                .all(|value| !value.is_empty() && seen.insert(value)),
            "counter label values must be nonempty and unique"
        );
        let collector = IntCounterVec::new(
            Opts::new(
                prometheus_metric_name(metric.name).expect("valid counter name"),
                metric.help,
            ),
            &[label_name],
        )
        .expect("valid counter definition");
        let counters = label_values
            .iter()
            .map(|value| collector.with_label_values(&[*value]))
            .collect();
        Self {
            metric,
            label_name,
            label_values,
            counters,
            collector,
        }
    }

    pub(super) fn assert_schema(
        &self,
        metric: &'static MetricDef,
        label_name: &'static str,
        label_values: &[&str],
    ) {
        assert_eq!(self.metric.help, metric.help, "counter help mismatch");
        assert_eq!(self.label_name, label_name, "counter label name mismatch");
        assert_eq!(
            self.label_values, label_values,
            "counter label values mismatch"
        );
    }

    pub(super) fn handles<const N: usize>(&self) -> [IntCounter; N] {
        std::array::from_fn(|index| self.counters[index].clone())
    }

    pub(super) fn register(&self, registry: &Registry) -> prometheus::Result<()> {
        registry.register(Box::new(self.collector.clone()))
    }

    pub fn definition(&self) -> &'static MetricDef {
        self.metric
    }

    pub fn label_name(&self) -> &'static str {
        self.label_name
    }

    pub fn samples(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        self.label_values
            .iter()
            .zip(self.counters.iter())
            .map(|(&value, counter)| (value, counter.get()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use prometheus::{proto::MetricType, TextEncoder};

    use super::*;
    use crate::{ExecutorMetrics, ExecutorMetricsChain};

    counter_labels! {
        struct Errors {
            socket_io => "io",
            timeout => "timeout",
        }
    }
    crate::metric_consts! {
        ERRORS { name: "test.errors_total", help: "Test errors" }
        CURRENT { name: "test.current", help: "Current value" }
    }

    #[test]
    fn exports_zeroes_and_live_labeled_counters_alongside_gauges() {
        let mut metrics = ExecutorMetrics::with_metric_defs(&[CURRENT]);
        let errors = Errors::new(&mut metrics, ERRORS, "reason");
        let registry = Registry::new_custom(
            None,
            Some(HashMap::from([(
                "network".to_owned(),
                "testnet".to_owned(),
            )])),
        )
        .unwrap();
        metrics.register(&registry).unwrap();

        let families = registry.gather();
        let family = families
            .iter()
            .find(|family| family.name() == "test_errors_total")
            .unwrap();
        assert_eq!(family.get_field_type(), MetricType::COUNTER);
        assert_eq!(family.get_metric().len(), 2);
        assert!(family
            .get_metric()
            .iter()
            .all(|metric| metric.get_counter().value() == 0.0));

        // Cloned helpers and repeated lookups must update the already-registered family.
        errors.clone().socket_io.inc();
        Errors::new(&mut metrics, ERRORS, "reason")
            .timeout
            .inc_by(3);
        metrics.gauge(CURRENT).set(7);
        let text = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        for (reason, value) in [("io", 1), ("timeout", 3)] {
            let line = text
                .lines()
                .find(|line| {
                    line.starts_with("test_errors_total{")
                        && line.contains(&format!("reason=\"{reason}\""))
                })
                .unwrap();
            assert!(line.contains("network=\"testnet\""));
            assert!(line.ends_with(&format!(" {value}")));
        }
        assert!(text.contains("test_current{network=\"testnet\"} 7\n"));
        assert_eq!(errors.timeout.get(), 3);
    }

    #[test]
    fn chain_deduplicates_clones_without_losing_label_values() {
        let mut metrics = ExecutorMetrics::default();
        let errors = Errors::new(&mut metrics, ERRORS, "reason");
        let cloned = metrics.clone();
        let chain = ExecutorMetricsChain::from(&metrics)
            .push(&cloned)
            .push(&metrics);
        let registry = Registry::new();
        chain.register(&registry).unwrap();
        errors.timeout.inc_by(2);
        let families = chain.counter_families();
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].label_name(), "reason");
        assert_eq!(
            families[0].samples().collect::<Vec<_>>(),
            [("io", 0), ("timeout", 2)]
        );
        assert_eq!(registry.gather()[0].get_metric().len(), 2);
    }

    #[test]
    fn independent_families_with_the_same_name_are_rejected() {
        let mut first = ExecutorMetrics::default();
        Errors::new(&mut first, ERRORS, "reason");
        let mut second = ExecutorMetrics::default();
        Errors::new(&mut second, ERRORS, "reason");
        assert!(ExecutorMetricsChain::from(&first)
            .push(&second)
            .register(&Registry::new())
            .is_err());
    }

    #[test]
    #[should_panic(expected = "counter label name mismatch")]
    fn rejects_conflicting_label_names() {
        let mut metrics = ExecutorMetrics::default();
        Errors::new(&mut metrics, ERRORS, "reason");
        Errors::new(&mut metrics, ERRORS, "error");
    }

    #[test]
    #[should_panic(expected = "counter label values mismatch")]
    fn rejects_conflicting_label_values() {
        counter_labels! { struct Other { failure => "failure" } }
        let mut metrics = ExecutorMetrics::default();
        Errors::new(&mut metrics, ERRORS, "reason");
        Other::new(&mut metrics, ERRORS, "reason").failure.inc();
    }

    #[test]
    #[should_panic(expected = "counter label values must be nonempty and unique")]
    fn rejects_duplicate_label_values() {
        counter_labels! { struct Duplicate { io => "io", also_io => "io" } }
        let mut metrics = ExecutorMetrics::default();
        let errors = Duplicate::new(&mut metrics, ERRORS, "reason");
        errors.io.inc();
        errors.also_io.inc();
    }

    #[test]
    #[should_panic(expected = "metric is already a gauge")]
    fn rejects_using_a_gauge_as_a_counter() {
        Errors::new(
            &mut ExecutorMetrics::with_metric_defs(&[ERRORS]),
            ERRORS,
            "reason",
        );
    }
}
