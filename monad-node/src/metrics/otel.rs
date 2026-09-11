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

use monad_executor::ExecutorMetricsChain;
use opentelemetry::{metrics::Meter, KeyValue};

/// Register once at startup. Callbacks read shared cumulative counters on each
/// collection, preserving both counter semantics and the bounded label values.
pub fn register_otel_counters(meter: &Meter, metrics: ExecutorMetricsChain<'_>) {
    for family in metrics.counter_families() {
        meter
            .u64_observable_counter(family.definition().name)
            .with_description(family.definition().help)
            .with_callback(move |observer| {
                for (label_value, value) in family.samples() {
                    observer.observe(value, &[KeyValue::new(family.label_name(), label_value)]);
                }
            })
            .build();
    }
}

#[cfg(test)]
mod tests {
    use monad_executor::ExecutorMetrics;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider, Temporality,
    };

    use super::*;

    #[test]
    fn exports_labels_and_cumulative_counts_across_collections() {
        monad_executor::counter_labels! {
            struct Errors { io => "io", timeout => "timeout" }
        }
        monad_executor::metric_consts! {
            ERRORS { name: "test.errors_total", help: "Test errors" }
        }
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let mut metrics = ExecutorMetrics::default();
        let errors = Errors::new(&mut metrics, ERRORS, "reason");
        let cloned = metrics.clone();
        register_otel_counters(
            &provider.meter("test"),
            ExecutorMetricsChain::from(&metrics).push(&cloned),
        );

        // Repeated exports observe the cumulative value; they must not add it again.
        for (increment, expected) in [(0, 0), (3, 3), (0, 3), (2, 5)] {
            errors.timeout.inc_by(increment);
            provider.force_flush().unwrap();
            let finished = exporter.get_finished_metrics().unwrap();
            let exported = finished.last().unwrap();
            let metrics: Vec<_> = exported
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .collect();
            assert_eq!(metrics.len(), 1);
            assert_eq!(metrics[0].name(), ERRORS.name);
            let AggregatedMetrics::U64(MetricData::Sum(sum)) = metrics[0].data() else {
                panic!("expected an unsigned counter sum");
            };
            assert!(sum.is_monotonic());
            assert_eq!(sum.temporality(), Temporality::Cumulative);
            let mut samples: Vec<_> = sum
                .data_points()
                .map(|point| {
                    let attributes: Vec<_> = point.attributes().collect();
                    assert_eq!(attributes.len(), 1);
                    assert_eq!(attributes[0].key.as_str(), "reason");
                    (attributes[0].value.to_string(), point.value())
                })
                .collect();
            samples.sort();
            assert_eq!(
                samples,
                [("io".to_owned(), 0), ("timeout".to_owned(), expected)]
            );
        }
        assert_eq!(errors.io.get(), 0);
        provider.shutdown().unwrap();
    }
}
