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

use std::{sync::Arc, time::Duration};

use clap::error::ErrorKind;
use monad_consensus_types::metrics::Metrics as StateMetrics;
use monad_executor::{ExecutorMetricsChain, Gauge};
use opentelemetry::{
    metrics::{Meter, MeterProvider as _, ObservableGauge},
    KeyValue,
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;

use crate::{
    error::NodeSetupError,
    metrics::{NodePrometheusMetrics, GAUGE_TOTAL_UPTIME_US},
    MONAD_NODE_VERSION,
};

pub struct NodeOtelMetricsExporter {
    _provider: SdkMeterProvider,
    _gauges: Vec<ObservableGauge<u64>>,
}

impl NodeOtelMetricsExporter {
    pub fn new(
        endpoint: &str,
        service_name: String,
        network_name: String,
        export_interval: Duration,
        state_metrics: &StateMetrics,
        executor_metrics: ExecutorMetricsChain<'_>,
        node_metrics: Arc<NodePrometheusMetrics>,
    ) -> Result<Self, NodeSetupError> {
        let provider =
            build_otel_meter_provider(endpoint, service_name, network_name, export_interval)?;
        let meter = provider.meter("opentelemetry");
        let mut gauges = Vec::new();

        for (name, gauge, help) in state_metrics.metric_handles() {
            gauges.push(register_observable_gauge(&meter, name, help, gauge, || {}));
        }

        for (name, gauge, help) in executor_metrics.metric_handles() {
            gauges.push(register_observable_gauge(&meter, name, help, gauge, || {}));
        }

        for (name, gauge, help) in node_metrics.metric_handles() {
            let observable = if name == GAUGE_TOTAL_UPTIME_US.name {
                let node_metrics = Arc::clone(&node_metrics);
                register_observable_gauge(&meter, name, help, gauge, move || {
                    node_metrics.refresh_dynamic_metrics();
                })
            } else {
                register_observable_gauge(&meter, name, help, gauge, || {})
            };
            gauges.push(observable);
        }

        Ok(Self {
            _provider: provider,
            _gauges: gauges,
        })
    }
}

fn register_observable_gauge<F>(
    meter: &Meter,
    name: &'static str,
    help: &'static str,
    gauge: Gauge,
    before_observe: F,
) -> ObservableGauge<u64>
where
    F: Fn() + Send + Sync + 'static,
{
    let mut builder = meter.u64_observable_gauge(name);
    if !help.is_empty() {
        builder = builder.with_description(help);
    }

    builder
        .with_callback(move |observer| {
            before_observe();
            observer.observe(gauge.get(), &[]);
        })
        .build()
}

fn build_otel_meter_provider(
    endpoint: &str,
    service_name: String,
    network_name: String,
    export_interval: Duration,
) -> Result<SdkMeterProvider, NodeSetupError> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_timeout(export_interval * 2)
        .with_endpoint(endpoint)
        .build()
        .map_err(|err| NodeSetupError::Custom {
            kind: ErrorKind::ValueValidation,
            msg: format!("failed to build OTLP metrics exporter: {err}"),
        })?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(export_interval)
        .build();

    let mut attrs = vec![
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            service_name,
        ),
        KeyValue::new("network", network_name),
    ];
    if let Some(version) = MONAD_NODE_VERSION {
        attrs.push(KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
            version,
        ));
    }

    Ok(SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attributes(attrs)
                .build(),
        )
        .build())
}
