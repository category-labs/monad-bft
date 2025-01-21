use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use eyre::Result;
use opentelemetry::metrics::{Counter, Gauge, Meter, MeterProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;

#[derive(Clone)]
pub struct Metrics(Option<MetricsInner>);

#[derive(Clone)]
pub struct MetricsInner {
    pub gauges: Arc<DashMap<&'static str, Gauge<u64>>>,
    pub counters: Arc<DashMap<&'static str, Counter<u64>>>,
    pub provider: SdkMeterProvider,
    pub meter: Meter,
}

impl Metrics {
    pub fn new(
        otel_endpoint: Option<impl AsRef<str>>,
        service_name: impl Into<String>,
        replica_name: impl Into<String>,
        interval: Duration,
    ) -> Result<Metrics> {
        let provider = build_otel_meter_provider(
            otel_endpoint,
            service_name.into(),
            replica_name.into(),
            interval,
        )?;
        let meter = provider.meter("opentelemetry");

        Ok(Metrics(Some(MetricsInner {
            counters: Arc::new(DashMap::with_capacity(100)),
            gauges: Arc::new(DashMap::with_capacity(100)),
            provider,
            meter,
        })))
    }

    pub fn none() -> Metrics {
        Metrics(None)
    }

    pub fn inc_counter(&self, metric: &'static str) {
        self.counter(metric, 1)
    }

    pub fn counter(&self, metric: &'static str, val: u64) {
        if let Some(inner) = &self.0 {
            let counter = inner
                .counters
                .entry(metric)
                .or_insert_with(|| inner.meter.u64_counter(metric).init());

            counter.add(val, &[])
        }
    }

    pub fn gauge(&self, metric: &'static str, val: u64) {
        if let Some(inner) = &self.0 {
            let gauge = inner
                .gauges
                .entry(metric)
                .or_insert_with(|| inner.meter.u64_gauge(metric).init());
            gauge.record(val, &[]);
        }
    }
}

fn build_otel_meter_provider(
    otel_endpoint: Option<impl AsRef<str>>,
    service_name: String,
    replica_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider> {
    let mut provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                format!("{replica_name}-{service_name}"),
            ),
        ]));

    if let Some(otel_endpoint) = otel_endpoint {
        let exporter = opentelemetry_otlp::MetricsExporterBuilder::Tonic(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(otel_endpoint.as_ref()),
        )
        .build_metrics_exporter(
            Box::<opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector>::default(),
            Box::<opentelemetry_sdk::metrics::reader::DefaultAggregationSelector>::default(),
        )?;

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
            exporter,
            opentelemetry_sdk::runtime::Tokio,
        )
        .with_interval(interval / 2)
        .with_timeout(interval * 2)
        .build();

        provider_builder = provider_builder.with_reader(reader)
    }

    Ok(provider_builder.build())
}
