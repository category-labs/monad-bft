use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use eyre::Result;
use opentelemetry::{
    metrics::{Counter, Gauge, Meter, MeterProvider},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};
use tracing::trace;

#[derive(Clone)]
pub struct Metrics(Option<Arc<MetricsInner>>);

#[derive(Clone)]
pub struct MetricsInner {
    pub gauges: Arc<DashMap<&'static str, Gauge<u64>>>,
    pub periodic_gauges: Arc<DashMap<(&'static str, Vec<KeyValue>), u64>>,
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

        let metrics = Metrics(Some(Arc::new(MetricsInner {
            counters: Arc::new(DashMap::with_capacity(100)),
            gauges: Arc::new(DashMap::with_capacity(100)),
            provider,
            meter,
            periodic_gauges: Arc::new(DashMap::with_capacity(100)),
        })));

        {
            let metrics = metrics.clone();
            // Background worker to prevent no data for sparsely published gauges
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval / 5).await;

                    let inner = metrics.0.as_ref().unwrap();
                    for map_ref in inner.periodic_gauges.iter() {
                        let (metric, attributes) = map_ref.key();
                        let value = map_ref.value();
                        metrics.gauge_with_attrs(metric, *value, attributes);
                    }

                    trace!(
                        num_periodic_gauges = inner.periodic_gauges.len(),
                        "Published periodic gauges"
                    );
                }
            });
        }

        Ok(metrics)
    }

    pub fn none() -> Metrics {
        Metrics(None)
    }

    pub fn inc_counter(&self, metric: &'static str) {
        self.counter(metric, 1)
    }

    pub fn counter_with_attrs(&self, metric: &'static str, val: u64, attributes: &[KeyValue]) {
        if let Some(inner) = &self.0 {
            let counter = inner
                .counters
                .entry(metric)
                .or_insert_with(|| inner.meter.u64_counter(metric).build());

            counter.add(val, attributes)
        }
    }

    pub fn counter(&self, metric: &'static str, val: u64) {
        self.counter_with_attrs(metric, val, &[]);
    }

    pub fn periodic_gauge_with_attrs(
        &self,
        metric: &'static str,
        value: u64,
        attributes: Vec<KeyValue>,
    ) {
        self.gauge_with_attrs(metric, value, &attributes);
        if let Some(inner) = &self.0 {
            inner.periodic_gauges.insert((metric, attributes), value);
        }
    }

    pub fn gauge_with_attrs(&self, metric: &'static str, value: u64, attributes: &[KeyValue]) {
        if let Some(inner) = &self.0 {
            let gauge = inner
                .gauges
                .entry(metric)
                .or_insert_with(|| inner.meter.u64_gauge(metric).build());
            gauge.record(value, attributes);
        }
    }

    pub fn gauge(&self, metric: &'static str, value: u64) {
        self.gauge_with_attrs(metric, value, &[]);
    }
}

fn build_otel_meter_provider(
    otel_endpoint: Option<impl AsRef<str>>,
    service_name: String,
    replica_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider> {
    let mut provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_attributes(vec![opentelemetry::KeyValue::new(
                    opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                    format!("{replica_name}-{service_name}"),
                )])
                .build(),
        );

    if let Some(otel_endpoint) = otel_endpoint {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_temporality(Temporality::default())
            .with_timeout(interval * 2)
            .with_endpoint(otel_endpoint.as_ref())
            .build()?;

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
            .with_interval(interval / 2)
            .build();

        provider_builder = provider_builder.with_reader(reader)
    }

    Ok(provider_builder.build())
}
