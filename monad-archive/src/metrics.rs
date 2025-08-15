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

use dashmap::DashMap;
use eyre::Result;
use opentelemetry::{
    metrics::{Counter, Gauge, Histogram, Meter, MeterProvider},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};
use tracing::trace;

#[derive(Eq, Hash, PartialEq, Clone, Copy)]
#[allow(non_camel_case_types)]
pub enum MetricNames {
    // Store Type
    SINK_STORE_TYPE,
    SOURCE_STORE_TYPE,

    // KV Store
    KV_STORE_PUT_DURATION_MS,
    KV_STORE_PUT_SUCCESS,
    KV_STORE_PUT_FAILURE,
    KV_STORE_GET_DURATION_MS,
    KV_STORE_GET_SUCCESS,
    KV_STORE_GET_FAILURE,

    // Legacy AWS metrics for backwards compatibility
    AWS_S3_READS,
    AWS_S3_WRITES,
    AWS_S3_ERRORS,
    AWS_DYNAMODB_READS,
    AWS_DYNAMODB_WRITES,
    AWS_DYNAMODB_ERRORS,

    // Archive Or Index Workers
    TXS_INDEXED,
    BLOCK_ARCHIVE_WORKER_BLOCK_FALLBACK,
    BLOCK_ARCHIVE_WORKER_RECEIPTS_FALLBACK,
    BLOCK_ARCHIVE_WORKER_TRACES_FALLBACK,
    BLOCK_ARCHIVE_WORKER_TRACES_FAILED,

    SOURCE_LATEST_BLOCK_NUM,
    END_BLOCK_NUMBER,
    START_BLOCK_NUMBER,

    BFT_BLOCKS_UPLOADED,
    BFT_BLOCK_FILES_DISCOVERED,
    BFT_BLOCK_FILES_ALREADY_IN_S3,
    BFT_BLOCK_FILES_UPLOADED,
    BFT_BLOCK_FILES_FAILED_TO_PROCESS,

    // Archive Checker
    LATEST_TO_CHECK,
    NEXT_TO_CHECK,
    REPLICA_FAULTS_FIXED,
    REPLICA_FAULTS_FIX_FAILED,
    REPLICA_FAULTS_FIX_SUCCESS,
    REPLICA_FAULTS_BY_KIND,
    REPLICA_FAULTS_TOTAL,

    // Index Checker
    FAULTS_BLOCKS_WITH_FAULTS,
    FAULTS_ERROR_CHECKING,
    FAULTS_CORRUPTED_BLOCKS,
    FAULTS_MISSING_TXHASH,
    FAULTS_INCORRECT_TX_DATA,
    FAULTS_MISSING_ALL_TXHASH,
}

impl MetricNames {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricNames::SINK_STORE_TYPE => "sink_store_type",
            MetricNames::SOURCE_STORE_TYPE => "source_store_type",
            MetricNames::KV_STORE_PUT_DURATION_MS => "kv_store_put_duration_ms",
            MetricNames::KV_STORE_PUT_SUCCESS => "kv_store_put_success",
            MetricNames::KV_STORE_PUT_FAILURE => "kv_store_put_failure",
            MetricNames::KV_STORE_GET_DURATION_MS => "kv_store_get_duration_ms",
            MetricNames::KV_STORE_GET_SUCCESS => "kv_store_get_success",
            MetricNames::KV_STORE_GET_FAILURE => "kv_store_get_failure",
            MetricNames::AWS_S3_READS => "aws_s3_reads",
            MetricNames::AWS_S3_WRITES => "aws_s3_writes",
            MetricNames::AWS_S3_ERRORS => "aws_s3_errors",
            MetricNames::AWS_DYNAMODB_READS => "aws_dynamodb_reads",
            MetricNames::AWS_DYNAMODB_WRITES => "aws_dynamodb_writes",
            MetricNames::AWS_DYNAMODB_ERRORS => "aws_dynamodb_errors",
            MetricNames::BLOCK_ARCHIVE_WORKER_BLOCK_FALLBACK => {
                "block_archive_worker_block_fallback"
            }
            MetricNames::BLOCK_ARCHIVE_WORKER_RECEIPTS_FALLBACK => {
                "block_archive_worker_receipts_fallback"
            }
            MetricNames::BLOCK_ARCHIVE_WORKER_TRACES_FALLBACK => {
                "block_archive_worker_traces_fallback"
            }
            MetricNames::TXS_INDEXED => "txs_indexed",
            MetricNames::LATEST_TO_CHECK => "latest_to_check",
            MetricNames::NEXT_TO_CHECK => "next_to_check",
            MetricNames::SOURCE_LATEST_BLOCK_NUM => "source_latest_block_num",
            MetricNames::END_BLOCK_NUMBER => "end_block_number",
            MetricNames::START_BLOCK_NUMBER => "start_block_number",
            MetricNames::REPLICA_FAULTS_FIXED => "replica_faults__fixed",
            MetricNames::REPLICA_FAULTS_FIX_FAILED => "replica_faults__fix_failed",
            MetricNames::REPLICA_FAULTS_FIX_SUCCESS => "replica_faults__fix_success",
            MetricNames::BFT_BLOCKS_UPLOADED => "bft_blocks_uploaded",
            MetricNames::REPLICA_FAULTS_BY_KIND => "replica_faults__by_kind",
            MetricNames::REPLICA_FAULTS_TOTAL => "replica_faults__total",
            MetricNames::FAULTS_BLOCKS_WITH_FAULTS => "faults_blocks_with_faults",
            MetricNames::FAULTS_ERROR_CHECKING => "faults_error_checking",
            MetricNames::FAULTS_CORRUPTED_BLOCKS => "faults_corrupted_blocks",
            MetricNames::FAULTS_MISSING_TXHASH => "faults_missing_txhash",
            MetricNames::FAULTS_INCORRECT_TX_DATA => "faults_incorrect_tx_data",
            MetricNames::FAULTS_MISSING_ALL_TXHASH => "faults_missing_all_txhash",
            MetricNames::BFT_BLOCK_FILES_DISCOVERED => "bft_block_files_discovered",
            MetricNames::BFT_BLOCK_FILES_ALREADY_IN_S3 => "bft_block_files_already_in_s3",
            MetricNames::BFT_BLOCK_FILES_UPLOADED => "bft_block_files_uploaded",
            MetricNames::BFT_BLOCK_FILES_FAILED_TO_PROCESS => "bft_block_files_failed_to_process",
            MetricNames::BLOCK_ARCHIVE_WORKER_TRACES_FAILED => "block_archive_worker_traces_failed",
        }
    }
}

#[derive(Clone)]
pub struct Metrics(Option<Arc<MetricsInner>>);

#[derive(Clone)]
pub struct MetricsInner {
    pub gauges: Arc<DashMap<MetricNames, Gauge<u64>>>,
    pub periodic_gauges: Arc<DashMap<(MetricNames, Vec<KeyValue>), u64>>,
    pub counters: Arc<DashMap<MetricNames, Counter<u64>>>,
    pub histograms: Arc<DashMap<MetricNames, Histogram<f64>>>,
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
            counters: Arc::default(),
            gauges: Arc::default(),
            histograms: Arc::default(),
            periodic_gauges: Arc::default(),
            provider,
            meter,
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
                        metrics.gauge_with_attrs(*metric, *value, attributes);
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

    pub fn inc_counter(&self, metric: MetricNames) {
        self.counter(metric, 1)
    }

    pub fn counter_with_attrs(&self, metric: MetricNames, val: u64, attributes: &[KeyValue]) {
        if let Some(inner) = &self.0 {
            let counter = inner
                .counters
                .entry(metric)
                .or_insert_with(|| inner.meter.u64_counter(metric.as_str()).build());

            counter.add(val, attributes)
        }
    }

    pub fn counter(&self, metric: MetricNames, val: u64) {
        self.counter_with_attrs(metric, val, &[]);
    }

    pub fn histogram(&self, metric: MetricNames, value: f64) {
        self.histogram_with_attrs(metric, value, &[]);
    }

    pub fn histogram_with_attrs(&self, metric: MetricNames, value: f64, attributes: &[KeyValue]) {
        if let Some(inner) = &self.0 {
            let histogram = inner
                .histograms
                .entry(metric)
                .or_insert_with(|| inner.meter.f64_histogram(metric.as_str()).build());
            histogram.record(value, attributes);
        }
    }

    pub fn periodic_gauge_with_attrs(
        &self,
        metric: MetricNames,
        value: u64,
        attributes: Vec<KeyValue>,
    ) {
        self.gauge_with_attrs(metric, value, &attributes);
        if let Some(inner) = &self.0 {
            inner.periodic_gauges.insert((metric, attributes), value);
        }
    }

    pub fn gauge_with_attrs(&self, metric: MetricNames, value: u64, attributes: &[KeyValue]) {
        if let Some(inner) = &self.0 {
            let gauge = inner
                .gauges
                .entry(metric)
                .or_insert_with(|| inner.meter.u64_gauge(metric.as_str()).build());
            gauge.record(value, attributes);
        }
    }

    pub fn gauge(&self, metric: MetricNames, value: u64) {
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
            opentelemetry_sdk::Resource::builder_empty()
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
