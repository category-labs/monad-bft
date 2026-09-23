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

use std::{collections::HashMap, sync::Arc};

use actix_server::Server;
use actix_web::{http::header, web, App, HttpRequest, HttpResponse, HttpServer};
use dashmap::DashMap;
use eyre::Result;
use opentelemetry::metrics::{Gauge, MeterProvider};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{
    Encoder, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    ProtobufEncoder, Registry, TextEncoder,
};
use tracing::warn;

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

    // Generic Dir Archiver
    GENERIC_ARCHIVE_FILES_DISCOVERED,
    GENERIC_ARCHIVE_FILES_ALREADY_IN_S3,
    GENERIC_ARCHIVE_FILES_UPLOADED,
    GENERIC_ARCHIVE_FILES_FAILED_TO_PROCESS,

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
            MetricNames::GENERIC_ARCHIVE_FILES_DISCOVERED => "generic_archive_files_discovered",
            MetricNames::GENERIC_ARCHIVE_FILES_ALREADY_IN_S3 => {
                "generic_archive_files_already_in_s3"
            }
            MetricNames::GENERIC_ARCHIVE_FILES_UPLOADED => "generic_archive_files_uploaded",
            MetricNames::GENERIC_ARCHIVE_FILES_FAILED_TO_PROCESS => {
                "generic_archive_files_failed_to_process"
            }
        }
    }

    /// Returns the HELP description for Prometheus.
    pub fn description(&self) -> &'static str {
        match self {
            // Store Type
            MetricNames::SINK_STORE_TYPE => "Type of sink store being used",
            MetricNames::SOURCE_STORE_TYPE => "Type of source store being used",
            // KV Store
            MetricNames::KV_STORE_PUT_DURATION_MS => "KV store put operation duration in ms",
            MetricNames::KV_STORE_PUT_SUCCESS => "Successful KV store put operations",
            MetricNames::KV_STORE_PUT_FAILURE => "Failed KV store put operations",
            MetricNames::KV_STORE_GET_DURATION_MS => "KV store get operation duration in ms",
            MetricNames::KV_STORE_GET_SUCCESS => "Successful KV store get operations",
            MetricNames::KV_STORE_GET_FAILURE => "Failed KV store get operations",
            // AWS
            MetricNames::AWS_S3_READS => "AWS S3 read operations",
            MetricNames::AWS_S3_WRITES => "AWS S3 write operations",
            MetricNames::AWS_S3_ERRORS => "AWS S3 errors",
            MetricNames::AWS_DYNAMODB_READS => "AWS DynamoDB read operations",
            MetricNames::AWS_DYNAMODB_WRITES => "AWS DynamoDB write operations",
            MetricNames::AWS_DYNAMODB_ERRORS => "AWS DynamoDB errors",
            // Archive Workers
            MetricNames::TXS_INDEXED => "Transactions indexed",
            MetricNames::BLOCK_ARCHIVE_WORKER_BLOCK_FALLBACK => {
                "Block archive worker block fallbacks"
            }
            MetricNames::BLOCK_ARCHIVE_WORKER_RECEIPTS_FALLBACK => {
                "Block archive worker receipts fallbacks"
            }
            MetricNames::BLOCK_ARCHIVE_WORKER_TRACES_FALLBACK => {
                "Block archive worker traces fallbacks"
            }
            MetricNames::BLOCK_ARCHIVE_WORKER_TRACES_FAILED => {
                "Block archive worker traces failures"
            }
            MetricNames::SOURCE_LATEST_BLOCK_NUM => "Latest block number from source",
            MetricNames::END_BLOCK_NUMBER => "End block number for archival",
            MetricNames::START_BLOCK_NUMBER => "Start block number for archival",
            // BFT Block Files
            MetricNames::BFT_BLOCKS_UPLOADED => "BFT blocks uploaded",
            MetricNames::BFT_BLOCK_FILES_DISCOVERED => "BFT block files discovered",
            MetricNames::BFT_BLOCK_FILES_ALREADY_IN_S3 => "BFT block files already in S3",
            MetricNames::BFT_BLOCK_FILES_UPLOADED => "BFT block files uploaded",
            MetricNames::BFT_BLOCK_FILES_FAILED_TO_PROCESS => "BFT block files failed to process",
            // Generic Archive
            MetricNames::GENERIC_ARCHIVE_FILES_DISCOVERED => "Generic archive files discovered",
            MetricNames::GENERIC_ARCHIVE_FILES_ALREADY_IN_S3 => {
                "Generic archive files already in S3"
            }
            MetricNames::GENERIC_ARCHIVE_FILES_UPLOADED => "Generic archive files uploaded",
            MetricNames::GENERIC_ARCHIVE_FILES_FAILED_TO_PROCESS => {
                "Generic archive files failed to process"
            }
            // Archive Checker
            MetricNames::LATEST_TO_CHECK => "Latest block number to check",
            MetricNames::NEXT_TO_CHECK => "Next block number to check",
            MetricNames::REPLICA_FAULTS_FIXED => "Replica faults fixed",
            MetricNames::REPLICA_FAULTS_FIX_FAILED => "Replica fault fix failures",
            MetricNames::REPLICA_FAULTS_FIX_SUCCESS => "Successful replica fault fixes",
            MetricNames::REPLICA_FAULTS_BY_KIND => "Current replica faults by kind (snapshot)",
            MetricNames::REPLICA_FAULTS_TOTAL => "Current total replica faults (snapshot)",
            // Index Checker
            MetricNames::FAULTS_BLOCKS_WITH_FAULTS => "Blocks with faults",
            MetricNames::FAULTS_ERROR_CHECKING => "Errors during fault checking",
            MetricNames::FAULTS_CORRUPTED_BLOCKS => "Corrupted blocks found",
            MetricNames::FAULTS_MISSING_TXHASH => "Missing transaction hashes",
            MetricNames::FAULTS_INCORRECT_TX_DATA => "Incorrect transaction data entries",
            MetricNames::FAULTS_MISSING_ALL_TXHASH => "Blocks missing all transaction hashes",
        }
    }
}

/// A label key-value pair for metrics with labels.
///
/// This replaces `opentelemetry::KeyValue` in the public interface.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Label {
    pub key: &'static str,
    pub value: String,
}

impl Label {
    pub fn new(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}

#[derive(Clone)]
pub struct Metrics(Option<Arc<MetricsInner>>);

/// Internal state: all prometheus metric objects are lazily created and
/// registered in a `Registry`.
struct MetricsInner {
    registry: Registry,

    /// Plain counters (no labels).
    counters: DashMap<MetricNames, IntCounter>,

    /// Counters with labels.  The label key names are fixed at creation time;
    /// each `counter_with_attrs` call provides only the values.
    counter_vecs: DashMap<MetricNames, IntCounterVec>,

    /// Plain gauges (no labels).
    gauges: DashMap<MetricNames, IntGauge>,

    /// Gauges with labels.
    gauge_vecs: DashMap<MetricNames, IntGaugeVec>,

    /// Plain histograms (no labels).
    histograms: DashMap<MetricNames, Histogram>,

    /// Histograms with labels.
    histogram_vecs: DashMap<MetricNames, HistogramVec>,

    /// OTLP meter provider – kept alive so the PeriodicReader continues
    /// exporting.  The provider is `None` when no `--otel-endpoint` is given.
    _otel_provider: Option<SdkMeterProvider>,
}

impl Clone for MetricsInner {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            counters: self.counters.clone(),
            counter_vecs: self.counter_vecs.clone(),
            gauges: self.gauges.clone(),
            gauge_vecs: self.gauge_vecs.clone(),
            histograms: self.histograms.clone(),
            histogram_vecs: self.histogram_vecs.clone(),
            _otel_provider: self._otel_provider.clone(),
        }
    }
}

impl Metrics {
    /// Create a new `Metrics` instance backed by a Prometheus `Registry`.
    ///
    /// When `metrics_listen_addr` is `Some(addr)`, a `/metrics` HTTP server
    /// is started on that address.  The `service_name` and `replica_name`
    /// are attached as constant labels on every metric (matching the
    /// `service.name` resource attribute that OTEL previously provided).
    ///
    /// When `otel_endpoint` is `Some(url)`, an OTLP gRPC push exporter is
    /// started that periodically forwards all Prometheus metrics to the
    /// OpenTelemetry collector at the given endpoint.
    pub fn new(
        metrics_listen_addr: Option<impl AsRef<str>>,
        service_name: impl Into<String>,
        replica_name: impl Into<String>,
        interval: std::time::Duration,
    ) -> Result<Metrics> {
        Self::new_with_otel(
            metrics_listen_addr,
            service_name,
            replica_name,
            interval,
            None,
            None,
        )
    }

    /// Same as [`new`] but additionally configures OTLP gRPC push export when
    /// `otel_endpoint` is provided.
    pub fn new_with_otel(
        metrics_listen_addr: Option<impl AsRef<str>>,
        service_name: impl Into<String>,
        replica_name: impl Into<String>,
        interval: std::time::Duration,
        otel_endpoint: Option<String>,
        record_metrics_interval: Option<std::time::Duration>,
    ) -> Result<Metrics> {
        let service_name = service_name.into();
        let labels = HashMap::from([
            ("service_name".to_owned(), service_name.clone()),
            ("replica_name".to_owned(), replica_name.into()),
        ]);
        let registry = Registry::new_custom(None, Some(labels))
            .map_err(|e| eyre::eyre!("failed to create prometheus registry: {}", e))?;

        // Build OTLP provider if endpoint is specified.
        let otel_provider = match otel_endpoint {
            Some(endpoint) => {
                let provider = build_otel_meter_provider(&endpoint, service_name.clone(), interval)
                    .map_err(|e| eyre::eyre!("failed to build OTLP meter provider: {}", e))?;
                Some(provider)
            }
            None => None,
        };

        let inner = Arc::new(MetricsInner {
            registry: registry.clone(),
            counters: DashMap::new(),
            counter_vecs: DashMap::new(),
            gauges: DashMap::new(),
            gauge_vecs: DashMap::new(),
            histograms: DashMap::new(),
            histogram_vecs: DashMap::new(),
            _otel_provider: otel_provider.clone(),
        });

        if let Some(addr) = metrics_listen_addr {
            let addr = addr.as_ref().to_owned();
            let registry_for_server = registry.clone();
            tokio::spawn(async move {
                match start_metrics_server(addr.clone(), registry_for_server) {
                    Ok(server) => {
                        tracing::info!(addr = %addr, "Prometheus metrics server started");
                        if let Err(err) = server.await {
                            tracing::error!("metrics server failed: {}", err);
                        }
                    }
                    Err(err) => {
                        tracing::error!("failed to start metrics server: {}", err);
                    }
                }
            });
        }

        // Start OTLP forwarding loop if the provider was created.
        if let Some(provider) = otel_provider {
            let forward_interval =
                record_metrics_interval.unwrap_or_else(|| std::time::Duration::from_secs(5));
            let registry_for_otel = registry;
            tokio::spawn(otel_forwarder_loop(
                provider,
                registry_for_otel,
                service_name,
                forward_interval,
            ));
        }

        Ok(Metrics(Some(inner)))
    }

    pub fn none() -> Metrics {
        Metrics(None)
    }

    pub fn registry(&self) -> Option<&Registry> {
        self.0.as_ref().map(|inner| &inner.registry)
    }

    pub fn inc_counter(&self, metric: MetricNames) {
        self.counter(metric, 1)
    }

    pub fn counter_with_attrs(&self, metric: MetricNames, val: u64, attributes: &[Label]) {
        if let Some(inner) = &self.0 {
            if attributes.is_empty() {
                self.counter(metric, val);
                return;
            }

            let label_names: Vec<&str> = attributes.iter().map(|a| a.key).collect();
            let label_values: Vec<&str> = attributes.iter().map(|a| a.value.as_str()).collect();

            let counter_vec = inner.counter_vecs.entry(metric).or_insert_with(|| {
                let opts = Opts::new(metric.as_str(), metric.description());
                let cv = IntCounterVec::new(opts, &label_names).expect("valid counter vec opts");
                if let Err(e) = inner.registry.register(Box::new(cv.clone())) {
                    warn!(metric = metric.as_str(), err = %e, "failed to register counter vec");
                }
                cv
            });

            if let Err(e) = counter_vec
                .get_metric_with_label_values(&label_values)
                .map(|c| c.inc_by(val))
            {
                warn!(metric = metric.as_str(), err = %e, "label dimension mismatch on counter");
            }
        }
    }

    pub fn counter(&self, metric: MetricNames, val: u64) {
        if let Some(inner) = &self.0 {
            let counter = inner.counters.entry(metric).or_insert_with(|| {
                let c = IntCounter::new(metric.as_str(), metric.description())
                    .expect("valid counter opts");
                if let Err(e) = inner.registry.register(Box::new(c.clone())) {
                    warn!(metric = metric.as_str(), err = %e, "failed to register counter");
                }
                c
            });
            counter.inc_by(val);
        }
    }

    pub fn histogram(&self, metric: MetricNames, value: f64) {
        self.histogram_with_attrs(metric, value, &[]);
    }

    pub fn histogram_with_attrs(&self, metric: MetricNames, value: f64, attributes: &[Label]) {
        if let Some(inner) = &self.0 {
            if attributes.is_empty() {
                let h = inner.histograms.entry(metric).or_insert_with(|| {
                    let h = Histogram::with_opts(prometheus::HistogramOpts::new(
                        metric.as_str(),
                        metric.description(),
                    ))
                    .expect("valid histogram opts");
                    if let Err(e) = inner.registry.register(Box::new(h.clone())) {
                        warn!(metric = metric.as_str(), err = %e, "failed to register histogram");
                    }
                    h
                });
                h.observe(value);
            } else {
                let label_names: Vec<&str> = attributes.iter().map(|a| a.key).collect();
                let label_values: Vec<&str> = attributes.iter().map(|a| a.value.as_str()).collect();

                let hv = inner.histogram_vecs.entry(metric).or_insert_with(|| {
                    let hv = HistogramVec::new(
                        prometheus::HistogramOpts::new(metric.as_str(), metric.description()),
                        &label_names,
                    )
                    .expect("valid histogram vec opts");
                    if let Err(e) = inner.registry.register(Box::new(hv.clone())) {
                        warn!(metric = metric.as_str(), err = %e, "failed to register histogram vec");
                    }
                    hv
                });
                if let Err(e) = hv
                    .get_metric_with_label_values(&label_values)
                    .map(|h| h.observe(value))
                {
                    warn!(metric = metric.as_str(), err = %e, "label dimension mismatch on histogram");
                }
            }
        }
    }

    /// Set a gauge value.  For prometheus gauges, the value persists until
    /// changed, so there is no need for a background republish loop.
    pub fn periodic_gauge_with_attrs(
        &self,
        metric: MetricNames,
        value: u64,
        attributes: Vec<Label>,
    ) {
        self.gauge_with_attrs(metric, value, &attributes);
    }

    pub fn gauge_with_attrs(&self, metric: MetricNames, value: u64, attributes: &[Label]) {
        if let Some(inner) = &self.0 {
            if attributes.is_empty() {
                self.gauge(metric, value);
                return;
            }

            let label_names: Vec<&str> = attributes.iter().map(|a| a.key).collect();
            let label_values: Vec<&str> = attributes.iter().map(|a| a.value.as_str()).collect();

            let gv = inner.gauge_vecs.entry(metric).or_insert_with(|| {
                let gv = IntGaugeVec::new(
                    Opts::new(metric.as_str(), metric.description()),
                    &label_names,
                )
                .expect("valid gauge vec opts");
                if let Err(e) = inner.registry.register(Box::new(gv.clone())) {
                    warn!(metric = metric.as_str(), err = %e, "failed to register gauge vec");
                }
                gv
            });
            if let Err(e) = gv
                .get_metric_with_label_values(&label_values)
                .map(|g| g.set(value as i64))
            {
                warn!(metric = metric.as_str(), err = %e, "label dimension mismatch on gauge");
            }
        }
    }

    pub fn gauge(&self, metric: MetricNames, value: u64) {
        if let Some(inner) = &self.0 {
            let g = inner.gauges.entry(metric).or_insert_with(|| {
                let g =
                    IntGauge::new(metric.as_str(), metric.description()).expect("valid gauge opts");
                if let Err(e) = inner.registry.register(Box::new(g.clone())) {
                    warn!(metric = metric.as_str(), err = %e, "failed to register gauge");
                }
                g
            });
            g.set(value as i64);
        }
    }
}

// --- Prometheus metrics HTTP server ---

fn wants_protobuf(request: &HttpRequest) -> bool {
    request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains(prometheus::PROTOBUF_FORMAT))
}

async fn handle_metrics(request: HttpRequest, state: web::Data<Registry>) -> HttpResponse {
    let metric_families = state.gather();
    let mut buffer = Vec::new();

    let content_type = if wants_protobuf(&request) {
        let encoder = ProtobufEncoder::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return HttpResponse::InternalServerError().finish();
        }
        prometheus::PROTOBUF_FORMAT
    } else {
        let encoder = TextEncoder::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return HttpResponse::InternalServerError().finish();
        }
        prometheus::TEXT_FORMAT
    };

    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, content_type))
        .body(buffer)
}

pub fn start_metrics_server(addr: String, registry: Registry) -> std::io::Result<Server> {
    Ok(HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(registry.clone()))
            .route("/metrics", web::get().to(handle_metrics))
    })
    .bind(addr)?
    .workers(1)
    .run())
}

// --- OTLP gRPC push export ---

/// Build an OpenTelemetry `SdkMeterProvider` that exports metrics via OTLP
/// gRPC to the given endpoint (e.g. `http://127.0.0.1:4317`).
fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    interval: std::time::Duration,
) -> Result<SdkMeterProvider> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_timeout(interval * 2)
        .with_endpoint(otel_endpoint)
        .build()
        .map_err(|e| eyre::eyre!("failed to build OTLP metric exporter: {}", e))?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval / 2)
        .build();

    let attrs = vec![opentelemetry::KeyValue::new(
        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
        service_name,
    )];

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attributes(attrs)
                .build(),
        )
        .build();

    Ok(provider)
}

/// Background task that periodically reads all metric families from the
/// Prometheus `Registry` and records them as OTel gauge instruments so that
/// the `PeriodicReader` inside the `SdkMeterProvider` can export them via
/// OTLP gRPC.
async fn otel_forwarder_loop(
    provider: SdkMeterProvider,
    registry: Registry,
    service_name: String,
    interval: std::time::Duration,
) {
    let meter_name: &'static str = Box::leak(service_name.into_boxed_str());
    let meter = provider.meter(meter_name);
    let mut gauge_cache: HashMap<String, Gauge<f64>> = HashMap::new();

    tracing::info!(
        interval_secs = interval.as_secs(),
        "OTLP forwarding loop started"
    );

    loop {
        tokio::time::sleep(interval).await;

        let metric_families = registry.gather();
        for mf in &metric_families {
            let name = mf.name().to_owned();
            let gauge = gauge_cache.entry(name.clone()).or_insert_with(|| {
                let help = mf.help().to_owned();
                // Leak the name so it can be used as a &'static str for the
                // OTel instrument builder, which requires 'static lifetime.
                let static_name: &'static str = Box::leak(name.clone().into_boxed_str());
                if help.is_empty() {
                    meter.f64_gauge(static_name).build()
                } else {
                    let static_help: &'static str = Box::leak(help.into_boxed_str());
                    meter
                        .f64_gauge(static_name)
                        .with_description(static_help)
                        .build()
                }
            });

            // Each MetricFamily can contain multiple Metric entries (one per
            // label combination).  We record each one with its labels.
            for m in mf.get_metric() {
                let label_attrs: Vec<opentelemetry::KeyValue> = m
                    .get_label()
                    .iter()
                    .map(|lp| {
                        opentelemetry::KeyValue::new(lp.name().to_owned(), lp.value().to_owned())
                    })
                    .collect();

                let value = if mf.get_field_type() == prometheus::proto::MetricType::GAUGE {
                    m.get_gauge().value()
                } else if mf.get_field_type() == prometheus::proto::MetricType::COUNTER {
                    m.get_counter().value()
                } else {
                    // For histograms, forward the sample sum as a proxy.
                    m.get_histogram().get_sample_sum()
                };

                gauge.record(value, &label_attrs);
            }
        }
    }
}
