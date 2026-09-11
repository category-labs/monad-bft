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
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use actix_server::Server;
use actix_web::{http::header, web, App, HttpRequest, HttpResponse, HttpServer};
use monad_consensus_types::metrics::Metrics as StateMetrics;
use monad_executor::{
    metric_consts, ExecutorMetrics, ExecutorMetricsChain, Gauge, NativeHistogramRegistry,
};
use monad_triedb_utils::{MigrationPhase, StorageStats};
use prometheus::{Encoder, ProtobufEncoder, Registry, TextEncoder};

mod otel;
pub use otel::register_otel_counters;

pub fn default_prometheus_labels(
    service_name: String,
    network_name: String,
    version: Option<&str>,
) -> HashMap<String, String> {
    let mut labels = HashMap::from([
        ("service_name".to_owned(), service_name),
        ("network".to_owned(), network_name),
    ]);
    if let Some(version) = version {
        labels.insert("service_version".to_owned(), version.to_owned());
    }
    labels
}

metric_consts! {
    pub GAUGE_TOTAL_UPTIME_US {
        name: "monad.total_uptime_us",
        help: "Total node uptime in microseconds",
    }
    pub GAUGE_STATE_TOTAL_UPDATE_US {
        name: "monad.state.total_update_us",
        help: "Total time spent updating state in microseconds",
    }
    // Keep this already sanitized so Prometheus and OTel export the same info metric name.
    pub GAUGE_NODE_INFO {
        name: "monad_node_info",
        help: "Node info indicator (always 1)",
    }
}

metric_consts! {
    pub GAUGE_TRIEDB_MIGRATION_PHASE {
        name: "monad.triedb.migration_phase",
        help: "Dual-DB migration phase: 0=legacy (not started), 1=dual-timeline (migrating), 2=page-encoded (complete), 3=promoted (primary is page-encoded)",
    }
}

fn init_node_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_TOTAL_UPTIME_US,
        GAUGE_STATE_TOTAL_UPDATE_US,
        GAUGE_NODE_INFO,
    ])
}

pub fn init_triedb_phase_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[GAUGE_TRIEDB_MIGRATION_PHASE])
}

pub fn record_triedb_phase_metrics(metrics: &mut ExecutorMetrics, phase: MigrationPhase) {
    // Map to the published 0/1/2/3 codes explicitly so the metric's wire
    // contract stays fixed even if the MigrationPhase discriminants change
    // upstream (the enum is defined in monad-triedb).
    let code: u64 = match phase {
        MigrationPhase::Legacy => 0,
        MigrationPhase::DualTimeline => 1,
        MigrationPhase::PageEncoded => 2,
        MigrationPhase::Promoted => 3,
    };
    metrics.gauge(GAUGE_TRIEDB_MIGRATION_PHASE).set(code);
}

metric_consts! {
    pub GAUGE_TRIEDB_DISK_CAPACITY_BYTES {
        name: "monad.triedb.disk_capacity_bytes",
        help: "Total triedb storage-pool capacity in bytes: sum of file sizes (file pools) or raw device sizes (block-device pools).",
    }
    pub GAUGE_TRIEDB_DISK_USED_BYTES {
        name: "monad.triedb.disk_used_bytes",
        help: "triedb storage-pool bytes in use. Block-device pools sum appended bytes per chunk and under-report pool overhead by a few hundred MiB per device; file pools report filesystem-allocated blocks, a high-water mark that does not shrink as chunks are recycled. Treat the trend as the signal, not the absolute value.",
    }
}

pub fn init_triedb_storage_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_TRIEDB_DISK_CAPACITY_BYTES,
        GAUGE_TRIEDB_DISK_USED_BYTES,
    ])
}

pub fn record_triedb_storage_metrics(metrics: &mut ExecutorMetrics, stats: StorageStats) {
    metrics
        .gauge(GAUGE_TRIEDB_DISK_CAPACITY_BYTES)
        .set(stats.disk_capacity_bytes);
    metrics
        .gauge(GAUGE_TRIEDB_DISK_USED_BYTES)
        .set(stats.disk_used_bytes);
}

fn duration_micros_u64(duration: &Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

pub struct NodePrometheusMetrics {
    registry: Registry,
    native_histograms: Arc<NativeHistogramRegistry>,
    state_metrics: Vec<(&'static str, Gauge, &'static str)>,
    total_uptime: Gauge,
    total_state_update: Gauge,
    node_info: Gauge,
    process_start: Instant,
}

impl NodePrometheusMetrics {
    pub fn new(
        labels: HashMap<String, String>,
        state_metrics: &StateMetrics,
        executor_metrics: ExecutorMetricsChain<'_>,
        process_start: Instant,
    ) -> Result<Self, prometheus::Error> {
        let registry = Registry::new_custom(None, Some(labels.clone()))?;
        let state_metric_handles = state_metrics.metric_handles();
        for (_, gauge, _) in &state_metric_handles {
            registry.register(Box::new(gauge.clone()))?;
        }

        executor_metrics.register(&registry)?;

        let mut node_executor_metrics = init_node_executor_metrics();
        node_executor_metrics.gauge(GAUGE_NODE_INFO).set(1);
        node_executor_metrics.gauge(GAUGE_TOTAL_UPTIME_US).set(0);
        node_executor_metrics
            .gauge(GAUGE_STATE_TOTAL_UPDATE_US)
            .set(0);
        node_executor_metrics.register(&registry)?;
        let native_histograms =
            Arc::new(executor_metrics.native_histogram_registry(labels, &registry)?);

        Ok(Self {
            registry,
            native_histograms,
            state_metrics: state_metric_handles,
            total_uptime: node_executor_metrics.gauge(GAUGE_TOTAL_UPTIME_US).clone(),
            total_state_update: node_executor_metrics
                .gauge(GAUGE_STATE_TOTAL_UPDATE_US)
                .clone(),
            node_info: node_executor_metrics.gauge(GAUGE_NODE_INFO).clone(),
            process_start,
        })
    }

    pub fn native_histograms(&self) -> Arc<NativeHistogramRegistry> {
        Arc::clone(&self.native_histograms)
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn metric_handles(&self) -> Vec<(&'static str, Gauge, &'static str)> {
        self.state_metrics
            .iter()
            .map(|(name, gauge, help)| (*name, gauge.clone(), *help))
            .chain([
                (
                    GAUGE_TOTAL_UPTIME_US.name,
                    self.total_uptime.clone(),
                    GAUGE_TOTAL_UPTIME_US.help,
                ),
                (
                    GAUGE_STATE_TOTAL_UPDATE_US.name,
                    self.total_state_update.clone(),
                    GAUGE_STATE_TOTAL_UPDATE_US.help,
                ),
                (
                    GAUGE_NODE_INFO.name,
                    self.node_info.clone(),
                    GAUGE_NODE_INFO.help,
                ),
            ])
            .collect()
    }

    pub fn record_state_update_elapsed(&self, total_state_update_elapsed: &Duration) {
        self.total_state_update
            .set(duration_micros_u64(total_state_update_elapsed));
    }

    pub fn refresh_dynamic_metrics(&self) {
        self.total_uptime
            .set(duration_micros_u64(&self.process_start.elapsed()));
    }
}

#[derive(Clone)]
pub struct MetricsServerState {
    registry: Registry,
    native_histograms: Option<Arc<NativeHistogramRegistry>>,
    before_gather: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl MetricsServerState {
    pub fn new(registry: Registry, before_gather: Option<Arc<dyn Fn() + Send + Sync>>) -> Self {
        Self {
            registry,
            native_histograms: None,
            before_gather,
        }
    }
    pub fn with_native_histograms(mut self, histograms: Arc<NativeHistogramRegistry>) -> Self {
        self.native_histograms = Some(histograms);
        self
    }
}

fn wants_protobuf(request: &HttpRequest) -> bool {
    // Parse parameters rather than comparing formatting/whitespace in Accept.
    // Prefer protobuf on equal quality so a native-capable scrape gets its buckets.
    let mut best = (0.0_f32, false);
    for value in request.headers().get_all(header::ACCEPT) {
        let Ok(value) = value.to_str() else { continue };
        for format in value.split(',') {
            let mut parts = format.split(';').map(str::trim);
            let media_type = parts.next().unwrap_or_default();
            let (mut proto, mut encoding, mut quality) = ("", "", 1.0_f32);
            for part in parts {
                let Some((key, value)) = part.split_once('=') else {
                    continue;
                };
                let value = value.trim().trim_matches('"');
                match key.trim() {
                    "proto" => proto = value,
                    "encoding" => encoding = value,
                    "q" => {
                        quality = value
                            .parse::<f32>()
                            .ok()
                            .filter(|q| (0.0..=1.0).contains(q))
                            .unwrap_or(0.0)
                    }
                    _ => {}
                }
            }
            let protobuf = media_type == "application/vnd.google.protobuf"
                && proto == "io.prometheus.client.MetricFamily"
                && encoding == "delimited";
            if !protobuf && !matches!(media_type, "text/plain" | "text/*" | "*/*") {
                continue;
            }
            if quality > 0.0 && (quality > best.0 || (quality == best.0 && protobuf)) {
                best = (quality, protobuf);
            }
        }
    }
    best.1
}

async fn handle_metrics(
    request: HttpRequest,
    state: web::Data<MetricsServerState>,
) -> HttpResponse {
    if let Some(before_gather) = &state.before_gather {
        before_gather();
    }

    let mut metric_families = state.registry.gather();
    let mut buffer = Vec::new();

    let content_type = if wants_protobuf(&request) {
        let encoder = ProtobufEncoder::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return HttpResponse::InternalServerError().finish();
        }
        if let Some(histograms) = &state.native_histograms {
            match histograms.encode_protobuf() {
                Ok(native) => buffer.extend(native),
                Err(_) => return HttpResponse::InternalServerError().finish(),
            }
        }
        prometheus::PROTOBUF_FORMAT
    } else {
        if let Some(histograms) = &state.native_histograms {
            match histograms.classic_metric_families() {
                Ok(classic) => metric_families.extend(classic),
                Err(_) => return HttpResponse::InternalServerError().finish(),
            }
        }
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

pub fn start_metrics_server(addr: String, state: MetricsServerState) -> std::io::Result<Server> {
    Ok(HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .route("/metrics", web::get().to(handle_metrics))
    })
    .bind(addr)?
    .workers(1)
    .run())
}

#[cfg(test)]
mod migration_phase_tests {
    use monad_triedb_utils::MigrationPhase;

    use super::{
        init_triedb_phase_metrics, record_triedb_phase_metrics, GAUGE_TRIEDB_MIGRATION_PHASE,
    };

    #[test]
    fn records_phase_code() {
        for (phase, code) in [
            (MigrationPhase::Legacy, 0u64),
            (MigrationPhase::DualTimeline, 1),
            (MigrationPhase::PageEncoded, 2),
            (MigrationPhase::Promoted, 3),
        ] {
            let mut metrics = init_triedb_phase_metrics();
            record_triedb_phase_metrics(&mut metrics, phase);
            assert_eq!(metrics.gauge(GAUGE_TRIEDB_MIGRATION_PHASE).get(), code);
        }
    }
}

#[cfg(test)]
mod storage_metrics_tests {
    use monad_triedb_utils::StorageStats;
    use prometheus::{Encoder, Registry, TextEncoder};

    use super::{
        init_triedb_storage_metrics, record_triedb_storage_metrics,
        GAUGE_TRIEDB_DISK_CAPACITY_BYTES, GAUGE_TRIEDB_DISK_USED_BYTES,
    };

    #[test]
    fn records_capacity_and_used() {
        let mut metrics = init_triedb_storage_metrics();
        record_triedb_storage_metrics(
            &mut metrics,
            StorageStats {
                disk_capacity_bytes: 1_000,
                disk_used_bytes: 600,
            },
        );
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_DISK_CAPACITY_BYTES).get(), 1_000);
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_DISK_USED_BYTES).get(), 600);
    }

    // A refresh reaches the scrape only if it writes the same gauges the
    // registry holds, so assert through the encoded output rather than through
    // the ExecutorMetrics the ticker writes to.
    #[test]
    fn refresh_after_registration_reaches_the_scrape() {
        let mut metrics = init_triedb_storage_metrics();
        let registry = Registry::new();
        metrics.register(&registry).expect("gauges registered");

        for used in [600, 700] {
            record_triedb_storage_metrics(
                &mut metrics,
                StorageStats {
                    disk_capacity_bytes: 1_000,
                    disk_used_bytes: used,
                },
            );
        }

        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .expect("encoded");
        let scraped = String::from_utf8(buffer).expect("utf-8");
        assert!(
            scraped.contains("monad_triedb_disk_capacity_bytes 1000"),
            "{scraped}"
        );
        assert!(
            scraped.contains("monad_triedb_disk_used_bytes 700"),
            "{scraped}"
        );
    }
}

#[cfg(test)]
mod native_histogram_tests {
    use actix_web::{http::StatusCode, test};
    use prometheus_client::encoding::prometheus_protobuf::prometheus_data_model;
    use prost::Message;

    use super::*;

    monad_executor::histogram_labels! {
        struct Latencies { primary => "primary", secondary => "secondary" }
    }
    metric_consts! {
        LATENCY { name: "test.latency_seconds", help: "Test latency" }
        SCALAR { name: "test.scalar", help: "Test scalar" }
    }

    #[actix_web::test]
    async fn endpoint_exports_native_protobuf_and_classic_text_with_global_labels() {
        let mut metrics = ExecutorMetrics::with_metric_defs(&[SCALAR]);
        let latency = Latencies::new(&mut metrics, LATENCY, "mode");
        let node = NodePrometheusMetrics::new(
            HashMap::from([("network".into(), "testnet".into())]),
            &StateMetrics::default(),
            (&metrics).into(),
            Instant::now(),
        )
        .unwrap();
        let state = MetricsServerState::new(node.registry(), None)
            .with_native_histograms(node.native_histograms());
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/metrics", web::get().to(handle_metrics)),
        )
        .await;
        // Record after registration to check that collectors share the live handles.
        latency.primary.observe_duration(Duration::from_millis(12));
        latency.secondary.observe_duration(Duration::from_secs(30));
        metrics.gauge(SCALAR).set(7);
        let request = test::TestRequest::get().uri("/metrics").insert_header((header::ACCEPT,
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.9,text/plain;q=0.5")).to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            prometheus::PROTOBUF_FORMAT
        );
        let body = test::read_body(response).await;
        let mut remaining = body.as_ref();
        let mut families = Vec::new();
        while !remaining.is_empty() {
            families.push(
                prometheus_data_model::MetricFamily::decode_length_delimited(&mut remaining)
                    .unwrap(),
            );
        }
        assert_eq!(
            families
                .iter()
                .filter(|family| family.name == "test_latency_seconds")
                .count(),
            1
        );
        assert_eq!(
            families
                .iter()
                .find(|family| family.name == "test_scalar")
                .unwrap()
                .metric[0]
                .gauge
                .as_ref()
                .unwrap()
                .value,
            7.0
        );
        let family = families
            .iter()
            .find(|family| family.name == "test_latency_seconds")
            .unwrap();
        assert_eq!(family.metric.len(), 2);
        for metric in &family.metric {
            assert!(metric
                .label
                .iter()
                .any(|label| label.name == "network" && label.value == "testnet"));
            let histogram = metric.histogram.as_ref().unwrap();
            assert_eq!(histogram.sample_count, 1);
            assert!(!histogram.positive_span.is_empty());
            assert_eq!(histogram.positive_delta, [1]);
        }
        let response =
            test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            prometheus::TEXT_FORMAT
        );
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(body.contains("# TYPE test_latency_seconds histogram"));
        assert!(body.contains("test_scalar{network=\"testnet\"} 7"));
        let primary_count = body
            .lines()
            .find(|line| {
                line.starts_with("test_latency_seconds_count{") && line.contains("mode=\"primary\"")
            })
            .unwrap();
        assert!(primary_count.contains("network=\"testnet\""));
        assert!(primary_count.ends_with(" 1"));
        assert!(body
            .lines()
            .any(|line| line.starts_with("test_latency_seconds_bucket{")
                && line.contains("le=\"+Inf\"")));
    }

    #[actix_web::test]
    async fn negotiation_respects_quality_and_parameter_formatting() {
        for (accept, expected) in [
            (prometheus::PROTOBUF_FORMAT.to_owned(), true),
            ("application/vnd.google.protobuf;encoding=delimited;proto=\"io.prometheus.client.MetricFamily\"".to_owned(), true),
            (format!("{};q=0,text/plain", prometheus::PROTOBUF_FORMAT), false),
            (format!("{};q=0.5,text/plain;q=0.9", prometheus::PROTOBUF_FORMAT), false),
            ("application/vnd.google.protobuf;proto=openmetrics.MetricSet;encoding=delimited".to_owned(), false),
        ] {
            let request = test::TestRequest::get().insert_header((header::ACCEPT, accept)).to_http_request();
            assert_eq!(wants_protobuf(&request), expected);
        }
    }
}
