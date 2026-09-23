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

use std::{collections::HashMap, time::Duration};

use actix_server::Server;
use actix_web::{
    body::MessageBody,
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    http::header,
    web, App, HttpRequest, HttpResponse, HttpServer,
};
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use opentelemetry::metrics::{Gauge, Histogram, Meter, MeterProvider};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, HistogramVec, IntGauge, ProtobufEncoder, Registry, TextEncoder};
use tracing::info;

pub struct MetricsMiddleware<S> {
    service: S,
    inner: std::sync::Arc<Metrics>,
}

impl<S, B> Service<ServiceRequest> for MetricsMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let timer = std::time::Instant::now();

        self.inner.active_requests.inc();

        let request_metrics = self.inner.clone();
        Box::pin(self.service.call(req).map(move |res| {
            request_metrics.active_requests.dec();
            if let Ok(res) = &res {
                let elapsed = timer.elapsed();
                let status = res.status().as_u16().to_string();
                request_metrics
                    .request_duration
                    .with_label_values(&[&status])
                    .observe(elapsed.as_secs_f64());
            }
            res
        }))
    }
}

impl<S, B> Transform<S, ServiceRequest> for Metrics
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = MetricsMiddleware<S>;
    type Future = futures_util::future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        futures_util::future::ready(Ok(MetricsMiddleware {
            service,
            inner: std::sync::Arc::new(self.clone()),
        }))
    }
}

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,

    request_duration: HistogramVec,
    active_requests: IntGauge,
    active_websocket_connections: IntGauge,
    active_websocket_topics: IntGauge,
    pub(crate) execution_histogram: Histogram<f64>,
    _otel_provider: std::sync::Arc<SdkMeterProvider>,
}

impl Metrics {
    /// Create metrics backed by a Prometheus registry.
    ///
    /// Constant labels (`service_name`) are attached to every metric,
    /// matching the `service.name` resource attribute that OTEL previously
    /// provided.
    ///
    /// The `execution_histogram` is still an OpenTelemetry histogram because it is
    /// consumed by `monad-tracing-timing`'s `TimingsLayer`; it records internally
    /// and is not exposed on the Prometheus scrape endpoint.
    pub fn new(_service_name: String, registry: Registry) -> Self {
        const LOW_US_TO_S: &[f64] = &[
            0.000_001, 0.000_002, 0.000_005, 0.000_01, 0.000_02, 0.000_05, 0.000_1, 0.000_2,
            0.000_5, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0,
        ];

        let request_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "monad_rpc_request_duration",
                "Duration of inbound http requests in seconds",
            )
            .buckets(LOW_US_TO_S.to_vec()),
            &["status_code"],
        )
        .expect("valid histogram opts");

        let active_requests = IntGauge::new(
            "monad_rpc_active_requests",
            "Number of concurrent http requests that are in-flight",
        )
        .expect("valid gauge opts");

        let active_websocket_connections = IntGauge::new(
            "monad_ws_active_connections",
            "Number of active websocket connections",
        )
        .expect("valid gauge opts");

        let active_websocket_topics = IntGauge::new(
            "monad_ws_active_topics",
            "Number of active websocket topics",
        )
        .expect("valid gauge opts");

        registry
            .register(Box::new(request_duration.clone()))
            .expect("request_duration registered");
        registry
            .register(Box::new(active_requests.clone()))
            .expect("active_requests registered");
        registry
            .register(Box::new(active_websocket_connections.clone()))
            .expect("active_websocket_connections registered");
        registry
            .register(Box::new(active_websocket_topics.clone()))
            .expect("active_websocket_topics registered");

        // Build a no-export OTEL provider for the execution histogram; the provider
        // must be kept alive (_otel_provider) or the histogram becomes a no-op.
        let otel_provider = SdkMeterProvider::builder().build();
        let meter = otel_provider.meter("monad-rpc");
        let execution_histogram = meter
            .f64_histogram("monad.rpc.execution_duration")
            .with_description("duration of the rpc method execution")
            .with_unit("s")
            .with_boundaries(LOW_US_TO_S.to_vec())
            .build();

        Self {
            registry,
            request_duration,
            active_requests,
            active_websocket_connections,
            active_websocket_topics,
            execution_histogram,
            _otel_provider: std::sync::Arc::new(otel_provider),
        }
    }

    pub fn record_websocket_connection(&self, increment: i64) {
        if increment > 0 {
            self.active_websocket_connections.inc();
        } else if increment < 0 {
            self.active_websocket_connections.dec();
        }
    }

    pub fn record_websocket_topic(&self, increment: i64) {
        if increment > 0 {
            self.active_websocket_topics.add(increment);
        } else if increment < 0 {
            self.active_websocket_topics.sub(-increment);
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
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

pub fn default_prometheus_labels(service_name: String) -> HashMap<String, String> {
    HashMap::from([("service_name".to_owned(), service_name)])
}

// --- OTLP gRPC push export ---

/// Build an OpenTelemetry `SdkMeterProvider` that pushes metrics to an OTLP
/// gRPC endpoint (e.g. an OTel collector at `http://127.0.0.1:4317`).
///
/// The provider attaches `service.name`, `network`, and (if available)
/// `service.version` resource attributes, mirroring monad-node's pattern.
pub fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    network_name: String,
    version: Option<&str>,
    interval: Duration,
) -> Result<SdkMeterProvider, opentelemetry_otlp::ExporterBuildError> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_timeout(interval * 2)
        .with_endpoint(otel_endpoint)
        .build()?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval / 2)
        .build();

    let mut attrs = vec![
        opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            service_name,
        ),
        opentelemetry::KeyValue::new("network", network_name),
    ];
    if let Some(v) = version {
        attrs.push(opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
            v.to_owned(),
        ));
    }

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

/// Read all metric families from a Prometheus `Registry` and record their
/// current values into OTel gauge instruments.  This bridges the pull-based
/// Prometheus metrics into the push-based OTLP pipeline.
fn send_metrics(meter: &Meter, gauge_cache: &mut HashMap<String, Gauge<f64>>, registry: &Registry) {
    let metric_families = registry.gather();

    for mf in &metric_families {
        let name = mf.name().to_owned();
        let gauge = gauge_cache.entry(name.clone()).or_insert_with(|| {
            let help = mf.help().to_owned();
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

        for m in mf.get_metric() {
            let label_attrs: Vec<opentelemetry::KeyValue> = m
                .get_label()
                .iter()
                .map(|lp| opentelemetry::KeyValue::new(lp.name().to_owned(), lp.value().to_owned()))
                .collect();

            let value = if mf.get_field_type() == prometheus::proto::MetricType::GAUGE {
                m.get_gauge().value()
            } else if mf.get_field_type() == prometheus::proto::MetricType::COUNTER {
                m.get_counter().value()
            } else {
                m.get_histogram().get_sample_sum()
            };

            gauge.record(value, &label_attrs);
        }
    }
}

/// Spawn a tokio task that periodically reads from the Prometheus `Registry`
/// and forwards the values to the OTLP meter provider.
///
/// Returns a `JoinHandle` for the spawned task.
pub fn start_otel_forwarder(
    registry: Registry,
    provider: SdkMeterProvider,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let meter = provider.meter("monad-rpc");
        let mut gauge_cache = HashMap::new();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!("OTLP metrics forwarder started");

        loop {
            ticker.tick().await;
            send_metrics(&meter, &mut gauge_cache, &registry);
        }
    })
}
