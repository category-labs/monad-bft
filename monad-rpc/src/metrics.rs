use std::time::Duration;

use actix_web::{
    body::MessageBody,
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
};
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use opentelemetry::{
    metrics::{Histogram, Meter, UpDownCounter},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;

pub struct MetricsMiddleware<S> {
    service: S,
    inner: std::sync::Arc<RpcMetrics>,
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

        let mut attributes = attributes_from_request(&req);

        self.inner.active_requests.add(1, &attributes);

        let request_metrics = self.inner.clone();
        Box::pin(self.service.call(req).map(move |res| {
            request_metrics.active_requests.add(-1, &attributes);

            if let Ok(res) = res {
                let elapsed = timer.elapsed();

                attributes.push(KeyValue::new(
                    "http.response.status_code",
                    res.status().as_u16() as i64,
                ));

                request_metrics
                    .request_duration
                    .record(elapsed.as_secs_f64(), &attributes);
                Ok(res)
            } else {
                res
            }
        }))
    }
}

impl<S, B> Transform<S, ServiceRequest> for RpcMetrics
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

fn attributes_from_request(req: &ServiceRequest) -> Vec<KeyValue> {
    let conn_info = req.connection_info();

    let mut attributes = Vec::with_capacity(3);

    let mut host_parts = conn_info.host().split_terminator(':');
    if let Some(host) = host_parts.next() {
        attributes.push(KeyValue::new("server.address", host.to_string()));
    }
    if let Some(port) = host_parts.next().and_then(|port| port.parse::<i64>().ok()) {
        attributes.push(KeyValue::new("server.port", port))
    }

    attributes
}

#[derive(Clone)]
pub struct RpcMetrics {
    request_duration: Histogram<f64>,
    active_requests: UpDownCounter<i64>,
}

impl RpcMetrics {
    pub fn new(meter: Meter) -> Self {
        let request_duration = meter
            .f64_histogram("monad.rpc.request_duration")
            .with_description("Duration of inbound http requests")
            .with_unit("s")
            .init();

        let active_requests = meter
            .i64_up_down_counter("monad.rpc.active_requests")
            .with_description("Number of concurrent http requests that are in-flight")
            .init();

        Self {
            request_duration,
            active_requests,
        }
    }
}

pub struct VirtualPoolMetrics {
    pub pending_pool: UpDownCounter<i64>,
    pub queued_pool: UpDownCounter<i64>,
}

impl VirtualPoolMetrics {
    pub fn new(meter: Meter) -> Self {
        let pending_pool = meter
            .i64_up_down_counter("monad.vpool.pending_pool")
            .with_description("Number of transactions in the pending pool")
            .init();

        let queued_pool = meter
            .i64_up_down_counter("monad.vpool.queued_pool")
            .with_description("Number of transactions in the queued pool")
            .init();

        Self {
            pending_pool,
            queued_pool,
        }
    }
}

pub fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, std::io::Error> {
    let exporter = opentelemetry_otlp::MetricsExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_metrics_exporter(
        Box::<opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector>::default(),
        Box::<opentelemetry_sdk::metrics::reader::DefaultAggregationSelector>::default(),
    )
    .unwrap();

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
        exporter,
        opentelemetry_sdk::runtime::Tokio,
    )
    .with_interval(interval / 2)
    .with_timeout(interval * 2)
    .build();

    let provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new("service.name".to_string(), service_name),
        ]));

    Ok(provider_builder.build())
}
