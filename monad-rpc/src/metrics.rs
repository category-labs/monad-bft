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
use opentelemetry_otlp::{MetricExporter, WithExportConfig};

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
pub struct Metrics {
    request_duration: Histogram<f64>,
    active_requests: UpDownCounter<i64>,
    pub(crate) execution_histogram: Histogram<f64>,
}

impl Metrics {
    pub fn new(meter: Meter) -> Self {
        let request_duration = meter
            .f64_histogram("monad.rpc.request_duration")
            .with_description("Duration of inbound http requests")
            .with_unit("s")
            .with_boundaries(LOW_US_TO_S.to_vec())
            .build();

        let active_requests = meter
            .i64_up_down_counter("monad.rpc.active_requests")
            .with_description("Number of concurrent http requests that are in-flight")
            .build();

        let execution_histogram = meter
            .f64_histogram("monad.rpc.execution_duration")
            .with_description("duration of the rpc method execution")
            .with_unit("s")
            .with_boundaries(LOW_US_TO_S.to_vec())
            .build();

        Self {
            request_duration,
            active_requests,
            execution_histogram,
        }
    }
}

pub fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, std::io::Error> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .with_timeout(interval * 2)
        .build()
        .unwrap();

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval / 2)
        .build();

    let provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attributes(vec![opentelemetry::KeyValue::new(
                    "service.name".to_string(),
                    service_name,
                )])
                .build(),
        );

    Ok(provider_builder.build())
}

const LOW_US_TO_S: &[f64] = &[
    0.000_001, 0.000_002, 0.000_005, 0.000_01, 0.000_02, 0.000_05, 0.000_1, 0.000_2, 0.000_5,
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0,
];
