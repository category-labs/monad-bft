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

use std::sync::Arc;

use actix_server::Server;
use actix_web::{http::header, web, App, HttpRequest, HttpResponse, HttpServer};
use prometheus::{Encoder, ProtobufEncoder, Registry, TextEncoder};

#[derive(Clone)]
pub struct MetricsServerState {
    registry: Registry,
    before_gather: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl MetricsServerState {
    pub fn new(registry: Registry, before_gather: Option<Arc<dyn Fn() + Send + Sync>>) -> Self {
        Self {
            registry,
            before_gather,
        }
    }
}

fn wants_protobuf(request: &HttpRequest) -> bool {
    // Prometheus negotiates scrape response format with the request Accept header:
    // https://prometheus.io/docs/instrumenting/content_negotiation/
    request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains(prometheus::PROTOBUF_FORMAT))
}

async fn handle_metrics(
    request: HttpRequest,
    state: web::Data<MetricsServerState>,
) -> HttpResponse {
    if let Some(before_gather) = &state.before_gather {
        before_gather();
    }

    let metric_families = state.registry.gather();
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
mod tests {
    use actix_web::{
        http::{header, StatusCode},
        test, web, App,
    };
    use prometheus::{Gauge, Opts, Registry};

    use super::{handle_metrics, MetricsServerState};

    #[actix_web::test]
    async fn metrics_route_returns_text_by_default() {
        let registry = Registry::new();
        let gauge = Gauge::with_opts(Opts::new("test_metric", "Test metric")).unwrap();
        gauge.set(7.0);
        registry.register(Box::new(gauge)).unwrap();
        let state = MetricsServerState::new(registry, None);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/metrics", web::get().to(handle_metrics)),
        )
        .await;

        let request = test::TestRequest::get().uri("/metrics").to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(prometheus::TEXT_FORMAT)
        );

        let body = test::read_body(response).await;
        let body = std::str::from_utf8(&body).expect("text response");
        assert!(body.contains("test_metric 7"));
    }

    #[actix_web::test]
    async fn metrics_route_returns_protobuf_when_accepted() {
        let registry = Registry::new();
        let gauge = Gauge::with_opts(Opts::new("test_metric", "Test metric")).unwrap();
        gauge.set(9.0);
        registry.register(Box::new(gauge)).unwrap();
        let state = MetricsServerState::new(registry, None);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/metrics", web::get().to(handle_metrics)),
        )
        .await;

        let request = test::TestRequest::get()
            .uri("/metrics")
            .insert_header((header::ACCEPT, prometheus::PROTOBUF_FORMAT))
            .to_request();

        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(prometheus::PROTOBUF_FORMAT)
        );
        assert!(!test::read_body(response).await.is_empty());
    }

    #[actix_web::test]
    async fn metrics_route_ignores_request_content_type_for_negotiation() {
        let state = MetricsServerState::new(Registry::new(), None);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/metrics", web::get().to(handle_metrics)),
        )
        .await;

        let request = test::TestRequest::get()
            .uri("/metrics")
            .insert_header((header::CONTENT_TYPE, prometheus::PROTOBUF_FORMAT))
            .to_request();

        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(prometheus::TEXT_FORMAT)
        );
    }
}
