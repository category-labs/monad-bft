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

use actix_server::Server;
use actix_web::{
    App, HttpRequest, HttpResponse, HttpServer,
    http::header::{self, HeaderValue},
    web::{self, ServiceConfig},
};
use prometheus::{Encoder, ProtobufEncoder, Registry, TextEncoder};
use std::sync::Arc;

mod heap;

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

    pub fn empty() -> Self {
        Self::new(Registry::new(), None)
    }
}

#[derive(Clone)]
pub struct PprofServerConfig {
    pub metrics: Option<MetricsServerState>,
    pub enable_heap_profiling: bool,
}

impl Default for PprofServerConfig {
    fn default() -> Self {
        Self {
            metrics: Some(MetricsServerState::empty()),
            enable_heap_profiling: true,
        }
    }
}

fn wants_protobuf(request: &HttpRequest) -> bool {
    request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("application/vnd.google.protobuf"))
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
        encoder.format_type().to_owned()
    } else {
        let encoder = TextEncoder::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return HttpResponse::InternalServerError().finish();
        }
        encoder.format_type().to_owned()
    };

    HttpResponse::Ok()
        .insert_header((
            header::CONTENT_TYPE,
            HeaderValue::from_str(&content_type).unwrap_or_else(|_| {
                HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8")
            }),
        ))
        .body(buffer)
}

fn configure_pprof_routes(cfg: &mut ServiceConfig, server_config: &PprofServerConfig) {
    if let Some(metrics) = &server_config.metrics {
        cfg.app_data(web::Data::new(metrics.clone()));
        cfg.route("/metrics", web::get().to(handle_metrics));
    }

    if server_config.enable_heap_profiling {
        cfg.route("/debug/pprof/heap", web::get().to(heap::handle_get_heap));
        cfg.route(
            "/debug/pprof/heap/config",
            web::post().to(heap::handle_update_prof_config),
        );
    }
}

pub fn start_pprof_server(addr: String) -> std::io::Result<Server> {
    start_pprof_server_with_config(addr, PprofServerConfig::default())
}

pub fn start_pprof_server_with_metrics(
    addr: String,
    metrics: MetricsServerState,
) -> std::io::Result<Server> {
    start_pprof_server_with_config(
        addr,
        PprofServerConfig {
            metrics: Some(metrics),
            enable_heap_profiling: true,
        },
    )
}

pub fn start_pprof_server_with_config(
    addr: String,
    server_config: PprofServerConfig,
) -> std::io::Result<Server> {
    Ok(HttpServer::new(move || {
        let server_config = server_config.clone();
        App::new().configure(move |cfg| configure_pprof_routes(cfg, &server_config))
    })
    .bind(addr)?
    .workers(1)
    .run())
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test};

    use super::{MetricsServerState, PprofServerConfig, configure_pprof_routes};

    #[actix_web::test]
    async fn metrics_route_can_be_disabled() {
        let app = test::init_service(App::new().configure(|cfg| {
            configure_pprof_routes(
                cfg,
                &PprofServerConfig {
                    metrics: None,
                    enable_heap_profiling: true,
                },
            )
        }))
        .await;

        let request = test::TestRequest::get().uri("/metrics").to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn heap_routes_can_be_disabled() {
        let app = test::init_service(App::new().configure(|cfg| {
            configure_pprof_routes(
                cfg,
                &PprofServerConfig {
                    metrics: Some(MetricsServerState::empty()),
                    enable_heap_profiling: false,
                },
            )
        }))
        .await;

        let request = test::TestRequest::get()
            .uri("/debug/pprof/heap")
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn metrics_route_remains_available_when_enabled() {
        let app = test::init_service(App::new().configure(|cfg| {
            configure_pprof_routes(
                cfg,
                &PprofServerConfig {
                    metrics: Some(MetricsServerState::empty()),
                    enable_heap_profiling: false,
                },
            )
        }))
        .await;

        let request = test::TestRequest::get().uri("/metrics").to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::OK);
    }
}
