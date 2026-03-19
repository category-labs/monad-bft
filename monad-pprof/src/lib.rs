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
    web,
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

pub fn start_pprof_server(addr: String) -> std::io::Result<Server> {
    start_pprof_server_with_metrics(addr, MetricsServerState::empty())
}

pub fn start_pprof_server_with_metrics(
    addr: String,
    metrics: MetricsServerState,
) -> std::io::Result<Server> {
    Ok(HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(metrics.clone()))
            .route("/metrics", web::get().to(handle_metrics))
            .route("/debug/pprof/heap", web::get().to(heap::handle_get_heap))
            .route(
                "/debug/pprof/heap/config",
                web::post().to(heap::handle_update_prof_config),
            )
    })
    .bind(addr)?
    .workers(1)
    .run())
}
