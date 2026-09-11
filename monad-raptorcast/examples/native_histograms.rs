// Copyright (C) 2026 Category Labs, Inc.
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

//! Local demonstration: synthetic latencies recorded through UdpStateMetrics.
//! This endpoint always serves Prometheus protobuf, including native buckets.
use std::{
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use actix_web::{web, App, HttpResponse, HttpServer};
use monad_executor::{ExecutorMetricsChain, NativeHistogramRegistry};
use monad_raptorcast::{metrics::UdpStateMetrics, util::BroadcastMode};
use prometheus::{Encoder, ProtobufEncoder, Registry};

struct Demo {
    metrics: Mutex<UdpStateMetrics>,
    scalars: Registry,
    histograms: NativeHistogramRegistry,
}

async fn scrape(demo: web::Data<Demo>) -> HttpResponse {
    // A fixed workload per scrape makes count/rate and quantiles easy to inspect.
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    {
        let mut metrics = demo.metrics.lock().unwrap();
        for delay_ms in [1, 2, 5, 10, 20, 50] {
            metrics.record_broadcast_latency(BroadcastMode::Primary, now_ms - delay_ms);
            metrics.record_broadcast_latency(BroadcastMode::Secondary, now_ms - delay_ms * 2);
        }
    }
    let mut buffer = Vec::new();
    ProtobufEncoder::new()
        .encode(&demo.scalars.gather(), &mut buffer)
        .unwrap();
    buffer.extend(demo.histograms.encode_protobuf().unwrap());
    HttpResponse::Ok()
        .content_type(prometheus::PROTOBUF_FORMAT)
        .body(buffer)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:19100".into());
    let metrics = UdpStateMetrics::new();
    let scalars = Registry::new();
    metrics.executor_metrics().register(&scalars).unwrap();
    let histograms = ExecutorMetricsChain::from(metrics.executor_metrics())
        .native_histogram_registry(Default::default(), &scalars)
        .unwrap();
    let demo = web::Data::new(Demo {
        metrics: Mutex::new(metrics),
        scalars,
        histograms,
    });
    println!("Synthetic raptorcast latency metrics: http://{address}/metrics");
    HttpServer::new(move || {
        App::new()
            .app_data(demo.clone())
            .route("/metrics", web::get().to(scrape))
    })
    .bind(address)?
    .workers(1)
    .run()
    .await
}
