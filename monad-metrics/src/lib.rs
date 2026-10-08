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

use std::{io, marker::PhantomData, net::SocketAddrV4, time::Duration};

use actix_web::{dev::Server, web, App, HttpResponse, HttpServer};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    Resource,
};
use prometheus::{Encoder, Registry, TextEncoder};

/// defaults supplied by the service using the shared metrics configuration.
pub trait MetricsDefaults {
    const LISTEN_ADDR: &'static str;
}

/// shared export options, optionally accepted after subcommands.
#[derive(Clone, Debug, clap::Args, serde::Deserialize)]
#[serde(bound = "")]
pub struct MetricsConfig<D: MetricsDefaults, const GLOBAL: bool = false> {
    #[arg(long, global = GLOBAL, help = "push metrics to this otlp endpoint")]
    pub otel_endpoint: Option<String>,

    #[arg(
        long,
        global = GLOBAL,
        default_value = D::LISTEN_ADDR,
        help = "serve prometheus /metrics on this ipv4 address"
    )]
    #[serde(default = "MetricsConfig::<D, GLOBAL>::default_listen_addr")]
    pub metrics_listen_addr: Option<SocketAddrV4>,

    #[arg(skip)]
    #[serde(skip)]
    defaults: PhantomData<D>,
}

impl<D: MetricsDefaults, const GLOBAL: bool> Default for MetricsConfig<D, GLOBAL> {
    fn default() -> Self {
        Self {
            otel_endpoint: None,
            metrics_listen_addr: Self::default_listen_addr(),
            defaults: PhantomData,
        }
    }
}

impl<D: MetricsDefaults, const GLOBAL: bool> MetricsConfig<D, GLOBAL> {
    fn default_listen_addr() -> Option<SocketAddrV4> {
        Some(
            D::LISTEN_ADDR
                .parse()
                .expect("invalid default metrics listen address"),
        )
    }

    pub fn enabled(&self) -> bool {
        self.otel_endpoint.is_some() || self.metrics_listen_addr.is_some()
    }

    /// applies only explicitly supplied values, preserving config-file defaults.
    pub fn apply_overrides(&mut self, overrides: Self) {
        if let Some(endpoint) = overrides.otel_endpoint {
            self.otel_endpoint = Some(endpoint);
        }
        if let Some(addr) = overrides.metrics_listen_addr {
            self.metrics_listen_addr = Some(addr);
        }
    }

    /// initializes the provider and optional scrape server; the caller drives the server.
    pub fn init(
        &self,
        service_name: String,
        interval: Duration,
        disable_signals: bool,
    ) -> io::Result<(SdkMeterProvider, Option<Server>)> {
        let mut listen_addr = None;
        let (registry, server) = if let Some(addr) = self.metrics_listen_addr {
            let registry = Registry::new();
            let registry_data = web::Data::new(registry.clone());
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(registry_data.clone())
                    .route("/metrics", web::get().to(prometheus_metrics))
            })
            .bind(addr)?
            .workers(1);
            let server = if disable_signals {
                server.disable_signals()
            } else {
                server
            };
            listen_addr = server.addrs().first().copied();
            (Some(registry), Some(server.run()))
        } else {
            (None, None)
        };

        let mut provider = SdkMeterProvider::builder().with_resource(
            Resource::builder_empty()
                .with_attributes([KeyValue::new("service.name", service_name.clone())])
                .build(),
        );

        if let Some(endpoint) = &self.otel_endpoint {
            let exporter = MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(interval * 2)
                .build()
                .map_err(io::Error::other)?;
            let reader = PeriodicReader::builder(exporter)
                .with_interval(interval / 2)
                .build();
            provider = provider.with_reader(reader);
        }

        if let Some(registry) = registry {
            let reader = opentelemetry_prometheus::exporter()
                .with_registry(registry)
                .build()
                .map_err(io::Error::other)?;
            provider = provider.with_reader(reader);
        }
        let provider = provider.build();

        if self.enabled() {
            tracing::info!(
                %service_name,
                otlp_enabled = self.otel_endpoint.is_some(),
                prometheus_enabled = listen_addr.is_some(),
                ?listen_addr,
                "metrics configured"
            );
        }

        Ok((provider, server))
    }
}

/// serves metrics in the background, logging server failures.
pub fn spawn_metrics_server(server: Option<Server>) {
    if let Some(server) = server {
        tokio::spawn(async move {
            if let Err(err) = server.await {
                tracing::error!(?err, "metrics server exited");
            }
        });
    }
}

async fn prometheus_metrics(registry: web::Data<Registry>) -> actix_web::Result<HttpResponse> {
    // todo: support protobuf with proper accept-header negotiation.
    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    encoder
        .encode(&registry.gather(), &mut body)
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok()
        .content_type(encoder.format_type())
        .body(body))
}
