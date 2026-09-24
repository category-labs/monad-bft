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

use std::{collections::BTreeMap, time::Duration};

use actix_web::{test, web, App};
use monad_tracing_timing::{TimingSpanExtension, TimingsLayer};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        metrics_service_server::{MetricsService, MetricsServiceServer},
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    common::v1::any_value::Value,
    metrics::v1::metric::Data,
};
use prometheus::Registry;
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing_subscriber::layer::SubscriberExt;

use super::{prometheus_metrics, Metrics};

struct Collector(mpsc::UnboundedSender<ExportMetricsServiceRequest>);

#[tonic::async_trait]
impl MetricsService for Collector {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.0.send(request.into_inner()).unwrap();
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[actix_web::test]
async fn real_rpc_histogram_reaches_otlp_and_scrape_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (sender, mut received) = mpsc::unbounded_channel();
    let collector = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(MetricsServiceServer::new(Collector(sender)))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let registry = Registry::new();
    let metrics = Metrics::new_with_otel_endpoint(
        endpoint,
        "dual-export-test".into(),
        Duration::from_millis(200),
        Some(registry.clone()),
    );
    let subscriber = tracing_subscriber::registry().with(TimingsLayer::new());
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..2 {
            let main = tracing::info_span!("rpc_method")
                .with_main_timings(metrics.execution_histogram.clone());
            let _main = main.enter();
            let child = tracing::info_span!("database_read").with_sub_timings();
            let _child = child.enter();
        }
    });

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(registry.clone()))
            .route("/metrics", web::get().to(prometheus_metrics)),
    )
    .await;
    let response =
        test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request()).await;
    assert!(response.status().is_success());
    assert!(response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
    assert!(body.contains("monad_rpc_execution_duration_seconds_bucket"));
    assert!(body.contains("monad_rpc_execution_duration_seconds_sum"));
    assert!(body.contains("monad_rpc_execution_duration_seconds_count"));

    let otlp = timeout(Duration::from_secs(5), async {
        loop {
            let request = received.recv().await.unwrap();
            for resource in request.resource_metrics {
                for scope in resource.scope_metrics {
                    for metric in scope.metrics {
                        if metric.name == "monad.rpc.execution_duration" {
                            return metric;
                        }
                    }
                }
            }
        }
    })
    .await
    .unwrap();
    let Some(Data::Histogram(histogram)) = otlp.data else {
        panic!("execution duration must remain an otlp histogram");
    };
    assert_eq!(histogram.data_points.len(), 4);
    let gathered = registry.gather();
    let prometheus = gathered
        .iter()
        .find(|family| family.name() == "monad_rpc_execution_duration_seconds")
        .unwrap();
    assert_eq!(prometheus.get_metric().len(), 4);
    for point in &histogram.data_points {
        let labels: BTreeMap<_, _> = point
            .attributes
            .iter()
            .map(|attribute| {
                let Some(Value::StringValue(value)) = &attribute.value.as_ref().unwrap().value
                else {
                    panic!("expected a string label");
                };
                (attribute.key.as_str(), value.as_str())
            })
            .collect();
        let matching = prometheus
            .get_metric()
            .iter()
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .filter(|label| ["main", "secondary", "type"].contains(&label.name()))
                    .map(|label| (label.name(), label.value()))
                    .collect::<BTreeMap<_, _>>()
                    == labels
            })
            .unwrap()
            .get_histogram();
        assert_eq!(point.count, 2);
        assert_eq!(point.count, matching.get_sample_count());
        assert_eq!(point.sum.unwrap(), matching.get_sample_sum());
        assert_eq!(point.explicit_bounds.len(), matching.get_bucket().len());
        assert_eq!(point.bucket_counts.len(), point.explicit_bounds.len() + 1);
        let mut cumulative = 0;
        for ((count, boundary), bucket) in point
            .bucket_counts
            .iter()
            .zip(&point.explicit_bounds)
            .zip(matching.get_bucket())
        {
            cumulative += count;
            assert_eq!(*boundary, bucket.upper_bound());
            assert_eq!(cumulative, bucket.cumulative_count());
        }
    }
    println!("real rpc module: otlp grpc and /metrics contain matching counts, sums, buckets and labels for four timing series");
    tokio::task::spawn_blocking(move || drop(metrics))
        .await
        .unwrap();
    collector.abort();
}
