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
    future::{ready, Ready},
    pin::Pin,
    task::{Context, Poll},
};

use actix_http::{
    encoding::Decoder,
    header::{self},
    ContentEncoding, Payload,
};
use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use bytes::Bytes;
use futures::{future::LocalBoxFuture, StreamExt};
use futures_util::Stream;
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct DecompressionGuardConfig {
    max_request_size_bytes: usize,
}

#[derive(Clone)]
pub struct DecompressionGuard {
    config: DecompressionGuardConfig,
}

impl DecompressionGuard {
    pub fn new(max_request_size_bytes: usize) -> Self {
        Self {
            config: DecompressionGuardConfig {
                max_request_size_bytes,
            },
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for DecompressionGuard
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = DecompressionGuardService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(DecompressionGuardService {
            service,
            config: self.config.clone(),
        }))
    }
}

pub struct DecompressionGuardService<S> {
    service: S,
    config: DecompressionGuardConfig,
}

impl<S, B> Service<ServiceRequest> for DecompressionGuardService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, mut req: ServiceRequest) -> Self::Future {
        let Some(content_encoding_value) =
            req.headers_mut().remove(header::CONTENT_ENCODING).next()
        else {
            return Box::pin(self.service.call(req));
        };

        let Some(content_encoding) = content_encoding_value
            .to_str()
            .ok()
            .and_then(|encoding_value_str| ContentEncoding::try_from(encoding_value_str).ok())
        else {
            return Box::pin(async move {
                Err(actix_web::error::ErrorBadRequest(
                    "Invalid content encoding",
                ))
            });
        };

        match content_encoding {
            ContentEncoding::Identity => {
                return Box::pin(self.service.call(req));
            }
            ContentEncoding::Brotli
            | ContentEncoding::Deflate
            | ContentEncoding::Gzip
            | ContentEncoding::Zstd => {
                let Some(declared_compressed_size) = req
                    .headers_mut()
                    .remove(header::CONTENT_LENGTH)
                    .next()
                    .map(|h| h.to_str().ok().and_then(|s| s.parse::<usize>().ok()))
                    .unwrap_or_else(|| Some(0))
                else {
                    return Box::pin(async move {
                        Err(actix_web::error::ErrorBadRequest("Invalid content length"))
                    });
                };

                if declared_compressed_size > self.config.max_request_size_bytes {
                    return Box::pin(async move {
                        Err(actix_web::error::ErrorPayloadTooLarge(
                            "Compressed request payload exceeds maximum request size",
                        ))
                    });
                }

                let (http_req, payload) = req.into_parts();

                let tracked_payload = DecompressionGuardPayloadStream::new(
                    payload,
                    content_encoding,
                    self.config.clone(),
                );

                Box::pin(self.service.call(ServiceRequest::from_parts(
                    http_req,
                    Payload::Stream {
                        payload: Box::pin(tracked_payload),
                    },
                )))
            }
            // Enum marked non_exhaustive
            _ => {
                warn!(
                    ?content_encoding,
                    "Request specified unknown content encoding"
                );

                return Box::pin(async move {
                    Err(actix_web::error::ErrorBadRequest(
                        "Unsupported content encoding",
                    ))
                });
            }
        }
    }
}

struct DecompressionGuardPayloadStream {
    inner: Decoder<Payload>,
    config: DecompressionGuardConfig,
    content_encoding: ContentEncoding,

    decompressed_size: usize,
    limit_exceeded: bool,
}

impl DecompressionGuardPayloadStream {
    fn new(
        payload: Payload,
        content_encoding: ContentEncoding,
        config: DecompressionGuardConfig,
    ) -> Self {
        Self {
            inner: Decoder::new(payload, content_encoding),
            config,
            content_encoding,

            decompressed_size: 0,
            limit_exceeded: false,
        }
    }
}

impl Stream for DecompressionGuardPayloadStream {
    type Item = Result<Bytes, actix_web::error::PayloadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.limit_exceeded {
            return Poll::Ready(None);
        }

        let Poll::Ready(result) = self.inner.poll_next_unpin(cx) else {
            return Poll::Pending;
        };

        let Some(result) = result else {
            return Poll::Ready(None);
        };

        let chunk = match result {
            Err(err) => return Poll::Ready(Some(Err(err))),
            Ok(chunk) => chunk,
        };

        self.decompressed_size += chunk.len();

        let DecompressionGuardConfig {
            max_request_size_bytes,
        } = self.config;

        if self.decompressed_size > max_request_size_bytes {
            self.limit_exceeded = true;

            info!(
                ?self.config,
                ?self.content_encoding,
                decompressed_size = self.decompressed_size,
                "DecompressionGuard blocking request"
            );

            return Poll::Ready(Some(Err(actix_web::error::PayloadError::Overflow)));
        }

        Poll::Ready(Some(Ok(chunk)))
    }
}

#[cfg(test)]
mod tests {
    use actix_http::{Request, StatusCode};
    use actix_web::{
        body::{to_bytes, MessageBody},
        dev::{Service, ServiceResponse},
        test, web, App, Error,
    };
    use serde_json::json;

    use super::*;
    use crate::{
        handlers::{
            eth::call::EthCallStatsTracker, resources::MonadJsonRootSpanBuilder, rpc_handler,
        },
        jsonrpc::{Response, ResponseWrapper},
        txpool::EthTxPoolBridgeClient,
    };

    async fn init_server(
    ) -> impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error> {
        use std::sync::Arc;

        use tokio::sync::Semaphore;
        use tracing_actix_web::TracingLogger;

        use crate::handlers::resources::MonadRpcResources;

        let app_state = MonadRpcResources {
            txpool_bridge_client: Some(EthTxPoolBridgeClient::for_testing()),
            triedb_reader: None,
            eth_call_executor: None,
            eth_call_executor_fibers: 64,
            eth_call_stats_tracker: Some(Arc::new(EthCallStatsTracker::default())),
            archive_reader: None,
            chain_id: 1337,
            chain_state: None,
            batch_request_limit: 5,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            rate_limiter: Arc::new(Semaphore::new(1000)),
            total_permits: 1000,
            logs_max_block_range: 1000,
            eth_call_provider_gas_limit: u64::MAX,
            eth_estimate_gas_provider_gas_limit: u64::MAX,
            eth_send_raw_transaction_sync_default_timeout_ms: 2_000,
            eth_send_raw_transaction_sync_max_timeout_ms: 10_000,
            dry_run_get_logs_index: false,
            use_eth_get_logs_index: false,
            max_finalized_block_cache_len: 200,
            enable_eth_call_statistics: true,
            metrics: None,
            rpc_comparator: None,
        };

        test::init_service(
            App::new()
                .wrap(DecompressionGuard::new(2_000_000))
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::PayloadConfig::default().limit(2_000_000))
                .app_data(web::Data::new(app_state.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler))),
        )
        .await
    }

    async fn recover_response_body(
        resp: ServiceResponse<impl MessageBody>,
    ) -> ResponseWrapper<Response> {
        let b = to_bytes(resp.into_body())
            .await
            .unwrap_or_else(|_| panic!("body to_bytes failed"));

        ResponseWrapper::from_body_bytes(b).unwrap()
    }

    /// Helper function to compress JSON with different algorithms
    fn compress_json(json: &str, encoding: &str) -> Vec<u8> {
        use std::io::Write;

        use brotli::enc::BrotliEncoderParams;
        use flate2::{
            write::{GzEncoder, ZlibEncoder},
            Compression,
        };

        match encoding {
            "gzip" => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(json.as_bytes()).unwrap();
                encoder.finish().unwrap()
            }
            "deflate" => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(json.as_bytes()).unwrap();
                encoder.finish().unwrap()
            }
            "br" => {
                let mut output = Vec::new();
                let params = BrotliEncoderParams::default();
                brotli::BrotliCompress(
                    &mut std::io::Cursor::new(json.as_bytes()),
                    &mut output,
                    &params,
                )
                .unwrap();
                output
            }
            "zstd" => zstd::encode_all(json.as_bytes(), 3).unwrap(),
            _ => panic!("Unsupported encoding: {}", encoding),
        }
    }

    /// Helper to create large JSON payload
    fn create_large_json_payload(size_kb: usize) -> String {
        let array_size = (size_kb * 1024) / 2; // Approximate size
        format!(
            r#"{{"jsonrpc":"2.0","method":"eth_blockNumber","params":{},"id":1}}"#,
            serde_json::to_string(&vec![0u8; array_size]).unwrap()
        )
    }

    fn create_zstd_bomb(max_compressed_kb: usize, decompressed_mb: usize) -> Vec<u8> {
        // Create a payload of zeros which compress extremely well
        let decompressed_size = decompressed_mb * 1024 * 1024;
        let data = vec![0u8; decompressed_size];

        // Compress with maximum compression level
        let compressed = zstd::encode_all(&data[..], 22).unwrap();

        assert!(compressed.len() < max_compressed_kb * 1024);

        compressed

        // // Pad with additional data to reach target compressed size
        // let mut result = compressed;
        // let padding_needed = (max_compressed_kb * 1024) - result.len();
        // result.extend(vec![0u8; padding_needed]);
        // result
    }

    fn create_zstd_bomb_fast(decompressed_gb: usize) -> Vec<u8> {
        use std::io::Write;

        // Compress 1GB chunk
        let chunk_compressed = {
            let mut encoder = zstd::Encoder::new(Vec::new(), 19).unwrap();
            let data = vec![0u8; 1024 * 1024 * 1024];
            encoder.write_all(&data).unwrap();
            encoder.finish().unwrap()
        };

        // Repeat N times
        let mut result = Vec::with_capacity(chunk_compressed.len() * decompressed_gb);
        for _ in 0..decompressed_gb {
            result.extend_from_slice(&chunk_compressed);
        }

        result
    }

    #[actix_web::test]
    async fn test_uncompressed_request_passes_through() {
        let app = init_server().await;

        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        });

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = recover_response_body(resp).await;

        match resp {
            ResponseWrapper::Batch(_) => panic!("expected single response"),
            ResponseWrapper::Single(resp) => {
                assert!(resp.result.is_some());
                assert!(resp.error.is_none());
            }
        }
    }

    #[actix_web::test]
    async fn test_legitimate_gzip_request_succeeds() {
        let app = init_server().await;

        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        });

        let json_str = payload.to_string();
        let compressed = compress_json(&json_str, "gzip");

        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(("Content-Encoding", "gzip"))
            .insert_header(("Content-Length", compressed.len().to_string()))
            .set_payload(compressed)
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = recover_response_body(resp).await;

        match resp {
            ResponseWrapper::Batch(_) => panic!("expected single response"),
            ResponseWrapper::Single(resp) => {
                assert!(resp.result.is_some());
                assert!(resp.error.is_none());
            }
        }
    }

    #[actix_web::test]
    async fn test_legitimate_zstd_request_succeeds() {
        let app = init_server().await;

        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        });

        let json_str = payload.to_string();
        let compressed = compress_json(&json_str, "zstd");

        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(("Content-Encoding", "zstd"))
            .insert_header(("Content-Length", compressed.len().to_string()))
            .set_payload(compressed)
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = recover_response_body(resp).await;

        match resp {
            ResponseWrapper::Batch(_) => panic!("expected single response"),
            ResponseWrapper::Single(resp) => {
                assert!(resp.result.is_some());
                assert!(resp.error.is_none());
            }
        }
    }

    #[actix_web::test]
    async fn test_oversized_compressed_content_length_rejected() {
        let app = init_server().await;

        // Create a fake compressed payload with Content-Length exceeding max_request_size
        let fake_compressed = vec![0u8; 100]; // Actual payload doesn't matter

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(fake_compressed)
            .insert_header(("Content-Encoding", "gzip"))
            .insert_header(("Content-Length", "100000000")) // 100MB
            .to_request();

        let error = app.call(req).await.err().unwrap();
        assert_eq!(
            error.error_response().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[actix_web::test]
    async fn test_decompression_bomb_blocked_by_size() {
        let app = init_server().await;

        // Create a payload that decompresses to 3MB (exceeds 2MB limit)
        let large_json = create_large_json_payload(3000); // 3MB
        let compressed = compress_json(&large_json, "zstd");

        for override_content_length in [false, true] {
            let mut req = test::TestRequest::post()
                .uri("/")
                .set_payload(compressed.clone())
                .insert_header(("Content-Encoding", "zstd"));

            if override_content_length {
                req = req.insert_header(("Content-Length", compressed.len().to_string()));
            }

            let resp = app.call(req.to_request()).await.unwrap();
            assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        }
    }

    #[actix_web::test]
    async fn test_decompression_bomb_blocked_by_ratio() {
        let app = init_server().await;

        // Single 1GB compressed chunk
        let bomb = create_zstd_bomb_fast(1);
        assert_eq!(bomb.len(), 32785);

        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(("Content-Encoding", "zstd"))
            .insert_header(("Content-Length", bomb.len().to_string()))
            .set_payload(bomb)
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[actix_web::test]
    async fn test_extreme_decompression_bomb_zstd() {
        let app = init_server().await;

        // Create an extreme zstd bomb: tiny compressed -> huge decompressed
        let bomb = create_zstd_bomb(50, 10); // ~50KB compressed, 10MB decompressed = 200:1 ratio

        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(("Content-Encoding", "zstd"))
            .insert_header(("Content-Length", bomb.len().to_string()))
            .set_payload(bomb)
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[actix_web::test]
    async fn test_brotli_and_deflate_encodings() {
        let app = init_server().await;

        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        });

        let json_str = payload.to_string();

        // Test brotli
        let compressed_br = compress_json(&json_str, "br");
        let req_br = test::TestRequest::post()
            .uri("/")
            .insert_header(("Content-Encoding", "br"))
            .insert_header(("Content-Length", compressed_br.len().to_string()))
            .set_payload(compressed_br)
            .to_request();

        let resp_br = app.call(req_br).await.unwrap();
        assert_eq!(resp_br.status(), StatusCode::OK);

        // Test deflate
        let compressed_deflate = compress_json(&json_str, "deflate");
        let req_deflate = test::TestRequest::post()
            .uri("/")
            .insert_header(("Content-Encoding", "deflate"))
            .insert_header(("Content-Length", compressed_deflate.len().to_string()))
            .set_payload(compressed_deflate)
            .to_request();

        let resp_deflate = app.call(req_deflate).await.unwrap();
        assert_eq!(resp_deflate.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_malformed_compressed_data_handled_gracefully() {
        let app = init_server().await;

        // Send random bytes with gzip encoding header
        let malformed_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE];

        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(("Content-Encoding", "gzip"))
            .insert_header(("Content-Length", malformed_data.len().to_string()))
            .set_payload(malformed_data)
            .to_request();

        let resp = app.call(req).await.unwrap();
        // Should return an error (not panic/crash)
        // Could be 400 Bad Request or 500 Internal Server Error
        assert!(
            resp.status().is_client_error() || resp.status().is_server_error(),
            "Expected error status, got: {}",
            resp.status()
        );
    }

    #[actix_web::test]
    async fn test_batch_request_with_compression() {
        let app = init_server().await;

        let payload = json!([
            {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
            {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 2},
            {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 3}
        ]);

        let json_str = payload.to_string();
        let compressed = compress_json(&json_str, "gzip");

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(compressed)
            .insert_header(("Content-Encoding", "gzip"))
            // .insert_header(("Content-Length", compressed.len().to_string()))
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = recover_response_body(resp).await;

        match resp {
            ResponseWrapper::Batch(responses) => {
                assert_eq!(responses.len(), 3);
                for response in responses {
                    assert!(response.result.is_some());
                    assert!(response.error.is_none());
                }
            }
            ResponseWrapper::Single(_) => panic!("Expected batch response"),
        }
    }
}
