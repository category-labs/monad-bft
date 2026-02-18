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

#[derive(Clone, Debug)]
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
            ContentEncoding::Identity => Box::pin(self.service.call(req)),
            ContentEncoding::Brotli
            | ContentEncoding::Deflate
            | ContentEncoding::Gzip
            | ContentEncoding::Zstd => {
                let Some(declared_compressed_size_unparsed) =
                    req.headers_mut().remove(header::CONTENT_LENGTH).next()
                else {
                    return Box::pin(async move {
                        Err(actix_web::error::ErrorLengthRequired(
                            "Content-Length header required for compressed requests",
                        ))
                    });
                };

                let Some(declared_compressed_size) = declared_compressed_size_unparsed
                    .to_str()
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
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
                    declared_compressed_size,
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

                Box::pin(async move {
                    Err(actix_web::error::ErrorBadRequest(
                        "Unsupported content encoding",
                    ))
                })
            }
        }
    }
}

struct CompressedBytesTracker {
    payload: Payload,

    compressed_size_declared: usize,
    compressed_bytes_received: usize,
}

impl Stream for CompressedBytesTracker {
    type Item = Result<Bytes, actix_web::error::PayloadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Poll::Ready(result) = self.payload.poll_next_unpin(cx) else {
            return Poll::Pending;
        };

        let Some(result) = result else {
            return Poll::Ready(None);
        };

        let chunk = match result {
            Err(err) => return Poll::Ready(Some(Err(err))),
            Ok(chunk) => chunk,
        };

        self.compressed_bytes_received += chunk.len();

        if self.compressed_bytes_received > self.compressed_size_declared {
            info!(
                declared_compressed_size = self.compressed_size_declared,
                compressed_bytes_received = self.compressed_bytes_received,
                "DecompressionGuard blocking request: compressed bytes received exceeds Content-Length"
            );

            return Poll::Ready(Some(Err(actix_web::error::PayloadError::Overflow)));
        }

        Poll::Ready(Some(Ok(chunk)))
    }
}

struct DecompressionGuardPayloadStream {
    config: DecompressionGuardConfig,
    content_encoding: ContentEncoding,
    inner: Decoder<CompressedBytesTracker>,

    decompressed_size: usize,
    limit_exceeded: bool,
}

impl DecompressionGuardPayloadStream {
    fn new(
        payload: Payload,
        content_encoding: ContentEncoding,
        config: DecompressionGuardConfig,
        compressed_size_declared: usize,
    ) -> Self {
        let tracker = CompressedBytesTracker {
            payload,

            compressed_size_declared,
            compressed_bytes_received: 0,
        };

        Self {
            config,
            content_encoding,
            inner: Decoder::new(tracker, content_encoding),

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
    use actix_http::StatusCode;
    use actix_web::{dev::Service, test};
    use serde_json::json;

    use crate::{
        jsonrpc::ResponseWrapper,
        tests::{init_server, recover_response_body},
    };

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

    fn create_large_json_payload(size_kb: usize) -> String {
        let array_size = (size_kb * 1024) / 2;
        format!(
            r#"{{"jsonrpc":"2.0","method":"eth_blockNumber","params":{},"id":1}}"#,
            serde_json::to_string(&vec![0u8; array_size]).unwrap()
        )
    }

    /// Creates a highly-compressed zstd bomb (1GB of zeros compresses to ~32KB).
    fn create_zstd_bomb(decompressed_gb: usize) -> Vec<u8> {
        use std::io::Write;

        // Compress 1GB chunk once
        let chunk_compressed = {
            let mut encoder = zstd::Encoder::new(Vec::new(), 19).unwrap();
            let data = vec![0u8; 1024 * 1024 * 1024];
            encoder.write_all(&data).unwrap();
            encoder.finish().unwrap()
        };

        // Repeat the compressed chunk N times
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
        let compressed_len = compressed.len();

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(compressed)
            .insert_header(("Content-Encoding", "gzip"))
            .insert_header(("Content-Length", compressed_len.to_string()))
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
        let compressed_len = compressed.len();

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(compressed)
            .insert_header(("Content-Encoding", "zstd"))
            .insert_header(("Content-Length", compressed_len.to_string()))
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

        let fake_compressed = vec![0u8; 100];

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
    async fn test_large_decompression_bomb_blocked_by_size() {
        let app = init_server().await;

        let bomb = create_zstd_bomb(1);
        let bomb_len = bomb.len();
        assert!(bomb_len < 48 * 1024, "Compressed size should be under 48KB");

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(bomb)
            .insert_header(("Content-Encoding", "zstd"))
            .insert_header(("Content-Length", bomb_len.to_string()))
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[actix_web::test]
    async fn test_extreme_decompression_bomb_zstd() {
        let app = init_server().await;

        let bomb = create_zstd_bomb(16);
        let bomb_len = bomb.len();
        assert!(
            bomb_len < 1024 * 1024,
            "Compressed size should be under 1MB"
        );

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(bomb)
            .insert_header(("Content-Encoding", "zstd"))
            .insert_header(("Content-Length", bomb_len.to_string()))
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
        let compressed_br_len = compressed_br.len();
        let req_br = test::TestRequest::post()
            .uri("/")
            .set_payload(compressed_br)
            .insert_header(("Content-Encoding", "br"))
            .insert_header(("Content-Length", compressed_br_len.to_string()))
            .to_request();

        let resp_br = app.call(req_br).await.unwrap();
        assert_eq!(resp_br.status(), StatusCode::OK);

        // Test deflate
        let compressed_deflate = compress_json(&json_str, "deflate");
        let compressed_deflate_len = compressed_deflate.len();
        let req_deflate = test::TestRequest::post()
            .uri("/")
            .set_payload(compressed_deflate)
            .insert_header(("Content-Encoding", "deflate"))
            .insert_header(("Content-Length", compressed_deflate_len.to_string()))
            .to_request();

        let resp_deflate = app.call(req_deflate).await.unwrap();
        assert_eq!(resp_deflate.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_malformed_compressed_data_handled_gracefully() {
        let app = init_server().await;

        // Send random bytes with gzip encoding header
        let malformed_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE];
        let malformed_data_len = malformed_data.len();

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(malformed_data)
            .insert_header(("Content-Encoding", "gzip"))
            .insert_header(("Content-Length", malformed_data_len.to_string()))
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

    #[actix_web::test]
    async fn test_compressed_size_exceeds_declared_content_length() {
        let app = init_server().await;

        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        });

        let json_str = payload.to_string();
        let compressed = compress_json(&json_str, "gzip");

        // Claim a smaller Content-Length than actual compressed size
        // This should be caught by CompressedBytesTracker
        let fake_smaller_length = compressed.len() / 2;

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(compressed)
            .insert_header(("Content-Encoding", "gzip"))
            .insert_header(("Content-Length", fake_smaller_length.to_string()))
            .to_request();

        let resp = app.call(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "Should reject when compressed bytes exceed declared Content-Length"
        );
    }
}
