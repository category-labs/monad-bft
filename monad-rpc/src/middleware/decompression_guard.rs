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

use std::future::{ready, Ready};

use actix_http::{
    header::{self},
    ContentEncoding,
};
use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use futures::future::LocalBoxFuture;

#[derive(Clone, Debug, Default)]
pub struct DecompressionGuard;

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
        ready(Ok(DecompressionGuardService { service }))
    }
}

pub struct DecompressionGuardService<S> {
    service: S,
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
            _ => Box::pin(async move {
                Err(actix_web::error::ErrorBadRequest(
                    "Content encoding is not supported",
                ))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_http::StatusCode;
    use actix_web::{dev::Service, test};
    use serde_json::json;

    use crate::{
        tests::{init_server, recover_response_body},
        types::jsonrpc::ResponseWrapper,
    };

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
    async fn test_legitimate_encoding_request_fails() {
        for encoding in ["br", "deflate", "gzip", "zstd"] {
            let app = init_server().await;

            let payload = json!({
                "jsonrpc": "2.0",
                "method": "eth_chainId",
                "params": [],
                "id": 1
            })
            .to_string();

            let payload_len = payload.len();

            let req = test::TestRequest::post()
                .uri("/")
                .set_payload(payload)
                .insert_header(("Content-Encoding", encoding))
                .insert_header(("Content-Length", payload_len.to_string()))
                .to_request();

            let error = app.call(req).await.err().unwrap();

            assert_eq!(error.to_string(), "Content encoding is not supported");
        }
    }
}
