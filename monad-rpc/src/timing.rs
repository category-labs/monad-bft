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
    time::Instant,
};

use actix_http::Payload;
use actix_web::{
    body::{BoxBody, MessageBody},
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error, FromRequest, HttpMessage, HttpRequest,
};
use futures::future::LocalBoxFuture;
use tracing::debug;

pub struct TimingMiddleware;

impl<S, B> Transform<S, ServiceRequest> for TimingMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type InitError = ();
    type Transform = TimingMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(TimingMiddlewareService { service }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct TimingRequestId {
    id: u64,
}

impl TimingRequestId {
    pub fn random() -> Self {
        Self {
            id: rand::random::<u64>(),
        }
    }
}

impl FromRequest for TimingRequestId {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        match req.extensions().get::<TimingRequestId>() {
            Some(request_id) => ready(Ok(*request_id)),
            None => ready(Ok(TimingRequestId::random())),
        }
    }
}
pub struct TimingMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for TimingMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let start_time = Instant::now();

        let request_id = TimingRequestId::random();
        let (request, payload) = req.into_parts();
        let id_int = request_id.id;
        request.extensions_mut().insert(request_id);

        let request_size = request
            .headers()
            .get("content-length")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
        let client_ip = request
            .connection_info()
            .realip_remote_addr()
            .unwrap_or("unknown")
            .to_string();
        let req = ServiceRequest::from_parts(request, payload);
        let fut = self.service.call(req);

        Box::pin(async move {
            let resp = fut.await;
            let processing_end = Instant::now();

            let response_size = match &resp {
                Ok(resp) => match resp.response().body().size() {
                    actix_web::body::BodySize::None => 0,
                    actix_web::body::BodySize::Sized(size) => size,
                    actix_web::body::BodySize::Stream => 0,
                },
                Err(_) => 0,
            };

            debug!(
                request_id = id_int,
                request_size_bytes = request_size,
                response_size_bytes = response_size,
                processing_time = format!("{:?}", processing_end.duration_since(start_time)),
                client_ip,
                status = match &resp {
                    Ok(r) => r.status().as_u16(),
                    Err(_) => 500,
                },
                success = resp.is_ok(),
                "Request timing information"
            );

            resp.map(|r| r.map_into_boxed_body())
        })
    }
}
