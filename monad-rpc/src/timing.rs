use std::{
    future::{ready, Ready},
    time::Instant,
};

use actix_web::{
    body::{BoxBody, MessageBody},
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
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

    fn call(&self, mut req: ServiceRequest) -> Self::Future {
        let start_time = Instant::now();

        let request_size = req
            .request()
            .headers()
            .get("content-length")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
        let client_ip = req
            .connection_info()
            .realip_remote_addr()
            .unwrap_or("unknown")
            .to_string();
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
