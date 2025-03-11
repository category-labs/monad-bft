use std::{future::Future, pin::Pin};

use serde_json::Value;

use crate::jsonrpc::JsonRpcError;

mod logging;
mod rate_limit;

pub use logging::LoggingMiddleware;
pub use rate_limit::RateLimitMiddleware;

pub trait RpcHandler<State>: Send + Sync {
    fn handle<'a>(
        &'a self,
        request: &'a Value,
        state: &'a State,
    ) -> Pin<Box<dyn Future<Output = Result<Value, JsonRpcError>> + 'a>>;
}

pub trait Middleware<State>: Send + Sync {
    fn handle<'a>(
        &'a self,
        request: &'a Value,
        state: &'a State,
        next: Next<'a, State>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, JsonRpcError>> + 'a>>;
}

pub struct Next<'a, State> {
    middleware: Option<&'a [Box<dyn Middleware<State>>]>,
    handler: &'a dyn RpcHandler<State>,
}

impl<'a, State> Next<'a, State> {
    pub fn new(
        middleware: Option<&'a [Box<dyn Middleware<State>>]>,
        handler: &'a dyn RpcHandler<State>,
    ) -> Self {
        Self {
            middleware,
            handler,
        }
    }

    pub fn call<'s>(
        &'s self,
        request: &'s Value,
        state: &'s State,
    ) -> Pin<Box<dyn Future<Output = Result<Value, JsonRpcError>> + 's>>
    where
        State: 's,
        's: 'a,
    {
        Box::pin(async move {
            match self.middleware {
                Some([current, rest @ ..]) => {
                    let next = Next::new((!rest.is_empty()).then_some(rest), self.handler);
                    current.handle(request, state, next).await
                }
                _ => self.handler.handle(request, state).await,
            }
        })
    }
}

pub fn build_middleware_chain<'a, State>(
    request: &'a Value,
    state: &'a State,
    middlewares: &'a [Box<dyn Middleware<State>>],
    handler: &'a dyn RpcHandler<State>,
) -> Pin<Box<dyn Future<Output = Result<Value, JsonRpcError>> + 'a>> {
    Box::pin(async move {
        match (!middlewares.is_empty()).then_some(middlewares) {
            Some([current, rest @ ..]) => {
                let next = Next::new((!rest.is_empty()).then_some(rest), handler);
                current.handle(request, state, next).await
            }
            _ => handler.handle(request, state).await,
        }
    })
}
