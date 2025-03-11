use std::{future::Future, pin::Pin};

use serde_json::Value;
use tracing::{debug, info};

use super::{Middleware, Next};
use crate::jsonrpc::JsonRpcError;

#[derive(Clone)]
pub struct LoggingMiddleware;

impl LoggingMiddleware {
    pub fn new() -> Self {
        Self
    }

    fn log_response(request_body: &Value, response: &Value, is_error: bool) {
        if is_error {
            info!(?request_body, ?response, "rpc_request/response error");
        } else {
            debug!(?request_body, ?response, "rpc_request/response successful");
        }
    }
}

impl<State> Middleware<State> for LoggingMiddleware {
    fn handle<'a>(
        &'a self,
        request: &'a Value,
        state: &'a State,
        next: Next<'a, State>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, JsonRpcError>> + 'a>> {
        Box::pin(async move {
            let result = next.call(request, state).await;

            match &result {
                Ok(response) => {
                    Self::log_response(request, response, false);
                }
                Err(e) => {
                    Self::log_response(request, &Value::String(e.message.clone()), true);
                }
            }

            result
        })
    }
}
