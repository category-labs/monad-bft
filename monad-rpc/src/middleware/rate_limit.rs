use std::{future::Future, pin::Pin, collections::HashMap, sync::Arc};
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::jsonrpc::{JsonRpcError, Request};
use super::{Middleware, Next};

pub struct ConcurrencyLimiter {
    semaphore: Arc<Semaphore>,
}

pub struct RateLimitMiddleware<T>
where
    T: Into<usize> + Clone,
{
    limiters: Vec<Arc<ConcurrencyLimiter>>,
    method_mappings: HashMap<String, T>,
}

impl<T> RateLimitMiddleware<T>
where
    T: Into<usize> + Clone,
{
    pub fn new() -> Self {
        Self {
            limiters: Vec::new(),
            method_mappings: HashMap::new(),
        }
    }

    pub fn add_limiter(&mut self, id: T, concurrent_limit: u32) {
        let idx: usize = id.into();
        let limiter = ConcurrencyLimiter {
            semaphore: Arc::new(Semaphore::new(concurrent_limit as usize)),
        };

        // Ensure the vec is large enough to hold this index
        if idx >= self.limiters.len() {
            self.limiters.resize_with(idx + 1, || {
                Arc::new(ConcurrencyLimiter {
                    semaphore: Arc::new(Semaphore::new(0)) // Placeholder limiter
                })
            });
        }
        self.limiters[idx] = Arc::new(limiter);
    }

    pub fn map_method_to_limiter(&mut self, method: impl Into<String>, limiter_id: T) -> Result<(), String> {
        let idx: usize = limiter_id.clone().into();
        if idx >= self.limiters.len() {
            return Err("Limiter ID out of bounds".to_string());
        }
        self.method_mappings.insert(method.into(), limiter_id);
        Ok(())
    }

    async fn try_acquire_permit(&self, method: &str) -> Result<Option<tokio::sync::SemaphorePermit>, JsonRpcError> {
        if let Some(limiter_id) = self.method_mappings.get(method) {
            let idx: usize = limiter_id.clone().into();
            if let Some(limiter) = self.limiters.get(idx) {
                return Ok(Some(limiter.semaphore.try_acquire().map_err(|_| {
                    JsonRpcError::internal_error(format!("{} concurrent requests limit", method))
                })?));
            }
        }
        Ok(None)
    }
}

impl<T, State> Middleware<State> for RateLimitMiddleware<T>
where
    T: Into<usize> + Clone + Send + Sync + 'static,
{
    fn handle<'a>(
        &'a self,
        request: &'a Value,
        state: &'a State,
        next: Next<'a, State>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, JsonRpcError>> + 'a>> {
        Box::pin(async move {
            let method = match serde_json::from_value::<Request>(request.clone()) {
                Ok(req) => req.method,
                Err(_) => return next.call(request, state).await,
            };

            let _permit = self.try_acquire_permit(&method).await?;

            next.call(request, state).await
        })
    }
}
