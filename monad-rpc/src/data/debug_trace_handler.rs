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

//! Concurrency limiter for `debug_trace*` block/transaction methods that read pre-computed
//! call frames from triedb. Separate from the eth_call semaphore because these don't run the
//! EVM — they share no resources with `eth_call` / `eth_estimateGas` / `debug_traceCall` and
//! their per-call cost profile is different (memory-heavy rather than CPU-heavy).

use std::sync::Arc;

use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use tracing::error;

use crate::types::jsonrpc::JsonRpcError;

#[derive(Clone)]
pub struct DebugTraceHandler {
    rate_limiter: Arc<Semaphore>,
}

impl DebugTraceHandler {
    pub fn new(max_concurrent_permits: usize) -> Self {
        Self {
            rate_limiter: Arc::new(Semaphore::new(max_concurrent_permits)),
        }
    }

    pub fn acquire(&self) -> Result<DebugTracePermit<'_>, JsonRpcError> {
        match self.rate_limiter.try_acquire() {
            Ok(permit) => Ok(DebugTracePermit { _permit: permit }),
            Err(TryAcquireError::Closed) => {
                error!("DebugTraceHandler acquire rate_limiter closed");
                Err(JsonRpcError::internal_error("method unavailable".into()))
            }
            Err(TryAcquireError::NoPermits) => Err(JsonRpcError::internal_error(
                "concurrent requests limit".into(),
            )),
        }
    }

    pub fn available_permits(&self) -> usize {
        self.rate_limiter.available_permits()
    }
}

pub struct DebugTracePermit<'a> {
    _permit: SemaphorePermit<'a>,
}
