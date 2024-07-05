use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use monad_executor_glue::{StateSyncRequest, StateSyncResponse};

pub(crate) struct OutboundRequests {
    max_parallel_requests: usize,
    request_timeout: Duration,

    pending_requests: BTreeSet<StateSyncRequest>,
    in_flight_requests: BTreeMap<StateSyncRequest, Instant>,
}

impl OutboundRequests {
    pub fn new(max_parallel_requests: usize, request_timeout: Duration) -> Self {
        assert!(max_parallel_requests > 0);
        Self {
            max_parallel_requests,
            request_timeout,

            pending_requests: Default::default(),
            in_flight_requests: Default::default(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending_requests.is_empty() && self.in_flight_requests.is_empty()
    }

    pub fn queue_request(&mut self, request: StateSyncRequest) {
        tracing::debug!(?request, "queueing request");
        if self
            .pending_requests
            .first()
            .or(self.in_flight_requests.keys().next())
            .is_some_and(|old_request| old_request.target != request.target)
        {
            self.pending_requests.clear();
            self.in_flight_requests.clear();
        }
        let _batch_size = 256_usize.pow(request.prefix_bytes.into());
        self.pending_requests.insert(request);
    }

    #[must_use]
    pub fn handle_response(&mut self, response: &StateSyncResponse) -> bool {
        self.in_flight_requests.remove(&response.request).is_some()
    }

    #[must_use]
    pub async fn poll(&mut self) -> StateSyncRequest {
        // check if we can immediately queue another request
        if self.in_flight_requests.len() < self.max_parallel_requests
            && !self.pending_requests.is_empty()
        {
            let to_send = self.pending_requests.pop_first().expect("!is_empty()");
            self.in_flight_requests.insert(to_send, Instant::now());
            return to_send;
        }

        // find request that will timeout first
        let Some((request, time_sent)) = self
            .in_flight_requests
            .iter_mut()
            .min_by_key(|(_, instant)| **instant)
        else {
            // no outstanding requests, so yield forever
            return futures::future::pending().await;
        };

        if time_sent.elapsed() < self.request_timeout {
            // wait until request times out
            tokio::time::sleep_until((*time_sent + self.request_timeout).into()).await;
        }

        *time_sent = std::time::Instant::now();
        *request
    }
}
