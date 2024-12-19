use std::{
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    time::{Duration, Instant},
};

use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{
    StateSyncRequest, StateSyncResponse, StateSyncResponseBody, StateSyncResponseOk,
    StateSyncSessionId,
};
use monad_types::NodeId;
use rand::{prelude::SliceRandom, thread_rng, Rng};

pub(crate) struct OutboundRequests<PT: PubKey> {
    max_parallel_requests: usize,
    request_timeout: Duration,

    pending_requests: BTreeSet<StateSyncRequest>,
    in_flight_requests: HashMap<StateSyncSessionId, InFlightRequest<PT>>,
}

struct InFlightRequest<PT: PubKey> {
    request: StateSyncRequest,

    peer: NodeId<PT>,

    last_active: Instant,

    // out-of-order responses indexed by response_idx
    responses: BTreeMap<u64, StateSyncResponseOk>,

    // next expected response index
    response_index: u64,

    _pd: PhantomData<PT>,
}

impl<PT: PubKey> InFlightRequest<PT> {
    fn new(request: StateSyncRequest, peer: NodeId<PT>) -> Self {
        Self {
            request,
            peer,
            last_active: Instant::now(),
            responses: Default::default(),
            response_index: 0,
            _pd: PhantomData,
        }
    }

    fn apply_response(
        &mut self,
        from: &NodeId<PT>,
        response: StateSyncResponseOk,
    ) -> Vec<(StateSyncRequest, StateSyncResponseOk)> {
        self.last_active = std::time::Instant::now();

        if from != &self.peer {
            tracing::debug!(?from, ?response, "dropping statesync response, wrong peer");
            return Vec::new();
        }

        if response.response_index < self.response_index {
            tracing::debug!(
                ?from,
                ?response,
                ?self.response_index,
                "dropping statesync response, out-of-order"
            );
            return Vec::new();
        }

        if response.response_index == self.response_index {
            let curr_index = self.response_index;
            self.response_index += 1;
            let mut responses = vec![(self.request, response)];

            // Remove consequitive responses from out-of-order queue
            for index in self.responses.keys() {
                if *index == self.response_index {
                    self.response_index += 1;
                } else {
                    break;
                }
            }
            for index in curr_index + 1..self.response_index {
                let response = self.responses.remove(&index).expect("missing response");
                responses.push((self.request, response));
            }
            return responses;
        }

        tracing::debug!(?from, ?response, "queue out-of-order statesync response");
        let replaced = self.responses.insert(response.response_index, response);
        if !replaced.is_none() {
            tracing::warn!(?from, "server sent duplicate response_index");
        }
        Vec::new()
    }
}

impl<PT: PubKey> OutboundRequests<PT> {
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

    pub fn clear(&mut self) {
        tracing::debug!("about to set new target, clearing outstanding requests");
        self.pending_requests.clear();
        self.in_flight_requests.clear();
    }

    pub fn queue_request(&mut self, request: StateSyncRequest) {
        //tracing::debug!(?request, "queueing request");
        if let Some(current_target) = self
            .pending_requests
            .first()
            .or(self.in_flight_requests.values().next().map(|r| &r.request))
            .map(|request| &request.target)
        {
            assert_eq!(*current_target, request.target);
        }
        self.pending_requests.insert(request);
    }

    #[must_use]
    pub fn handle_response(
        &mut self,
        from: NodeId<PT>,
        response: StateSyncResponse,
    ) -> Vec<(StateSyncRequest, StateSyncResponseOk)> {
        match self.in_flight_requests.entry(response.session_id) {
            Entry::Occupied(mut in_flight_request) => {
                match response.body {
                    StateSyncResponseBody::Ok(response) => {
                        if in_flight_request.get().peer != from {
                            tracing::info!(?from, ?response, "dropping response, wrong peer");
                            return Vec::new();
                        }
                        let responses = in_flight_request.get_mut().apply_response(&from, response);
                        if responses.last().is_some_and(|(_, r)| r.response_n != 0) {
                            // Received last response, request is complete
                            in_flight_request.remove();
                        }
                        responses
                    }
                    StateSyncResponseBody::Err(e) => {
                        tracing::warn!(?from, ?response, ?e, "received error, retrying");
                        self.pending_requests
                            .insert(in_flight_request.remove().request);
                        return Vec::new();
                    }
                }
            }
            Entry::Vacant(_) => {
                tracing::debug!(
                    ?from,
                    ?response,
                    "dropping response, request is no longer queued"
                );
                Vec::new()
            }
        }
    }

    fn start_request(
        &mut self,
        peers: &[NodeId<PT>],
        mut request: StateSyncRequest,
    ) -> (NodeId<PT>, StateSyncRequest) {
        request.session_id = StateSyncSessionId(thread_rng().gen());
        let node_id = peers.choose(&mut thread_rng()).expect("!empty");
        let session_id = request.session_id;
        let in_flight_request = InFlightRequest::new(request.clone(), *node_id);
        let replaced = self
            .in_flight_requests
            .insert(session_id, in_flight_request);
        assert!(replaced.is_none());
        (*node_id, request)
    }

    #[must_use]
    pub async fn poll(&mut self, peers: &[NodeId<PT>]) -> (NodeId<PT>, StateSyncRequest) {
        // check if we can immediately queue another request
        if self.in_flight_requests.len() < self.max_parallel_requests
            && !self.pending_requests.is_empty()
        {
            let request = self.pending_requests.pop_first().expect("!is_empty()");
            return self.start_request(peers, request);
        }

        // find request that will timeout first
        let Some((session_id, in_flight_request)) = self
            .in_flight_requests
            .iter_mut()
            .min_by_key(|(_, in_flight_request)| in_flight_request.last_active)
        else {
            // no outstanding requests, so yield forever
            return futures::future::pending().await;
        };

        if in_flight_request.last_active.elapsed() < self.request_timeout {
            // wait until request times out
            tokio::time::sleep_until((in_flight_request.last_active + self.request_timeout).into())
                .await;
        }

        // Retransmit request that timed out
        let session_id = *session_id;
        let request = self.in_flight_requests.remove(&session_id).unwrap().request;
        self.start_request(peers, request)
    }
}
