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

use crate::{
    ffi::{available_peers, Peer},
    ipc::MAX_PENDING_REQUESTS,
    StateSyncConfig,
};

pub(crate) struct OutboundRequests<PT: PubKey> {
    max_parallel_requests: usize,
    request_timeout: Duration,
    min_backoff: Duration,
    max_backoff: Duration,

    peers: Vec<Peer<PT>>,
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

pub(crate) enum RequestPollResult<PT: PubKey> {
    Request(NodeId<PT>, StateSyncRequest),
    Timer(Option<Instant>),
}

impl<PT: PubKey> OutboundRequests<PT> {
    pub fn new(config: StateSyncConfig, peers: Vec<NodeId<PT>>) -> Self {
        assert!(config.max_parallel_requests > 0);
        let now = Instant::now();
        let peers = peers
            .into_iter()
            .map(|node_id| Peer {
                node_id,
                next_available: now,
                outstanding_requests: 0,
            })
            .collect();

        Self {
            max_parallel_requests: config.max_parallel_requests,
            request_timeout: config.request_timeout,
            min_backoff: config.min_backoff,
            max_backoff: config.max_backoff,

            peers,
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
        for peer in &mut self.peers {
            peer.outstanding_requests = 0;
        }
    }

    pub fn peer_request_completed(&mut self, node_id: NodeId<PT>, is_error: bool) {
        if let Some(peer) = self.peers.iter_mut().find(|p| p.node_id == node_id) {
            assert!(peer.outstanding_requests > 0);
            peer.outstanding_requests -= 1;
            if is_error {
                let mut rng = rand::thread_rng();
                let backoff = rng.gen_range(self.min_backoff..=self.max_backoff);
                peer.next_available = Instant::now() + backoff;
            }
        }
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
                            let node_id = in_flight_request.get().peer;
                            in_flight_request.remove();
                            self.peer_request_completed(node_id, false);
                        }
                        responses
                    }
                    StateSyncResponseBody::Err(e) => {
                        tracing::warn!(?from, ?response, ?e, "received error, retrying");
                        let node_id = in_flight_request.get().peer;
                        self.pending_requests
                            .insert(in_flight_request.remove().request);
                        self.peer_request_completed(node_id, true);
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

    #[must_use]
    pub fn poll(&mut self) -> RequestPollResult<PT> {
        let now = Instant::now();

        tracing::info!(
            "now {:?} in flight {:?} pending {:?}",
            now,
            self.in_flight_requests.len(),
            self.pending_requests.len()
        );

        // check all in-flight requests for timeout
        let expired_requests = self
            .in_flight_requests
            .iter()
            .filter_map(|(session_id, in_flight_request)| {
                (now - in_flight_request.last_active >= self.request_timeout).then_some(*session_id)
            })
            .collect::<Vec<_>>();
        for session_id in expired_requests {
            if let Some(in_flight_request) = self.in_flight_requests.remove(&session_id) {
                tracing::warn!(
                    ?in_flight_request.peer,
                    ?in_flight_request.request,
                    "request timed out"
                );
                let node_id = in_flight_request.peer;
                self.pending_requests.insert(in_flight_request.request);
                self.peer_request_completed(node_id, true);
            }
        }
        // check if we can immediately queue another request
        if self.in_flight_requests.len() < self.max_parallel_requests
            && !self.pending_requests.is_empty()
        {
            let mut request = self.pending_requests.pop_first().expect("!is_empty()");
            let available_peers = available_peers(&mut self.peers, now);
            if !available_peers.is_empty() {
                request.session_id = StateSyncSessionId(thread_rng().gen());
                let peer_index = available_peers.choose(&mut thread_rng()).expect("!empty");
                let peer = &mut self.peers[*peer_index];
                let session_id = request.session_id;
                let in_flight_request = InFlightRequest::new(request.clone(), peer.node_id);
                let replaced = self
                    .in_flight_requests
                    .insert(session_id, in_flight_request);
                peer.outstanding_requests += 1;
                assert!(replaced.is_none());
                return RequestPollResult::Request(peer.node_id, request);
            } else {
                tracing::warn!("no available peers for request");
            }
        }

        // find request that will timeout first
        let request_expires = self
            .in_flight_requests
            .values()
            .map(|in_flight_request| in_flight_request.last_active + self.request_timeout)
            .min();
        // find a peer with available request slots but in backoff state
        let peer_active = if self.in_flight_requests.len() < self.max_parallel_requests {
            self.peers
                .iter()
                .filter_map(|peer| {
                    (peer.outstanding_requests < MAX_PENDING_REQUESTS && peer.next_available > now)
                        .then_some(peer.next_available)
                })
                .min()
        } else {
            None
        };
        RequestPollResult::Timer(request_expires.into_iter().chain(peer_active).min())
    }
}
