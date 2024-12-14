use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    time::{Duration, Instant},
};

use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{StateSyncRequest, StateSyncResponse};
use monad_types::NodeId;

pub(crate) struct OutboundRequests<PT: PubKey> {
    max_parallel_requests: usize,
    request_timeout: Duration,

    pending_requests: BTreeSet<StateSyncRequest>,
    in_flight_requests: BTreeMap<StateSyncRequest, InFlightRequest<PT>>,

    /// for each prefix, the node (if any) that all further responses must come from
    prefix_peers: HashMap<u64, NodeId<PT>>,
}

struct InFlightRequest<PT: PubKey> {
    last_active: Instant,
    // out-of-order responses indexed by response_idx
    responses: BTreeMap<u32, StateSyncResponse>,

    // next expected response index
    response_index: u32,

    // map from nonce -> num responses received
    // TODO bound size of this
    seen_nonces: HashMap<u64, usize>,

    _pd: PhantomData<PT>,
}

impl<PT: PubKey> Default for InFlightRequest<PT> {
    fn default() -> Self {
        Self {
            last_active: std::time::Instant::now(),
            responses: BTreeMap::default(),
            response_index: 0,

            seen_nonces: Default::default(),

            _pd: PhantomData,
        }
    }
}

/// Timeout after which a chunked response can get evicted
/// This can happen if one of the chunks in the (large) response gets dropped
/// Currently, the entire chunked response will be retried
const STATESYNC_CHUNKED_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

impl<PT: PubKey> InFlightRequest<PT> {
    fn apply_response(
        &mut self,
        from: &NodeId<PT>,
        response: StateSyncResponse,
    ) -> Vec<StateSyncResponse> {
        let num_nonce_seen = self.seen_nonces.entry(response.nonce).or_default();
        *num_nonce_seen += 1;
        if self
            .responses
            .values()
            .next()
            .is_some_and(|existing_response| existing_response.nonce != response.nonce)
        {
            let existing_response_nonce = self.responses.values().next().unwrap().nonce;
            if self.last_active.elapsed() > STATESYNC_CHUNKED_RESPONSE_TIMEOUT
                && num_nonce_seen == &1
            {
                tracing::debug!(
                    ?from,
                    ?response,
                    ?existing_response_nonce,
                    "resetting statesync response for existing nonce, long time elapsed since update"
                );
                self.responses.clear();
            } else {
                tracing::debug!(
                    ?from,
                    ?response,
                    ?existing_response_nonce,
                    "dropping statesync response, already fixed to different response nonce"
                );
                return Vec::new();
            }
        }

        self.last_active = std::time::Instant::now();

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
            let mut responses = vec![response];

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
                responses.push(response);
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

            prefix_peers: Default::default(),
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

    pub fn clear_prefix_peers(&mut self) {
        self.prefix_peers.clear();
    }

    pub fn queue_request(&mut self, request: StateSyncRequest) {
        //tracing::debug!(?request, "queueing request");
        if let Some(current_target) = self
            .pending_requests
            .first()
            .or(self.in_flight_requests.keys().next())
            .map(|request| request.target)
        {
            assert_eq!(current_target, request.target);
        }
        self.pending_requests.insert(request);
    }

    #[must_use]
    pub fn handle_response(
        &mut self,
        from: NodeId<PT>,
        response: StateSyncResponse,
    ) -> Vec<StateSyncResponse> {
        let maybe_prefix_peer = self.prefix_peers.get(&response.request.prefix);
        if maybe_prefix_peer.is_some_and(|prefix_peer| prefix_peer != &from) {
            tracing::debug!(
                ?from,
                ?response,
                "dropping statesync response, already fixed to different prefix_peer"
            );
            return Vec::new();
        }
        // valid request
        self.prefix_peers.insert(response.request.prefix, from);

        let Entry::Occupied(mut in_flight_request) =
            self.in_flight_requests.entry(response.request)
        else {
            tracing::debug!(
                ?from,
                ?response,
                "dropping response, request is no longer queued"
            );
            return Vec::new();
        };
        let responses = in_flight_request.get_mut().apply_response(&from, response);
        if responses.last().is_some_and(|r| r.response_n != 0) {
            // Received last response, request is complete
            in_flight_request.remove();
        }
        responses
    }

    #[must_use]
    pub async fn poll(&mut self) -> (Option<&NodeId<PT>>, StateSyncRequest) {
        // check if we can immediately queue another request
        if self.in_flight_requests.len() < self.max_parallel_requests
            && !self.pending_requests.is_empty()
        {
            let to_send = self.pending_requests.pop_first().expect("!is_empty()");
            self.in_flight_requests
                .insert(to_send, InFlightRequest::default());
            return (self.prefix_peers.get(&to_send.prefix), to_send);
        }

        // find request that will timeout first
        let Some((request, in_flight_request)) = self
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

        in_flight_request.last_active = std::time::Instant::now();
        (self.prefix_peers.get(&request.prefix), *request)
    }
}
