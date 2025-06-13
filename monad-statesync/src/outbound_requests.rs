use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    time::{Duration, Instant},
};

use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{
    SessionId, StateSyncBadVersion, StateSyncError, StateSyncRequest, StateSyncResponse,
    StateSyncVersion, SELF_STATESYNC_VERSION, STATESYNC_VERSION_MIN,
};
use monad_types::NodeId;
use rand::{seq::IteratorRandom, Rng};

use crate::ffi::MAX_SERVER_REQUESTS;

#[derive(Clone, Copy, Debug)]
struct Peer {
    version: StateSyncVersion,
    outstanding_requests: usize,
    inactive_until: Option<Instant>,
}

impl Peer {
    fn new() -> Self {
        Self {
            version: SELF_STATESYNC_VERSION,
            outstanding_requests: 0,
            inactive_until: None,
        }
    }

    fn available(&self) -> bool {
        self.outstanding_requests < MAX_SERVER_REQUESTS && self.inactive_until.is_none()
    }
}

pub(crate) struct OutboundRequests<PT: PubKey> {
    // List of peers with their negotiated state sync version
    peers: HashMap<NodeId<PT>, Peer>,

    max_parallel_requests: usize,
    request_timeout: Duration,

    pending_requests: BTreeSet<StateSyncRequest>,
    in_flight_requests: BTreeMap<StateSyncRequest, InFlightRequest<PT>>,

    /// for each prefix, the node (if any) that all further responses must come from
    prefix_peers: HashMap<u64, NodeId<PT>>,
}

struct InFlightRequest<PT: PubKey> {
    peer: NodeId<PT>,

    last_active: Instant,
    // response indexed by response_idx
    responses: BTreeMap<u32, StateSyncResponse>,

    // next expected response index
    response_index: u32,

    // map from nonce -> num responses received
    // TODO bound size of this
    seen_nonces: HashMap<SessionId, usize>,

    _pd: PhantomData<PT>,
}

impl<PT: PubKey> InFlightRequest<PT> {
    fn new(peer: NodeId<PT>) -> Self {
        Self {
            peer,
            last_active: std::time::Instant::now(),
            responses: BTreeMap::default(),
            seen_nonces: Default::default(),
            response_index: 0,

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
        let num_nonce_seen = self.seen_nonces.entry(response.session_id).or_default();
        *num_nonce_seen += 1;
        if self
            .responses
            .values()
            .next()
            .is_some_and(|existing_response| existing_response.session_id != response.session_id)
        {
            let existing_response_nonce = self.responses.values().next().unwrap().session_id;
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
        tracing::debug!(?from, ?response, "applying statesync response");
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

            // Remove consecutive responses from out-of-order queue
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

        let replaced = self.responses.insert(response.response_index, response);
        assert!(replaced.is_none(), "server sent duplicate response_index");

        Vec::new()
    }
}

pub(crate) enum RequestPollResult<PT: PubKey> {
    Request(NodeId<PT>, StateSyncRequest),
    Timer(Option<Instant>),
}

fn random_backoff() -> Duration {
    let mut rng = rand::thread_rng();
    let range = PEER_BACKOFF_MIN.as_millis()..=PEER_BACKOFF_MAX.as_millis();
    Duration::from_millis(rng.gen_range(range).try_into().unwrap())
}

const PEER_BACKOFF_MIN: Duration = Duration::from_secs(5);
const PEER_BACKOFF_MAX: Duration = Duration::from_secs(10);

impl<PT: PubKey> OutboundRequests<PT> {
    pub fn new(
        max_parallel_requests: usize,
        request_timeout: Duration,
        peers: Vec<NodeId<PT>>,
    ) -> Self {
        assert!(max_parallel_requests > 0);
        // Initialize peers with the maximum state sync version, it will be negotiated
        // down if not supported by peer.
        Self {
            peers: peers.into_iter().map(|peer| (peer, Peer::new())).collect(),

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

    pub fn clear_prefix_peers(&mut self) {
        self.prefix_peers.clear();
    }

    pub fn queue_request(&mut self, request: StateSyncRequest) {
        tracing::debug!(?request, "queueing request");
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
        if let Some(response) = responses.last() {
            if response.response_n != 0 {
                in_flight_request.remove();
                self.dec_outstanding_requests(&from);
            }
        }
        responses
    }

    fn dec_outstanding_requests(&mut self, node: &NodeId<PT>) {
        if let Some(peer) = self.peers.get_mut(node) {
            peer.outstanding_requests -= 1;
        }
    }

    fn remove_request(&mut self, request: &StateSyncRequest) {
        if let Some(in_flight_request) = self.in_flight_requests.remove(request) {
            self.dec_outstanding_requests(&in_flight_request.peer);
        }
    }

    // Cancel and retry requests that match the predicate
    fn cancel_requests(
        &mut self,
        predicate: impl Fn(&(&StateSyncRequest, &InFlightRequest<PT>)) -> bool,
        reason: &str,
    ) {
        let requests_to_remove: Vec<_> = self
            .in_flight_requests
            .iter()
            .filter(predicate)
            .map(|(k, v)| (*k, v.peer))
            .collect();

        for (request, peer) in requests_to_remove {
            tracing::warn!("retrying request {:?} peer {}, {}", request, peer, reason);
            self.remove_request(&request);
            self.pending_requests.insert(request);
        }
    }

    // Cancel all requests to this peer that have version greater than maximum supported
    // version reported in bad_version
    pub fn handle_bad_version(&mut self, from: NodeId<PT>, bad_version: StateSyncBadVersion) {
        tracing::debug!(
            ?from,
            ?bad_version,
            "peer sent bad version, cancelling requests"
        );
        // Update the peer's version to the maximum supported version
        if bad_version.max_version < STATESYNC_VERSION_MIN
            || bad_version.min_version > SELF_STATESYNC_VERSION
        {
            tracing::debug!(
                "removing peer {} from peer list, incompatible version: {:?}",
                from,
                bad_version
            );
            self.peers.remove(&from);
        } else if let Some(peer) = self.peers.get_mut(&from) {
            peer.version = bad_version.max_version;
        }

        self.cancel_requests(
            |(request, in_flight_request)| {
                in_flight_request.peer == from && request.version > bad_version.max_version
            },
            "version mismatch",
        );
    }

    pub fn handle_server_error(&mut self, from: NodeId<PT>, error: StateSyncError) {
        let Some(peer) = self.peers.get_mut(&from) else {
            tracing::debug!("peer {} not found", from);
            return;
        };

        // Schedule peer to be activated after a backoff period
        peer.inactive_until = Some(Instant::now() + random_backoff());

        // Cancel any in flight requests to this peer
        self.cancel_requests(
            |(request, in_flight_request)| {
                in_flight_request.peer == from && request.session_id == error.session_id
            },
            "peer error",
        );
    }

    // Activate peers that passed the backoff period
    pub fn activate_peers(&mut self) {
        let now = Instant::now();
        for (_, peer) in self.peers.iter_mut() {
            if let Some(inactive_until) = peer.inactive_until {
                if now >= inactive_until {
                    peer.inactive_until = None;
                }
            }
        }
    }

    /// Choose a peer to send the request to
    /// If a prefix peer is set, it will be used if available
    /// Otherwise, a random available peer will be chosen
    /// If no available peer is found, None is returned
    fn choose_peer(&self, prefix: u64) -> Option<NodeId<PT>> {
        if let Some(node) = self.prefix_peers.get(&prefix) {
            self.peers
                .get(node)
                .filter(|peer| peer.available())
                .map(|_| *node)
        } else {
            self.peers
                .iter()
                .filter(|(_, peer)| peer.available())
                .choose(&mut rand::thread_rng())
                .map(|(node, _)| *node)
        }
    }

    // Update version to the peer's version and insert to inflight requests
    fn insert_request(
        &mut self,
        node: NodeId<PT>,
        mut request: StateSyncRequest,
    ) -> StateSyncRequest {
        self.pending_requests.remove(&request);
        let peer = self.peers.get_mut(&node).expect("peer not found");
        request.version = peer.version;
        request.session_id = SessionId(rand::random());
        self.in_flight_requests
            .insert(request, InFlightRequest::new(node));
        peer.outstanding_requests += 1;
        request
    }

    fn timeout_requests(&mut self) {
        let now = Instant::now();
        let timeout = self.request_timeout;

        self.cancel_requests(
            |(_, in_flight_request)| now.duration_since(in_flight_request.last_active) > timeout,
            "timeout",
        );
    }

    #[must_use]
    pub fn poll(&mut self) -> RequestPollResult<PT> {
        self.activate_peers();

        self.timeout_requests();

        // check if we can immediately queue another request
        // must have available peer and not exceed max parallel requests
        if self.in_flight_requests.len() < self.max_parallel_requests {
            if let Some((peer, request)) = self.pending_requests.iter().find_map(|request| {
                self.choose_peer(request.prefix)
                    .map(|peer| (peer, *request))
            }) {
                return RequestPollResult::Request(peer, self.insert_request(peer, request));
            }
        }

        // Calculate the next timeout if can't send a request
        let request_timeout = self
            .in_flight_requests
            .values()
            .map(|r| r.last_active + self.request_timeout)
            .min();
        let activation_timeout = self
            .peers
            .values()
            .filter_map(|peer| peer.inactive_until)
            .min();
        RequestPollResult::Timer(request_timeout.min(activation_timeout))
    }
}
