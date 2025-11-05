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

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    time::{Duration, Instant},
};

use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{
    StateSyncBadVersion, StateSyncRequest, StateSyncResponse, StateSyncVersion,
    SELF_STATESYNC_VERSION, STATESYNC_VERSION_MIN,
};
use monad_types::NodeId;
use rand::seq::IteratorRandom;

pub(crate) struct OutboundRequests<PT: PubKey> {
    // List of peers with their negotiated state sync version
    peers: HashMap<NodeId<PT>, StateSyncVersion>,
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
    seen_nonces: HashMap<u64, usize>,

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
            peers: peers
                .into_iter()
                .map(|peer| (peer, SELF_STATESYNC_VERSION))
                .collect(),
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
            }
        }
        responses
    }

    pub fn handle_bad_version(&mut self, from: NodeId<PT>, bad_version: StateSyncBadVersion) {
        // Cancel all requests to this peer that have version greater than maximum supported version reported in bad_version
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
        } else {
            self.peers.insert(from, bad_version.max_version);
        }
        let requests_to_remove: Vec<_> = self
            .in_flight_requests
            .iter()
            .filter(|(request, in_flight_request)| {
                in_flight_request.peer == from && request.version > bad_version.max_version
            })
            .map(|(request, _)| *request)
            .collect();

        for request in requests_to_remove {
            tracing::debug!("retrying request {:?} because of version mismatch", request);
            self.in_flight_requests.remove(&request);
            self.pending_requests.insert(request);
        }
    }

    pub fn handle_not_whitelisted(&mut self, from: NodeId<PT>) {
        tracing::debug!(
            ?from,
            "peer does not serve statesync request, removing from peer list"
        );
        self.peers.remove(&from);

        let requests_to_remove: Vec<_> = self
            .in_flight_requests
            .iter()
            .filter(|(_, in_flight_request)| in_flight_request.peer == from)
            .map(|(request, _)| *request)
            .collect();

        for request in requests_to_remove {
            self.in_flight_requests.remove(&request);
            self.pending_requests.insert(request);
        }
    }

    fn choose_peer(&self, prefix: u64) -> NodeId<PT> {
        *self
            .prefix_peers
            .get(&prefix)
            .or_else(|| self.peers.keys().choose(&mut rand::thread_rng()))
            .expect("unable to send statesync request, no peers")
    }

    // Select new peer, update version to the peer's version and insert to inflight requests
    fn insert_request(&mut self, mut to_send: StateSyncRequest) -> (NodeId<PT>, StateSyncRequest) {
        let peer = self.choose_peer(to_send.prefix);
        to_send.version = self.peers.get(&peer).copied().expect("peer not found");
        self.in_flight_requests
            .insert(to_send, InFlightRequest::new(peer));
        (peer, to_send)
    }

    #[must_use]
    pub fn poll(&mut self) -> RequestPollResult<PT> {
        // check if we can immediately queue another request
        if self.in_flight_requests.len() < self.max_parallel_requests
            && !self.pending_requests.is_empty()
        {
            let to_send = self.pending_requests.pop_first().expect("!is_empty()");
            let (peer, request) = self.insert_request(to_send);
            return RequestPollResult::Request(peer, request);
        }

        // find request that will timeout first
        let Some((request, in_flight_request)) = self
            .in_flight_requests
            .iter()
            .min_by_key(|(_, in_flight_request)| in_flight_request.last_active)
        else {
            // no outstanding requests, so yield forever
            return RequestPollResult::Timer(None);
        };

        if in_flight_request.last_active.elapsed() < self.request_timeout {
            // wait until request times out
            return RequestPollResult::Timer(Some(
                in_flight_request.last_active + self.request_timeout,
            ));
        }

        // Reinitialize request since selecting new peer may change the version
        let to_send = *request;
        self.in_flight_requests.remove(&to_send);

        let (peer, request) = self.insert_request(to_send);
        RequestPollResult::Request(peer, request)
    }
}
