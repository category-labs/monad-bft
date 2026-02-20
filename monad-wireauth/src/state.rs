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
    collections::{BTreeSet, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use monad_executor::ExecutorMetrics;

use crate::{
    metrics::MetricNames,
    protocol::tai64::Tai64N,
    session::{InitiatorState, ResponderState, SessionIndex, TransportState},
};

/// Stores up to 2 sessions per role (initiator or responder) to handle network reordering.
/// When a new session is established, the previous one is kept so that in-flight packets
/// encrypted with the old session can still be decrypted. Only when a 3rd session arrives
/// is the oldest one evicted. During planned rekey, the previous session will be evicted
/// shortly after rekey completes due to `max_session_duration` (6h5m) exceeding
/// `rekey_interval` (6h). See `tests::test_reordered_data_packet_after_reinit`.
#[derive(Default)]
struct RoleSessions {
    current: Option<(SessionIndex, Duration)>,
    previous: Option<(SessionIndex, Duration)>,
}

impl RoleSessions {
    fn push(&mut self, session_id: SessionIndex, created: Duration) -> Option<SessionIndex> {
        let evicted = self.previous.map(|(id, _)| id);
        self.previous = self.current.take();
        self.current = Some((session_id, created));
        evicted
    }

    fn remove(&mut self, session_id: SessionIndex) {
        if self.current.map(|(id, _)| id) == Some(session_id) {
            self.current = self.previous.take();
        } else if self.previous.map(|(id, _)| id) == Some(session_id) {
            self.previous = None;
        }
    }

    fn is_empty(&self) -> bool {
        self.current.is_none() && self.previous.is_none()
    }

    fn iter(&self) -> impl Iterator<Item = (SessionIndex, Duration)> + '_ {
        self.current.into_iter().chain(self.previous)
    }
}

#[derive(Default)]
struct EstablishedSessions {
    initiator: RoleSessions,
    responder: RoleSessions,
}

impl EstablishedSessions {
    fn get_latest(&self) -> Option<SessionIndex> {
        match (self.initiator.current, self.responder.current) {
            (Some((id0, ts0)), Some((id1, ts1))) => {
                if ts0 >= ts1 {
                    Some(id0)
                } else {
                    Some(id1)
                }
            }
            (Some((id, _)), None) => Some(id),
            (None, Some((id, _))) => Some(id),
            (None, None) => None,
        }
    }

    fn is_empty(&self) -> bool {
        self.initiator.is_empty() && self.responder.is_empty()
    }
}

pub(crate) struct SessionIndexReservation<'a> {
    state: &'a mut State,
    index: SessionIndex,
}

impl<'a> SessionIndexReservation<'a> {
    pub(crate) fn index(&self) -> SessionIndex {
        self.index
    }

    pub(crate) fn commit(self) {
        self.state.next_session_index = self.index;
        self.state.next_session_index.increment();
        self.state.allocated_indices.insert(self.index);
        self.state.metrics[self.state.metric_names.state_allocated_indices] =
            self.state.allocated_indices.len() as u64;
    }
}

pub struct State {
    initiating_sessions: HashMap<SessionIndex, InitiatorState>,
    responding_sessions: HashMap<SessionIndex, ResponderState>,
    transport_sessions: HashMap<SessionIndex, TransportState>,
    last_established_session_by_public_key: HashMap<monad_secp::PubKey, EstablishedSessions>,
    last_established_session_by_socket: HashMap<SocketAddr, EstablishedSessions>,
    allocated_indices: HashSet<SessionIndex>,
    next_session_index: SessionIndex,
    initiated_session_by_peer: HashMap<monad_secp::PubKey, SessionIndex>,
    accepted_sessions_by_peer: BTreeSet<(monad_secp::PubKey, SessionIndex)>,
    ip_session_counts: HashMap<IpAddr, usize>,
    total_sessions: usize,
    metrics: ExecutorMetrics,
    metric_names: &'static MetricNames,
}

impl State {
    pub fn new(metric_names: &'static MetricNames) -> Self {
        Self {
            initiating_sessions: HashMap::new(),
            responding_sessions: HashMap::new(),
            transport_sessions: HashMap::new(),
            last_established_session_by_public_key: HashMap::new(),
            last_established_session_by_socket: HashMap::new(),
            allocated_indices: HashSet::new(),
            next_session_index: SessionIndex::new(0),
            initiated_session_by_peer: HashMap::new(),
            accepted_sessions_by_peer: BTreeSet::new(),
            ip_session_counts: HashMap::new(),
            total_sessions: 0,
            metrics: ExecutorMetrics::default(),
            metric_names,
        }
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    #[cfg(test)]
    pub fn get_transport(&self, session_index: &SessionIndex) -> Option<&TransportState> {
        self.transport_sessions.get(session_index)
    }

    pub fn get_transport_mut(
        &mut self,
        session_index: &SessionIndex,
    ) -> Option<&mut TransportState> {
        self.transport_sessions.get_mut(session_index)
    }

    pub fn has_transport_by_public_key(&self, public_key: &monad_secp::PubKey) -> bool {
        self.last_established_session_by_public_key
            .get(public_key)
            .and_then(|sessions| sessions.get_latest())
            .map(|session_id| self.transport_sessions.contains_key(&session_id))
            .unwrap_or(false)
    }

    pub fn has_any_session_by_public_key(&self, public_key: &monad_secp::PubKey) -> bool {
        if self.has_transport_by_public_key(public_key) {
            return true;
        }

        if self.initiated_session_by_peer.contains_key(public_key) {
            return true;
        }

        self.accepted_sessions_by_peer
            .range((*public_key, SessionIndex::new(0))..=(*public_key, SessionIndex::new(u32::MAX)))
            .next()
            .is_some()
    }

    pub fn has_transport_by_socket(&self, socket_addr: &SocketAddr) -> bool {
        self.last_established_session_by_socket
            .get(socket_addr)
            .and_then(|sessions| sessions.get_latest())
            .map(|session_id| self.transport_sessions.contains_key(&session_id))
            .unwrap_or(false)
    }

    pub fn has_transport_by_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &monad_secp::PubKey,
    ) -> bool {
        self.last_established_session_by_socket
            .get(socket_addr)
            .and_then(|sessions| sessions.get_latest())
            .and_then(|session_id| self.transport_sessions.get(&session_id))
            .map(|transport| &transport.remote_public_key == public_key)
            .unwrap_or(false)
    }

    pub fn get_transport_by_public_key(
        &mut self,
        public_key: &monad_secp::PubKey,
    ) -> Option<&mut TransportState> {
        let session_id = self
            .last_established_session_by_public_key
            .get(public_key)
            .and_then(|sessions| sessions.get_latest())?;
        self.transport_sessions.get_mut(&session_id)
    }

    pub fn get_socket_by_public_key(&self, public_key: &monad_secp::PubKey) -> Option<SocketAddr> {
        let session_id = self
            .last_established_session_by_public_key
            .get(public_key)
            .and_then(|sessions| sessions.get_latest())?;
        self.transport_sessions
            .get(&session_id)
            .map(|t| t.remote_addr)
    }

    pub fn get_transport_by_socket(
        &mut self,
        socket_addr: &SocketAddr,
    ) -> Option<&mut TransportState> {
        let session_id = self
            .last_established_session_by_socket
            .get(socket_addr)
            .and_then(|sessions| sessions.get_latest())?;
        self.transport_sessions.get_mut(&session_id)
    }

    pub(crate) fn reserve_session_index(&mut self) -> Option<SessionIndexReservation<'_>> {
        let start_index = self.next_session_index;
        let mut candidate = self.next_session_index;

        loop {
            if !self.allocated_indices.contains(&candidate) {
                return Some(SessionIndexReservation {
                    state: self,
                    index: candidate,
                });
            }

            candidate.increment();
            if candidate == start_index {
                return None;
            }
        }
    }

    pub fn insert_transport(&mut self, session_id: SessionIndex, transport: TransportState) {
        let remote_public_key = &transport.remote_public_key;
        let remote_addr = transport.remote_addr;
        let created = transport.created;
        let is_initiator = transport.is_initiator;

        if is_initiator {
            self.metrics[self.metric_names.state_session_established_initiator] += 1;
            self.initiating_sessions.remove(&session_id);
            self.metrics[self.metric_names.state_initiating_sessions] =
                self.initiating_sessions.len() as u64;
        } else {
            self.metrics[self.metric_names.state_session_established_responder] += 1;
            self.responding_sessions.remove(&session_id);
            self.metrics[self.metric_names.state_responding_sessions] =
                self.responding_sessions.len() as u64;
        }

        let mut evicted_sessions = Vec::new();

        let sessions = self
            .last_established_session_by_public_key
            .entry(*remote_public_key)
            .or_default();

        let evicted = if is_initiator {
            sessions.initiator.push(session_id, created)
        } else {
            sessions.responder.push(session_id, created)
        };
        if let Some(evicted_id) = evicted {
            evicted_sessions.push(evicted_id);
        }
        self.metrics[self.metric_names.state_sessions_by_public_key] =
            self.last_established_session_by_public_key.len() as u64;

        let sessions = self
            .last_established_session_by_socket
            .entry(remote_addr)
            .or_default();

        let evicted = if is_initiator {
            sessions.initiator.push(session_id, created)
        } else {
            sessions.responder.push(session_id, created)
        };
        if let Some(evicted_id) = evicted {
            if !evicted_sessions.contains(&evicted_id) {
                evicted_sessions.push(evicted_id);
            }
        }
        self.metrics[self.metric_names.state_sessions_by_socket] =
            self.last_established_session_by_socket.len() as u64;

        for evicted_session_id in evicted_sessions {
            if let Some(session) = self.transport_sessions.get(&evicted_session_id) {
                let evicted_remote_public_key = session.remote_public_key;
                let evicted_remote_addr = session.remote_addr;
                self.terminate_session(
                    evicted_session_id,
                    &evicted_remote_public_key,
                    evicted_remote_addr,
                );
            }
        }

        self.transport_sessions.insert(session_id, transport);
        self.metrics[self.metric_names.state_transport_sessions] =
            self.transport_sessions.len() as u64;
    }

    pub(crate) fn terminate_session(
        &mut self,
        session_id: SessionIndex,
        remote_public_key: &monad_secp::PubKey,
        remote_addr: SocketAddr,
    ) {
        self.metrics[self.metric_names.state_session_terminated] += 1;

        if let Some(count) = self.ip_session_counts.get_mut(&remote_addr.ip()) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.ip_session_counts.remove(&remote_addr.ip());
                self.metrics[self.metric_names.state_ip_session_counts_size] =
                    self.ip_session_counts.len() as u64;
            }
        }
        self.total_sessions = self.total_sessions.saturating_sub(1);
        self.metrics[self.metric_names.state_total_sessions] = self.total_sessions as u64;

        let transport = self.transport_sessions.remove(&session_id);
        self.metrics[self.metric_names.state_transport_sessions] =
            self.transport_sessions.len() as u64;
        self.initiating_sessions.remove(&session_id);
        self.metrics[self.metric_names.state_initiating_sessions] =
            self.initiating_sessions.len() as u64;
        self.responding_sessions.remove(&session_id);
        self.metrics[self.metric_names.state_responding_sessions] =
            self.responding_sessions.len() as u64;
        self.allocated_indices.remove(&session_id);
        self.metrics[self.metric_names.state_allocated_indices] =
            self.allocated_indices.len() as u64;

        if let Some(transport) = transport {
            if let Some(sessions) = self
                .last_established_session_by_socket
                .get_mut(&remote_addr)
            {
                if transport.is_initiator {
                    sessions.initiator.remove(session_id);
                } else {
                    sessions.responder.remove(session_id);
                }

                if sessions.is_empty() {
                    self.last_established_session_by_socket.remove(&remote_addr);
                    self.metrics[self.metric_names.state_sessions_by_socket] =
                        self.last_established_session_by_socket.len() as u64;
                }
            }

            if let Some(sessions) = self
                .last_established_session_by_public_key
                .get_mut(remote_public_key)
            {
                if transport.is_initiator {
                    sessions.initiator.remove(session_id);
                } else {
                    sessions.responder.remove(session_id);
                }

                if sessions.is_empty() {
                    self.last_established_session_by_public_key
                        .remove(remote_public_key);
                    self.metrics[self.metric_names.state_sessions_by_public_key] =
                        self.last_established_session_by_public_key.len() as u64;
                }
            }
        }

        if let Some(&initiated_id) = self.initiated_session_by_peer.get(remote_public_key) {
            if initiated_id == session_id {
                self.initiated_session_by_peer.remove(remote_public_key);
                self.metrics[self.metric_names.state_initiated_session_by_peer_size] =
                    self.initiated_session_by_peer.len() as u64;
            }
        }

        self.accepted_sessions_by_peer
            .remove(&(*remote_public_key, session_id));
        self.metrics[self.metric_names.state_accepted_sessions_by_peer_size] =
            self.accepted_sessions_by_peer.len() as u64;
    }

    #[cfg(test)]
    pub fn get_initiator(&self, session_index: &SessionIndex) -> Option<&InitiatorState> {
        self.initiating_sessions.get(session_index)
    }

    pub fn get_initiator_mut(
        &mut self,
        session_index: &SessionIndex,
    ) -> Option<&mut InitiatorState> {
        self.initiating_sessions.get_mut(session_index)
    }

    #[cfg(test)]
    pub fn get_responder(&self, session_index: &SessionIndex) -> Option<&ResponderState> {
        self.responding_sessions.get(session_index)
    }

    pub fn get_responder_mut(
        &mut self,
        session_index: &SessionIndex,
    ) -> Option<&mut ResponderState> {
        self.responding_sessions.get_mut(session_index)
    }

    pub fn remove_initiator(&mut self, session_index: &SessionIndex) -> Option<InitiatorState> {
        let session = self.initiating_sessions.remove(session_index)?;
        self.metrics[self.metric_names.state_initiating_sessions] =
            self.initiating_sessions.len() as u64;
        let remote_public_key = session.remote_public_key;
        if let Some(&stored_session_index) = self.initiated_session_by_peer.get(&remote_public_key)
        {
            if stored_session_index == *session_index {
                self.initiated_session_by_peer.remove(&remote_public_key);
                self.metrics[self.metric_names.state_initiated_session_by_peer_size] =
                    self.initiated_session_by_peer.len() as u64;
            }
        }
        Some(session)
    }

    pub fn remove_responder(&mut self, session_index: &SessionIndex) -> Option<ResponderState> {
        let session = self.responding_sessions.remove(session_index)?;
        self.metrics[self.metric_names.state_responding_sessions] =
            self.responding_sessions.len() as u64;
        let remote_public_key = session.remote_public_key;
        self.accepted_sessions_by_peer
            .remove(&(remote_public_key, *session_index));
        self.metrics[self.metric_names.state_accepted_sessions_by_peer_size] =
            self.accepted_sessions_by_peer.len() as u64;
        Some(session)
    }

    pub fn insert_initiator(
        &mut self,
        session_index: SessionIndex,
        session: InitiatorState,
        remote_key: monad_secp::PubKey,
    ) {
        let remote_addr = session.remote_addr;
        self.initiating_sessions.insert(session_index, session);
        self.metrics[self.metric_names.state_initiating_sessions] =
            self.initiating_sessions.len() as u64;
        self.initiated_session_by_peer
            .insert(remote_key, session_index);
        self.metrics[self.metric_names.state_initiated_session_by_peer_size] =
            self.initiated_session_by_peer.len() as u64;
        *self.ip_session_counts.entry(remote_addr.ip()).or_insert(0) += 1;
        self.metrics[self.metric_names.state_ip_session_counts_size] =
            self.ip_session_counts.len() as u64;
        self.total_sessions += 1;
        self.metrics[self.metric_names.state_total_sessions] = self.total_sessions as u64;
        self.metrics[self.metric_names.state_session_index_allocated] += 1;
    }

    pub fn insert_responder(
        &mut self,
        session_index: SessionIndex,
        session: ResponderState,
        remote_key: monad_secp::PubKey,
    ) {
        let remote_addr = session.remote_addr;
        self.responding_sessions.insert(session_index, session);
        self.metrics[self.metric_names.state_responding_sessions] =
            self.responding_sessions.len() as u64;
        self.accepted_sessions_by_peer
            .insert((remote_key, session_index));
        self.metrics[self.metric_names.state_accepted_sessions_by_peer_size] =
            self.accepted_sessions_by_peer.len() as u64;
        *self.ip_session_counts.entry(remote_addr.ip()).or_insert(0) += 1;
        self.metrics[self.metric_names.state_ip_session_counts_size] =
            self.ip_session_counts.len() as u64;
        self.total_sessions += 1;
        self.metrics[self.metric_names.state_total_sessions] = self.total_sessions as u64;
    }

    pub fn lookup_cookie_from_initiated_sessions(
        &self,
        remote_key: &monad_secp::PubKey,
    ) -> Option<[u8; 16]> {
        self.initiated_session_by_peer
            .get(remote_key)
            .and_then(|&session_id| {
                self.initiating_sessions
                    .get(&session_id)
                    .and_then(|s| s.stored_cookie())
            })
    }

    pub fn lookup_cookie_from_accepted_sessions(
        &self,
        remote_key: monad_secp::PubKey,
    ) -> Option<[u8; 16]> {
        self.accepted_sessions_by_peer
            .range((remote_key, SessionIndex::new(0))..=(remote_key, SessionIndex::new(u32::MAX)))
            .find_map(|(_, session_id)| {
                self.responding_sessions
                    .get(session_id)
                    .and_then(|s| s.stored_cookie())
            })
    }

    pub fn get_max_timestamp(&self, remote_key: &monad_secp::PubKey) -> Option<Tai64N> {
        let accepted_max = self
            .accepted_sessions_by_peer
            .range((*remote_key, SessionIndex::new(0))..=(*remote_key, SessionIndex::new(u32::MAX)))
            .filter_map(|(_, session_id)| self.responding_sessions.get(session_id))
            .filter_map(|s| s.initiator_timestamp())
            .max();

        let open_max = self
            .last_established_session_by_public_key
            .get(remote_key)
            .and_then(|sessions| sessions.responder.current)
            .map(|(session_id, _)| session_id)
            .and_then(|session_id| self.transport_sessions.get(&session_id))
            .and_then(|s| s.initiator_timestamp());

        match (accepted_max, open_max) {
            (Some(a), Some(o)) => Some(a.max(o)),
            (Some(a), None) => Some(a),
            (None, Some(o)) => Some(o),
            (None, None) => None,
        }
    }

    pub fn terminate_by_public_key(&mut self, public_key: &monad_secp::PubKey) -> Vec<SocketAddr> {
        let mut session_ids = HashSet::new();

        if let Some(&session_id) = self.initiated_session_by_peer.get(public_key) {
            session_ids.insert(session_id);
        }

        for (key, session_id) in self
            .accepted_sessions_by_peer
            .range((*public_key, SessionIndex::new(0))..=(*public_key, SessionIndex::new(u32::MAX)))
        {
            if key == public_key {
                session_ids.insert(*session_id);
            }
        }

        if let Some(sessions) = self.last_established_session_by_public_key.get(public_key) {
            for (session_id, _) in sessions.initiator.iter() {
                session_ids.insert(session_id);
            }
            for (session_id, _) in sessions.responder.iter() {
                session_ids.insert(session_id);
            }
        }

        let mut terminated_addrs = Vec::new();

        for session_id in session_ids {
            let remote_addr = self
                .transport_sessions
                .get(&session_id)
                .map(|t| t.remote_addr)
                .or_else(|| {
                    self.initiating_sessions
                        .get(&session_id)
                        .map(|i| i.remote_addr)
                })
                .or_else(|| {
                    self.responding_sessions
                        .get(&session_id)
                        .map(|r| r.remote_addr)
                });

            if let Some(addr) = remote_addr {
                self.terminate_session(session_id, public_key, addr);
                terminated_addrs.push(addr);
            }
        }

        terminated_addrs
    }

    pub fn total_sessions(&self) -> usize {
        self.total_sessions
    }

    pub fn ip_session_count(&self, ip: &IpAddr) -> usize {
        self.ip_session_counts.get(ip).copied().unwrap_or(0)
    }
}

#[cfg(test)]
pub(crate) fn insert_test_initiator_session(
    state: &mut State,
    remote_addr: SocketAddr,
) -> SessionIndex {
    use secp256k1::rand::rng;

    use crate::config::Config;
    let mut rng = rng();
    let keypair = monad_secp::KeyPair::generate(&mut rng);
    let remote_public_key = keypair.pubkey();
    let local_keypair = monad_secp::KeyPair::generate(&mut rng);
    let config = Config::default();
    let local_index = SessionIndex::new(1);
    let (initiator, _) = InitiatorState::new(
        &mut rng,
        std::time::SystemTime::now(),
        Duration::ZERO,
        &config,
        local_index,
        &local_keypair,
        remote_public_key,
        remote_addr,
        None,
        0,
    );
    state.insert_initiator(local_index, initiator, remote_public_key);
    local_index
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::SystemTime,
    };

    use secp256k1::rand::rng;

    use super::*;
    use crate::{config::Config, metrics::DEFAULT_METRICS};

    fn create_dummy_hash_output() -> crate::protocol::common::HashOutput {
        crate::protocol::common::HashOutput([0u8; 32])
    }

    fn create_test_transport(
        session_index: SessionIndex,
        remote_public_key: &monad_secp::PubKey,
        remote_addr: SocketAddr,
        is_initiator: bool,
    ) -> TransportState {
        let hash1 = create_dummy_hash_output();
        let hash2 = create_dummy_hash_output();
        let send_key = crate::protocol::common::CipherKey::from(&hash1);
        let recv_key = crate::protocol::common::CipherKey::from(&hash2);
        let common = crate::session::SessionState::new(
            remote_addr,
            *remote_public_key,
            session_index,
            Duration::ZERO,
            0,
            None,
            is_initiator,
        );
        TransportState::new(session_index, send_key, recv_key, common)
    }

    fn create_test_initiator(remote_public_key: &monad_secp::PubKey) -> InitiatorState {
        create_test_initiator_with_addr(
            remote_public_key,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820),
        )
    }

    fn create_test_initiator_with_addr(
        remote_public_key: &monad_secp::PubKey,
        remote_addr: SocketAddr,
    ) -> InitiatorState {
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let config = Config::default();
        let local_index = SessionIndex::new(1);
        let (initiator, _) = InitiatorState::new(
            &mut rng,
            SystemTime::now(),
            Duration::ZERO,
            &config,
            local_index,
            &keypair,
            *remote_public_key,
            remote_addr,
            None,
            0,
        );
        initiator
    }

    fn create_test_responder(
        remote_public_key: &monad_secp::PubKey,
        _cookie: Option<[u8; 16]>,
    ) -> ResponderState {
        let mut rng = rng();
        let _local_keypair = monad_secp::KeyPair::generate(&mut rng);

        let remote_index = SessionIndex::new(42);
        let sender_index = SessionIndex::new(1);

        let hash1 = create_dummy_hash_output();
        let hash2 = create_dummy_hash_output();

        let ephemeral_keypair = monad_secp::KeyPair::generate(&mut rng);
        let ephemeral_public = ephemeral_keypair.pubkey();

        let handshake_state = crate::protocol::handshake::HandshakeState {
            hash: hash1.into(),
            chaining_key: hash2.into(),
            remote_static: Some(*remote_public_key),
            receiver_index: remote_index.as_u32(),
            sender_index: sender_index.as_u32(),
            ephemeral_private: Some(ephemeral_keypair),
            remote_ephemeral: Some(ephemeral_public),
        };

        let validated_init = crate::session::responder::ValidatedHandshakeInit {
            handshake_state,
            remote_public_key: *remote_public_key,
            timestamp: SystemTime::now().into(),
        };

        let config = Config::default();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 51820);
        let local_index = SessionIndex::new(2);

        ResponderState::new(
            &mut rng,
            Duration::ZERO,
            &config,
            local_index,
            None,
            validated_init,
            remote_addr,
        )
        .0
    }

    #[test]
    fn test_allocate_session_index() {
        let mut state = State::new(DEFAULT_METRICS);

        let reservation0 = state.reserve_session_index().unwrap();
        let idx0 = reservation0.index();
        reservation0.commit();

        let reservation1 = state.reserve_session_index().unwrap();
        let idx1 = reservation1.index();
        reservation1.commit();

        let reservation2 = state.reserve_session_index().unwrap();
        let idx2 = reservation2.index();
        reservation2.commit();

        assert_eq!(idx0, SessionIndex::new(0));
        assert_eq!(idx1, SessionIndex::new(1));
        assert_eq!(idx2, SessionIndex::new(2));
        assert!(state.allocated_indices.contains(&idx0));
        assert!(state.allocated_indices.contains(&idx1));
        assert!(state.allocated_indices.contains(&idx2));
    }

    #[test]
    fn test_allocate_session_index_skips_allocated() {
        let mut state = State::new(DEFAULT_METRICS);

        let reservation0 = state.reserve_session_index().unwrap();
        let idx0 = reservation0.index();
        reservation0.commit();

        state.allocated_indices.remove(&idx0);
        state.next_session_index = SessionIndex::new(0);

        let reservation1 = state.reserve_session_index().unwrap();
        let idx1 = reservation1.index();
        reservation1.commit();

        assert_eq!(idx1, SessionIndex::new(0));
    }

    #[test]
    fn test_get_transport_mut() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.insert_transport(session_id, transport);

        assert!(state.get_transport_mut(&session_id).is_some());
        assert!(state.get_transport_mut(&SessionIndex::new(999)).is_none());
    }

    #[test]
    fn test_get_transport() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.insert_transport(session_id, transport);

        assert!(state.get_transport(&session_id).is_some());
        assert!(state.get_transport(&SessionIndex::new(999)).is_none());
    }

    #[test]
    fn test_get_transport_by_public_key_empty() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        assert!(state.get_transport_by_public_key(&public_key).is_none());
    }

    #[test]
    fn test_get_transport_by_public_key_single_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(1);

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.insert_transport(session_id, transport);

        assert!(state.get_transport_by_public_key(&public_key).is_some());
    }

    #[test]
    fn test_get_transport_by_public_key_single_responder() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(2);

        let transport = create_test_transport(session_id, &public_key, remote_addr, false);
        state.insert_transport(session_id, transport);

        assert!(state.get_transport_by_public_key(&public_key).is_some());
    }

    #[test]
    fn test_get_transport_by_public_key_both_newer_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id_init = SessionIndex::new(1);
        let session_id_resp = SessionIndex::new(2);

        let mut transport_resp =
            create_test_transport(session_id_resp, &public_key, remote_addr, false);
        transport_resp.created = Duration::from_secs(100);
        state.insert_transport(session_id_resp, transport_resp);

        let mut transport_init =
            create_test_transport(session_id_init, &public_key, remote_addr, true);
        transport_init.created = Duration::from_secs(200);
        state.insert_transport(session_id_init, transport_init);

        let retrieved = state.get_transport_by_public_key(&public_key).unwrap();
        assert_eq!(retrieved.local_index, session_id_init);
    }

    #[test]
    fn test_get_transport_by_public_key_both_newer_responder() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id_init = SessionIndex::new(1);
        let session_id_resp = SessionIndex::new(2);

        let mut transport_init =
            create_test_transport(session_id_init, &public_key, remote_addr, true);
        transport_init.created = Duration::from_secs(100);
        state.insert_transport(session_id_init, transport_init);

        let mut transport_resp =
            create_test_transport(session_id_resp, &public_key, remote_addr, false);
        transport_resp.created = Duration::from_secs(200);
        state.insert_transport(session_id_resp, transport_resp);

        let retrieved = state.get_transport_by_public_key(&public_key).unwrap();
        assert_eq!(retrieved.local_index, session_id_resp);
    }

    #[test]
    fn test_get_transport_by_socket_empty() {
        let mut state = State::new(DEFAULT_METRICS);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        assert!(state.get_transport_by_socket(&addr).is_none());
    }

    #[test]
    fn test_get_transport_by_socket_single() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(5);

        let transport = create_test_transport(session_id, &public_key, addr, true);
        state.insert_transport(session_id, transport);

        assert!(state.get_transport_by_socket(&addr).is_some());
    }

    #[test]
    fn test_get_transport_by_socket_both_newer_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id_init = SessionIndex::new(3);
        let session_id_resp = SessionIndex::new(4);

        let mut transport_resp = create_test_transport(session_id_resp, &public_key, addr, false);
        transport_resp.created = Duration::from_secs(100);
        state.insert_transport(session_id_resp, transport_resp);

        let mut transport_init = create_test_transport(session_id_init, &public_key, addr, true);
        transport_init.created = Duration::from_secs(300);
        state.insert_transport(session_id_init, transport_init);

        let retrieved = state.get_transport_by_socket(&addr).unwrap();
        assert_eq!(retrieved.local_index, session_id_init);
    }

    #[test]
    fn test_insert_and_get_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let session_id = SessionIndex::new(10);
        let initiator = create_test_initiator(&public_key);
        let remote_ip = initiator.remote_addr.ip();

        assert_eq!(state.total_sessions(), 0);
        assert_eq!(state.ip_session_count(&remote_ip), 0);

        state.insert_initiator(session_id, initiator, key_bytes);

        assert!(state.get_initiator(&session_id).is_some());
        assert!(state.initiated_session_by_peer.contains_key(&key_bytes));
        assert_eq!(state.initiated_session_by_peer[&key_bytes], session_id);
        assert_eq!(state.total_sessions(), 1);
        assert_eq!(state.ip_session_count(&remote_ip), 1);
    }

    #[test]
    fn test_insert_and_get_responder() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let session_id = SessionIndex::new(20);
        let responder = create_test_responder(&public_key, None);
        let remote_ip = responder.remote_addr.ip();

        assert_eq!(state.total_sessions(), 0);
        assert_eq!(state.ip_session_count(&remote_ip), 0);

        state.insert_responder(session_id, responder, key_bytes);

        assert!(state.get_responder(&session_id).is_some());
        assert!(state
            .accepted_sessions_by_peer
            .contains(&(key_bytes, session_id)));
        assert_eq!(state.total_sessions(), 1);
        assert_eq!(state.ip_session_count(&remote_ip), 1);
    }

    #[test]
    fn test_get_initiator_mut() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let session_id = SessionIndex::new(10);
        let initiator = create_test_initiator(&public_key);

        state.insert_initiator(session_id, initiator, key_bytes);
        assert!(state.get_initiator_mut(&session_id).is_some());
    }

    #[test]
    fn test_get_responder_mut() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let session_id = SessionIndex::new(20);
        let responder = create_test_responder(&public_key, None);

        state.insert_responder(session_id, responder, key_bytes);
        assert!(state.get_responder_mut(&session_id).is_some());
    }

    #[test]
    fn test_remove_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let session_id = SessionIndex::new(10);
        let initiator = create_test_initiator(&public_key);

        state.insert_initiator(session_id, initiator, key_bytes);
        assert!(state.remove_initiator(&session_id).is_some());
        assert!(state.get_initiator(&session_id).is_none());
    }

    #[test]
    fn test_remove_responder() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let session_id = SessionIndex::new(20);
        let responder = create_test_responder(&public_key, None);

        state.insert_responder(session_id, responder, key_bytes);
        assert!(state.remove_responder(&session_id).is_some());
        assert!(state.get_responder(&session_id).is_none());
    }

    #[test]
    fn test_insert_transport_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);

        state.insert_transport(session_id, transport);

        assert!(state.get_transport(&session_id).is_some());
        let key_bytes = public_key;
        assert!(state
            .last_established_session_by_public_key
            .contains_key(&key_bytes));
        assert!(state
            .last_established_session_by_socket
            .contains_key(&remote_addr));
    }

    #[test]
    fn test_insert_transport_keeps_previous_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let remote_ip = remote_addr.ip();

        let first_session_id = SessionIndex::new(100);
        let transport1 = create_test_transport(first_session_id, &public_key, remote_addr, true);
        state.insert_transport(first_session_id, transport1);

        assert_eq!(state.total_sessions(), 0);

        let second_session_id = SessionIndex::new(101);
        let transport2 = create_test_transport(second_session_id, &public_key, remote_addr, true);
        state.insert_transport(second_session_id, transport2);

        assert!(state.get_transport(&first_session_id).is_some());
        assert!(state.get_transport(&second_session_id).is_some());
        assert_eq!(state.total_sessions(), 0);
        assert_eq!(state.ip_session_count(&remote_ip), 0);

        let third_session_id = SessionIndex::new(102);
        let transport3 = create_test_transport(third_session_id, &public_key, remote_addr, true);
        state.insert_transport(third_session_id, transport3);

        assert!(state.get_transport(&first_session_id).is_none());
        assert!(state.get_transport(&second_session_id).is_some());
        assert!(state.get_transport(&third_session_id).is_some());
    }

    #[test]
    fn test_insert_transport_responder() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(200);

        let transport = create_test_transport(session_id, &public_key, remote_addr, false);

        state.insert_transport(session_id, transport);

        assert!(state.get_transport(&session_id).is_some());
    }

    #[test]
    fn test_insert_transport_both_initiator_and_responder() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);

        let init_session_id = SessionIndex::new(100);
        let transport_init = create_test_transport(init_session_id, &public_key, remote_addr, true);
        state.insert_transport(init_session_id, transport_init);

        let resp_session_id = SessionIndex::new(200);
        let transport_resp =
            create_test_transport(resp_session_id, &public_key, remote_addr, false);
        state.insert_transport(resp_session_id, transport_resp);

        assert!(state.get_transport(&init_session_id).is_some());
        assert!(state.get_transport(&resp_session_id).is_some());

        let key_bytes = public_key;
        let sessions = state
            .last_established_session_by_public_key
            .get(&key_bytes)
            .unwrap();
        assert!(!sessions.initiator.is_empty());
        assert!(!sessions.responder.is_empty());
    }

    #[test]
    fn test_handle_terminate_removes_transport() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.insert_transport(session_id, transport);

        let reservation = state.reserve_session_index().unwrap();
        reservation.commit();

        state.terminate_session(session_id, &key_bytes, remote_addr);

        assert!(state.get_transport(&session_id).is_none());
        assert!(!state.allocated_indices.contains(&session_id));
    }

    #[test]
    fn test_handle_terminate_cleans_up_by_public_key() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.insert_transport(session_id, transport);

        state.terminate_session(session_id, &key_bytes, remote_addr);

        assert!(!state
            .last_established_session_by_public_key
            .contains_key(&key_bytes));
    }

    #[test]
    fn test_handle_terminate_preserves_other_slot() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);

        let init_session_id = SessionIndex::new(100);
        let transport_init = create_test_transport(init_session_id, &public_key, remote_addr, true);
        state.insert_transport(init_session_id, transport_init);

        let resp_session_id = SessionIndex::new(200);
        let transport_resp =
            create_test_transport(resp_session_id, &public_key, remote_addr, false);
        state.insert_transport(resp_session_id, transport_resp);

        state.terminate_session(init_session_id, &key_bytes, remote_addr);

        assert!(state
            .last_established_session_by_public_key
            .contains_key(&key_bytes));
        let sessions = state
            .last_established_session_by_public_key
            .get(&key_bytes)
            .unwrap();
        assert!(sessions.initiator.is_empty());
        assert!(!sessions.responder.is_empty());
    }

    #[test]
    fn test_handle_terminate_cleans_up_by_socket() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.insert_transport(session_id, transport);

        state.terminate_session(session_id, &key_bytes, remote_addr);

        assert!(!state
            .last_established_session_by_socket
            .contains_key(&remote_addr));
    }

    #[test]
    fn test_handle_terminate_removes_initiator() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);
        let remote_ip = remote_addr.ip();

        let initiator = create_test_initiator(&public_key);
        state.insert_initiator(session_id, initiator, key_bytes);

        assert_eq!(state.total_sessions(), 1);
        assert_eq!(state.ip_session_count(&remote_ip), 1);

        state.terminate_session(session_id, &key_bytes, remote_addr);

        assert!(state.get_initiator(&session_id).is_none());
        assert_eq!(state.total_sessions(), 0);
        assert_eq!(state.ip_session_count(&remote_ip), 0);
    }

    #[test]
    fn test_handle_terminate_removes_responder() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let session_id = SessionIndex::new(200);

        let responder = create_test_responder(&public_key, None);
        let remote_addr = responder.remote_addr;
        let remote_ip = remote_addr.ip();
        state.insert_responder(session_id, responder, key_bytes);

        assert_eq!(state.total_sessions(), 1);
        assert_eq!(state.ip_session_count(&remote_ip), 1);

        state.terminate_session(session_id, &key_bytes, remote_addr);

        assert!(state.get_responder(&session_id).is_none());
        assert!(!state
            .accepted_sessions_by_peer
            .contains(&(key_bytes, session_id)));
        assert_eq!(state.total_sessions(), 0);
        assert_eq!(state.ip_session_count(&remote_ip), 0);
    }

    #[test]
    fn test_handle_terminate_removes_initiated_session_by_peer() {
        let mut state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let initiator = create_test_initiator(&public_key);
        state.insert_initiator(session_id, initiator, key_bytes);

        state.terminate_session(session_id, &key_bytes, remote_addr);

        assert!(!state.initiated_session_by_peer.contains_key(&key_bytes));
    }

    #[test]
    fn test_lookup_cookie_from_initiated_sessions_none() {
        let state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        assert!(state
            .lookup_cookie_from_initiated_sessions(&key_bytes)
            .is_none());
    }

    #[test]
    fn test_lookup_cookie_from_accepted_sessions_none() {
        let state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        assert!(state
            .lookup_cookie_from_accepted_sessions(key_bytes)
            .is_none());
    }

    #[test]
    fn test_get_max_timestamp_empty() {
        let state = State::new(DEFAULT_METRICS);
        let mut rng = rng();
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();
        let key_bytes = public_key;
        assert!(state.get_max_timestamp(&key_bytes).is_none());
    }

    #[test]
    fn test_reserve_success_and_commit() {
        let mut state = State::new(DEFAULT_METRICS);

        let index = {
            let reservation = state.reserve_session_index().unwrap();
            reservation.index()
        };
        assert_eq!(index, SessionIndex::new(0));
        assert_eq!(state.next_session_index, SessionIndex::new(0));

        let reservation = state.reserve_session_index().unwrap();
        assert_eq!(reservation.index(), SessionIndex::new(0));
        reservation.commit();
        assert_eq!(state.next_session_index, SessionIndex::new(1));
        assert!(state.allocated_indices.contains(&SessionIndex::new(0)));

        let reservation2 = state.reserve_session_index().unwrap();
        let index2 = reservation2.index();
        assert_eq!(index2, SessionIndex::new(1));
        reservation2.commit();
        assert_eq!(state.next_session_index, SessionIndex::new(2));
        assert!(state.allocated_indices.contains(&SessionIndex::new(1)));
    }

    #[test]
    fn test_reserve_drop_without_commit() {
        let mut state = State::new(DEFAULT_METRICS);

        {
            let _reservation = state.reserve_session_index().unwrap();
            assert_eq!(state.next_session_index, SessionIndex::new(0));
        }

        assert_eq!(state.next_session_index, SessionIndex::new(0));

        let reservation2 = state.reserve_session_index().unwrap();
        let index2 = reservation2.index();
        assert_eq!(index2, SessionIndex::new(0));
        reservation2.commit();
        assert_eq!(state.next_session_index, SessionIndex::new(1));
        assert!(state.allocated_indices.contains(&SessionIndex::new(0)));
    }
}
