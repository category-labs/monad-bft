use std::{
    collections::{BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use monad_wireauth_protocol::common::SerializedPublicKey;
use monad_wireauth_session::{Config, SessionIndex};

use crate::{InitiatorState, ResponderState, TransportState};

#[derive(Default)]
struct EstablishedSessions {
    initiator: Option<(SessionIndex, Duration)>,
    responder: Option<(SessionIndex, Duration)>,
}

impl EstablishedSessions {
    fn get_latest(&self) -> Option<SessionIndex> {
        match (&self.initiator, &self.responder) {
            (Some((id0, ts0)), Some((id1, ts1))) => {
                if ts0 >= ts1 {
                    Some(*id0)
                } else {
                    Some(*id1)
                }
            }
            (Some((id, _)), None) => Some(*id),
            (None, Some((id, _))) => Some(*id),
            (None, None) => None,
        }
    }

    fn is_empty(&self) -> bool {
        self.initiator.is_none() && self.responder.is_none()
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
    }
}

pub struct State {
    initiating_sessions: HashMap<SessionIndex, InitiatorState>,
    responding_sessions: HashMap<SessionIndex, ResponderState>,
    transport_sessions: HashMap<SessionIndex, TransportState>,
    last_established_session_by_public_key: HashMap<SerializedPublicKey, EstablishedSessions>,
    last_established_session_by_socket: HashMap<SocketAddr, EstablishedSessions>,
    allocated_indices: HashSet<SessionIndex>,
    next_session_index: SessionIndex,
    initiated_session_by_peer: HashMap<SerializedPublicKey, SessionIndex>,
    accepted_sessions_by_peer: BTreeSet<(SerializedPublicKey, SessionIndex)>,
}

impl State {
    pub fn new() -> Self {
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
        }
    }

    pub fn get_transport(&self, session_index: &SessionIndex) -> Option<&TransportState> {
        self.transport_sessions.get(session_index)
    }

    pub fn get_transport_mut(
        &mut self,
        session_index: &SessionIndex,
    ) -> Option<&mut TransportState> {
        self.transport_sessions.get_mut(session_index)
    }

    pub fn get_session_id_by_public_key(
        &self,
        public_key: &SerializedPublicKey,
    ) -> Option<SessionIndex> {
        self.last_established_session_by_public_key
            .get(public_key)
            .and_then(|sessions| sessions.get_latest())
    }

    pub fn get_session_id_by_socket(&self, socket_addr: &SocketAddr) -> Option<SessionIndex> {
        self.last_established_session_by_socket
            .get(socket_addr)
            .and_then(|sessions| sessions.get_latest())
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

    pub fn handle_established(
        &mut self,
        session_id: SessionIndex,
        transport: TransportState,
        _config: &Config,
    ) -> Vec<SessionIndex> {
        let remote_public_key = &transport.remote_public_key;
        let remote_addr = transport.remote_addr;
        let created = transport.created;
        let is_initiator = transport.is_initiator;

        let key_bytes = SerializedPublicKey::from(remote_public_key);

        let mut replaced_sessions = Vec::new();

        let sessions = self
            .last_established_session_by_public_key
            .entry(key_bytes)
            .or_default();

        if is_initiator {
            if let Some((existing_id, _)) = sessions.initiator {
                replaced_sessions.push(existing_id);
            }
            sessions.initiator = Some((session_id, created));
        } else {
            if let Some((existing_id, _)) = sessions.responder {
                replaced_sessions.push(existing_id);
            }
            sessions.responder = Some((session_id, created));
        }

        let sessions = self
            .last_established_session_by_socket
            .entry(remote_addr)
            .or_default();

        if is_initiator {
            if let Some((existing_id, _)) = sessions.initiator {
                if !replaced_sessions.contains(&existing_id) {
                    replaced_sessions.push(existing_id);
                }
            }
            sessions.initiator = Some((session_id, created));
        } else {
            if let Some((existing_id, _)) = sessions.responder {
                if !replaced_sessions.contains(&existing_id) {
                    replaced_sessions.push(existing_id);
                }
            }
            sessions.responder = Some((session_id, created));
        }

        self.transport_sessions.insert(session_id, transport);

        replaced_sessions
    }

    pub fn handle_terminate(
        &mut self,
        session_id: SessionIndex,
        remote_public_key: &SerializedPublicKey,
        remote_addr: SocketAddr,
    ) {
        let transport = self.transport_sessions.remove(&session_id);
        self.initiating_sessions.remove(&session_id);
        self.responding_sessions.remove(&session_id);
        self.allocated_indices.remove(&session_id);

        if let Some(transport) = transport {
            if let Some(sessions) = self
                .last_established_session_by_socket
                .get_mut(&remote_addr)
            {
                if transport.is_initiator {
                    if sessions.initiator.map(|(id, _)| id) == Some(session_id) {
                        sessions.initiator = None;
                    }
                } else if sessions.responder.map(|(id, _)| id) == Some(session_id) {
                    sessions.responder = None;
                }

                if sessions.is_empty() {
                    self.last_established_session_by_socket.remove(&remote_addr);
                }
            }

            if let Some(sessions) = self
                .last_established_session_by_public_key
                .get_mut(remote_public_key)
            {
                if transport.is_initiator {
                    if sessions.initiator.map(|(id, _)| id) == Some(session_id) {
                        sessions.initiator = None;
                    }
                } else if sessions.responder.map(|(id, _)| id) == Some(session_id) {
                    sessions.responder = None;
                }

                if sessions.is_empty() {
                    self.last_established_session_by_public_key
                        .remove(remote_public_key);
                }
            }
        }

        if let Some(&initiated_id) = self.initiated_session_by_peer.get(remote_public_key) {
            if initiated_id == session_id {
                self.initiated_session_by_peer.remove(remote_public_key);
            }
        }

        self.accepted_sessions_by_peer
            .remove(&(*remote_public_key, session_id));
    }

    pub fn get_initiator(&self, session_index: &SessionIndex) -> Option<&InitiatorState> {
        self.initiating_sessions.get(session_index)
    }

    pub fn get_initiator_mut(
        &mut self,
        session_index: &SessionIndex,
    ) -> Option<&mut InitiatorState> {
        self.initiating_sessions.get_mut(session_index)
    }

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
        self.initiating_sessions.remove(session_index)
    }

    pub fn remove_responder(&mut self, session_index: &SessionIndex) -> Option<ResponderState> {
        self.responding_sessions.remove(session_index)
    }

    pub fn insert_initiator(
        &mut self,
        session_index: SessionIndex,
        session: InitiatorState,
        remote_key: SerializedPublicKey,
    ) {
        self.initiating_sessions.insert(session_index, session);
        self.initiated_session_by_peer
            .insert(remote_key, session_index);
    }

    pub fn insert_responder(
        &mut self,
        session_index: SessionIndex,
        session: ResponderState,
        remote_key: SerializedPublicKey,
    ) {
        self.responding_sessions.insert(session_index, session);
        self.accepted_sessions_by_peer
            .insert((remote_key, session_index));
    }

    pub fn lookup_cookie_from_initiated_sessions(
        &self,
        remote_key: &SerializedPublicKey,
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
        remote_key: SerializedPublicKey,
    ) -> Option<[u8; 16]> {
        self.accepted_sessions_by_peer
            .range((remote_key, SessionIndex::new(0))..=(remote_key, SessionIndex::new(u32::MAX)))
            .find_map(|(_, session_id)| {
                self.responding_sessions
                    .get(session_id)
                    .and_then(|s| s.stored_cookie())
            })
    }

    pub fn get_max_timestamp(&self, remote_key: &SerializedPublicKey) -> Option<SystemTime> {
        let accepted_max = self
            .accepted_sessions_by_peer
            .range((*remote_key, SessionIndex::new(0))..=(*remote_key, SessionIndex::new(u32::MAX)))
            .filter_map(|(_, session_id)| self.responding_sessions.get(session_id))
            .filter_map(|s| s.initiator_system_time())
            .max();

        let open_max = self
            .last_established_session_by_public_key
            .get(remote_key)
            .and_then(|sessions| sessions.responder)
            .map(|(session_id, _)| session_id)
            .and_then(|session_id| self.transport_sessions.get(&session_id))
            .and_then(|s| s.initiator_system_time());

        match (accepted_max, open_max) {
            (Some(a), Some(o)) => Some(a.max(o)),
            (Some(a), None) => Some(a),
            (None, Some(o)) => Some(o),
            (None, None) => None,
        }
    }

    pub fn terminate_by_public_key(&mut self, public_key: &SerializedPublicKey) -> Vec<SocketAddr> {
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
            if let Some((session_id, _)) = sessions.initiator {
                session_ids.insert(session_id);
            }
            if let Some((session_id, _)) = sessions.responder {
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
                self.handle_terminate(session_id, public_key, addr);
                terminated_addrs.push(addr);
            }
        }

        terminated_addrs
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn reset_replay_filter(&mut self, session_id: &SessionIndex) {
        if let Some(session) = self.transport_sessions.get_mut(session_id) {
            session.reset_replay_filter();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::SystemTime,
    };

    use monad_wireauth_protocol::{common::PublicKey, crypto};
    use secp256k1::rand::rng;

    use super::*;

    fn create_dummy_hash_output() -> monad_wireauth_protocol::common::HashOutput {
        monad_wireauth_protocol::common::HashOutput([0u8; 32])
    }

    fn create_test_transport(
        session_index: SessionIndex,
        remote_public_key: &PublicKey,
        remote_addr: SocketAddr,
        is_initiator: bool,
    ) -> TransportState {
        let hash1 = create_dummy_hash_output();
        let hash2 = create_dummy_hash_output();
        let send_key = monad_wireauth_protocol::common::CipherKey::from(&hash1);
        let recv_key = monad_wireauth_protocol::common::CipherKey::from(&hash2);
        let common = monad_wireauth_session::SessionState::new(
            remote_addr,
            remote_public_key.clone(),
            session_index,
            Duration::ZERO,
            0,
            None,
            is_initiator,
        );
        TransportState::new(session_index, send_key, recv_key, common)
    }

    fn create_test_initiator(remote_public_key: &PublicKey) -> InitiatorState {
        let mut rng = rng();
        let (public_key, private_key) = crypto::generate_keypair(&mut rng).unwrap();
        let config = Config::default();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let local_index = SessionIndex::new(1);
        InitiatorState::new(
            &mut rng,
            SystemTime::now(),
            Duration::ZERO,
            &config,
            local_index,
            &private_key,
            public_key,
            remote_public_key.clone(),
            remote_addr,
            None,
            0,
        )
        .unwrap()
        .0
    }

    fn create_test_responder(
        remote_public_key: &PublicKey,
        _cookie: Option<[u8; 16]>,
    ) -> ResponderState {
        let mut rng = rng();
        let (_local_public_key, _local_private_key) = crypto::generate_keypair(&mut rng).unwrap();

        let remote_index = SessionIndex::new(42);
        let sender_index = SessionIndex::new(1);

        let hash1 = create_dummy_hash_output();
        let hash2 = create_dummy_hash_output();

        let (ephemeral_public, ephemeral_private) = crypto::generate_keypair(&mut rng).unwrap();

        let handshake_state = monad_wireauth_protocol::handshake::HandshakeState {
            hash: hash1.into(),
            chaining_key: hash2.into(),
            remote_static: Some(SerializedPublicKey::from(remote_public_key)),
            receiver_index: remote_index.as_u32(),
            sender_index: sender_index.as_u32(),
            ephemeral_private: Some(ephemeral_private),
            remote_ephemeral: Some(SerializedPublicKey::from(&ephemeral_public)),
        };

        let validated_init = monad_wireauth_session::ValidatedHandshakeInit {
            handshake_state,
            remote_public_key: remote_public_key.clone(),
            system_time: SystemTime::now(),
            remote_index,
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
        .unwrap()
        .0
    }

    #[test]
    fn test_new() {
        let state = State::new();
        assert_eq!(state.next_session_index, SessionIndex::new(0));
        assert!(state.allocated_indices.is_empty());
        assert!(state.transport_sessions.is_empty());
        assert!(state.initiating_sessions.is_empty());
        assert!(state.responding_sessions.is_empty());
    }

    #[test]
    fn test_allocate_session_index() {
        let mut state = State::new();

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
        let mut state = State::new();

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
    fn test_get_nonexistent_transport() {
        let state = State::new();
        let session_id = SessionIndex::new(42);
        assert!(state.get_transport(&session_id).is_none());
    }

    #[test]
    fn test_get_transport_mut() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);
        let config = Config::default();

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.handle_established(session_id, transport, &config);

        assert!(state.get_transport_mut(&session_id).is_some());
        assert!(state.get_transport_mut(&SessionIndex::new(999)).is_none());
    }

    #[test]
    fn test_get_transport() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);
        let config = Config::default();

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.handle_established(session_id, transport, &config);

        assert!(state.get_transport(&session_id).is_some());
        assert!(state.get_transport(&SessionIndex::new(999)).is_none());
    }

    #[test]
    fn test_get_session_id_by_public_key_empty() {
        let state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        assert!(state.get_session_id_by_public_key(&key_bytes).is_none());
    }

    #[test]
    fn test_get_session_id_by_public_key_single_initiator() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(1);
        let created = Duration::from_secs(100);

        state.last_established_session_by_public_key.insert(
            key_bytes,
            EstablishedSessions {
                initiator: Some((session_id, created)),
                responder: None,
            },
        );

        assert_eq!(
            state.get_session_id_by_public_key(&key_bytes),
            Some(session_id)
        );
    }

    #[test]
    fn test_get_session_id_by_public_key_single_responder() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(2);
        let created = Duration::from_secs(100);

        state.last_established_session_by_public_key.insert(
            key_bytes,
            EstablishedSessions {
                initiator: None,
                responder: Some((session_id, created)),
            },
        );

        assert_eq!(
            state.get_session_id_by_public_key(&key_bytes),
            Some(session_id)
        );
    }

    #[test]
    fn test_get_session_id_by_public_key_both_newer_initiator() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id_init = SessionIndex::new(1);
        let session_id_resp = SessionIndex::new(2);

        state.last_established_session_by_public_key.insert(
            key_bytes,
            EstablishedSessions {
                initiator: Some((session_id_init, Duration::from_secs(200))),
                responder: Some((session_id_resp, Duration::from_secs(100))),
            },
        );

        assert_eq!(
            state.get_session_id_by_public_key(&key_bytes),
            Some(session_id_init)
        );
    }

    #[test]
    fn test_get_session_id_by_public_key_both_newer_responder() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id_init = SessionIndex::new(1);
        let session_id_resp = SessionIndex::new(2);

        state.last_established_session_by_public_key.insert(
            key_bytes,
            EstablishedSessions {
                initiator: Some((session_id_init, Duration::from_secs(100))),
                responder: Some((session_id_resp, Duration::from_secs(200))),
            },
        );

        assert_eq!(
            state.get_session_id_by_public_key(&key_bytes),
            Some(session_id_resp)
        );
    }

    #[test]
    fn test_get_session_id_by_socket_empty() {
        let state = State::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        assert!(state.get_session_id_by_socket(&addr).is_none());
    }

    #[test]
    fn test_get_session_id_by_socket_single() {
        let mut state = State::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(5);
        let created = Duration::from_secs(100);

        state.last_established_session_by_socket.insert(
            addr,
            EstablishedSessions {
                initiator: Some((session_id, created)),
                responder: None,
            },
        );

        assert_eq!(state.get_session_id_by_socket(&addr), Some(session_id));
    }

    #[test]
    fn test_get_session_id_by_socket_both_newer_initiator() {
        let mut state = State::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id_init = SessionIndex::new(3);
        let session_id_resp = SessionIndex::new(4);

        state.last_established_session_by_socket.insert(
            addr,
            EstablishedSessions {
                initiator: Some((session_id_init, Duration::from_secs(300))),
                responder: Some((session_id_resp, Duration::from_secs(100))),
            },
        );

        assert_eq!(state.get_session_id_by_socket(&addr), Some(session_id_init));
    }

    #[test]
    fn test_insert_and_get_initiator() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(10);
        let initiator = create_test_initiator(&public_key);

        state.insert_initiator(session_id, initiator, key_bytes);

        assert!(state.get_initiator(&session_id).is_some());
        assert!(state.initiated_session_by_peer.contains_key(&key_bytes));
        assert_eq!(state.initiated_session_by_peer[&key_bytes], session_id);
    }

    #[test]
    fn test_insert_and_get_responder() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(20);
        let responder = create_test_responder(&public_key, None);

        state.insert_responder(session_id, responder, key_bytes);

        assert!(state.get_responder(&session_id).is_some());
        assert!(state
            .accepted_sessions_by_peer
            .contains(&(key_bytes, session_id)));
    }

    #[test]
    fn test_get_initiator_mut() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(10);
        let initiator = create_test_initiator(&public_key);

        state.insert_initiator(session_id, initiator, key_bytes);
        assert!(state.get_initiator_mut(&session_id).is_some());
    }

    #[test]
    fn test_get_responder_mut() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(20);
        let responder = create_test_responder(&public_key, None);

        state.insert_responder(session_id, responder, key_bytes);
        assert!(state.get_responder_mut(&session_id).is_some());
    }

    #[test]
    fn test_remove_initiator() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(10);
        let initiator = create_test_initiator(&public_key);

        state.insert_initiator(session_id, initiator, key_bytes);
        assert!(state.remove_initiator(&session_id).is_some());
        assert!(state.get_initiator(&session_id).is_none());
    }

    #[test]
    fn test_remove_responder() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let session_id = SessionIndex::new(20);
        let responder = create_test_responder(&public_key, None);

        state.insert_responder(session_id, responder, key_bytes);
        assert!(state.remove_responder(&session_id).is_some());
        assert!(state.get_responder(&session_id).is_none());
    }

    #[test]
    fn test_handle_established_initiator() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);
        let config = Config::default();

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);

        let terminated = state.handle_established(session_id, transport, &config);

        assert!(terminated.is_empty());
        assert!(state.get_transport(&session_id).is_some());
        let key_bytes = SerializedPublicKey::from(&public_key);
        assert!(state
            .last_established_session_by_public_key
            .contains_key(&key_bytes));
        assert!(state
            .last_established_session_by_socket
            .contains_key(&remote_addr));
    }

    #[test]
    fn test_handle_established_replaces_old_initiator() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let config = Config::default();

        let old_session_id = SessionIndex::new(100);
        let transport1 = create_test_transport(old_session_id, &public_key, remote_addr, true);
        state.handle_established(old_session_id, transport1, &config);

        let new_session_id = SessionIndex::new(101);
        let transport2 = create_test_transport(new_session_id, &public_key, remote_addr, true);
        let terminated = state.handle_established(new_session_id, transport2, &config);

        assert_eq!(terminated.len(), 1);
        assert_eq!(terminated[0], old_session_id);
    }

    #[test]
    fn test_handle_established_responder() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(200);
        let config = Config::default();

        let transport = create_test_transport(session_id, &public_key, remote_addr, false);

        let terminated = state.handle_established(session_id, transport, &config);

        assert!(terminated.is_empty());
        assert!(state.get_transport(&session_id).is_some());
    }

    #[test]
    fn test_handle_established_both_initiator_and_responder() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let config = Config::default();

        let init_session_id = SessionIndex::new(100);
        let transport_init = create_test_transport(init_session_id, &public_key, remote_addr, true);
        state.handle_established(init_session_id, transport_init, &config);

        let resp_session_id = SessionIndex::new(200);
        let transport_resp =
            create_test_transport(resp_session_id, &public_key, remote_addr, false);
        let terminated = state.handle_established(resp_session_id, transport_resp, &config);

        assert!(terminated.is_empty());
        assert!(state.get_transport(&init_session_id).is_some());
        assert!(state.get_transport(&resp_session_id).is_some());

        let key_bytes = SerializedPublicKey::from(&public_key);
        let sessions = state
            .last_established_session_by_public_key
            .get(&key_bytes)
            .unwrap();
        assert!(sessions.initiator.is_some());
        assert!(sessions.responder.is_some());
    }

    #[test]
    fn test_handle_terminate_removes_transport() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);
        let config = Config::default();

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.handle_established(session_id, transport, &config);

        let reservation = state.reserve_session_index().unwrap();
        reservation.commit();

        state.handle_terminate(session_id, &key_bytes, remote_addr);

        assert!(state.get_transport(&session_id).is_none());
        assert!(!state.allocated_indices.contains(&session_id));
    }

    #[test]
    fn test_handle_terminate_cleans_up_by_public_key() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);
        let config = Config::default();

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.handle_established(session_id, transport, &config);

        state.handle_terminate(session_id, &key_bytes, remote_addr);

        assert!(!state
            .last_established_session_by_public_key
            .contains_key(&key_bytes));
    }

    #[test]
    fn test_handle_terminate_preserves_other_slot() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let config = Config::default();

        let init_session_id = SessionIndex::new(100);
        let transport_init = create_test_transport(init_session_id, &public_key, remote_addr, true);
        state.handle_established(init_session_id, transport_init, &config);

        let resp_session_id = SessionIndex::new(200);
        let transport_resp =
            create_test_transport(resp_session_id, &public_key, remote_addr, false);
        state.handle_established(resp_session_id, transport_resp, &config);

        state.handle_terminate(init_session_id, &key_bytes, remote_addr);

        assert!(state
            .last_established_session_by_public_key
            .contains_key(&key_bytes));
        let sessions = state
            .last_established_session_by_public_key
            .get(&key_bytes)
            .unwrap();
        assert!(sessions.initiator.is_none());
        assert!(sessions.responder.is_some());
    }

    #[test]
    fn test_handle_terminate_cleans_up_by_socket() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);
        let config = Config::default();

        let transport = create_test_transport(session_id, &public_key, remote_addr, true);
        state.handle_established(session_id, transport, &config);

        state.handle_terminate(session_id, &key_bytes, remote_addr);

        assert!(!state
            .last_established_session_by_socket
            .contains_key(&remote_addr));
    }

    #[test]
    fn test_handle_terminate_removes_initiator() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let initiator = create_test_initiator(&public_key);
        state.insert_initiator(session_id, initiator, key_bytes);

        state.handle_terminate(session_id, &key_bytes, remote_addr);

        assert!(state.get_initiator(&session_id).is_none());
    }

    #[test]
    fn test_handle_terminate_removes_responder() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(200);

        let responder = create_test_responder(&public_key, None);
        state.insert_responder(session_id, responder, key_bytes);

        state.handle_terminate(session_id, &key_bytes, remote_addr);

        assert!(state.get_responder(&session_id).is_none());
        assert!(!state
            .accepted_sessions_by_peer
            .contains(&(key_bytes, session_id)));
    }

    #[test]
    fn test_handle_terminate_removes_initiated_session_by_peer() {
        let mut state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 51820);
        let session_id = SessionIndex::new(100);

        let initiator = create_test_initiator(&public_key);
        state.insert_initiator(session_id, initiator, key_bytes);

        state.handle_terminate(session_id, &key_bytes, remote_addr);

        assert!(!state.initiated_session_by_peer.contains_key(&key_bytes));
    }

    #[test]
    fn test_lookup_cookie_from_initiated_sessions_none() {
        let state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        assert!(state
            .lookup_cookie_from_initiated_sessions(&key_bytes)
            .is_none());
    }

    #[test]
    fn test_lookup_cookie_from_accepted_sessions_none() {
        let state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        assert!(state
            .lookup_cookie_from_accepted_sessions(key_bytes)
            .is_none());
    }

    #[test]
    fn test_get_max_timestamp_empty() {
        let state = State::new();
        let mut rng = rng();
        let (public_key, _) = crypto::generate_keypair(&mut rng).unwrap();
        let key_bytes = SerializedPublicKey::from(&public_key);
        assert!(state.get_max_timestamp(&key_bytes).is_none());
    }

    #[test]
    fn test_reserve_success_and_commit() {
        let mut state = State::new();

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
        let mut state = State::new();

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
