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
    collections::{BTreeSet, VecDeque},
    net::SocketAddr,
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_executor::{ExecutorMetrics, ExecutorMetricsChain};
use monad_secp::PubKey;
use tracing::{debug, instrument, trace, Level};

use crate::{
    context::Context,
    cookie::Cookies,
    error::{Error, Result, SessionErrorContext},
    filter::{Filter, FilterAction},
    metrics::*,
    protocol::messages::{
        ControlPacket, CookieReply, DataPacket, DataPacketHeader, HandshakeInitiation,
        HandshakeResponse, Plaintext,
    },
    session::{Config, InitiatorState, ResponderState, SessionIndex},
    state::State,
};

struct CompressedPublicKey([u8; monad_secp::COMPRESSED_PUBLIC_KEY_SIZE]);

impl From<&monad_secp::PubKey> for CompressedPublicKey {
    fn from(pubkey: &monad_secp::PubKey) -> Self {
        CompressedPublicKey(pubkey.bytes_compressed())
    }
}

impl std::fmt::Debug for CompressedPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

pub struct API<C: Context> {
    state: State,
    timers: BTreeSet<(Duration, SessionIndex)>,
    packet_queue: VecDeque<(SocketAddr, Bytes)>,
    config: Config,
    local_static_key: monad_secp::KeyPair,
    local_serialized_public: CompressedPublicKey,
    cookies: Cookies,
    filter: Filter,
    context: C,
    metrics: ExecutorMetrics,
}

impl<C: Context> API<C> {
    pub fn new(config: Config, local_static_key: monad_secp::KeyPair, mut context: C) -> Self {
        let local_static_public = local_static_key.pubkey();
        let cookies = Cookies::new(
            context.rng(),
            local_static_public,
            config.cookie_refresh_duration,
        );

        let filter = Filter::new(
            config.handshake_rate_limit,
            config.handshake_rate_reset_interval,
            config.ip_rate_limit_window,
            config.ip_history_capacity,
            config.max_sessions_per_ip,
            config.low_watermark_sessions,
            config.high_watermark_sessions,
        );
        let local_serialized_public = CompressedPublicKey::from(&local_static_public);
        debug!(local_public_key=?local_serialized_public, "initialized manager");
        Self {
            state: State::new(),
            timers: BTreeSet::new(),
            packet_queue: VecDeque::new(),
            config,
            local_static_key,
            local_serialized_public,
            cookies,
            filter,
            context,
            metrics: ExecutorMetrics::default(),
        }
    }

    pub fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default()
            .push(&self.metrics)
            .push(self.state.metrics())
            .push(self.filter.metrics())
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn next_packet(&mut self) -> Option<(SocketAddr, Bytes)> {
        self.metrics[GAUGE_WIREAUTH_API_NEXT_PACKET] += 1;
        self.packet_queue.pop_front()
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn next_deadline(&self) -> Option<Instant> {
        let session_deadline = self.timers.iter().next().map(|&(deadline, _)| deadline);

        let filter_deadline = self.filter.next_reset_time();

        let deadline = match session_deadline {
            Some(sd) => sd.min(filter_deadline),
            None => filter_deadline,
        };

        Some(
            self.context
                .convert_duration_since_start_to_deadline(deadline),
        )
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn tick(&mut self) {
        self.metrics[GAUGE_WIREAUTH_API_TICK] += 1;
        let duration_since_start = self.context.duration_since_start();

        self.filter.tick(duration_since_start);

        let expired_timers: Vec<(Duration, SessionIndex)> = self
            .timers
            .range(..=(duration_since_start, SessionIndex::MAX))
            .copied()
            .collect();

        for (duration, session_id) in expired_timers {
            self.timers.remove(&(duration, session_id));

            let tick_result = if let Some(s) = self.state.get_initiator_mut(&session_id) {
                s.tick(duration_since_start)
                    .map(|(timer, r)| (timer, None, r.rekey, Some(r.terminated)))
            } else if let Some(s) = self.state.get_responder_mut(&session_id) {
                s.tick(duration_since_start)
                    .map(|(timer, r)| (timer, None, r.rekey, Some(r.terminated)))
            } else if let Some(transport) = self.state.get_transport_mut(&session_id) {
                Some(transport.tick(&self.config, duration_since_start))
            } else {
                None
            };

            let Some((timer, message, rekey, terminated)) = tick_result else {
                continue;
            };

            if let Some(message) = message {
                self.metrics[GAUGE_WIREAUTH_ENQUEUED_KEEPALIVE] += 1;
                self.packet_queue
                    .push_back((message.remote_addr, message.header.into()));
            }

            if let Some(rekey) = rekey {
                if let Ok((new_session_index, timer, message)) = self.init_session_with_cookie(
                    rekey.remote_public_key,
                    rekey.remote_addr,
                    rekey.stored_cookie,
                    rekey.retry_attempts,
                ) {
                    self.metrics[GAUGE_WIREAUTH_ENQUEUED_HANDSHAKE_INIT] += 1;
                    self.packet_queue
                        .push_back((rekey.remote_addr, message.into()));
                    self.timers.insert((timer, new_session_index));
                }
            }

            if let Some(timer) = timer {
                self.timers.insert((timer, session_id));
            }

            if let Some(terminated) = terminated {
                self.state.terminate_session(
                    session_id,
                    &terminated.remote_public_key,
                    terminated.remote_addr,
                );
            }
        }
    }

    #[instrument(level = Level::TRACE, skip(self, remote_static_key), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn connect(
        &mut self,
        remote_static_key: monad_secp::PubKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<()> {
        self.metrics[GAUGE_WIREAUTH_API_CONNECT] += 1;
        debug!(retry_attempts, "initiating connection");
        let cookie = self
            .state
            .lookup_cookie_from_initiated_sessions(&remote_static_key);

        let (local_index, timer, message) = self
            .init_session_with_cookie(remote_static_key, remote_addr, cookie, retry_attempts)
            .map_err(|e| {
                self.metrics[GAUGE_WIREAUTH_ERROR_CONNECT] += 1;
                e
            })?;

        self.metrics[GAUGE_WIREAUTH_ENQUEUED_HANDSHAKE_INIT] += 1;
        self.packet_queue.push_back((remote_addr, message.into()));
        self.timers.insert((timer, local_index));

        Ok(())
    }

    fn init_session_with_cookie(
        &mut self,
        remote_static_key: monad_secp::PubKey,
        remote_addr: SocketAddr,
        cookie: Option<[u8; 16]>,
        retry_attempts: u64,
    ) -> Result<(SessionIndex, Duration, HandshakeInitiation)> {
        let reservation = self.state.reserve_session_index().ok_or_else(|| {
            self.metrics[GAUGE_WIREAUTH_ERROR_SESSION_EXHAUSTED] += 1;
            Error::SessionIndexExhausted
        })?;
        debug!(local_session_id=?reservation.index(), "allocating session index for new connection");
        let system_time = self.context.system_time();
        let duration_since_start = self.context.duration_since_start();
        let (session, (timer, message)) = InitiatorState::new(
            self.context.rng(),
            system_time,
            duration_since_start,
            &self.config,
            reservation.index(),
            &self.local_static_key,
            remote_static_key,
            remote_addr,
            cookie,
            retry_attempts,
        );
        let index = reservation.index();
        reservation.commit();

        self.state
            .insert_initiator(index, session, remote_static_key);

        Ok((index, timer, message))
    }

    fn check_under_load<M: crate::protocol::messages::MacMessage>(
        &mut self,
        remote_addr: SocketAddr,
        sender_index: u32,
        message: &M,
    ) -> bool {
        let duration_since_start = self.context.duration_since_start();
        let action = self.filter.apply(
            &self.state,
            remote_addr,
            duration_since_start,
            self.cookies
                .verify(&remote_addr, message, duration_since_start)
                .is_ok(),
        );

        match action {
            FilterAction::Pass => true,
            FilterAction::SendCookie => {
                debug!(?remote_addr, sender_index, "sending cookie reply");
                let reply =
                    self.cookies
                        .create(remote_addr, sender_index, message, duration_since_start);
                self.metrics[GAUGE_WIREAUTH_ENQUEUED_COOKIE_REPLY] += 1;
                self.packet_queue.push_back((remote_addr, reply.into()));
                false
            }
            FilterAction::Drop => {
                debug!(
                    ?remote_addr,
                    sender_index, "dropping packet due to rate limit"
                );
                false
            }
        }
    }

    fn accept_handshake_init(
        &mut self,
        handshake_packet: &mut HandshakeInitiation,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        crate::protocol::crypto::verify_mac1(handshake_packet, &self.local_static_key.pubkey())
            .map_err(|source| {
                self.metrics[GAUGE_WIREAUTH_ERROR_MAC1_VERIFICATION_FAILED] += 1;
                Error::Mac1VerificationFailed {
                    addr: remote_addr,
                    source,
                }
            })?;

        if !self.check_under_load(
            remote_addr,
            handshake_packet.sender_index.get(),
            handshake_packet,
        ) {
            debug!(?remote_addr, "handshake initiation dropped under load");
            return Ok(());
        }

        let duration_since_start = self.context.duration_since_start();

        let validated_init =
            ResponderState::validate_init(&self.local_static_key, handshake_packet).map_err(
                |e| {
                    self.metrics[GAUGE_WIREAUTH_ERROR_HANDSHAKE_INIT_VALIDATION] += 1;
                    e.with_addr(remote_addr)
                },
            )?;

        let remote_key = validated_init.remote_public_key;
        if self
            .state
            .get_max_timestamp(&remote_key)
            .is_some_and(|max| validated_init.system_time <= max)
        {
            self.metrics[GAUGE_WIREAUTH_ERROR_TIMESTAMP_REPLAY] += 1;
            debug!(?remote_addr, ?remote_key, "timestamp replay detected");
            return Err(Error::TimestampReplay { addr: remote_addr });
        }

        let stored_cookie = self.state.lookup_cookie_from_accepted_sessions(remote_key);

        let reservation = self.state.reserve_session_index().ok_or_else(|| {
            self.metrics[GAUGE_WIREAUTH_ERROR_SESSION_EXHAUSTED] += 1;
            Error::SessionIndexExhausted
        })?;
        let local_index = reservation.index();

        let (session, timer, message) = ResponderState::new(
            self.context.rng(),
            duration_since_start,
            &self.config,
            local_index,
            stored_cookie.as_ref(),
            validated_init,
            remote_addr,
        )
        .map_err(|e| {
            self.metrics[GAUGE_WIREAUTH_ERROR_HANDSHAKE_INIT_RESPONDER_NEW] += 1;
            e.with_addr(remote_addr)
        })?;

        reservation.commit();

        self.state
            .insert_responder(local_index, session, remote_key);

        self.metrics[GAUGE_WIREAUTH_ENQUEUED_HANDSHAKE_RESPONSE] += 1;
        self.packet_queue.push_back((remote_addr, message.into()));
        self.timers.insert((timer, local_index));

        Ok(())
    }

    fn accept_cookie(
        &mut self,
        cookie_reply: &mut CookieReply,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        let receiver_session_index = cookie_reply.receiver_index.into();

        if let Some(session) = self.state.get_initiator_mut(&receiver_session_index) {
            session.handle_cookie(cookie_reply).map_err(|e| {
                self.metrics[GAUGE_WIREAUTH_ERROR_COOKIE_REPLY] += 1;
                e.with_addr(remote_addr)
            })?;
        } else if let Some(session) = self.state.get_responder_mut(&receiver_session_index) {
            session.handle_cookie(cookie_reply).map_err(|e| {
                self.metrics[GAUGE_WIREAUTH_ERROR_COOKIE_REPLY] += 1;
                e.with_addr(remote_addr)
            })?;
        }
        Ok(())
    }

    #[instrument(level = Level::TRACE, skip(self, control), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn dispatch_control(
        &mut self,
        control: ControlPacket,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        self.metrics[GAUGE_WIREAUTH_API_DISPATCH_CONTROL] += 1;
        let result = match control {
            ControlPacket::HandshakeInitiation(handshake) => {
                debug!("processing handshake initiation");
                self.metrics[GAUGE_WIREAUTH_DISPATCH_HANDSHAKE_INIT] += 1;
                self.accept_handshake_init(handshake, remote_addr)
            }
            ControlPacket::HandshakeResponse(response) => {
                debug!("processing handshake response");
                self.metrics[GAUGE_WIREAUTH_DISPATCH_HANDSHAKE_RESPONSE] += 1;
                self.complete_handshake(response, remote_addr)
            }
            ControlPacket::CookieReply(cookie_reply) => {
                debug!("processing cookie reply");
                self.metrics[GAUGE_WIREAUTH_DISPATCH_COOKIE_REPLY] += 1;
                self.accept_cookie(cookie_reply, remote_addr)
            }
            ControlPacket::Keepalive(data_packet) => {
                trace!("processing keepalive packet");
                self.metrics[GAUGE_WIREAUTH_DISPATCH_KEEPALIVE] += 1;
                self.decrypt(data_packet, remote_addr)?;
                Ok(())
            }
        };
        if result.is_err() {
            self.metrics[GAUGE_WIREAUTH_ERROR_DISPATCH_CONTROL] += 1;
        }
        result
    }

    #[instrument(level = Level::TRACE, skip(self, data_packet), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn decrypt<'a>(
        &mut self,
        data_packet: DataPacket<'a>,
        remote_addr: SocketAddr,
    ) -> Result<(Plaintext<'a>, PubKey)> {
        self.metrics[GAUGE_WIREAUTH_API_DECRYPT] += 1;
        let receiver_index = data_packet.header().receiver_index.into();
        let nonce: u64 = data_packet.header().counter.into();
        trace!(local_session_id=?receiver_index, nonce, "decrypting data packet");

        let (timer, remote_public_key, plaintext) = if let Some(transport) =
            self.state.get_transport_mut(&receiver_index)
        {
            let duration_since_start = self.context.duration_since_start();
            let remote_addr = transport.remote_addr;
            let (timer, plaintext) = transport
                .decrypt(&self.config, duration_since_start, data_packet)
                .map_err(|e| {
                    self.metrics[GAUGE_WIREAUTH_ERROR_DECRYPT] += 1;
                    e.with_addr(remote_addr)
                })?;
            let remote_public_key = transport.remote_public_key;
            (timer, remote_public_key, plaintext)
        } else if let Some(responder) = self.state.get_responder_mut(&receiver_index) {
            let duration_since_start = self.context.duration_since_start();
            match responder.decrypt(&self.config, duration_since_start, data_packet) {
                Ok((_timer, plaintext)) => {
                    let remote_public_key = responder.transport.remote_public_key;
                    let responder = self.state.remove_responder(&receiver_index).unwrap();
                    let (transport, establish_timer) =
                        responder.establish(self.context.rng(), &self.config, duration_since_start);
                    debug!(local_session_id=?receiver_index, "responder session established");
                    self.state.insert_transport(receiver_index, transport);
                    (establish_timer, remote_public_key, plaintext)
                }
                Err(e) => {
                    self.metrics[GAUGE_WIREAUTH_ERROR_DECRYPT] += 1;
                    return Err(e.with_addr(remote_addr));
                }
            }
        } else {
            self.metrics[GAUGE_WIREAUTH_ERROR_DECRYPT] += 1;
            self.metrics[GAUGE_WIREAUTH_ERROR_SESSION_INDEX_NOT_FOUND] += 1;
            return Err(Error::SessionIndexNotFound {
                index: receiver_index,
            });
        };

        self.timers.insert((timer, receiver_index));

        Ok((plaintext, remote_public_key))
    }

    fn complete_handshake(
        &mut self,
        response: &mut HandshakeResponse,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        crate::protocol::crypto::verify_mac1(response, &self.local_static_key.pubkey()).map_err(
            |source| {
                self.metrics[GAUGE_WIREAUTH_ERROR_MAC1_VERIFICATION_FAILED] += 1;
                Error::Mac1VerificationFailed {
                    addr: remote_addr,
                    source,
                }
            },
        )?;

        if !self.check_under_load(remote_addr, response.sender_index.get(), response) {
            debug!(?remote_addr, "handshake response dropped under load");
            return Ok(());
        }

        let receiver_session_index = response.receiver_index.into();

        let initiator = self
            .state
            .get_initiator_mut(&receiver_session_index)
            .ok_or_else(|| {
                self.metrics[GAUGE_WIREAUTH_ERROR_INVALID_RECEIVER_INDEX] += 1;
                Error::InvalidReceiverIndex {
                    index: receiver_session_index,
                    addr: remote_addr,
                }
            })?;

        let validated_response = initiator
            .validate_response(&self.config, &self.local_static_key, response)
            .map_err(|e| {
                self.metrics[GAUGE_WIREAUTH_ERROR_HANDSHAKE_RESPONSE_VALIDATION] += 1;
                e.with_addr(remote_addr)
            })?;

        let initiator = self
            .state
            .remove_initiator(&receiver_session_index)
            .unwrap();

        let duration_since_start = self.context.duration_since_start();
        debug!(local_session_id=?receiver_session_index, "initiator session established");
        let (transport, timer, message) = initiator.establish(
            self.context.rng(),
            &self.config,
            duration_since_start,
            validated_response,
            remote_addr,
        );

        self.state
            .insert_transport(receiver_session_index, transport);

        self.packet_queue.push_back((remote_addr, message.into()));
        self.timers.insert((timer, receiver_session_index));

        Ok(())
    }

    #[instrument(level = Level::TRACE, skip(self, public_key, plaintext), fields(local_public_key = ?self.local_serialized_public))]
    pub fn encrypt_by_public_key(
        &mut self,
        public_key: &monad_secp::PubKey,
        plaintext: &mut [u8],
    ) -> Result<DataPacketHeader> {
        self.metrics[GAUGE_WIREAUTH_API_ENCRYPT_BY_PUBLIC_KEY] += 1;
        let transport = self
            .state
            .get_transport_by_public_key(public_key)
            .ok_or_else(|| {
                self.metrics[GAUGE_WIREAUTH_ERROR_ENCRYPT_BY_PUBLIC_KEY] += 1;
                self.metrics[GAUGE_WIREAUTH_ERROR_SESSION_NOT_FOUND] += 1;
                Error::SessionNotFound
            })?;
        let (header, timer) =
            transport.encrypt(&self.config, self.context.duration_since_start(), plaintext);
        let session_id = transport.common.local_index;
        self.timers.insert((timer, session_id));
        Ok(header)
    }

    #[instrument(level = Level::TRACE, skip(self, plaintext), fields(local_public_key = ?self.local_serialized_public, socket_addr = ?socket_addr))]
    pub fn encrypt_by_socket(
        &mut self,
        socket_addr: &SocketAddr,
        plaintext: &mut [u8],
    ) -> Result<DataPacketHeader> {
        self.metrics[GAUGE_WIREAUTH_API_ENCRYPT_BY_SOCKET] += 1;
        let transport = self
            .state
            .get_transport_by_socket(socket_addr)
            .ok_or_else(|| {
                self.metrics[GAUGE_WIREAUTH_ERROR_ENCRYPT_BY_SOCKET] += 1;
                self.metrics[GAUGE_WIREAUTH_ERROR_SESSION_NOT_ESTABLISHED_FOR_ADDRESS] += 1;
                Error::SessionNotEstablishedForAddress { addr: *socket_addr }
            })?;
        let (header, timer) =
            transport.encrypt(&self.config, self.context.duration_since_start(), plaintext);
        let session_id = transport.common.local_index;
        self.timers.insert((timer, session_id));
        Ok(header)
    }

    #[instrument(level = Level::TRACE, skip(self, public_key), fields(local_public_key = ?self.local_serialized_public))]
    pub fn disconnect(&mut self, public_key: &monad_secp::PubKey) {
        self.metrics[GAUGE_WIREAUTH_API_DISCONNECT] += 1;
        self.state.terminate_by_public_key(public_key);
    }

    pub fn is_connected_socket(&self, socket_addr: &SocketAddr) -> bool {
        self.state.has_transport_by_socket(socket_addr)
    }

    pub fn is_connected_public_key(&self, public_key: &monad_secp::PubKey) -> bool {
        self.state.has_transport_by_public_key(public_key)
    }

    pub fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &monad_secp::PubKey,
    ) -> bool {
        self.state
            .has_transport_by_socket_and_public_key(socket_addr, public_key)
    }

    pub fn get_socket_by_public_key(&self, public_key: &monad_secp::PubKey) -> Option<SocketAddr> {
        self.state.get_socket_by_public_key(public_key)
    }
}
