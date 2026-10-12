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

use bytes::{Bytes, BytesMut};
use monad_executor::{ExecutorMetrics, ExecutorMetricsChain};
use monad_secp::PubKey;
use tracing::{debug, error, instrument, trace, warn, Level};
use zerocopy::IntoBytes;

use crate::{
    config::Config,
    context::Context,
    cookie::Cookies,
    error::{Error, Result},
    filter::{Filter, FilterAction, HandshakeKind},
    messages::MacMessage,
    metrics::{init_api_executor_metrics, MetricNames},
    protocol::messages::{
        ControlPacket, CookieReply, DataPacket, DataPacketHeader, HandshakeInitiation,
        HandshakeResponse, Plaintext,
    },
    session::{
        InitiatorState, RenewedTimer, ResponderState, SessionError, SessionIndex, TransportState,
    },
    state::State,
};

pub struct API<C: Context, K: AsRef<monad_secp::KeyPair> = monad_secp::KeyPair> {
    state: State,
    timers: BTreeSet<(Duration, SessionIndex)>,
    packet_queue: VecDeque<(SocketAddr, Bytes)>,
    config: Config,
    local_static_key: K,
    // Cached compressed public key to avoid recomputing when logging
    local_serialized_public: CompressedPublicKey,
    cookies: Cookies,
    filter: Filter,
    context: C,
    metrics: ExecutorMetrics,
    metric_names: &'static MetricNames,
    last_tick: Option<Duration>,
    connect_rate_counter: u64,
    connect_rate_last_reset: Duration,
}

impl<C: Context, K: AsRef<monad_secp::KeyPair>> API<C, K> {
    /// Creates a new API instance, it should be created for an individual socket.
    pub fn new(
        metric_names: &'static MetricNames,
        config: Config,
        local_static_key: K,
        mut context: C,
    ) -> Self {
        let local_static_public = local_static_key.as_ref().pubkey();
        let cookies = Cookies::new(
            context.rng(),
            local_static_public,
            config.cookie_refresh_duration,
        );

        let filter = Filter::new(
            metric_names,
            config.handshake_cookie_unverified_rate_limit,
            config.handshake_cookie_verified_rate_limit,
            config.handshake_rate_reset_interval,
            config.ip_rate_limit_window,
            config.ip_history_capacity,
            config.total_transport_sessions,
            config.max_pending_accepted_sessions,
        );
        let local_serialized_public = CompressedPublicKey::from(&local_static_public);
        debug!(local_public_key=?local_serialized_public, "initialized manager");
        Self {
            state: State::with_limits(
                metric_names,
                config.cookie_cache_capacity,
                config.total_transport_sessions,
                config.max_established_peers_per_ip,
            ),
            timers: BTreeSet::new(),
            packet_queue: VecDeque::new(),
            config,
            local_static_key,
            local_serialized_public,
            cookies,
            filter,
            context,
            metrics: init_api_executor_metrics(metric_names),
            metric_names,
            last_tick: None,
            connect_rate_counter: 0,
            connect_rate_last_reset: Duration::ZERO,
        }
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(&self.metrics)
            .push(self.state.metrics())
            .push(self.filter.metrics())
    }

    /// Returns the next packet to send over the network.
    ///
    /// Note: There are no limits for the internal queue, so it is better to use a separate
    /// queue for pacing.
    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn next_packet(&mut self) -> Option<(SocketAddr, Bytes)> {
        self.metrics.gauge(self.metric_names.api_next_packet).inc();
        let result = self.packet_queue.pop_front();
        self.metrics
            .gauge(self.metric_names.state_packet_queue_size)
            .set(self.packet_queue.len() as u64);
        result
    }

    fn enqueue_packet(&mut self, addr: SocketAddr, pkt: impl Into<Bytes>) {
        self.packet_queue.push_back((addr, pkt.into()));
        self.metrics
            .gauge(self.metric_names.state_packet_queue_size)
            .set(self.packet_queue.len() as u64);
    }

    /// Returns the next deadline.
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

    fn insert_timer(&mut self, timer: Duration, session_id: SessionIndex) {
        self.timers.insert((timer, session_id));
        self.metrics
            .gauge(self.metric_names.state_timers_size)
            .set(self.timers.len() as u64);
    }

    fn replace_timer(&mut self, timer: RenewedTimer, session_index: SessionIndex) {
        if let Some(previous) = timer.previous {
            self.timers.remove(&(previous, session_index));
        }
        self.timers.insert((timer.current, session_index));
        self.metrics
            .gauge(self.metric_names.state_timers_size)
            .set(self.timers.len() as u64);
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn tick(&mut self) {
        self.metrics.gauge(self.metric_names.api_tick).inc();
        let duration_since_start = self.context.duration_since_start();

        self.filter.tick(duration_since_start);
        let max_expired_timers_per_tick = self.config.max_expired_timers_per_tick;

        let has_expired_timer = self
            .timers
            .first()
            .is_some_and(|&(deadline, _)| deadline <= duration_since_start);
        if let Some(last_tick) = self.last_tick {
            let checked_duration = duration_since_start.saturating_sub(last_tick);
            trace!(
                checked_duration_ms = checked_duration.as_millis(),
                has_expired_timer,
                timers_size = self.timers.len(),
                "tick"
            );
        } else {
            trace!(has_expired_timer, timers_size = self.timers.len(), "tick");
        }

        let mut processed_timers = 0usize;

        while processed_timers < max_expired_timers_per_tick {
            let Some((deadline, _)) = self.timers.first().copied() else {
                break;
            };
            if deadline > duration_since_start {
                break;
            }
            let (duration, session_id) = self
                .timers
                .pop_first()
                .expect("timer disappeared after checking it exists");
            self.metrics
                .gauge(self.metric_names.state_timers_size)
                .set(self.timers.len() as u64);
            processed_timers += 1;

            if let Some(elapsed) = duration_since_start.checked_sub(duration) {
                let elapsed_ms = elapsed.as_millis();
                trace!(
                    session_id=?session_id,
                    elapsed_ms=elapsed_ms,
                    "timer triggered"
                );
                if elapsed_ms > 100 {
                    warn!(
                        session_id=?session_id,
                        elapsed_ms=elapsed_ms,
                        "deadline is too old"
                    );
                }
            } else {
                error!(
                    session_id=?session_id,
                    deadline_duration=?duration,
                    duration_since_start=?duration_since_start,
                    "deadline is in the future"
                );
            }

            let tick_result = if let Some(s) = self.state.get_initiator_mut(&session_id) {
                s.tick(duration_since_start)
                    .map(|(timer, r)| (timer, None, r.rekey, Some(r.terminated)))
            } else if let Some(s) = self.state.get_responder_mut(&session_id) {
                s.tick(duration_since_start)
                    .map(|(timer, r)| (timer, None, r.rekey, Some(r.terminated)))
            } else if let Some(transport) = self.state.get_transport_mut(&session_id) {
                Some(transport.tick(self.context.rng(), &self.config, duration_since_start))
            } else {
                None
            };

            let Some((timer, message, rekey, terminated)) = tick_result else {
                continue;
            };

            if let Some(message) = message {
                self.metrics
                    .gauge(self.metric_names.enqueued_keepalive)
                    .inc();
                self.enqueue_packet(message.remote_addr, message.header);
            }

            // TODO: Enforce the pending initiated sessions limit for retries and rekeys.
            if let Some(rekey) = rekey {
                let stored_cookie = self.state.lookup_cookie(&rekey.remote_public_key);
                if let Ok((new_session_index, timer, message)) = self.init_session_with_cookie(
                    rekey.remote_public_key,
                    rekey.remote_addr,
                    stored_cookie,
                    rekey.retry_attempts,
                ) {
                    self.metrics
                        .gauge(self.metric_names.enqueued_handshake_init)
                        .inc();
                    self.enqueue_packet(rekey.remote_addr, message);
                    self.insert_timer(timer, new_session_index);
                }
            }

            if let Some(timer) = timer {
                self.insert_timer(timer, session_id);
            }

            if let Some(terminated) = terminated {
                debug!(
                    session_id=?session_id,
                    remote_public_key=?terminated.remote_public_key,
                    remote_addr=?terminated.remote_addr,
                    "terminating session"
                );
                self.state.terminate_session(
                    session_id,
                    &terminated.remote_public_key,
                    terminated.remote_addr,
                );
            }
        }

        self.last_tick = Some(duration_since_start);
    }

    /// Initiates a connection with a peer.
    ///
    /// This will initiate a connection even if the peer is already connected.
    /// To avoid this, the caller can check for existing sessions before calling this method.
    #[instrument(level = Level::TRACE, skip(self, remote_static_key), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn connect(
        &mut self,
        remote_static_key: monad_secp::PubKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<()> {
        self.metrics.gauge(self.metric_names.api_connect).inc();
        debug!(retry_attempts, "initiating connection");

        self.check_connect_rate_limit()?;
        if self.state.pending_initiated_sessions_count()
            >= self.config.max_pending_initiated_sessions
        {
            self.metrics.gauge(self.metric_names.error_connect).inc();
            self.metrics
                .gauge(self.metric_names.error_pending_initiated_session_limit)
                .inc();
            return Err(Error::TooManyInitiatedSessions {
                limit: self.config.max_pending_initiated_sessions,
            });
        }

        let cookie = self.state.lookup_cookie(&remote_static_key);

        let (local_index, timer, message) = self
            .init_session_with_cookie(remote_static_key, remote_addr, cookie, retry_attempts)
            .inspect_err(|_| {
                self.metrics.gauge(self.metric_names.error_connect).inc();
            })?;

        self.metrics
            .gauge(self.metric_names.enqueued_handshake_init)
            .inc();
        self.enqueue_packet(remote_addr, message);
        self.insert_timer(timer, local_index);

        Ok(())
    }

    fn check_connect_rate_limit(&mut self) -> Result<()> {
        let duration_since_start = self.context.duration_since_start();
        let reset_interval = self.config.connect_rate_reset_interval;
        if duration_since_start.saturating_sub(self.connect_rate_last_reset) >= reset_interval {
            self.connect_rate_counter = 0;
            self.connect_rate_last_reset = duration_since_start;
        }

        if self.connect_rate_counter >= self.config.connect_rate_limit {
            self.metrics
                .gauge(self.metric_names.rate_limit_connect)
                .inc();
            self.metrics.gauge(self.metric_names.error_connect).inc();
            return Err(Error::ConnectRateLimited {
                limit: self.config.connect_rate_limit,
                interval: self.config.connect_rate_reset_interval,
            });
        }

        self.connect_rate_counter += 1;
        Ok(())
    }

    fn init_session_with_cookie(
        &mut self,
        remote_static_key: monad_secp::PubKey,
        remote_addr: SocketAddr,
        cookie: Option<[u8; 16]>,
        retry_attempts: u64,
    ) -> Result<(SessionIndex, Duration, HandshakeInitiation)> {
        debug!(%remote_addr, ?remote_static_key, cookie = cookie.is_some(), ?retry_attempts, "init session");

        // reservation should be committed when code is no longer fallible
        let reservation = self.state.reserve_session_index().ok_or_else(|| {
            self.metrics
                .gauge(self.metric_names.error_session_exhausted)
                .inc();
            Error::SessionIndexExhausted
        })?;
        trace!(local_session_id=?reservation.index(), "allocating session index for new connection");
        let system_time = self.context.system_time();
        let duration_since_start = self.context.duration_since_start();
        let (session, (timer, message)) = InitiatorState::new(
            self.context.rng(),
            system_time,
            duration_since_start,
            &self.config,
            reservation.index(),
            self.local_static_key.as_ref(),
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

    fn promote_transport(
        &mut self,
        session_id: SessionIndex,
        transport: TransportState,
    ) -> Result<()> {
        let remote_public_key = transport.remote_public_key;
        let remote_addr = transport.remote_addr;
        if let Err(err) = self.state.insert_transport(session_id, transport) {
            match &err {
                Error::TooManyTransportSessions { .. } => self
                    .metrics
                    .gauge(self.metric_names.error_transport_session_limit)
                    .inc(),
                Error::TooManyEstablishedPeersForIp { .. } => self
                    .metrics
                    .gauge(self.metric_names.error_established_peer_limit)
                    .inc(),
                _ => {}
            }
            self.state
                .terminate_session(session_id, &remote_public_key, remote_addr);
            return Err(err);
        }
        Ok(())
    }

    fn is_under_load(
        &mut self,
        handshake_kind: HandshakeKind,
        remote_addr: SocketAddr,
        sender_index: u32,
        message: &impl MacMessage,
    ) -> bool {
        let duration_since_start = self.context.duration_since_start();
        let action = self.filter.apply(
            &self.state,
            handshake_kind,
            remote_addr,
            duration_since_start,
            self.cookies
                .verify(remote_addr.ip(), message, duration_since_start)
                .is_ok(),
        );

        match action {
            FilterAction::Pass => true,
            FilterAction::SendCookie => {
                debug!(?remote_addr, sender_index, "sending cookie reply");
                let reply = self.cookies.create(
                    remote_addr.ip(),
                    sender_index,
                    message,
                    duration_since_start,
                );
                self.metrics
                    .gauge(self.metric_names.enqueued_cookie_reply)
                    .inc();
                self.enqueue_packet(remote_addr, reply);
                false
            }
            FilterAction::Drop => {
                self.metrics.gauge(self.metric_names.rate_limit_drop).inc();
                false
            }
        }
    }

    fn accept_handshake_init(
        &mut self,
        handshake_packet: &mut HandshakeInitiation,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        crate::protocol::crypto::verify_mac1(
            handshake_packet,
            &self.local_static_key.as_ref().pubkey(),
        )
        .inspect_err(|_| {
            self.metrics
                .gauge(self.metric_names.error_mac1_verification_failed)
                .inc();
        })?;

        if !self.is_under_load(
            HandshakeKind::Initiation,
            remote_addr,
            handshake_packet.sender_index.get(),
            handshake_packet,
        ) {
            debug!(?remote_addr, "handshake initiation dropped under load");
            return Ok(());
        }

        let duration_since_start = self.context.duration_since_start();

        let validated_init =
            ResponderState::validate_init(self.local_static_key.as_ref(), handshake_packet)
                .inspect_err(|_| {
                    self.metrics
                        .gauge(self.metric_names.error_handshake_init_validation)
                        .inc();
                })?;

        let remote_key = validated_init.remote_public_key;
        if self
            .state
            .get_max_timestamp(&remote_key)
            .is_some_and(|max| validated_init.timestamp <= max)
        {
            self.metrics
                .gauge(self.metric_names.error_timestamp_replay)
                .inc();
            debug!(?remote_addr, ?remote_key, "timestamp replay detected");
            return Err(Error::TimestampReplay);
        }

        let stored_cookie = self.state.lookup_cookie(&remote_key);

        // Reservation should be committed only when code is no longer fallible
        // TODO(dshulyak): Get rid of reservation; code was refactored to be non-fallible when index is allocated
        let reservation = self.state.reserve_session_index().ok_or_else(|| {
            self.metrics
                .gauge(self.metric_names.error_session_exhausted)
                .inc();
            Error::SessionIndexExhausted
        })?;
        let local_index = reservation.index();
        reservation.commit();

        let (session, timer, message) = ResponderState::new(
            self.context.rng(),
            duration_since_start,
            &self.config,
            local_index,
            stored_cookie.as_ref(),
            validated_init,
            remote_addr,
        );

        self.state
            .insert_responder(local_index, session, remote_key);

        self.metrics
            .gauge(self.metric_names.enqueued_handshake_response)
            .inc();
        self.enqueue_packet(remote_addr, message);
        self.insert_timer(timer, local_index);

        Ok(())
    }

    fn accept_cookie(&mut self, cookie_reply: &mut CookieReply) -> Result<()> {
        let receiver_session_index = cookie_reply.receiver_index.into();
        let stored_cookie =
            if let Some(session) = self.state.get_initiator_mut(&receiver_session_index) {
                let remote_public_key = session.remote_public_key;
                let cookie = session.handle_cookie(cookie_reply).inspect_err(|_| {
                    self.metrics
                        .gauge(self.metric_names.error_cookie_reply)
                        .inc();
                })?;
                cookie.map(|cookie| (remote_public_key, cookie))
            } else if let Some(session) = self.state.get_responder_mut(&receiver_session_index) {
                let remote_public_key = session.remote_public_key;
                let cookie = session.handle_cookie(cookie_reply).inspect_err(|_| {
                    self.metrics
                        .gauge(self.metric_names.error_cookie_reply)
                        .inc();
                })?;
                cookie.map(|cookie| (remote_public_key, cookie))
            } else {
                None
            };

        if let Some((remote_public_key, cookie)) = stored_cookie {
            // Only pending handshakes can supply a cookie, once per handshake.
            // NOTE: We accept the risk that an attacker who observes a handshake
            // can inject a forged cookie reply. Reply encryption uses public inputs;
            // a forged reply arriving first blocks the genuine reply for that handshake.
            self.state.store_cookie(remote_public_key, cookie);
        }
        Ok(())
    }

    /// Processes any control message.
    ///
    /// Note: Keepalive is a control message. For payloads with data, the caller must use the
    /// [`decrypt`](Self::decrypt) method.
    #[instrument(level = Level::TRACE, skip(self, control), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn dispatch_control(
        &mut self,
        control: ControlPacket,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        self.metrics
            .gauge(self.metric_names.api_dispatch_control)
            .inc();
        let result = match control {
            ControlPacket::HandshakeInitiation(handshake) => {
                debug!("processing handshake initiation");
                self.metrics
                    .gauge(self.metric_names.dispatch_handshake_init)
                    .inc();
                self.accept_handshake_init(handshake, remote_addr)
            }
            ControlPacket::HandshakeResponse(response) => {
                debug!("processing handshake response");
                self.metrics
                    .gauge(self.metric_names.dispatch_handshake_response)
                    .inc();
                self.complete_handshake(response, remote_addr)
            }
            ControlPacket::CookieReply(cookie_reply) => {
                debug!("processing cookie reply");
                self.metrics
                    .gauge(self.metric_names.dispatch_cookie_reply)
                    .inc();
                self.accept_cookie(cookie_reply)
            }
            ControlPacket::Keepalive(data_packet) => {
                trace!("processing keepalive packet");
                self.metrics
                    .gauge(self.metric_names.dispatch_keepalive)
                    .inc();
                self.decrypt(data_packet, remote_addr)?;
                Ok(())
            }
        };
        if result.is_err() {
            self.metrics
                .gauge(self.metric_names.error_dispatch_control)
                .inc();
        }
        result
    }

    /// Decrypts a data packet in place, returning the plaintext and the originator of the packet.
    #[instrument(level = Level::TRACE, skip(self, data_packet), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn decrypt<'a>(
        &mut self,
        data_packet: DataPacket<'a>,
        remote_addr: SocketAddr,
    ) -> Result<(Plaintext<'a>, PubKey)> {
        self.metrics.gauge(self.metric_names.api_decrypt).inc();
        let receiver_index = data_packet.header().receiver_index.into();
        let nonce: u64 = data_packet.header().nonce.into();
        trace!(local_session_id=?receiver_index, nonce, "decrypting data packet");

        let (remote_public_key, plaintext) = if let Some(transport) =
            self.state.get_transport_mut(&receiver_index)
        {
            let duration_since_start = self.context.duration_since_start();
            let (timer, plaintext) = transport
                .decrypt(&self.config, duration_since_start, data_packet)
                .inspect_err(|e| {
                    track_decrypt_error_metrics(&mut self.metrics, self.metric_names, e);
                })?;
            let remote_public_key = transport.remote_public_key;
            self.replace_timer(timer, receiver_index);
            (remote_public_key, plaintext)
        } else if let Some(responder) = self.state.get_responder_mut(&receiver_index) {
            // The session responder needs to receive at least one packet from the originator
            // to prove private key ownership. We implement this by storing the
            // responder separately until it has received that packet.
            let duration_since_start = self.context.duration_since_start();
            let decrypt_result = responder
                .decrypt(&self.config, duration_since_start, data_packet)
                .map(|(_timer, plaintext)| (plaintext, responder.transport.remote_public_key));
            match decrypt_result {
                Ok((plaintext, remote_public_key)) => {
                    // unwrap() is safe as we have &mut and it was accessed right before this line
                    let responder = self.state.remove_responder(&receiver_index).unwrap();
                    let (transport, establish_timer) =
                        responder.establish(self.context.rng(), &self.config, duration_since_start);
                    self.promote_transport(receiver_index, transport)?;
                    debug!(local_session_id=?receiver_index, "responder session established");
                    self.timers.insert((establish_timer, receiver_index));
                    self.metrics
                        .gauge(self.metric_names.state_timers_size)
                        .set(self.timers.len() as u64);
                    (remote_public_key, plaintext)
                }
                Err(e) => {
                    track_decrypt_error_metrics(&mut self.metrics, self.metric_names, &e);
                    return Err(e.into());
                }
            }
        } else {
            self.metrics.gauge(self.metric_names.error_decrypt).inc();
            self.metrics
                .gauge(self.metric_names.error_session_index_not_found)
                .inc();
            return Err(Error::SessionIndexNotFound {
                index: receiver_index,
            });
        };

        Ok((plaintext, remote_public_key))
    }

    fn complete_handshake(
        &mut self,
        response: &mut HandshakeResponse,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        crate::protocol::crypto::verify_mac1(response, &self.local_static_key.as_ref().pubkey())
            .inspect_err(|_| {
                self.metrics
                    .gauge(self.metric_names.error_mac1_verification_failed)
                    .inc();
            })?;

        if !self.is_under_load(
            HandshakeKind::Response,
            remote_addr,
            response.sender_index.get(),
            response,
        ) {
            debug!(?remote_addr, "handshake response dropped under load");
            return Ok(());
        }

        let receiver_session_index = response.receiver_index.into();

        let validated_response = {
            let initiator = self
                .state
                .get_initiator_mut(&receiver_session_index)
                .ok_or_else(|| {
                    self.metrics
                        .gauge(self.metric_names.error_session_index_not_found)
                        .inc();
                    Error::InvalidReceiverIndex {
                        index: receiver_session_index,
                    }
                })?;
            let expected_remote_addr = initiator.remote_addr;
            if remote_addr != expected_remote_addr {
                self.metrics
                    .gauge(self.metric_names.error_handshake_response_validation)
                    .inc();
                return Err(Error::HandshakeResponseAddressMismatch {
                    expected: expected_remote_addr,
                    actual: remote_addr,
                });
            }

            initiator
                .validate_response(&self.config, self.local_static_key.as_ref(), response)
                .inspect_err(|_| {
                    self.metrics
                        .gauge(self.metric_names.error_handshake_response_validation)
                        .inc();
                })?
        };

        // Remove the pending initiator only after validating the response;
        // failures after this point must not be triggerable by unauthenticated peers.
        let initiator = self
            .state
            .remove_initiator(&receiver_session_index)
            .expect("initiator was accessed above");

        let buffered_message_count = initiator.buffered_message_count();
        let duration_since_start = self.context.duration_since_start();
        let (transport, messages) = initiator.establish(
            self.context.rng(),
            &self.config,
            duration_since_start,
            validated_response,
        );
        let is_buffered = messages.is_buffered();

        self.promote_transport(receiver_session_index, transport)?;
        debug!(
            local_session_id=?receiver_session_index,
            buffered_messages=buffered_message_count,
            "initiator session established"
        );

        // Code should not be fallible after this point
        for msg in messages {
            let mut packet = BytesMut::with_capacity(DataPacketHeader::SIZE + msg.len());
            packet.resize(DataPacketHeader::SIZE, 0);
            packet.extend_from_slice(&msg);

            let transport = self
                .state
                .get_transport_mut(&receiver_session_index)
                .expect("transport was just inserted");
            let (header, timer) = transport.encrypt(
                self.context.rng(),
                &self.config,
                duration_since_start,
                &mut packet[DataPacketHeader::SIZE..],
            );
            packet[..DataPacketHeader::SIZE].copy_from_slice(header.as_bytes());

            self.replace_timer(timer, receiver_session_index);
            self.enqueue_packet(remote_addr, packet.freeze());
            if is_buffered {
                self.metrics
                    .gauge(self.metric_names.initiator_messages_sent_from_buffer)
                    .inc();
            }
        }

        Ok(())
    }

    /// Encrypts plaintext in place using the latest established session for a public key.
    #[instrument(level = Level::TRACE, skip(self, public_key, plaintext), fields(local_public_key = ?self.local_serialized_public))]
    pub fn encrypt_by_public_key(
        &mut self,
        public_key: &monad_secp::PubKey,
        plaintext: &mut [u8],
    ) -> Result<DataPacketHeader> {
        self.metrics
            .gauge(self.metric_names.api_encrypt_by_public_key)
            .inc();
        let transport = self
            .state
            .get_transport_by_public_key(public_key)
            .ok_or_else(|| {
                self.metrics
                    .gauge(self.metric_names.error_encrypt_by_public_key)
                    .inc();
                self.metrics
                    .gauge(self.metric_names.error_session_not_found)
                    .inc();
                Error::SessionNotFound
            })?;
        let duration_since_start = self.context.duration_since_start();
        let (header, timer) = transport.encrypt(
            self.context.rng(),
            &self.config,
            duration_since_start,
            plaintext,
        );
        let session_id = transport.common.local_index;
        self.replace_timer(timer, session_id);
        Ok(header)
    }

    /// Encrypts plaintext in place using the latest established session for a socket address.
    #[instrument(level = Level::TRACE, skip(self, plaintext), fields(local_public_key = ?self.local_serialized_public, socket_addr = ?socket_addr))]
    pub fn encrypt_by_socket(
        &mut self,
        socket_addr: &SocketAddr,
        plaintext: &mut [u8],
    ) -> Result<DataPacketHeader> {
        self.metrics
            .gauge(self.metric_names.api_encrypt_by_socket)
            .inc();
        let transport = self
            .state
            .get_transport_by_socket(socket_addr)
            .ok_or_else(|| {
                self.metrics
                    .gauge(self.metric_names.error_encrypt_by_socket)
                    .inc();
                Error::SessionNotEstablishedForAddress { addr: *socket_addr }
            })?;
        let duration_since_start = self.context.duration_since_start();
        let (header, timer) = transport.encrypt(
            self.context.rng(),
            &self.config,
            duration_since_start,
            plaintext,
        );
        let session_id = transport.common.local_index;
        self.replace_timer(timer, session_id);
        Ok(header)
    }

    /// Buffers a message for a peer that has an initiator session (handshake in progress).
    /// Returns Ok(()) if the message was buffered, or Err if no initiator session exists
    /// or the buffer limit would be exceeded.
    #[instrument(level = Level::TRACE, skip(self, public_key, message), fields(local_public_key = ?self.local_serialized_public))]
    pub fn buffer_message(
        &mut self,
        public_key: &monad_secp::PubKey,
        message: Bytes,
    ) -> Result<()> {
        let initiator = self
            .state
            .get_initiator_by_public_key_mut(public_key)
            .ok_or(Error::SessionNotFound)?;
        let new_size = initiator
            .buffered_bytes()
            .checked_add(message.len())
            .ok_or(Error::BufferLimitExceeded {
                size: usize::MAX,
                limit: self.config.max_buffered_bytes_per_session,
            })?;
        if new_size > self.config.max_buffered_bytes_per_session {
            return Err(Error::BufferLimitExceeded {
                size: new_size,
                limit: self.config.max_buffered_bytes_per_session,
            });
        }
        initiator.buffer_message(message);
        self.metrics
            .gauge(self.metric_names.initiator_buffered_messages)
            .inc();
        trace!(
            buffered_message_count = initiator.buffered_message_count(),
            public_key = ?CompressedPublicKey::from(public_key),
            "message buffered in initiator"
        );
        Ok(())
    }

    /// Disconnects and removes all sessions with the given public key.
    #[instrument(level = Level::TRACE, skip(self, public_key), fields(local_public_key = ?self.local_serialized_public))]
    pub fn disconnect(&mut self, public_key: &monad_secp::PubKey) {
        self.metrics.gauge(self.metric_names.api_disconnect).inc();
        self.state.terminate_by_public_key(public_key);
    }

    /// Checks if there is a session for the given socket.
    pub fn is_connected_socket(&self, socket_addr: &SocketAddr) -> bool {
        self.state.has_transport_by_socket(socket_addr)
    }

    /// Checks if there is a session for the given public key.
    pub fn is_connected_public_key(&self, public_key: &monad_secp::PubKey) -> bool {
        self.state.has_transport_by_public_key(public_key)
    }

    /// Checks if there is any session (initiated, accepted, or established) with the given public key.
    /// Returns true if a session exists in any state:
    /// - Initiated: handshake in progress from initiator side
    /// - Accepted: handshake in progress from responder side
    /// - Established: ready for data transmission
    pub fn has_any_session_by_public_key(&self, public_key: &monad_secp::PubKey) -> bool {
        self.state.has_any_session_by_public_key(public_key)
    }

    /// Checks if there is an initiator session for the given public key.
    pub fn has_initiator_session_by_public_key(&self, public_key: &monad_secp::PubKey) -> bool {
        self.state.has_initiator_session_by_public_key(public_key)
    }

    pub fn has_initiator_session_by_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &monad_secp::PubKey,
    ) -> bool {
        self.state
            .has_initiator_session_by_socket_and_public_key(socket_addr, public_key)
    }

    pub fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &monad_secp::PubKey,
    ) -> bool {
        self.state
            .has_transport_by_socket_and_public_key(socket_addr, public_key)
    }

    /// Returns the socket address of the latest initiated session with the given public key.
    pub fn get_socket_by_public_key(&self, public_key: &monad_secp::PubKey) -> Option<SocketAddr> {
        self.state.get_socket_by_public_key(public_key)
    }
}

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

fn track_decrypt_error_metrics(
    metrics: &mut ExecutorMetrics,
    metric_names: &'static MetricNames,
    e: &SessionError,
) {
    metrics.gauge(metric_names.error_decrypt).inc();
    match e {
        SessionError::NonceOutsideWindow { .. } => {
            metrics
                .gauge(metric_names.error_decrypt_nonce_outside_window)
                .inc();
        }
        SessionError::NonceDuplicate { .. } => {
            metrics
                .gauge(metric_names.error_decrypt_nonce_duplicate)
                .inc();
        }
        SessionError::InvalidMac(_) => {
            metrics.gauge(metric_names.error_decrypt_mac).inc();
        }
        _ => {
            warn!(error=?e, "unexpected decrypt error variant");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use secp256k1::rand::rng;

    use super::*;
    use crate::{
        protocol::{
            cookies, crypto, handshake,
            messages::{Packet, TYPE_COOKIE_REPLY, TYPE_HANDSHAKE_RESPONSE},
        },
        TestContext, DEFAULT_METRICS,
    };

    fn dispatch(
        api: &mut API<TestContext>,
        packet: &(impl IntoBytes + zerocopy::Immutable),
        addr: SocketAddr,
    ) {
        let mut bytes = packet.as_bytes().to_vec();
        let Packet::Control(control) = Packet::try_from(bytes.as_mut_slice()).unwrap() else {
            panic!("expected control packet");
        };
        api.dispatch_control(control, addr).unwrap();
    }

    fn cookie_reply(public_key: &PubKey, sender_index: u32, mac1: &[u8; 16]) -> CookieReply {
        cookies::send_cookie_reply(
            &[1; 32],
            u128::from(sender_index),
            public_key,
            sender_index,
            mac1,
            &[2; 16],
        )
    }

    #[rstest::rstest]
    #[case::defaults(Config::default(), true)]
    #[case::small_cache(Config { cookie_cache_capacity: 1_024, ..Config::default() }, false)]
    #[case::unverified_flood_session_limit(
        Config { handshake_cookie_unverified_rate_limit: 30_000, ..Config::default() }, true
    )]
    #[case::unverified_flood_eviction(
        Config {
            handshake_cookie_unverified_rate_limit: 30_000,
            max_pending_accepted_sessions: 300_000,
            ..Config::default()
        }, false
    )]
    #[case::verified_flood_eviction(
        Config {
            handshake_cookie_verified_rate_limit: 30_000,
            max_pending_accepted_sessions: 300_000,
            ..Config::default()
        }, false
    )]
    #[case::outbound_flood_session_limit(
        Config { connect_rate_limit: 30_000, ..Config::default() }, true
    )]
    #[case::outbound_flood_eviction(
        Config {
            connect_rate_limit: 30_000,
            max_pending_initiated_sessions: 300_000,
            ..Config::default()
        }, false
    )]
    fn cookie_retention_under_api_flood(#[case] mut config: Config, #[case] retained: bool) {
        // Fix the workload independently of the configuration. Unsafe defaults must
        // expose actual eviction, rather than also reducing the generated traffic.
        const ATTACK_PEERS: usize = 256_001;
        let mut rng = rng();
        config.session_timeout_jitter = Duration::ZERO;
        let context = TestContext::new();
        let local_key = monad_secp::KeyPair::generate(&mut rng);
        let local_public = local_key.pubkey();
        let mut api = API::new(DEFAULT_METRICS, config.clone(), local_key, context.clone());
        let local_addr: SocketAddr = "192.0.2.1:9000".parse().unwrap();
        let honest_addr: SocketAddr = "192.0.2.2:9000".parse().unwrap();

        // Start with a full cache, so this tests eviction pressure rather than unused
        // space. Only this historical-cache fixture bypasses the API. All handshakes,
        // cookie deliveries, rate-limit resets and the honest retry below use the API.
        // Share only immutable attacker identities across cases to avoid regenerating
        // hundreds of thousands of keys. Each case has independent API/cache state.
        static ATTACKERS: std::sync::OnceLock<Vec<monad_secp::KeyPair>> =
            std::sync::OnceLock::new();
        let attackers = ATTACKERS.get_or_init(|| {
            (0..ATTACK_PEERS)
                .map(|_| monad_secp::KeyPair::generate(&mut rng))
                .collect()
        });
        for index in 0..config.cookie_cache_capacity {
            let public = attackers.get(index).map_or_else(
                || monad_secp::KeyPair::generate(&mut rng).pubkey(),
                |key| key.pubkey(),
            );
            api.state.store_cookie(public, [0; 16]);
        }

        // Obtain the honest cookie from a real peer that requires cookies. Begin just
        // before a rate-limit reset to exercise the extra boundary burst in 10 seconds.
        context.advance_time(Duration::from_millis(900));
        let honest_context = TestContext::new();
        let honest_key = monad_secp::KeyPair::generate(&mut rng);
        let honest_public = honest_key.pubkey();
        let mut honest = API::new(
            DEFAULT_METRICS,
            Config {
                handshake_cookie_unverified_rate_limit: 0,
                ..config.clone()
            },
            honest_key,
            honest_context.clone(),
        );
        api.connect(honest_public, honest_addr, 1).unwrap();
        let (_, init_bytes) = api.next_packet().unwrap();
        let init = <&HandshakeInitiation>::try_from(init_bytes.as_ref()).unwrap();
        dispatch(&mut honest, init, local_addr);
        let (_, reply_bytes) = honest.next_packet().unwrap();
        let reply = <&CookieReply>::try_from(reply_bytes.as_ref()).unwrap();
        let honest_cookie =
            cookies::accept_cookie_reply(&honest_public, &mut reply.clone(), init.mac1.as_ref())
                .unwrap();
        dispatch(&mut api, reply, honest_addr);
        let cookie_received_at = context.duration_since_start();

        let mut cookie_deliveries = 0;
        let mut unsolicited_deliveries = 0;
        let mut replay_deliveries = 0;
        let mut saved_replies = Vec::new();
        let mut attempted_peers = 0;
        let batch_size = attackers.len().div_ceil(11);
        for (batch, peers) in attackers.chunks(batch_size).enumerate() {
            if batch > 0 {
                context.advance_time(if batch == 1 {
                    Duration::from_millis(100)
                } else {
                    Duration::from_secs(1)
                });
                api.tick();
                assert!(
                    api.next_packet().is_none(),
                    "no session is due to retry yet"
                );
            }
            for key in peers {
                attempted_peers += 1;
                let public = key.pubkey();
                // Separate IPs avoid the per-IP limiter hiding the global limits.
                let addr =
                    SocketAddr::from((Ipv4Addr::from(0x0a00_0000 + attempted_peers as u32), 9000));
                let (mut initiation, _) = handshake::send_handshake_init(
                    &mut rng,
                    context.system_time(),
                    attempted_peers as u32,
                    key,
                    &local_public,
                    None,
                );
                dispatch(&mut api, &initiation, addr);
                let mut response_bytes = api.next_packet().map(|(_, bytes)| bytes);
                if let Some(bytes) = response_bytes
                    .as_ref()
                    .filter(|bytes| bytes[0] == TYPE_COOKIE_REPLY)
                {
                    // Retry with the cookie actually issued by the API. This exercises
                    // the verified budget as well as the unverified admission budget.
                    let mut challenge = <&CookieReply>::try_from(bytes.as_ref()).unwrap().clone();
                    let cookie = cookies::accept_cookie_reply(
                        &local_public,
                        &mut challenge,
                        initiation.mac1.as_ref(),
                    )
                    .unwrap();
                    let cookie_key =
                        crate::hash!(crypto::LABEL_COOKIE, &local_public.bytes_compressed());
                    initiation.mac2 =
                        crate::keyed_hash!(cookie_key.as_ref(), initiation.mac2_input(), &cookie)
                            .into();
                    dispatch(&mut api, &initiation, addr);
                    response_bytes = api.next_packet().map(|(_, bytes)| bytes);
                }
                if let Some(bytes) = response_bytes {
                    let response = <&HandshakeResponse>::try_from(bytes.as_ref()).unwrap();
                    let reply =
                        cookie_reply(&public, response.sender_index.get(), response.mac1.as_ref());
                    dispatch(&mut api, &reply, addr);
                    cookie_deliveries += 1;
                    saved_replies.push((addr, reply));
                }

                // Try the initiator-side path too. Even if a caller requests connects
                // for every attacker, the API must enforce its outbound budget.
                match api.connect(public, addr, 0) {
                    Ok(()) => {
                        let (_, bytes) = api.next_packet().unwrap();
                        let init = <&HandshakeInitiation>::try_from(bytes.as_ref()).unwrap();
                        let reply =
                            cookie_reply(&public, init.sender_index.get(), init.mac1.as_ref());
                        dispatch(&mut api, &reply, addr);
                        cookie_deliveries += 1;
                        saved_replies.push((addr, reply));
                    }
                    Err(
                        Error::ConnectRateLimited { .. } | Error::TooManyInitiatedSessions { .. },
                    ) => {}
                    Err(error) => panic!("unexpected connect failure: {error}"),
                }

                // Every identity also sends an unsolicited encrypted cookie. These
                // receiver indices are far above the indices allocated in this test.
                let unsolicited = cookie_reply(
                    &public,
                    u32::MAX - attempted_peers as u32,
                    initiation.mac1.as_ref(),
                );
                dispatch(&mut api, &unsolicited, addr);
                cookie_deliveries += 1;
                unsolicited_deliveries += 1;

                // Replay valid replies from both handshake directions; these should
                // neither add entries nor refresh the previously accepted cookies.
                if !saved_replies.is_empty() {
                    let (replay_addr, replay) =
                        &saved_replies[attempted_peers % saved_replies.len()];
                    dispatch(&mut api, replay, *replay_addr);
                    cookie_deliveries += 1;
                    replay_deliveries += 1;
                }
                assert!(api.next_packet().is_none());
            }
        }
        assert_eq!(attempted_peers, ATTACK_PEERS);
        assert!(cookie_deliveries >= ATTACK_PEERS);
        assert_eq!(unsolicited_deliveries, attempted_peers);
        assert!(replay_deliveries > 0);
        assert!(context.duration_since_start() - cookie_received_at < config.session_timeout);

        // Do not look up the honest cookie while flooding: that would refresh its
        // recency and mask eviction. Check the actual retry packet at exactly 10s.
        context.advance_time(Duration::from_millis(900));
        honest_context.advance_time(config.session_timeout);
        assert_eq!(
            context.duration_since_start() - cookie_received_at,
            config.session_timeout
        );
        api.tick();
        let (addr, retry_bytes) = api.next_packet().expect("honest handshake must retry");
        assert_eq!(addr, honest_addr);
        let retry = <&HandshakeInitiation>::try_from(retry_bytes.as_ref()).unwrap();
        assert_eq!(
            crypto::verify_mac2(retry, &honest_public, &honest_cookie).is_ok(),
            retained,
            "unexpected honest-cookie retention after the API flood"
        );
        dispatch(&mut honest, retry, local_addr);
        let (_, response_bytes) = honest.next_packet().unwrap();
        if !retained {
            assert_eq!(
                response_bytes[0], TYPE_COOKIE_REPLY,
                "eviction must require a fresh cookie"
            );
            return;
        }
        assert_eq!(response_bytes[0], TYPE_HANDSHAKE_RESPONSE);
        let response = <&HandshakeResponse>::try_from(response_bytes.as_ref()).unwrap();
        // The attack may have exhausted the local unverified budget; use its next
        // reset to complete the handshake without another cookie round trip.
        context.advance_time(Duration::from_millis(100));
        api.tick();
        dispatch(&mut api, response, honest_addr);
        assert!(api.is_connected_public_key(&honest_public));
    }

    #[rstest::rstest]
    #[case(1)]
    #[case(2)]
    fn configured_cookie_cache_capacity_is_respected(#[case] capacity: usize) {
        let mut rng = rng();
        let mut api = API::new(
            DEFAULT_METRICS,
            Config {
                cookie_cache_capacity: capacity,
                ..Config::default()
            },
            monad_secp::KeyPair::generate(&mut rng),
            TestContext::new(),
        );
        let addr: SocketAddr = "192.0.2.2:9000".parse().unwrap();
        let peers: Vec<_> = (0..=capacity)
            .map(|_| monad_secp::KeyPair::generate(&mut rng).pubkey())
            .collect();
        for public in &peers {
            api.connect(*public, addr, 0).unwrap();
            let (_, bytes) = api.next_packet().unwrap();
            let init = <&HandshakeInitiation>::try_from(bytes.as_ref()).unwrap();
            let reply = cookie_reply(public, init.sender_index.get(), init.mac1.as_ref());
            dispatch(&mut api, &reply, addr);
            api.disconnect(public);
        }

        // The configured capacity must evict the oldest peer's cookie, while keeping
        // the latest one reusable across disconnects. Check outgoing packets via API.
        api.connect(peers[0], addr, 0).unwrap();
        let (_, bytes) = api.next_packet().unwrap();
        let init = <&HandshakeInitiation>::try_from(bytes.as_ref()).unwrap();
        assert_eq!(init.mac2.0, [0; 16]);

        let latest = peers.last().unwrap();
        api.connect(*latest, addr, 0).unwrap();
        let (_, bytes) = api.next_packet().unwrap();
        let init = <&HandshakeInitiation>::try_from(bytes.as_ref()).unwrap();
        crypto::verify_mac2(init, latest, &[2; 16]).unwrap();
    }
}
