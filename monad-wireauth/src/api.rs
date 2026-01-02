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
    filter::{Filter, FilterAction},
    messages::MacMessage,
    metrics::MetricNames,
    protocol::messages::{
        ControlPacket, CookieReply, DataPacket, DataPacketHeader, HandshakeInitiation,
        HandshakeResponse, Plaintext,
    },
    session::{InitiatorState, RenewedTimer, ResponderState, SessionError, SessionIndex},
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
            state: State::new(metric_names),
            timers: BTreeSet::new(),
            packet_queue: VecDeque::new(),
            config,
            local_static_key,
            local_serialized_public,
            cookies,
            filter,
            context,
            metrics: ExecutorMetrics::default(),
            metric_names,
            last_tick: None,
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
        self.metrics[self.metric_names.api_next_packet] += 1;
        let result = self.packet_queue.pop_front();
        self.metrics[self.metric_names.state_packet_queue_size] = self.packet_queue.len() as u64;
        result
    }

    fn enqueue_packet(&mut self, addr: SocketAddr, pkt: impl Into<Bytes>) {
        self.packet_queue.push_back((addr, pkt.into()));
        self.metrics[self.metric_names.state_packet_queue_size] = self.packet_queue.len() as u64;
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
        self.metrics[self.metric_names.state_timers_size] = self.timers.len() as u64;
    }

    fn replace_timer(&mut self, timer: RenewedTimer, session_index: SessionIndex) {
        if let Some(previous) = timer.previous {
            self.timers.remove(&(previous, session_index));
        }
        self.timers.insert((timer.current, session_index));
        self.metrics[self.metric_names.state_timers_size] = self.timers.len() as u64;
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn tick(&mut self) {
        self.metrics[self.metric_names.api_tick] += 1;
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
            self.metrics[self.metric_names.state_timers_size] = self.timers.len() as u64;
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
                self.metrics[self.metric_names.enqueued_keepalive] += 1;
                self.enqueue_packet(message.remote_addr, message.header);
            }

            if let Some(rekey) = rekey {
                if let Ok((new_session_index, timer, message)) = self.init_session_with_cookie(
                    rekey.remote_public_key,
                    rekey.remote_addr,
                    rekey.stored_cookie,
                    rekey.retry_attempts,
                ) {
                    self.metrics[self.metric_names.enqueued_handshake_init] += 1;
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
        self.metrics[self.metric_names.api_connect] += 1;
        debug!(retry_attempts, "initiating connection");

        let initiated_count = self.state.initiated_sessions_count();
        if initiated_count >= self.config.max_initiated_sessions {
            self.metrics[self.metric_names.error_connect] += 1;
            return Err(Error::TooManyInitiatedSessions {
                limit: self.config.max_initiated_sessions,
            });
        }

        // Cookies are looked up from initiated sessions for simplicity.
        // In the future, this can be improved to look up from both initiated and accepted sessions.
        let cookie = self
            .state
            .lookup_cookie_from_initiated_sessions(&remote_static_key);

        let (local_index, timer, message) = self
            .init_session_with_cookie(remote_static_key, remote_addr, cookie, retry_attempts)
            .inspect_err(|_| {
                self.metrics[self.metric_names.error_connect] += 1;
            })?;

        self.metrics[self.metric_names.enqueued_handshake_init] += 1;
        self.enqueue_packet(remote_addr, message);
        self.insert_timer(timer, local_index);

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
            self.metrics[self.metric_names.error_session_exhausted] += 1;
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

    fn is_under_load(
        &mut self,
        remote_addr: SocketAddr,
        sender_index: u32,
        message: &impl MacMessage,
    ) -> bool {
        let duration_since_start = self.context.duration_since_start();
        let action = self.filter.apply(
            &self.state,
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
                self.metrics[self.metric_names.enqueued_cookie_reply] += 1;
                self.enqueue_packet(remote_addr, reply);
                false
            }
            FilterAction::Drop => {
                self.metrics[self.metric_names.rate_limit_drop] += 1;
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
            self.metrics[self.metric_names.error_mac1_verification_failed] += 1;
        })?;

        if !self.is_under_load(
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
                    self.metrics[self.metric_names.error_handshake_init_validation] += 1;
                })?;

        let remote_key = validated_init.remote_public_key;
        if self
            .state
            .get_max_timestamp(&remote_key)
            .is_some_and(|max| validated_init.timestamp <= max)
        {
            self.metrics[self.metric_names.error_timestamp_replay] += 1;
            debug!(?remote_addr, ?remote_key, "timestamp replay detected");
            return Err(Error::TimestampReplay);
        }

        // Cookie is looked up from accepted sessions for simplicity.
        // There is technically no reason not to reuse cookies between initiated and accepted sessions,
        // and this can be improved in the future.
        let stored_cookie = self.state.lookup_cookie_from_accepted_sessions(remote_key);

        // Reservation should be committed only when code is no longer fallible
        // TODO(dshulyak): Get rid of reservation; code was refactored to be non-fallible when index is allocated
        let reservation = self.state.reserve_session_index().ok_or_else(|| {
            self.metrics[self.metric_names.error_session_exhausted] += 1;
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

        self.metrics[self.metric_names.enqueued_handshake_response] += 1;
        self.enqueue_packet(remote_addr, message);
        self.insert_timer(timer, local_index);

        Ok(())
    }

    fn accept_cookie(&mut self, cookie_reply: &mut CookieReply) -> Result<()> {
        let receiver_session_index = cookie_reply.receiver_index.into();

        if let Some(session) = self.state.get_initiator_mut(&receiver_session_index) {
            session.handle_cookie(cookie_reply).inspect_err(|_| {
                self.metrics[self.metric_names.error_cookie_reply] += 1;
            })?;
        } else if let Some(session) = self.state.get_responder_mut(&receiver_session_index) {
            session.handle_cookie(cookie_reply).inspect_err(|_| {
                self.metrics[self.metric_names.error_cookie_reply] += 1;
            })?;
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
        self.metrics[self.metric_names.api_dispatch_control] += 1;
        let result = match control {
            ControlPacket::HandshakeInitiation(handshake) => {
                debug!("processing handshake initiation");
                self.metrics[self.metric_names.dispatch_handshake_init] += 1;
                self.accept_handshake_init(handshake, remote_addr)
            }
            ControlPacket::HandshakeResponse(response) => {
                debug!("processing handshake response");
                self.metrics[self.metric_names.dispatch_handshake_response] += 1;
                self.complete_handshake(response, remote_addr)
            }
            ControlPacket::CookieReply(cookie_reply) => {
                debug!("processing cookie reply");
                self.metrics[self.metric_names.dispatch_cookie_reply] += 1;
                self.accept_cookie(cookie_reply)
            }
            ControlPacket::Keepalive(data_packet) => {
                trace!("processing keepalive packet");
                self.metrics[self.metric_names.dispatch_keepalive] += 1;
                self.decrypt(data_packet, remote_addr)?;
                Ok(())
            }
        };
        if result.is_err() {
            self.metrics[self.metric_names.error_dispatch_control] += 1;
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
        self.metrics[self.metric_names.api_decrypt] += 1;
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
            match responder.decrypt(&self.config, duration_since_start, data_packet) {
                Ok((_timer, plaintext)) => {
                    let remote_public_key = responder.transport.remote_public_key;
                    // unwrap() is safe as we have &mut and it was accessed right before this line
                    let responder = self.state.remove_responder(&receiver_index).unwrap();
                    let (transport, establish_timer) =
                        responder.establish(self.context.rng(), &self.config, duration_since_start);
                    debug!(local_session_id=?receiver_index, "responder session established");
                    self.state.insert_transport(receiver_index, transport);
                    self.timers.insert((establish_timer, receiver_index));
                    self.metrics[self.metric_names.state_timers_size] = self.timers.len() as u64;
                    (remote_public_key, plaintext)
                }
                Err(e) => {
                    track_decrypt_error_metrics(&mut self.metrics, self.metric_names, &e);
                    return Err(e.into());
                }
            }
        } else {
            self.metrics[self.metric_names.error_decrypt] += 1;
            self.metrics[self.metric_names.error_session_index_not_found] += 1;
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
        // The initiator is transitioned into transport in 2 stages.
        // All validators and other fallible actions must be done before removing the initiator from state.
        crate::protocol::crypto::verify_mac1(response, &self.local_static_key.as_ref().pubkey())
            .inspect_err(|_| {
                self.metrics[self.metric_names.error_mac1_verification_failed] += 1;
            })?;

        if !self.is_under_load(remote_addr, response.sender_index.get(), response) {
            debug!(?remote_addr, "handshake response dropped under load");
            return Ok(());
        }

        let receiver_session_index = response.receiver_index.into();

        let initiator = self
            .state
            .get_initiator_mut(&receiver_session_index)
            .ok_or_else(|| {
                self.metrics[self.metric_names.error_session_index_not_found] += 1;
                Error::InvalidReceiverIndex {
                    index: receiver_session_index,
                }
            })?;

        let validated_response = initiator
            .validate_response(&self.config, self.local_static_key.as_ref(), response)
            .inspect_err(|_| {
                self.metrics[self.metric_names.error_handshake_response_validation] += 1;
            })?;

        // Code should not be fallible after this point
        let initiator = self
            .state
            .remove_initiator(&receiver_session_index)
            .expect("initiator was accessed above");

        let buffered_message_count = initiator.buffered_message_count();
        let duration_since_start = self.context.duration_since_start();
        debug!(
            local_session_id=?receiver_session_index,
            buffered_messages=buffered_message_count,
            "initiator session established"
        );
        let (transport, messages) = initiator.establish(
            self.context.rng(),
            &self.config,
            duration_since_start,
            validated_response,
        );
        let is_buffered = messages.is_buffered();

        self.state
            .insert_transport(receiver_session_index, transport);

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
                self.metrics[self.metric_names.initiator_messages_sent_from_buffer] += 1;
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
        self.metrics[self.metric_names.api_encrypt_by_public_key] += 1;
        let transport = self
            .state
            .get_transport_by_public_key(public_key)
            .ok_or_else(|| {
                self.metrics[self.metric_names.error_encrypt_by_public_key] += 1;
                self.metrics[self.metric_names.error_session_not_found] += 1;
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
        self.metrics[self.metric_names.api_encrypt_by_socket] += 1;
        let transport = self
            .state
            .get_transport_by_socket(socket_addr)
            .ok_or_else(|| {
                self.metrics[self.metric_names.error_encrypt_by_socket] += 1;
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
        let new_size = initiator.buffered_bytes() + message.len();
        if new_size > self.config.max_buffered_bytes_per_session {
            return Err(Error::BufferLimitExceeded {
                size: new_size,
                limit: self.config.max_buffered_bytes_per_session,
            });
        }
        initiator.buffer_message(message);
        self.metrics[self.metric_names.initiator_buffered_messages] += 1;
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
        self.metrics[self.metric_names.api_disconnect] += 1;
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
    metrics[metric_names.error_decrypt] += 1;
    match e {
        SessionError::NonceOutsideWindow { .. } => {
            metrics[metric_names.error_decrypt_nonce_outside_window] += 1;
        }
        SessionError::NonceDuplicate { .. } => {
            metrics[metric_names.error_decrypt_nonce_duplicate] += 1;
        }
        SessionError::InvalidMac(_) => {
            metrics[metric_names.error_decrypt_mac] += 1;
        }
        _ => {
            warn!(error=?e, "unexpected decrypt error variant");
        }
    }
}
