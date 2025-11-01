use std::{
    collections::{BTreeSet, VecDeque},
    convert::TryFrom,
    net::SocketAddr,
    time::Duration,
};

use bytes::Bytes;
use monad_wireauth_protocol::{
    common::{PrivateKey, PublicKey, SerializedPublicKey},
    messages::{
        CookieReply, DataPacket, DataPacketHeader, HandshakeInitiation, HandshakeResponse,
        TYPE_COOKIE_REPLY, TYPE_DATA, TYPE_HANDSHAKE_INITIATION, TYPE_HANDSHAKE_RESPONSE,
    },
};
use monad_wireauth_session::{Config, SessionIndex};
use tracing::{debug, instrument, trace, Level};

use crate::{
    context::Context,
    cookie::Cookies,
    error::{Error, ProtocolErrorContext, Result, SessionErrorContext},
    filter::{Filter, FilterAction},
    state::State,
    InitiatorState, ResponderState, TransportState,
};

pub struct API<C: Context> {
    state: State,
    next_timers: BTreeSet<(Duration, SessionIndex)>,
    packet_queue: VecDeque<(SocketAddr, Bytes)>,
    config: Config,
    local_static_key: PrivateKey,
    local_static_public: PublicKey,
    local_serialized_public: SerializedPublicKey,
    cookies: Cookies,
    filter: Filter,
    context: C,
}

impl<C: Context> API<C> {
    pub fn new(
        config: Config,
        local_static_key: PrivateKey,
        local_static_public: PublicKey,
        mut context: C,
    ) -> Self {
        let cookies = Cookies::new(
            context.rng(),
            (&local_static_public).into(),
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
        let local_serialized_public: SerializedPublicKey = (&local_static_public).into();
        debug!(local_public_key=?local_serialized_public, "initialized manager");
        Self {
            state: State::new(),
            next_timers: BTreeSet::new(),
            packet_queue: VecDeque::new(),
            config,
            local_static_key,
            local_static_public,
            local_serialized_public,
            cookies,
            filter,
            context,
        }
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn next_packet(&mut self) -> Option<(SocketAddr, Bytes)> {
        self.packet_queue.pop_front()
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn next_timer(&self) -> Option<Duration> {
        let duration_since_start = self.context.duration_since_start();

        let session_timer = self.next_timers.iter().next().map(|&(deadline, _)| {
            if deadline > duration_since_start {
                deadline - duration_since_start
            } else {
                Duration::ZERO
            }
        });

        let filter_reset_time = self.filter.next_reset_time();
        let filter_timer = if filter_reset_time > duration_since_start {
            filter_reset_time - duration_since_start
        } else {
            Duration::ZERO
        };

        match session_timer {
            Some(st) => Some(st.min(filter_timer)),
            None => Some(filter_timer),
        }
    }

    #[instrument(level = Level::TRACE, skip(self), fields(local_public_key = ?self.local_serialized_public))]
    pub fn tick(&mut self) {
        let duration_since_start = self.context.duration_since_start();

        self.filter.tick(duration_since_start);

        let expired_sessions: Vec<(Duration, SessionIndex)> = self
            .next_timers
            .range(..=(duration_since_start, SessionIndex::new(u32::MAX)))
            .copied()
            .collect();

        for timer in &expired_sessions {
            self.next_timers.remove(timer);
        }

        for (_, session_id) in expired_sessions {
            let tick_result = if self.state.get_initiator(&session_id).is_some() {
                self.state.get_initiator_mut(&session_id).and_then(|s| {
                    s.tick(duration_since_start)
                        .map(|(timer, r)| (timer, None, r.rekey, Some(r.terminated)))
                })
            } else if self.state.get_responder(&session_id).is_some() {
                self.state.get_responder_mut(&session_id).and_then(|s| {
                    s.tick(duration_since_start)
                        .map(|(timer, r)| (timer, None, r.rekey, Some(r.terminated)))
                })
            } else if let Some(transport) = self.state.get_transport_mut(&session_id) {
                Some(transport.tick(&self.config, duration_since_start))
            } else {
                None
            };

            if let Some((timer, message, rekey, terminated)) = tick_result {
                if let Some(message) = message {
                    self.packet_queue
                        .push_back((message.remote_addr, message.header.into()));
                }

                if let Some(rekey) = &rekey {
                    self.handle_rekey_event(
                        rekey.remote_public_key.clone(),
                        rekey.remote_addr,
                        rekey.retry_attempts,
                        rekey.stored_cookie,
                    );
                }

                if let Some(timer) = timer {
                    self.next_timers.insert((timer, session_id));
                }

                if let Some(terminated) = &terminated {
                    self.handle_terminate_event(
                        session_id,
                        terminated.remote_public_key.clone(),
                        terminated.remote_addr,
                    );
                }
            }
        }
    }

    fn handle_terminate_event(
        &mut self,
        session_id: SessionIndex,
        remote_public_key: PublicKey,
        remote_addr: SocketAddr,
    ) {
        self.filter.on_session_removed(remote_addr.ip());
        self.state
            .handle_terminate(session_id, &(&remote_public_key).into(), remote_addr);
    }

    fn handle_rekey_event(
        &mut self,
        remote_public_key: PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
        stored_cookie: Option<[u8; 16]>,
    ) {
        if let Ok((new_session_index, timer, message)) = self.init_session_with_cookie(
            remote_public_key,
            remote_addr,
            stored_cookie,
            retry_attempts,
        ) {
            self.packet_queue.push_back((remote_addr, message.into()));
            self.next_timers.insert((timer, new_session_index));
        }
    }

    fn handle_established(&mut self, session_id: SessionIndex, transport: TransportState) {
        debug!(local_session_id=?session_id, "handling established session");
        let replaced_sessions = self
            .state
            .handle_established(session_id, transport, &self.config);

        for replaced_session_id in replaced_sessions {
            let session = self
                .state
                .get_transport(&replaced_session_id)
                .expect("session must exist");
            self.handle_terminate_event(
                replaced_session_id,
                session.remote_public_key.clone(),
                session.remote_addr,
            );
        }
    }

    #[instrument(level = Level::TRACE, skip(self, remote_static_key), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn connect(
        &mut self,
        remote_static_key: PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<()> {
        debug!(retry_attempts, "initiating connection");
        let cookie = self
            .state
            .lookup_cookie_from_initiated_sessions(&(&remote_static_key).into());

        let (local_index, timer, message) =
            self.init_session_with_cookie(remote_static_key, remote_addr, cookie, retry_attempts)?;

        self.packet_queue.push_back((remote_addr, message.into()));
        self.next_timers.insert((timer, local_index));

        Ok(())
    }

    fn init_session_with_cookie(
        &mut self,
        remote_static_key: PublicKey,
        remote_addr: SocketAddr,
        cookie: Option<[u8; 16]>,
        retry_attempts: u64,
    ) -> Result<(SessionIndex, Duration, HandshakeInitiation)> {
        let reservation = self
            .state
            .reserve_session_index()
            .ok_or(Error::SessionIndexExhausted)?;
        let local_index = reservation.index();
        debug!(local_session_id=?local_index, "allocating session index for new connection");
        let system_time = self.context.system_time();
        let duration_since_start = self.context.duration_since_start();
        let (session, (timer, message)) = InitiatorState::new(
            self.context.rng(),
            system_time,
            duration_since_start,
            &self.config,
            local_index,
            &self.local_static_key,
            self.local_static_public.clone(),
            remote_static_key.clone(),
            remote_addr,
            cookie,
            retry_attempts,
        )
        .map_err(|e| e.with_addr(remote_addr))?;

        reservation.commit();

        self.state
            .insert_initiator(local_index, session, (&remote_static_key).into());

        self.filter.on_session_added(remote_addr.ip());

        Ok((local_index, timer, message))
    }

    fn check_under_load<M: monad_wireauth_protocol::messages::MacMessage>(
        &mut self,
        remote_addr: SocketAddr,
        sender_index: u32,
        message: &M,
    ) -> Result<bool> {
        let duration_since_start = self.context.duration_since_start();
        let action = self.filter.apply(
            remote_addr,
            duration_since_start,
            self.cookies
                .verify(&remote_addr, message, duration_since_start)
                .is_ok(),
        );

        match action {
            FilterAction::Pass => Ok(true),
            FilterAction::SendCookie => {
                debug!(?remote_addr, sender_index, "sending cookie reply");
                let reply = self.cookies.create(
                    remote_addr,
                    sender_index,
                    message,
                    duration_since_start,
                )?;
                self.packet_queue.push_back((remote_addr, reply.into()));
                Ok(false)
            }
            FilterAction::Drop => {
                debug!(
                    ?remote_addr,
                    sender_index, "dropping packet due to rate limit"
                );
                Ok(false)
            }
        }
    }

    fn accept_handshake_init(
        &mut self,
        handshake_packet: &mut HandshakeInitiation,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        monad_wireauth_protocol::crypto::verify_mac1(
            handshake_packet,
            &(&self.local_static_public).into(),
        )
        .map_err(|source| Error::Mac1VerificationFailed {
            addr: remote_addr,
            source,
        })?;

        if !self.check_under_load(
            remote_addr,
            handshake_packet.sender_index.get(),
            handshake_packet,
        )? {
            debug!(?remote_addr, "handshake initiation dropped under load");
            return Ok(());
        }

        let duration_since_start = self.context.duration_since_start();

        let validated_init = ResponderState::validate_init(
            &self.local_static_key,
            &self.local_static_public,
            handshake_packet,
        )
        .map_err(|e| e.with_addr(remote_addr))?;

        let remote_key = SerializedPublicKey::from(&validated_init.remote_public_key);
        if self
            .state
            .get_max_timestamp(&remote_key)
            .is_some_and(|max| validated_init.system_time <= max)
        {
            debug!(?remote_addr, ?remote_key, "timestamp replay detected");
            return Err(Error::TimestampReplay { addr: remote_addr });
        }

        let stored_cookie = self.state.lookup_cookie_from_accepted_sessions(remote_key);

        let reservation = self
            .state
            .reserve_session_index()
            .ok_or(Error::SessionIndexExhausted)?;
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
        .map_err(|e| e.with_addr(remote_addr))?;

        reservation.commit();

        self.state
            .insert_responder(local_index, session, remote_key);

        self.filter.on_session_added(remote_addr.ip());

        self.packet_queue.push_back((remote_addr, message.into()));
        self.next_timers.insert((timer, local_index));

        Ok(())
    }

    fn accept_cookie(
        &mut self,
        cookie_reply: &mut CookieReply,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        let receiver_session_index = cookie_reply.receiver_index.into();

        if let Some(session) = self.state.get_initiator_mut(&receiver_session_index) {
            session
                .handle_cookie(cookie_reply)
                .map_err(|e| e.with_addr(remote_addr))?;
        } else if let Some(session) = self.state.get_responder_mut(&receiver_session_index) {
            session
                .handle_cookie(cookie_reply)
                .map_err(|e| e.with_addr(remote_addr))?;
        }
        Ok(())
    }

    fn decrypt_data_packet(&mut self, packet: &mut [u8], remote_addr: SocketAddr) -> Result<bool> {
        let is_keepalive = packet.len() == DataPacketHeader::SIZE;
        let data_packet = DataPacket::try_from(packet).map_err(|e| e.with_addr(remote_addr))?;
        let receiver_index = data_packet.header.receiver_index.into();
        let nonce: u64 = data_packet.header.counter.into();
        trace!(local_session_id=?receiver_index, nonce, "decrypting data packet");

        let timer = if let Some(transport) = self.state.get_transport_mut(&receiver_index) {
            let duration_since_start = self.context.duration_since_start();
            let remote_addr = transport.remote_addr;
            transport
                .decrypt(&self.config, duration_since_start, data_packet)
                .map_err(|e| e.with_addr(remote_addr))?
        } else if let Some(responder) = self.state.get_responder_mut(&receiver_index) {
            let duration_since_start = self.context.duration_since_start();
            match responder.decrypt(&self.config, duration_since_start, data_packet) {
                Ok(_) => {
                    let responder = self.state.remove_responder(&receiver_index).unwrap();
                    let (transport, establish_timer) =
                        responder.establish(self.context.rng(), &self.config, duration_since_start);
                    debug!(local_session_id=?receiver_index, "responder session established");
                    self.handle_established(receiver_index, transport);
                    establish_timer
                }
                Err(e) => {
                    return Err(e.with_addr(remote_addr));
                }
            }
        } else {
            return Err(Error::SessionIndexNotFound {
                index: receiver_index,
            });
        };

        self.next_timers.insert((timer, receiver_index));

        Ok(is_keepalive)
    }

    fn complete_handshake(
        &mut self,
        response: &mut HandshakeResponse,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        monad_wireauth_protocol::crypto::verify_mac1(response, &(&self.local_static_public).into())
            .map_err(|source| Error::Mac1VerificationFailed {
                addr: remote_addr,
                source,
            })?;

        if !self.check_under_load(remote_addr, response.sender_index.get(), response)? {
            debug!(?remote_addr, "handshake response dropped under load");
            return Ok(());
        }

        let receiver_session_index = response.receiver_index.into();

        let initiator = self
            .state
            .get_initiator_mut(&receiver_session_index)
            .ok_or(Error::InvalidReceiverIndex {
                index: receiver_session_index,
                addr: remote_addr,
            })?;

        let validated_response = initiator
            .validate_response(
                &self.config,
                &self.local_static_key,
                &self.local_static_public,
                response,
            )
            .map_err(|e| e.with_addr(remote_addr))?;

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

        self.handle_established(receiver_session_index, transport);

        self.packet_queue.push_back((remote_addr, message.into()));
        self.next_timers.insert((timer, receiver_session_index));

        Ok(())
    }

    fn encrypt(
        &mut self,
        session_id: SessionIndex,
        plaintext: &mut [u8],
    ) -> Result<DataPacketHeader> {
        let transport = self
            .state
            .get_transport_mut(&session_id)
            .ok_or(Error::SessionNotFound)?;
        let (header, timer) =
            transport.encrypt(&self.config, self.context.duration_since_start(), plaintext);
        self.next_timers.insert((timer, session_id));
        Ok(header)
    }

    #[instrument(level = Level::TRACE, skip(self, public_key, plaintext), fields(local_public_key = ?self.local_serialized_public))]
    pub fn encrypt_by_public_key(
        &mut self,
        public_key: &PublicKey,
        plaintext: &mut [u8],
    ) -> Result<DataPacketHeader> {
        self.encrypt(
            self.state
                .get_session_id_by_public_key(&public_key.into())
                .ok_or(Error::SessionNotFound)?,
            plaintext,
        )
    }

    #[instrument(level = Level::TRACE, skip(self, plaintext), fields(local_public_key = ?self.local_serialized_public, socket_addr = ?socket_addr))]
    pub fn encrypt_by_socket(
        &mut self,
        socket_addr: &SocketAddr,
        plaintext: &mut [u8],
    ) -> Result<DataPacketHeader> {
        self.encrypt(
            self.state
                .get_session_id_by_socket(socket_addr)
                .ok_or(Error::SessionNotEstablishedForAddress { addr: *socket_addr })?,
            plaintext,
        )
    }

    #[instrument(level = Level::TRACE, skip(self, packet), fields(local_public_key = ?self.local_serialized_public, remote_addr = ?remote_addr))]
    pub fn dispatch(
        &mut self,
        packet: &mut [u8],
        remote_addr: SocketAddr,
    ) -> Result<Option<Bytes>> {
        if packet.is_empty() {
            return Err(Error::EmptyPacket { addr: remote_addr });
        }

        match packet[0] {
            TYPE_HANDSHAKE_INITIATION => {
                debug!("processing handshake initiation");
                let handshake = <&mut HandshakeInitiation>::try_from(packet)
                    .map_err(|e| e.with_addr(remote_addr))?;
                self.accept_handshake_init(handshake, remote_addr)?;
                Ok(None)
            }
            TYPE_HANDSHAKE_RESPONSE => {
                debug!("processing handshake response");
                let response = <&mut HandshakeResponse>::try_from(packet)
                    .map_err(|e| e.with_addr(remote_addr))?;
                self.complete_handshake(response, remote_addr)?;
                Ok(None)
            }
            TYPE_COOKIE_REPLY => {
                debug!("processing cookie reply");
                let cookie_reply =
                    <&mut CookieReply>::try_from(packet).map_err(|e| e.with_addr(remote_addr))?;
                self.accept_cookie(cookie_reply, remote_addr)?;
                Ok(None)
            }
            TYPE_DATA => {
                trace!("processing data packet");
                let is_keepalive = self.decrypt_data_packet(packet, remote_addr)?;
                if is_keepalive {
                    trace!("keepalive packet");
                    Ok(None)
                } else {
                    Ok(Some(packet[DataPacketHeader::SIZE..].to_vec().into()))
                }
            }
            _ => {
                debug!(packet_type = packet[0], "unknown packet type");
                Err(Error::InvalidMessageType {
                    msg_type: packet[0] as u32,
                    addr: remote_addr,
                })
            }
        }
    }

    #[instrument(level = Level::TRACE, skip(self, public_key), fields(local_public_key = ?self.local_serialized_public))]
    pub fn disconnect(&mut self, public_key: &PublicKey) {
        let terminated_addrs = self.state.terminate_by_public_key(&(public_key).into());
        for addr in terminated_addrs {
            self.filter.on_session_removed(addr.ip());
        }
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn reset_replay_filter_for_receiver(&mut self, receiver_index: u32) {
        let receiver_session_index = SessionIndex::new(receiver_index);
        self.state.reset_replay_filter(&receiver_session_index);
    }
}
