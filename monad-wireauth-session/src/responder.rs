use std::{
    net::SocketAddr,
    ops::{Deref, DerefMut},
    time::Duration,
};

use monad_wireauth_protocol::{
    common::*,
    handshake::{self},
    messages::{DataPacket, HandshakeInitiation, HandshakeResponse, Plaintext},
};

use crate::{
    common::{add_jitter, Config, SessionError, SessionState, SessionTimeoutResult},
    transport::TransportState,
};

pub struct ValidatedHandshakeInit {
    pub handshake_state: monad_wireauth_protocol::handshake::HandshakeState,
    pub remote_public_key: monad_secp::PubKey,
    pub system_time: std::time::SystemTime,
    pub remote_index: SessionIndex,
}

pub struct ResponderState {
    pub transport: TransportState,
}

impl Deref for ResponderState {
    type Target = SessionState;

    fn deref(&self) -> &Self::Target {
        &self.transport
    }
}

impl DerefMut for ResponderState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transport
    }
}

impl ResponderState {
    pub fn validate_init(
        local_static_key: &monad_secp::KeyPair,
        _local_static_public: &monad_secp::PubKey,
        handshake_packet: &mut HandshakeInitiation,
    ) -> Result<ValidatedHandshakeInit, SessionError> {
        let (handshake_state, system_time) =
            handshake::accept_handshake_init(local_static_key, handshake_packet)
                .map_err(SessionError::InvalidHandshake)?;

        let remote_public_key = handshake_state
            .remote_static
            .expect("remote static key must be set");

        let remote_index = handshake_state.receiver_index.into();

        Ok(ValidatedHandshakeInit {
            handshake_state,
            remote_public_key,
            system_time,
            remote_index,
        })
    }

    pub fn new<R: secp256k1::rand::Rng + secp256k1::rand::CryptoRng>(
        rng: &mut R,
        duration_since_start: Duration,
        config: &Config,
        local_session_index: SessionIndex,
        stored_cookie: Option<&[u8; 16]>,
        validated_init: ValidatedHandshakeInit,
        remote_addr: SocketAddr,
    ) -> Result<(ResponderState, Duration, HandshakeResponse), SessionError> {
        let mut handshake_state = validated_init.handshake_state;
        let (response_msg, transport_keys) = handshake::send_handshake_response(
            rng,
            local_session_index.as_u32(),
            &mut handshake_state,
            &config.psk,
            stored_cookie,
        )
        .map_err(SessionError::InvalidHandshake)?;

        let response_mac1 = response_msg.mac1.0;

        let mut common = SessionState::new(
            remote_addr,
            validated_init.remote_public_key,
            local_session_index,
            duration_since_start,
            0,
            Some(validated_init.system_time),
            false,
        );
        common.last_handshake_mac1 = Some(response_mac1);

        let timeout_with_jitter =
            add_jitter(rng, config.session_timeout, config.session_timeout_jitter);
        common.reset_session_timeout(duration_since_start, timeout_with_jitter);

        let timer = common
            .get_next_deadline()
            .expect("expected at least one timer to be set");

        let transport = TransportState::new(
            handshake_state.receiver_index.into(),
            transport_keys.send_key,
            transport_keys.recv_key,
            common,
        );
        Ok((ResponderState { transport }, timer, response_msg))
    }

    pub fn decrypt<'a>(
        &mut self,
        config: &Config,
        duration_since_start: Duration,
        data_packet: DataPacket<'a>,
    ) -> Result<(Duration, Plaintext<'a>), SessionError> {
        self.transport
            .decrypt(config, duration_since_start, data_packet)
    }

    pub fn establish<R: secp256k1::rand::Rng>(
        mut self,
        rng: &mut R,
        config: &Config,
        duration_since_start: Duration,
    ) -> (TransportState, Duration) {
        self.transport
            .common
            .reset_session_timeout(duration_since_start, config.session_timeout);
        self.transport.common.reset_keepalive(
            duration_since_start,
            add_jitter(rng, config.keepalive_interval, config.keepalive_jitter),
        );
        self.transport
            .common
            .set_max_session_duration(duration_since_start, config.max_session_duration);

        let timer = self
            .transport
            .common
            .get_next_deadline()
            .expect("expected at least one timer to be set");

        (self.transport, timer)
    }

    pub fn tick(
        &mut self,
        duration_since_start: Duration,
    ) -> Option<(Option<Duration>, SessionTimeoutResult)> {
        let session_timeout_expired = self
            .session_timeout_deadline
            .is_some_and(|deadline| deadline <= duration_since_start);

        if !session_timeout_expired {
            return None;
        }

        self.clear_session_timeout();
        let (terminated, rekey) = self.handle_session_timeout();
        let timer = self.get_next_deadline();
        Some((timer, SessionTimeoutResult { terminated, rekey }))
    }

    pub fn handle_cookie(
        &mut self,
        cookie_reply: &mut monad_wireauth_protocol::messages::CookieReply,
    ) -> Result<(), SessionError> {
        self.transport.common.handle_cookie(cookie_reply)
    }
}
