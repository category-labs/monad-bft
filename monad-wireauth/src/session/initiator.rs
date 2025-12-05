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
    net::SocketAddr,
    ops::{Deref, DerefMut},
    time::{Duration, SystemTime},
};

use super::{
    common::{add_jitter, RenewedTimer, SessionError, SessionState, SessionTimeoutResult},
    transport::TransportState,
};
use crate::{
    config::Config,
    protocol::{
        common::*,
        handshake::{self},
        messages::{CookieReply, DataPacketHeader, HandshakeInitiation, HandshakeResponse},
    },
};

pub struct ValidatedHandshakeResponse {
    transport_keys: crate::protocol::common::TransportKeys,
    remote_index: SessionIndex,
}

pub struct InitiatorState {
    handshake_state: handshake::HandshakeState,
    common: SessionState,
}

impl InitiatorState {
    #[allow(clippy::too_many_arguments)]
    pub fn new<R: secp256k1::rand::Rng + secp256k1::rand::CryptoRng>(
        rng: &mut R,
        system_time: SystemTime,
        duration_since_start: Duration,
        config: &Config,
        local_session_index: SessionIndex,
        local_static_key: &monad_secp::KeyPair,
        remote_static_key: monad_secp::PubKey,
        remote_addr: SocketAddr,
        cookie_secret: Option<[u8; 16]>,
        retry_attempts: u64,
    ) -> (Self, (Duration, HandshakeInitiation)) {
        let (init_msg, handshake_state) = handshake::send_handshake_init(
            rng,
            system_time,
            local_session_index.as_u32(),
            local_static_key,
            &remote_static_key,
            cookie_secret.as_ref(),
        );

        let mac1 = init_msg.mac1.0;
        let mut common = SessionState::new(
            remote_addr,
            remote_static_key,
            local_session_index,
            duration_since_start,
            retry_attempts,
            None,
            true,
        );
        common.stored_cookie = cookie_secret;
        common.last_handshake_mac1 = Some(mac1);

        let mut session = InitiatorState {
            handshake_state,
            common,
        };

        let timeout_with_jitter =
            add_jitter(rng, config.session_timeout, config.session_timeout_jitter);
        session
            .common
            .reset_session_timeout(duration_since_start, timeout_with_jitter);

        let timer = session
            .common
            .get_next_deadline()
            .expect("expected at least one timer to be set");

        (session, (timer, init_msg))
    }

    pub fn validate_response(
        &mut self,
        config: &Config,
        local_static_key: &monad_secp::KeyPair,
        msg: &mut HandshakeResponse,
    ) -> Result<ValidatedHandshakeResponse, SessionError> {
        let transport_keys = handshake::accept_handshake_response(
            local_static_key,
            msg,
            &mut self.handshake_state,
            &config.psk,
        )
        .map_err(SessionError::HandshakeError)?;

        Ok(ValidatedHandshakeResponse {
            transport_keys,
            remote_index: self.handshake_state.receiver_index.into(),
        })
    }

    pub fn establish<R: secp256k1::rand::Rng>(
        mut self,
        rng: &mut R,
        config: &Config,
        duration_since_start: Duration,
        validated_response: ValidatedHandshakeResponse,
        _remote_addr: SocketAddr,
    ) -> (TransportState, RenewedTimer, DataPacketHeader) {
        self.common.reset_session_timeout(
            duration_since_start,
            add_jitter(rng, config.session_timeout, config.session_timeout_jitter),
        );
        self.common.reset_rekey(
            duration_since_start,
            add_jitter(rng, config.rekey_interval, config.rekey_jitter),
        );
        self.common
            .set_max_session_duration(duration_since_start, config.max_session_duration);

        let mut transport = TransportState::new(
            validated_response.remote_index,
            validated_response.transport_keys.send_key,
            validated_response.transport_keys.recv_key,
            self.common,
        );
        let (header, timer) = transport.encrypt(rng, config, duration_since_start, &mut []);
        (transport, timer, header)
    }

    pub fn handle_cookie(&mut self, cookie_reply: &mut CookieReply) -> Result<(), SessionError> {
        self.common.handle_cookie(cookie_reply)
    }

    pub fn tick(
        &mut self,
        duration_since_start: Duration,
    ) -> Option<(Option<Duration>, SessionTimeoutResult)> {
        let session_timeout_expired = self
            .common
            .session_timeout_deadline
            .is_some_and(|deadline| deadline <= duration_since_start);

        if !session_timeout_expired {
            return None;
        }

        self.common.clear_session_timeout();
        let (terminated, rekey) = self.handle_session_timeout();
        let timer = self.common.get_next_deadline();
        Some((timer, SessionTimeoutResult { terminated, rekey }))
    }
}

impl Deref for InitiatorState {
    type Target = SessionState;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl DerefMut for InitiatorState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}
