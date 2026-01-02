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
    ops::{Deref, DerefMut},
    time::Duration,
};

use tracing::debug;

use super::{
    common::{MessageEvent, RekeyEvent, RenewedTimer, SessionError, SessionState, TerminatedEvent},
    replay_filter::ReplayFilter,
};
use crate::{
    config::Config,
    protocol::{
        common::{CipherKey, SessionIndex},
        messages::{DataPacket, DataPacketHeader, Plaintext},
    },
};

pub struct TransportState {
    pub remote_index: SessionIndex,
    pub send_key: CipherKey,
    pub send_nonce: u64,
    pub recv_key: CipherKey,
    pub replay_filter: ReplayFilter,
    pub common: SessionState,
}

impl TransportState {
    pub fn new(
        remote_index: SessionIndex,
        send_key: CipherKey,
        recv_key: CipherKey,
        common: SessionState,
    ) -> Self {
        TransportState {
            remote_index,
            send_key,
            send_nonce: 0,
            recv_key,
            replay_filter: ReplayFilter::new(),
            common,
        }
    }

    pub fn encrypt<R: secp256k1::rand::Rng>(
        &mut self,
        rng: &mut R,
        config: &Config,
        duration_since_start: Duration,
        plaintext: &mut [u8],
    ) -> (DataPacketHeader, RenewedTimer) {
        use crate::protocol::crypto;

        let header = DataPacketHeader {
            receiver_index: self.remote_index.as_u32().into(),
            nonce: self.send_nonce.into(),
            tag: crypto::encrypt_in_place(&self.send_key, &self.send_nonce.into(), plaintext, &[]),
            ..Default::default()
        };

        self.send_nonce += 1;

        if !plaintext.is_empty() {
            self.common
                .reset_gc_deadline(duration_since_start, config.gc_idle_timeout);
        }

        let keepalive_timer = self.common.reset_keepalive(
            duration_since_start,
            super::common::add_jitter(rng, config.keepalive_interval, config.keepalive_jitter),
        );

        let next_deadline = self
            .common
            .get_next_deadline()
            .expect("expected at least one timer to be set");

        let timer = RenewedTimer {
            previous: keepalive_timer.previous,
            current: next_deadline,
        };

        (header, timer)
    }

    pub fn decrypt<'a>(
        &mut self,
        config: &Config,
        duration_since_start: Duration,
        mut data_packet: DataPacket<'a>,
    ) -> Result<(RenewedTimer, Plaintext<'a>), SessionError> {
        use crate::protocol::crypto;

        self.replay_filter.check(data_packet.header().nonce.get())?;

        let counter = data_packet.header().nonce.get();
        let tag = data_packet.header().tag;

        crypto::decrypt_in_place(
            &self.recv_key,
            &counter.into(),
            data_packet.data_mut(),
            &tag,
            &[],
        )
        .map_err(SessionError::InvalidMac)?;

        self.replay_filter.update(counter);

        if !data_packet.data().is_empty() {
            self.common
                .reset_gc_deadline(duration_since_start, config.gc_idle_timeout);
        }

        let session_timer = self
            .common
            .reset_session_timeout(duration_since_start, config.session_timeout);

        let next_deadline = self
            .common
            .get_next_deadline()
            .expect("expected at least one timer to be set");

        let timer = RenewedTimer {
            previous: session_timer.previous,
            current: next_deadline,
        };

        Ok((timer, Plaintext::new(data_packet)))
    }

    #[allow(clippy::type_complexity)]
    pub fn tick<R: secp256k1::rand::Rng>(
        &mut self,
        rng: &mut R,
        config: &Config,
        duration_since_start: Duration,
    ) -> (
        Option<Duration>,
        Option<MessageEvent>,
        Option<RekeyEvent>,
        Option<TerminatedEvent>,
    ) {
        // Termination takes precedence: on any termination we should avoid doing extra work
        // (e.g. sending keepalives) in the same tick.
        let max_session_duration_expired = self
            .common
            .max_session_duration_deadline
            .is_some_and(|deadline| deadline <= duration_since_start);
        if max_session_duration_expired {
            self.common.clear_max_session_duration();

            debug!(
                remote_addr = ?self.common.remote_addr,
                "max session duration expired"
            );

            let (terminated_event, _) = self.common.handle_session_timeout();
            return (None, None, None, Some(terminated_event));
        }

        let session_timeout_expired = self
            .common
            .session_timeout_deadline
            .is_some_and(|deadline| deadline <= duration_since_start);
        if session_timeout_expired {
            self.common.clear_session_timeout();

            debug!(
                remote_addr = ?self.common.remote_addr,
                "session timeout expired"
            );

            let (terminated_event, rekey_event) = self.common.handle_session_timeout();
            return (None, None, rekey_event, Some(terminated_event));
        }

        let gc_expired = self
            .common
            .gc_deadline
            .is_some_and(|deadline| deadline <= duration_since_start);
        if gc_expired {
            self.common.clear_gc_deadline();

            debug!(
                remote_addr = ?self.common.remote_addr,
                "gc timer expired (no useful data)"
            );

            return (
                None,
                None,
                None,
                Some(TerminatedEvent {
                    remote_public_key: self.common.remote_public_key,
                    remote_addr: self.common.remote_addr,
                }),
            );
        }

        let mut message = None;
        let mut rekey = None;
        let terminated = None;

        let keepalive_expired = self
            .common
            .keepalive_deadline
            .is_some_and(|deadline| deadline <= duration_since_start);
        if keepalive_expired {
            self.common.clear_keepalive();
            debug!(
                duration_since_start = ?duration_since_start,
                remote_addr = ?self.common.remote_addr,
                "sending keepalive packet"
            );
            let (header, _) = self.encrypt(rng, config, duration_since_start, &mut []);
            message = Some(MessageEvent {
                remote_addr: self.common.remote_addr,
                header,
            });
        }

        let rekey_expired = self
            .common
            .rekey_deadline
            .is_some_and(|deadline| deadline <= duration_since_start);
        if rekey_expired {
            self.common.clear_rekey();
            debug!(
                remote_addr = ?self.common.remote_addr,
                "rekey timer expired"
            );
            rekey = Some(RekeyEvent {
                remote_public_key: self.common.remote_public_key,
                remote_addr: self.common.remote_addr,
                retry_attempts: self.common.retry_attempts,
                stored_cookie: self.common.stored_cookie,
            });
        }

        let next_timer = self.common.get_next_deadline();
        (next_timer, message, rekey, terminated)
    }
}

impl Deref for TransportState {
    type Target = SessionState;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl DerefMut for TransportState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}
