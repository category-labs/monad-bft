use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use monad_wireauth_protocol::{
    common::{CipherKey, SessionIndex},
    messages::{DataPacket, DataPacketHeader},
};
use tracing::debug;

use crate::{
    common::{CommonSessionData, Config, MessageEvent, RekeyEvent, SessionError, TerminatedEvent},
    replay_filter::ReplayFilter,
};

pub struct Transport {
    pub remote_index: SessionIndex,
    pub send_key: CipherKey,
    pub send_nonce: u64,
    pub recv_key: CipherKey,
    pub replay_filter: ReplayFilter,
    pub common: CommonSessionData,
}

impl Transport {
    pub fn new(
        remote_index: SessionIndex,
        send_key: CipherKey,
        recv_key: CipherKey,
        common: CommonSessionData,
    ) -> Self {
        Transport {
            remote_index,
            send_key,
            send_nonce: 0,
            recv_key,
            replay_filter: ReplayFilter::new(),
            common,
        }
    }

    pub fn encrypt(
        &mut self,
        config: &Config,
        duration_since_start: Duration,
        plaintext: &mut [u8],
    ) -> (DataPacketHeader, Duration) {
        use monad_wireauth_protocol::crypto;

        let header = DataPacketHeader {
            receiver_index: self.remote_index.as_u32().into(),
            counter: self.send_nonce.into(),
            tag: crypto::encrypt_in_place(&self.send_key, &self.send_nonce.into(), plaintext, &[]),
            ..Default::default()
        };

        self.send_nonce += 1;

        self.common
            .reset_keepalive(duration_since_start, config.keepalive_interval);
        let timer = self
            .common
            .get_next_deadline()
            .expect("expected at least one timer to be set");
        (header, timer)
    }

    pub fn decrypt(
        &mut self,
        config: &Config,
        duration_since_start: Duration,
        data_packet: DataPacket,
    ) -> Result<Duration, SessionError> {
        use monad_wireauth_protocol::crypto;

        self.replay_filter.check(data_packet.header.counter.get())?;

        crypto::decrypt_in_place(
            &self.recv_key,
            &data_packet.header.counter.get().into(),
            data_packet.plaintext,
            &data_packet.header.tag,
            &[],
        )
        .map_err(SessionError::InvalidMac)?;

        self.replay_filter.update(data_packet.header.counter.get());

        self.common
            .reset_session_timeout(duration_since_start, config.session_timeout);
        let timer = self
            .common
            .get_next_deadline()
            .expect("expected at least one timer to be set");
        Ok(timer)
    }

    #[allow(clippy::type_complexity)]
    pub fn tick(
        &mut self,
        config: &Config,
        duration_since_start: Duration,
    ) -> Result<
        (
            Option<Duration>,
            Option<MessageEvent>,
            Option<RekeyEvent>,
            Option<TerminatedEvent>,
        ),
        SessionError,
    > {
        let mut message = None;
        let mut rekey = None;
        let mut terminated = None;

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
            let (header, _) = self.encrypt(config, duration_since_start, &mut []);
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
            let rekey_event = self.common.handle_rekey_timer();
            rekey = Some(rekey_event);
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

            let (terminated_event, rekey_event) = self.common.handle_session_timeout()?;
            terminated = Some(terminated_event);
            rekey = rekey.or(rekey_event);
        }

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

            let (terminated_event, _) = self.common.handle_session_timeout()?;
            terminated = Some(terminated_event);
            rekey = None;
        }

        let next_timer = self.common.get_next_deadline();
        Ok((next_timer, message, rekey, terminated))
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn reset_replay_filter(&mut self) {
        self.replay_filter = ReplayFilter::new();
    }
}

impl Deref for Transport {
    type Target = CommonSessionData;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl DerefMut for Transport {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}
