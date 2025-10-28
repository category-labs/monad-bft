use std::{net::SocketAddr, time::Duration};

use monad_wireauth_protocol::{
    common::SerializedPublicKey, errors::ProtocolError, messages::CookieReply,
};

use crate::error::{ProtocolErrorContext, Result};

pub struct Cookies {
    nonce_secret: [u8; 32],
    cookie_secret: [u8; 32],
    nonce: u128,
    local_static_public: SerializedPublicKey,
    refresh_duration: Duration,
}

impl Cookies {
    pub fn new<R: secp256k1::rand::Rng + secp256k1::rand::CryptoRng>(
        rng: &mut R,
        local_static_public: SerializedPublicKey,
        refresh_duration: Duration,
    ) -> Self {
        let mut cookie_secret = [0u8; 32];
        rng.fill_bytes(&mut cookie_secret);

        let mut nonce_secret = [0u8; 32];
        rng.fill_bytes(&mut nonce_secret);

        Self {
            cookie_secret,
            nonce_secret,
            nonce: 0,
            local_static_public,
            refresh_duration,
        }
    }

    pub fn create<M: monad_wireauth_protocol::messages::MacMessage>(
        &mut self,
        addr: SocketAddr,
        sender_index: u32,
        message: &M,
        duration_since_start: Duration,
    ) -> Result<CookieReply> {
        let time_counter = duration_since_start.as_secs() / self.refresh_duration.as_secs();
        let cookie = monad_wireauth_protocol::cookies::generate_cookie(
            &self.cookie_secret,
            time_counter,
            &addr,
        );

        let nonce_counter = self.nonce;
        self.nonce += 1;

        monad_wireauth_protocol::cookies::send_cookie_reply(
            &self.nonce_secret,
            nonce_counter,
            &self.local_static_public,
            sender_index,
            message.mac1().as_ref(),
            &cookie,
        )
        .map_err(|e| match e {
            ProtocolError::Cookie(c) => c.with_addr(addr),
            ProtocolError::Crypto(c) => c.with_addr(addr),
            ProtocolError::Handshake(h) => h.with_addr(addr),
            ProtocolError::Message(m) => m.with_addr(addr),
        })
    }

    pub fn verify<M: monad_wireauth_protocol::messages::MacMessage>(
        &self,
        remote_addr: &SocketAddr,
        message: &M,
        duration_since_start: Duration,
    ) -> Result<()> {
        let time_counter = duration_since_start.as_secs() / self.refresh_duration.as_secs();

        monad_wireauth_protocol::cookies::verify_cookie(
            &self.cookie_secret,
            time_counter,
            remote_addr,
            &self.local_static_public,
            message,
        )
        .map_err(|e| match e {
            ProtocolError::Cookie(c) => c.with_addr(*remote_addr),
            ProtocolError::Crypto(c) => c.with_addr(*remote_addr),
            ProtocolError::Handshake(h) => h.with_addr(*remote_addr),
            ProtocolError::Message(m) => m.with_addr(*remote_addr),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use monad_wireauth_protocol::handshake::send_handshake_init;
    use secp256k1::rand::rng;

    use super::*;

    #[test]
    fn test_sanity() {
        let mut rng = rng();
        let (initiator_public, initiator_private) =
            monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
        let (responder_public, _responder_private) =
            monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
        let refresh_duration = Duration::from_secs(120);

        let mut cookies = Cookies::new(
            &mut rng,
            SerializedPublicKey::from(&responder_public),
            refresh_duration,
        );

        let addr: SocketAddr = "192.168.1.100:51820".parse().unwrap();
        let duration_since_start = Duration::from_secs(10);

        let (init_msg, _state) = send_handshake_init(
            &mut rng,
            SystemTime::now(),
            12345,
            &initiator_private,
            &SerializedPublicKey::from(&initiator_public),
            &SerializedPublicKey::from(&responder_public),
            None,
        )
        .unwrap();

        let mut cookie_reply = cookies
            .create(addr, 12345, &init_msg, duration_since_start)
            .unwrap();

        let decrypted_cookie = monad_wireauth_protocol::cookies::accept_cookie_reply(
            &SerializedPublicKey::from(&responder_public),
            &mut cookie_reply,
            init_msg.mac1.as_ref(),
        )
        .unwrap();

        let (init_msg_with_cookie, _state) = send_handshake_init(
            &mut rng,
            SystemTime::now(),
            12346,
            &initiator_private,
            &SerializedPublicKey::from(&initiator_public),
            &SerializedPublicKey::from(&responder_public),
            Some(&decrypted_cookie),
        )
        .unwrap();

        let verify_result = cookies.verify(&addr, &init_msg_with_cookie, duration_since_start);
        assert!(verify_result.is_ok());
    }

    #[test]
    fn test_rotation_invalidates_old_cookie() {
        let mut rng = rng();
        let (initiator_public, initiator_private) =
            monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
        let (responder_public, _responder_private) =
            monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
        let refresh_duration = Duration::from_secs(10);

        let mut cookies = Cookies::new(
            &mut rng,
            SerializedPublicKey::from(&responder_public),
            refresh_duration,
        );

        let addr: SocketAddr = "192.168.1.100:51820".parse().unwrap();

        let (init_msg, _state) = send_handshake_init(
            &mut rng,
            SystemTime::now(),
            12345,
            &initiator_private,
            &SerializedPublicKey::from(&initiator_public),
            &SerializedPublicKey::from(&responder_public),
            None,
        )
        .unwrap();

        let duration_at_time_0 = Duration::from_secs(5);
        let mut cookie_reply = cookies
            .create(addr, 12345, &init_msg, duration_at_time_0)
            .unwrap();

        let decrypted_cookie = monad_wireauth_protocol::cookies::accept_cookie_reply(
            &SerializedPublicKey::from(&responder_public),
            &mut cookie_reply,
            init_msg.mac1.as_ref(),
        )
        .unwrap();

        let (init_msg_with_cookie, _state) = send_handshake_init(
            &mut rng,
            SystemTime::now(),
            12346,
            &initiator_private,
            &SerializedPublicKey::from(&initiator_public),
            &SerializedPublicKey::from(&responder_public),
            Some(&decrypted_cookie),
        )
        .unwrap();

        let verify_before_rotation =
            cookies.verify(&addr, &init_msg_with_cookie, duration_at_time_0);
        assert!(verify_before_rotation.is_ok());

        let duration_after_rotation = Duration::from_secs(25);
        let verify_after_rotation =
            cookies.verify(&addr, &init_msg_with_cookie, duration_after_rotation);
        assert!(verify_after_rotation.is_err());
    }

    #[test]
    fn test_cookies_different_after_reset() {
        let mut rng = rng();
        let (initiator_public, initiator_private) =
            monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
        let (responder_public, _responder_private) =
            monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
        let refresh_duration = Duration::from_secs(120);

        let mut cookies1 = Cookies::new(
            &mut rng,
            SerializedPublicKey::from(&responder_public),
            refresh_duration,
        );

        let addr: SocketAddr = "192.168.1.100:51820".parse().unwrap();
        let duration_since_start = Duration::from_secs(10);

        let (init_msg, _state) = send_handshake_init(
            &mut rng,
            SystemTime::now(),
            12345,
            &initiator_private,
            &SerializedPublicKey::from(&initiator_public),
            &SerializedPublicKey::from(&responder_public),
            None,
        )
        .unwrap();

        let cookie_reply_1 = cookies1
            .create(addr, 12345, &init_msg, duration_since_start)
            .unwrap();
        let cookie_reply_2 = cookies1
            .create(addr, 12345, &init_msg, duration_since_start)
            .unwrap();
        let cookie_reply_3 = cookies1
            .create(addr, 12345, &init_msg, duration_since_start)
            .unwrap();

        let mut cookies2 = Cookies::new(
            &mut rng,
            SerializedPublicKey::from(&responder_public),
            refresh_duration,
        );

        let cookie_reply_4 = cookies2
            .create(addr, 12345, &init_msg, duration_since_start)
            .unwrap();
        let cookie_reply_5 = cookies2
            .create(addr, 12345, &init_msg, duration_since_start)
            .unwrap();
        let cookie_reply_6 = cookies2
            .create(addr, 12345, &init_msg, duration_since_start)
            .unwrap();

        assert_ne!(
            cookie_reply_1.encrypted_cookie,
            cookie_reply_4.encrypted_cookie
        );
        assert_ne!(
            cookie_reply_2.encrypted_cookie,
            cookie_reply_5.encrypted_cookie
        );
        assert_ne!(
            cookie_reply_3.encrypted_cookie,
            cookie_reply_6.encrypted_cookie
        );
    }
}
