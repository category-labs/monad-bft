use std::net::SocketAddr;

use super::{
    common::*,
    crypto::{decrypt_in_place, encrypt_in_place, LABEL_COOKIE},
    errors::{CookieError, ProtocolError},
    messages::*,
};
use crate::{hash, keyed_hash};

pub fn send_cookie_reply(
    nonce_secret: &[u8; 32],
    nonce_counter: u128,
    responder_static_public: &monad_secp::PubKey,
    msg_sender_index: u32,
    msg_mac1: &[u8; 16],
    cookie: &[u8; 16],
) -> CookieReply {
    let nonce_hash = keyed_hash!(nonce_secret, &nonce_counter.to_le_bytes());
    let hash_bytes: &[u8] = nonce_hash.as_ref();
    let mut nonce_bytes = [0u8; 16];
    nonce_bytes.copy_from_slice(&hash_bytes[..16]);
    let nonce = CipherNonce(nonce_bytes);

    let mut reply = CookieReply {
        receiver_index: msg_sender_index.into(),
        nonce,
        ..Default::default()
    };

    let temp_key = hash!(LABEL_COOKIE, &responder_static_public.bytes_compressed());

    reply.encrypted_cookie = *cookie;
    reply.encrypted_cookie_tag = encrypt_in_place(
        &(&temp_key).into(),
        &reply.nonce,
        &mut reply.encrypted_cookie,
        msg_mac1,
    );

    reply
}

/// decrypts cookie in place and returns the decrypted cookie as a separate buffer for convenience
pub fn accept_cookie_reply(
    responder_static_public: &monad_secp::PubKey,
    reply: &mut CookieReply,
    msg_mac1: &[u8; 16],
) -> Result<[u8; 16], ProtocolError> {
    let temp_key = hash!(LABEL_COOKIE, &responder_static_public.bytes_compressed());

    decrypt_in_place(
        &(&temp_key).into(),
        &reply.nonce,
        &mut reply.encrypted_cookie,
        &reply.encrypted_cookie_tag,
        msg_mac1,
    )
    .map_err(CookieError::CookieDecryptionFailed)?;

    Ok(reply.encrypted_cookie)
}

pub fn generate_cookie(cookie_secret: &[u8; 32], nonce: u64, remote_addr: &SocketAddr) -> [u8; 16] {
    let mut address_bytes = [0u8; 18];
    match remote_addr.ip() {
        std::net::IpAddr::V4(addr) => {
            address_bytes[..4].copy_from_slice(&addr.octets());
        }
        std::net::IpAddr::V6(addr) => {
            address_bytes[..16].copy_from_slice(&addr.octets());
        }
    };
    address_bytes[16..18].copy_from_slice(&remote_addr.port().to_le_bytes());

    let cookie_hash = keyed_hash!(cookie_secret, &nonce.to_le_bytes(), &address_bytes);
    let mut cookie = [0u8; 16];
    let hash_bytes: &[u8] = cookie_hash.as_ref();
    cookie.copy_from_slice(&hash_bytes[..16]);
    cookie
}

pub fn verify_cookie<M: crate::messages::MacMessage>(
    cookie_secret: &[u8; 32],
    nonce: u64,
    remote_addr: &SocketAddr,
    static_public: &monad_secp::PubKey,
    message: &M,
) -> Result<(), ProtocolError> {
    let expected_cookie = generate_cookie(cookie_secret, nonce, remote_addr);
    crate::crypto::verify_mac2(message, static_public, &expected_cookie)
        .map_err(|e| CookieError::InvalidCookieMac(e).into())
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use secp256k1::rand::rng;
    use zerocopy::IntoBytes;

    use super::*;
    use crate::protocol::{
        common,
        messages::{CookieReply, HandshakeInitiation, HandshakeResponse},
    };

    #[test]
    fn test_cookie_send_and_accept() {
        let mut rng = rng();

        let _initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_public = responder_keypair.pubkey();

        let msg_sender_index = 12345u32;
        let msg_mac1 = [0x42u8; 16];

        let cookie_secret = [0x33u8; 32];
        let nonce = 555u64;
        let initiator_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();
        let cookie = generate_cookie(&cookie_secret, nonce, &initiator_addr);

        let nonce_secret = [0x44u8; 32];
        let cookie_nonce = 0u128;
        let reply = send_cookie_reply(
            &nonce_secret,
            cookie_nonce,
            &responder_public,
            msg_sender_index,
            &msg_mac1,
            &cookie,
        );

        let reply_bytes = reply.as_bytes();
        let mut reply_bytes_mut = reply_bytes.to_vec();
        let reply = <&mut CookieReply>::try_from(reply_bytes_mut.as_mut_slice())
            .expect("Failed to parse cookie reply");

        let decrypted_cookie = accept_cookie_reply(&responder_public, reply, &msg_mac1)
            .expect("Failed to accept cookie reply");

        assert_eq!(cookie, decrypted_cookie);
    }

    #[test]
    fn test_cookie_with_wrong_mac1_fails() {
        let mut rng = rng();

        let _initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_public = responder_keypair.pubkey();

        let msg_sender_index = 12345u32;
        let msg_mac1 = [0x42u8; 16];
        let wrong_mac1 = [0x99u8; 16];

        let cookie_secret = [0x33u8; 32];
        let nonce = 555u64;
        let initiator_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();
        let cookie = generate_cookie(&cookie_secret, nonce, &initiator_addr);

        let nonce_secret = [0x55u8; 32];
        let cookie_nonce = 1u128;
        let reply = send_cookie_reply(
            &nonce_secret,
            cookie_nonce,
            &responder_public,
            msg_sender_index,
            &msg_mac1,
            &cookie,
        );

        let reply_bytes = reply.as_bytes();
        let mut reply_bytes_mut = reply_bytes.to_vec();
        let reply = <&mut CookieReply>::try_from(reply_bytes_mut.as_mut_slice())
            .expect("Failed to parse cookie reply");

        let result = accept_cookie_reply(&responder_public, reply, &wrong_mac1);

        assert!(result.is_err());
    }

    #[test]
    fn test_cookie_with_wrong_public_key_fails() {
        let mut rng = rng();

        let _initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_public = responder_keypair.pubkey();
        let wrong_keypair = monad_secp::KeyPair::generate(&mut rng);
        let wrong_public = wrong_keypair.pubkey();

        let msg_sender_index = 12345u32;
        let msg_mac1 = [0x42u8; 16];

        let cookie_secret = [0x33u8; 32];
        let nonce = 555u64;
        let initiator_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();
        let cookie = generate_cookie(&cookie_secret, nonce, &initiator_addr);

        let nonce_secret = [0x66u8; 32];
        let cookie_nonce = 2u128;
        let reply = send_cookie_reply(
            &nonce_secret,
            cookie_nonce,
            &responder_public,
            msg_sender_index,
            &msg_mac1,
            &cookie,
        );

        let reply_bytes = reply.as_bytes();
        let mut reply_bytes_mut = reply_bytes.to_vec();
        let reply = <&mut CookieReply>::try_from(reply_bytes_mut.as_mut_slice())
            .expect("Failed to parse cookie reply");

        let result = accept_cookie_reply(&wrong_public, reply, &msg_mac1);

        assert!(result.is_err());
    }

    #[test]
    fn test_generate_cookie_ipv4() {
        let cookie_secret = [0x11u8; 32];
        let nonce = 42u64;
        let addr: SocketAddr = "192.168.1.1:8080".parse().unwrap();

        let cookie1 = generate_cookie(&cookie_secret, nonce, &addr);
        let cookie2 = generate_cookie(&cookie_secret, nonce, &addr);
        assert_eq!(cookie1, cookie2);

        let addr2: SocketAddr = "192.168.1.2:8080".parse().unwrap();
        let cookie3 = generate_cookie(&cookie_secret, nonce, &addr2);
        assert_ne!(cookie1, cookie3);
    }

    #[test]
    fn test_generate_cookie_ipv6() {
        let cookie_secret = [0x22u8; 32];
        let nonce = 99u64;
        let addr: SocketAddr = "[2001:db8::1]:8080".parse().unwrap();

        let cookie1 = generate_cookie(&cookie_secret, nonce, &addr);
        let cookie2 = generate_cookie(&cookie_secret, nonce, &addr);
        assert_eq!(cookie1, cookie2);

        let addr2: SocketAddr = "[2001:db8::2]:8080".parse().unwrap();
        let cookie3 = generate_cookie(&cookie_secret, nonce, &addr2);
        assert_ne!(cookie1, cookie3);
    }

    #[test]
    fn test_verify_cookie_with_zero_mac2() {
        let mut rng = rng();
        let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_public = responder_keypair.pubkey();

        let cookie_secret = [0x33u8; 32];
        let nonce = 555u64;
        let addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();

        let msg = HandshakeInitiation {
            mac2: [0u8; 16].into(),
            ..Default::default()
        };

        let result = verify_cookie(&cookie_secret, nonce, &addr, &responder_public, &msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_cookie_with_valid_mac2() {
        let mut rng = rng();
        let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_public = responder_keypair.pubkey();

        let cookie_secret = [0x33u8; 32];
        let nonce = 555u64;
        let addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();

        let cookie = generate_cookie(&cookie_secret, nonce, &addr);

        let mut msg = HandshakeInitiation::default();

        let responder_static_bytes = responder_public.bytes_compressed();
        let cookie_key = crate::hash!(crate::crypto::LABEL_COOKIE, &responder_static_bytes);
        let mac2: common::MacTag =
            crate::keyed_hash!(cookie_key.as_ref(), msg.mac2_input(), &cookie).into();
        msg.mac2 = mac2;

        let result = verify_cookie(&cookie_secret, nonce, &addr, &responder_public, &msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_cookie_with_wrong_mac2() {
        let mut rng = rng();
        let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_public = responder_keypair.pubkey();

        let cookie_secret = [0x33u8; 32];
        let nonce = 555u64;
        let addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();

        let msg = HandshakeInitiation {
            mac2: [0xFFu8; 16].into(),
            ..Default::default()
        };

        let result = verify_cookie(&cookie_secret, nonce, &addr, &responder_public, &msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_cookie_response() {
        let mut rng = rng();
        let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let initiator_public = initiator_keypair.pubkey();

        let cookie_secret = [0x44u8; 32];
        let nonce = 666u64;
        let addr: SocketAddr = "10.0.0.1:12345".parse().unwrap();

        let cookie = generate_cookie(&cookie_secret, nonce, &addr);

        let mut msg = HandshakeResponse::default();

        let initiator_static_bytes = initiator_public.bytes_compressed();
        let cookie_key = crate::hash!(crate::crypto::LABEL_COOKIE, &initiator_static_bytes);
        let mac2 = crate::keyed_hash!(cookie_key.as_ref(), msg.mac2_input(), &cookie).into();
        msg.mac2 = mac2;

        let result = verify_cookie(&cookie_secret, nonce, &addr, &initiator_public, &msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_cookie_response_with_wrong_mac2() {
        let mut rng = rng();
        let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let initiator_public = initiator_keypair.pubkey();

        let cookie_secret = [0x55u8; 32];
        let nonce = 777u64;
        let addr: SocketAddr = "10.0.0.2:54321".parse().unwrap();

        let msg = HandshakeResponse {
            mac2: [0xAAu8; 16].into(),
            ..Default::default()
        };

        let result = verify_cookie(&cookie_secret, nonce, &addr, &initiator_public, &msg);
        assert!(result.is_err());
    }
}
