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

use zeroize::Zeroizing;

use super::{
    common::*,
    crypto::{
        decrypt_in_place, ecdh, encrypt_in_place, CONSTRUCTION, IDENTIFIER, LABEL_COOKIE,
        LABEL_MAC1,
    },
    errors::{CryptoError, HandshakeError},
    messages::*,
    tai64::Tai64N,
};
use crate::{hash, keyed_hash};

pub struct HandshakeState {
    pub chaining_key: Zeroizing<HashOutput>,
    pub hash: Zeroizing<HashOutput>,
    pub ephemeral_private: Option<monad_secp::KeyPair>,
    pub remote_ephemeral: Option<monad_secp::PubKey>,
    pub remote_static: Option<monad_secp::PubKey>,
    pub sender_index: u32,
    pub receiver_index: u32,
}

impl Default for HandshakeState {
    fn default() -> Self {
        let zero_hash = hash!(&[]);
        Self {
            chaining_key: zero_hash.clone().into(),
            hash: zero_hash.into(),
            ephemeral_private: None,
            remote_ephemeral: None,
            remote_static: None,
            sender_index: 0,
            receiver_index: 0,
        }
    }
}

pub fn send_handshake_init<R, T>(
    rng: &mut R,
    timestamp: T,
    local_session_index: u32,
    initiator_static_keypair: &monad_secp::KeyPair,
    responder_static_public: &monad_secp::PubKey,
    stored_cookie: Option<&[u8; 16]>,
) -> (HandshakeInitiation, HandshakeState)
where
    R: secp256k1::rand::Rng + secp256k1::rand::CryptoRng,
    T: Into<Tai64N>,
{
    let initiator_static_public = initiator_static_keypair.pubkey().bytes_compressed();
    let ephemeral_keypair = monad_secp::KeyPair::generate(rng);
    let ephemeral_public = ephemeral_keypair.pubkey();
    let mut msg = HandshakeInitiation {
        ephemeral_public: ephemeral_public.bytes_compressed(),
        sender_index: local_session_index.into(),
        ..Default::default()
    };
    let mut inititiator = HandshakeState {
        chaining_key: hash!(CONSTRUCTION).into(),
        sender_index: local_session_index,
        ephemeral_private: Some(ephemeral_keypair),
        ..Default::default()
    };

    inititiator.hash = hash!(
        hash!(inititiator.chaining_key.as_ref(), IDENTIFIER).as_ref(),
        &responder_static_public.bytes_compressed()
    )
    .into();
    inititiator.hash = hash!(inititiator.hash.as_ref(), &msg.ephemeral_public).into();

    let temp = keyed_hash!(inititiator.chaining_key.as_ref(), &msg.ephemeral_public);
    inititiator.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let ecdh_es = ecdh(
        inititiator
            .ephemeral_private
            .as_ref()
            .expect("ephemeral private key must be set"),
        responder_static_public,
    );
    let temp = keyed_hash!(inititiator.chaining_key.as_ref(), ecdh_es.as_ref());
    inititiator.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();
    let key = keyed_hash!(temp.as_ref(), inititiator.chaining_key.as_ref(), &[0x2]);

    msg.encrypted_static = initiator_static_public;
    msg.encrypted_static_tag = encrypt_in_place(
        &(&key).into(),
        &(0u64.into()),
        &mut msg.encrypted_static,
        inititiator.hash.as_ref(),
    );
    inititiator.hash = hash!(
        inititiator.hash.as_ref(),
        &msg.encrypted_static,
        &msg.encrypted_static_tag
    )
    .into();

    let ecdh_ss = ecdh(initiator_static_keypair, responder_static_public);
    let temp = keyed_hash!(inititiator.chaining_key.as_ref(), ecdh_ss.as_ref());
    inititiator.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();
    let key = keyed_hash!(temp.as_ref(), inititiator.chaining_key.as_ref(), &[0x2]);

    let timestamp: Tai64N = timestamp.into();
    msg.encrypted_timestamp = timestamp.into();
    msg.encrypted_timestamp_tag = encrypt_in_place(
        &(&key).into(),
        &(0u64.into()),
        &mut msg.encrypted_timestamp,
        inititiator.hash.as_ref(),
    );
    inititiator.hash = hash!(
        inititiator.hash.as_ref(),
        &msg.encrypted_timestamp,
        &msg.encrypted_timestamp_tag
    )
    .into();

    let mac_key = hash!(LABEL_MAC1, &responder_static_public.bytes_compressed());
    msg.mac1 = keyed_hash!(mac_key.as_ref(), msg.mac1_input()).into();
    if let Some(cookie) = stored_cookie {
        let cookie_key = hash!(LABEL_COOKIE, &responder_static_public.bytes_compressed());
        msg.mac2 = keyed_hash!(cookie_key.as_ref(), msg.mac2_input(), cookie).into();
    }

    (msg, inititiator)
}

pub fn accept_handshake_init(
    responder_static_keypair: &monad_secp::KeyPair,
    msg: &mut HandshakeInitiation,
) -> Result<(HandshakeState, Tai64N), HandshakeError> {
    let responder_static_public = responder_static_keypair.pubkey();

    let mut state = HandshakeState {
        chaining_key: hash!(CONSTRUCTION).into(),
        ..Default::default()
    };

    let hash_ck_id = hash!(state.chaining_key.as_ref(), IDENTIFIER);
    state.hash = hash!(
        hash_ck_id.as_ref(),
        &responder_static_public.bytes_compressed()
    )
    .into();

    state.hash = hash!(state.hash.as_ref(), &msg.ephemeral_public).into();
    let remote_ephemeral = monad_secp::PubKey::from_slice(&msg.ephemeral_public)
        .map_err(|e| HandshakeError::StaticKeyDecryptionFailed(CryptoError::InvalidKey(e)))?;
    state.remote_ephemeral = Some(remote_ephemeral);
    state.receiver_index = msg.sender_index.get();

    let temp = keyed_hash!(state.chaining_key.as_ref(), &msg.ephemeral_public);
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let ecdh_ee = ecdh(responder_static_keypair, &remote_ephemeral);
    let temp = keyed_hash!(state.chaining_key.as_ref(), ecdh_ee.as_ref());
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();
    let key = keyed_hash!(temp.as_ref(), state.chaining_key.as_ref(), &[0x2]);

    let hash = state.hash.clone();
    state.hash = hash!(
        state.hash.as_ref(),
        &msg.encrypted_static,
        &msg.encrypted_static_tag
    )
    .into();
    decrypt_in_place(
        &(&key).into(),
        &(0u64).into(),
        &mut msg.encrypted_static,
        &msg.encrypted_static_tag,
        hash.as_ref(),
    )
    .map_err(HandshakeError::StaticKeyDecryptionFailed)?;

    let remote_static = monad_secp::PubKey::from_slice(&msg.encrypted_static)
        .map_err(|e| HandshakeError::StaticKeyDecryptionFailed(CryptoError::InvalidKey(e)))?;
    state.remote_static = Some(remote_static);

    let ecdh_ss = ecdh(responder_static_keypair, &remote_static);
    let temp = keyed_hash!(state.chaining_key.as_ref(), ecdh_ss.as_ref());
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();
    let key = keyed_hash!(temp.as_ref(), state.chaining_key.as_ref(), &[0x2]);

    let hash = state.hash.clone();
    state.hash = hash!(
        state.hash.as_ref(),
        &msg.encrypted_timestamp,
        &msg.encrypted_timestamp_tag
    )
    .into();
    decrypt_in_place(
        &(&key).into(),
        &(0u64).into(),
        &mut msg.encrypted_timestamp,
        &msg.encrypted_timestamp_tag,
        hash.as_ref(),
    )
    .map_err(HandshakeError::TimestampDecryptionFailed)?;

    let timestamp = Tai64N::try_from(msg.encrypted_timestamp)?;

    Ok((state, timestamp))
}

pub fn send_handshake_response<R: secp256k1::rand::Rng + secp256k1::rand::CryptoRng>(
    rng: &mut R,
    local_session_index: u32,
    state: &mut HandshakeState,
    psk: &[u8; 32],
    stored_cookie: Option<&[u8; 16]>,
) -> (HandshakeResponse, TransportKeys) {
    let ephemeral_keypair = monad_secp::KeyPair::generate(rng);
    let ephemeral_public = ephemeral_keypair.pubkey();
    state.sender_index = local_session_index;
    let initiator_static_public = state
        .remote_static
        .as_ref()
        .expect("remote static key must be set");
    let mut msg = HandshakeResponse {
        sender_index: local_session_index.into(),
        receiver_index: state.receiver_index.into(),
        ephemeral_public: ephemeral_public.bytes_compressed(),
        ..Default::default()
    };

    state.hash = hash!(state.hash.as_ref(), &msg.ephemeral_public).into();

    let temp = keyed_hash!(state.chaining_key.as_ref(), &msg.ephemeral_public);
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let remote_ephemeral = state
        .remote_ephemeral
        .as_ref()
        .expect("remote ephemeral key must be set");

    let ecdh_ee = ecdh(&ephemeral_keypair, remote_ephemeral);
    let temp = keyed_hash!(state.chaining_key.as_ref(), ecdh_ee.as_ref());
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let ecdh_se = ecdh(&ephemeral_keypair, initiator_static_public);
    let temp = keyed_hash!(state.chaining_key.as_ref(), ecdh_se.as_ref());
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let temp = keyed_hash!(state.chaining_key.as_ref(), psk);
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();
    let temp2 = keyed_hash!(temp.as_ref(), state.chaining_key.as_ref(), &[0x2]);
    let key = keyed_hash!(temp.as_ref(), temp2.as_ref(), &[0x3]);

    state.hash = hash!(state.hash.as_ref(), temp2.as_ref()).into();

    msg.encrypted_nothing_tag =
        encrypt_in_place(&(&key).into(), &(0u64).into(), &mut [], state.hash.as_ref());
    state.hash = hash!(state.hash.as_ref(), &msg.encrypted_nothing_tag).into();

    let mac_key = hash!(LABEL_MAC1, &initiator_static_public.bytes_compressed());
    msg.mac1 = keyed_hash!(mac_key.as_ref(), msg.mac1_input()).into();

    if let Some(cookie) = stored_cookie {
        let cookie_key = hash!(LABEL_COOKIE, &initiator_static_public.bytes_compressed());
        msg.mac2 = keyed_hash!(cookie_key.as_ref(), msg.mac2_input(), cookie).into();
    }

    (msg, derive_transport_keys(&state.chaining_key, false))
}

pub fn accept_handshake_response(
    initiator_static_keypair: &monad_secp::KeyPair,
    msg: &mut HandshakeResponse,
    state: &mut HandshakeState,
    psk: &[u8; 32],
) -> Result<TransportKeys, HandshakeError> {
    state.receiver_index = msg.sender_index.get();
    let remote_ephemeral = monad_secp::PubKey::from_slice(&msg.ephemeral_public)
        .map_err(|e| HandshakeError::StaticKeyDecryptionFailed(CryptoError::InvalidKey(e)))?;
    state.remote_ephemeral = Some(remote_ephemeral);
    state.hash = hash!(state.hash.as_ref(), &msg.ephemeral_public).into();

    let temp = keyed_hash!(state.chaining_key.as_ref(), &msg.ephemeral_public);
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let ecdh_ee = ecdh(
        state
            .ephemeral_private
            .as_ref()
            .expect("ephemeral private key must be set"),
        &remote_ephemeral,
    );
    let temp = keyed_hash!(state.chaining_key.as_ref(), ecdh_ee.as_ref());
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let ecdh_se = ecdh(initiator_static_keypair, &remote_ephemeral);
    let temp = keyed_hash!(state.chaining_key.as_ref(), ecdh_se.as_ref());
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();

    let temp = keyed_hash!(state.chaining_key.as_ref(), psk);
    state.chaining_key = keyed_hash!(temp.as_ref(), &[0x1]).into();
    let temp2 = keyed_hash!(temp.as_ref(), state.chaining_key.as_ref(), &[0x2]);
    let key = keyed_hash!(temp.as_ref(), temp2.as_ref(), &[0x3]);
    state.hash = hash!(state.hash.as_ref(), temp2.as_ref()).into();

    let hash = state.hash.clone();
    state.hash = hash!(state.hash.as_ref(), &msg.encrypted_nothing_tag).into();
    decrypt_in_place(
        &(&key).into(),
        &(0u64).into(),
        &mut [],
        &msg.encrypted_nothing_tag,
        hash.as_ref(),
    )
    .map_err(HandshakeError::EmptyMessageDecryptionFailed)?;

    Ok(derive_transport_keys(&state.chaining_key, true))
}

pub fn derive_transport_keys(chaining_key: &HashOutput, is_initiator: bool) -> TransportKeys {
    let temp1 = keyed_hash!(chaining_key.as_ref(), &[]);
    let temp2 = keyed_hash!(temp1.as_ref(), &[0x1]);
    let temp3 = keyed_hash!(temp1.as_ref(), temp2.as_ref(), &[0x2]);

    if is_initiator {
        TransportKeys {
            send_key: CipherKey::from(&temp2),
            recv_key: CipherKey::from(&temp3),
        }
    } else {
        TransportKeys {
            send_key: CipherKey::from(&temp3),
            recv_key: CipherKey::from(&temp2),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use proptest::prelude::*;
    use secp256k1::rand::{rngs::StdRng, Rng, SeedableRng};
    use serde::Serialize;
    use zerocopy::IntoBytes;

    use super::*;
    use crate::protocol::{common, cookies, crypto};

    #[derive(Serialize)]
    struct ProtocolTrace {
        test_name: String,
        seed: u64,
        timestamp: u64,
        initiator_static_private: String,
        initiator_static_public: String,
        responder_static_private: String,
        responder_static_public: String,
        initiator_session_index: u32,
        responder_session_index: u32,
        init_message: MessageTrace,
        response_message: MessageTrace,
        transport_keys: TransportKeysTrace,
    }

    #[derive(Serialize)]
    struct MessageTrace {
        raw_bytes: String,
        sender_index: u32,
        receiver_index: u32,
        ephemeral_public: String,
        encrypted_static: String,
        encrypted_static_tag: String,
        encrypted_timestamp: String,
        encrypted_timestamp_tag: String,
        encrypted_nothing_tag: Option<String>,
        mac1: String,
        mac2: String,
    }

    #[derive(Serialize)]
    struct TransportKeysTrace {
        initiator_send_key: String,
        initiator_recv_key: String,
        responder_send_key: String,
        responder_recv_key: String,
    }

    #[derive(Serialize)]
    struct CookieProtocolTrace {
        test_name: String,
        seed: u64,
        cookie_secret: String,
        cookie_value: String,
        init_without_cookie: MessageTrace,
        cookie_reply: CookieReplyTrace,
        init_with_cookie: MessageTrace,
        response_with_cookie: MessageTrace,
    }

    #[derive(Serialize)]
    struct CookieReplyTrace {
        raw_bytes: String,
        receiver_index: u32,
        nonce: String,
        encrypted_cookie: String,
    }

    #[derive(Serialize)]
    struct EncryptedDataTrace {
        test_name: String,
        plaintext: String,
        nonce: String,
        sender_index: u32,
        receiver_index: u32,
        encrypted_payload: String,
        auth_tag: String,
        complete_packet: String,
    }

    fn to_hex(data: &[u8]) -> String {
        hex::encode(data)
    }

    fn extract_init_message_trace(msg: &HandshakeInitiation) -> MessageTrace {
        MessageTrace {
            raw_bytes: to_hex(msg.as_bytes()),
            sender_index: msg.sender_index.get(),
            receiver_index: 0,
            ephemeral_public: to_hex(msg.ephemeral_public.as_bytes()),
            encrypted_static: to_hex(msg.encrypted_static.as_bytes()),
            encrypted_static_tag: to_hex(&msg.encrypted_static_tag),
            encrypted_timestamp: to_hex(&msg.encrypted_timestamp),
            encrypted_timestamp_tag: to_hex(&msg.encrypted_timestamp_tag),
            encrypted_nothing_tag: None,
            mac1: to_hex(msg.mac1.as_ref()),
            mac2: to_hex(msg.mac2.as_ref()),
        }
    }

    fn extract_response_trace(msg: &HandshakeResponse) -> MessageTrace {
        MessageTrace {
            raw_bytes: to_hex(msg.as_bytes()),
            sender_index: msg.sender_index.get(),
            receiver_index: msg.receiver_index.get(),
            ephemeral_public: to_hex(msg.ephemeral_public.as_bytes()),
            encrypted_static: String::new(),
            encrypted_static_tag: String::new(),
            encrypted_timestamp: String::new(),
            encrypted_timestamp_tag: String::new(),
            encrypted_nothing_tag: Some(to_hex(&msg.encrypted_nothing_tag)),
            mac1: to_hex(msg.mac1.as_ref()),
            mac2: to_hex(msg.mac2.as_ref()),
        }
    }

    #[test]
    fn test_complete_handshake_trace() {
        let seed = 42u64;
        let mut rng = StdRng::seed_from_u64(seed);
        let timestamp = 1700000000u64;
        let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp);

        let initiator_static_keypair = monad_secp::KeyPair::generate(&mut rng);
        let initiator_static_public = initiator_static_keypair.pubkey();
        let responder_static_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_static_public = responder_static_keypair.pubkey();

        let initiator_index = 100u32;
        let responder_index = 200u32;

        let (init_msg, init_state) = send_handshake_init(
            &mut rng,
            system_time,
            initiator_index,
            &initiator_static_keypair,
            &responder_static_public,
            None,
        );

        let init_trace = extract_init_message_trace(&init_msg);

        let mut init_msg_mut = init_msg;
        let (responder_state, _timestamp) =
            accept_handshake_init(&responder_static_keypair, &mut init_msg_mut).unwrap();

        let mut responder_state_mut = responder_state;
        let psk = [0u8; 32];
        let (resp_msg, responder_transport_keys) = send_handshake_response(
            &mut rng,
            responder_index,
            &mut responder_state_mut,
            &psk,
            None,
        );

        let response_trace = extract_response_trace(&resp_msg);

        let mut resp_msg_mut = resp_msg;
        let mut init_state_mut = init_state;
        let initiator_transport_keys = accept_handshake_response(
            &initiator_static_keypair,
            &mut resp_msg_mut,
            &mut init_state_mut,
            &psk,
        )
        .unwrap();

        let trace = ProtocolTrace {
            test_name: "standard_handshake".to_string(),
            seed,
            timestamp,
            initiator_static_private: initiator_static_keypair.privkey_view().to_string(),
            initiator_static_public: to_hex(&initiator_static_public.bytes_compressed()),
            responder_static_private: responder_static_keypair.privkey_view().to_string(),
            responder_static_public: to_hex(&responder_static_public.bytes_compressed()),
            initiator_session_index: initiator_index,
            responder_session_index: responder_index,
            init_message: init_trace,
            response_message: response_trace,
            transport_keys: TransportKeysTrace {
                initiator_send_key: to_hex(initiator_transport_keys.send_key.as_ref()),
                initiator_recv_key: to_hex(initiator_transport_keys.recv_key.as_ref()),
                responder_send_key: to_hex(responder_transport_keys.send_key.as_ref()),
                responder_recv_key: to_hex(responder_transport_keys.recv_key.as_ref()),
            },
        };

        insta::assert_yaml_snapshot!("complete_handshake_trace", trace);
    }

    #[test]
    fn test_cookie_handshake_trace() {
        let seed = 43u64;
        let mut rng = StdRng::seed_from_u64(seed);
        let timestamp = 1700000000u64;
        let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp);

        let initiator_static_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_static_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_static_public = responder_static_keypair.pubkey();

        let initiator_index = 101u32;
        let responder_index = 201u32;

        let (init_msg, _init_state) = send_handshake_init(
            &mut rng,
            system_time,
            initiator_index,
            &initiator_static_keypair,
            &responder_static_public,
            None,
        );

        let init_no_cookie_trace = extract_init_message_trace(&init_msg);
        let init_mac1 = init_msg.mac1;
        let init_sender_index = init_msg.sender_index.get();

        let cookie_secret = [0x77u8; 32];
        let nonce = 42u64;
        let initiator_ip: std::net::IpAddr = "192.168.1.1".parse().unwrap();
        let cookie = cookies::generate_cookie(&cookie_secret, nonce, initiator_ip);

        let nonce_secret = [0x77u8; 32];
        let cookie_nonce = 1234u128;
        let cookie_reply = cookies::send_cookie_reply(
            &nonce_secret,
            cookie_nonce,
            &responder_static_public,
            init_sender_index,
            init_mac1.as_ref(),
            &cookie,
        );

        let cookie_reply_trace = CookieReplyTrace {
            raw_bytes: to_hex(cookie_reply.as_bytes()),
            receiver_index: cookie_reply.receiver_index.get(),
            nonce: to_hex(cookie_reply.nonce.as_ref()),
            encrypted_cookie: to_hex(&cookie_reply.encrypted_cookie),
        };

        let mut cookie_reply_mut = cookie_reply;
        let extracted_cookie = cookies::accept_cookie_reply(
            &responder_static_public,
            &mut cookie_reply_mut,
            init_mac1.as_ref(),
        )
        .unwrap();

        let (init_msg_with_cookie, _init_state_with_cookie) = send_handshake_init(
            &mut rng,
            system_time,
            initiator_index + 1,
            &initiator_static_keypair,
            &responder_static_public,
            Some(&extracted_cookie),
        );

        let init_with_cookie_trace = extract_init_message_trace(&init_msg_with_cookie);

        let mut init_msg_mut = init_msg_with_cookie;
        let (responder_state, _timestamp) =
            accept_handshake_init(&responder_static_keypair, &mut init_msg_mut).unwrap();

        let mut responder_state_mut = responder_state;
        let psk = [0u8; 32];
        let (resp_msg, _responder_keys) = send_handshake_response(
            &mut rng,
            responder_index,
            &mut responder_state_mut,
            &psk,
            Some(&cookie),
        );

        let response_with_cookie_trace = extract_response_trace(&resp_msg);

        let trace = CookieProtocolTrace {
            test_name: "cookie_handshake".to_string(),
            seed,
            cookie_secret: to_hex(&cookie_secret),
            cookie_value: to_hex(&cookie),
            init_without_cookie: init_no_cookie_trace,
            cookie_reply: cookie_reply_trace,
            init_with_cookie: init_with_cookie_trace,
            response_with_cookie: response_with_cookie_trace,
        };

        insta::assert_yaml_snapshot!("cookie_handshake_trace", trace);
    }

    #[test]
    fn test_data_encryption_trace() {
        let seed = 44u64;
        let mut rng = StdRng::seed_from_u64(seed);
        let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000);

        let initiator_static_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_static_keypair = monad_secp::KeyPair::generate(&mut rng);
        let responder_static_public = responder_static_keypair.pubkey();

        let initiator_index = 102u32;
        let responder_index = 202u32;

        let psk = [0u8; 32];
        let (init_msg, init_state) = send_handshake_init(
            &mut rng,
            system_time,
            initiator_index,
            &initiator_static_keypair,
            &responder_static_public,
            None,
        );

        let mut init_msg_mut = init_msg;
        let (responder_state, _timestamp) =
            accept_handshake_init(&responder_static_keypair, &mut init_msg_mut).unwrap();

        let mut responder_state_mut = responder_state;
        let (resp_msg, responder_transport_keys) = send_handshake_response(
            &mut rng,
            responder_index,
            &mut responder_state_mut,
            &psk,
            None,
        );

        let mut resp_msg_mut = resp_msg;
        let mut init_state_mut = init_state;
        let initiator_transport_keys = accept_handshake_response(
            &initiator_static_keypair,
            &mut resp_msg_mut,
            &mut init_state_mut,
            &psk,
        )
        .unwrap();

        // Encrypt data from initiator to responder
        let test_message = b"Hello, encrypted world!";
        let mut encrypted_message = test_message.to_vec();
        let nonce = common::CipherNonce::from(1u64);
        let tag = crypto::encrypt_in_place(
            &initiator_transport_keys.send_key,
            &nonce,
            &mut encrypted_message,
            &[],
        );

        let mut data_packet = Vec::new();
        data_packet.extend_from_slice(&[0x04]); // Data message type
        data_packet.extend_from_slice(&responder_index.to_le_bytes());
        data_packet.extend_from_slice(nonce.as_ref());
        data_packet.extend_from_slice(&encrypted_message);
        data_packet.extend_from_slice(&tag);

        let initiator_to_responder = EncryptedDataTrace {
            test_name: "initiator_to_responder".to_string(),
            plaintext: String::from_utf8_lossy(test_message).to_string(),
            nonce: to_hex(nonce.as_ref()),
            sender_index: initiator_index,
            receiver_index: responder_index,
            encrypted_payload: to_hex(&encrypted_message),
            auth_tag: to_hex(&tag),
            complete_packet: to_hex(&data_packet),
        };

        // Encrypt data from responder to initiator
        let test_message_2 = b"Response from responder!";
        let mut encrypted_message_2 = test_message_2.to_vec();
        let nonce_2 = common::CipherNonce::from(1u64);
        let tag_2 = crypto::encrypt_in_place(
            &responder_transport_keys.send_key,
            &nonce_2,
            &mut encrypted_message_2,
            &[],
        );

        let mut data_packet_2 = Vec::new();
        data_packet_2.extend_from_slice(&[0x04]); // Data message type
        data_packet_2.extend_from_slice(&initiator_index.to_le_bytes());
        data_packet_2.extend_from_slice(nonce_2.as_ref());
        data_packet_2.extend_from_slice(&encrypted_message_2);
        data_packet_2.extend_from_slice(&tag_2);

        let responder_to_initiator = EncryptedDataTrace {
            test_name: "responder_to_initiator".to_string(),
            plaintext: String::from_utf8_lossy(test_message_2).to_string(),
            nonce: to_hex(nonce_2.as_ref()),
            sender_index: responder_index,
            receiver_index: initiator_index,
            encrypted_payload: to_hex(&encrypted_message_2),
            auth_tag: to_hex(&tag_2),
            complete_packet: to_hex(&data_packet_2),
        };

        insta::assert_yaml_snapshot!(
            "data_encryption_traces",
            vec![initiator_to_responder, responder_to_initiator,]
        );
    }

    #[test]
    fn test_multiple_protocol_vectors() {
        let seeds = [100u64, 200u64, 300u64];
        let mut all_traces = Vec::new();

        for seed in &seeds {
            let mut rng = StdRng::seed_from_u64(*seed);
            let timestamp = 1700000000u64 + seed;
            let system_time = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp);

            let initiator_static_keypair = monad_secp::KeyPair::generate(&mut rng);
            let initiator_static_public = initiator_static_keypair.pubkey();
            let responder_static_keypair = monad_secp::KeyPair::generate(&mut rng);
            let responder_static_public = responder_static_keypair.pubkey();

            let initiator_index = rng.random::<u32>();
            let responder_index = rng.random::<u32>();

            let (init_msg, init_state) = send_handshake_init(
                &mut rng,
                system_time,
                initiator_index,
                &initiator_static_keypair,
                &responder_static_public,
                None,
            );

            let init_trace = extract_init_message_trace(&init_msg);

            let mut init_msg_mut = init_msg;
            let (responder_state, _timestamp) =
                accept_handshake_init(&responder_static_keypair, &mut init_msg_mut).unwrap();

            let mut responder_state_mut = responder_state;
            let psk = [0u8; 32];
            let (resp_msg, responder_keys) = send_handshake_response(
                &mut rng,
                responder_index,
                &mut responder_state_mut,
                &psk,
                None,
            );

            let response_trace = extract_response_trace(&resp_msg);

            let mut resp_msg_mut = resp_msg;
            let mut init_state_mut = init_state;
            let initiator_keys = accept_handshake_response(
                &initiator_static_keypair,
                &mut resp_msg_mut,
                &mut init_state_mut,
                &psk,
            )
            .unwrap();

            all_traces.push(ProtocolTrace {
                test_name: format!("vector_{}", seed),
                seed: *seed,
                timestamp,
                initiator_static_private: initiator_static_keypair.privkey_view().to_string(),
                initiator_static_public: to_hex(&initiator_static_public.bytes_compressed()),
                responder_static_private: responder_static_keypair.privkey_view().to_string(),
                responder_static_public: to_hex(&responder_static_public.bytes_compressed()),
                initiator_session_index: initiator_index,
                responder_session_index: responder_index,
                init_message: init_trace,
                response_message: response_trace,
                transport_keys: TransportKeysTrace {
                    initiator_send_key: to_hex(initiator_keys.send_key.as_ref()),
                    initiator_recv_key: to_hex(initiator_keys.recv_key.as_ref()),
                    responder_send_key: to_hex(responder_keys.send_key.as_ref()),
                    responder_recv_key: to_hex(responder_keys.recv_key.as_ref()),
                },
            });
        }

        insta::assert_yaml_snapshot!("multiple_protocol_vectors", all_traces);
    }

    fn tai64n_timestamp_strategy() -> impl Strategy<Value = (u64, u32)> {
        prop_oneof![
            // edge cases
            Just((0u64, 0u32)),
            Just((u64::MAX, 0u32)),
            Just((u64::MAX, 999_999_999u32)),
            // random timestamps
            (any::<u64>(), 0u32..1_000_000_000u32),
        ]
    }

    proptest! {
        #[test]
        fn prop_handshake_timestamp_no_panic(
            (seconds, nanos) in tai64n_timestamp_strategy(),
        ) {
            let mut timestamp_bytes = [0u8; 12];
            timestamp_bytes[..8].copy_from_slice(&seconds.to_be_bytes());
            timestamp_bytes[8..].copy_from_slice(&nanos.to_be_bytes());

            let mut rng = StdRng::seed_from_u64(999);

            let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
            let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
            let responder_public = responder_keypair.pubkey();

            let timestamp = Tai64N::try_from(timestamp_bytes).unwrap();

            let (init_msg, _) = send_handshake_init(
                &mut rng,
                timestamp,
                1,
                &initiator_keypair,
                &responder_public,
                None,
            );

            let mut init_msg_mut = init_msg;
            let (_, decoded_timestamp) =
                accept_handshake_init(&responder_keypair, &mut init_msg_mut).unwrap();

            prop_assert_eq!(decoded_timestamp, timestamp);

            let normal_timestamp: Tai64N = SystemTime::now().into();
            let _ = decoded_timestamp.cmp(&normal_timestamp);
            let _ = decoded_timestamp <= normal_timestamp;
            let _ = decoded_timestamp >= normal_timestamp;

            let stored: Option<Tai64N> = Some(decoded_timestamp);
            let _ = stored.is_some_and(|max| normal_timestamp <= max);
        }
    }
}
