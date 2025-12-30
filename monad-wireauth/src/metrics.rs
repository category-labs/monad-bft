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

pub const GAUGE_WIREAUTH_STATE_INITIATING_SESSIONS: &str =
    "monad.wireauth.state.initiating_sessions";
pub const GAUGE_WIREAUTH_STATE_RESPONDING_SESSIONS: &str =
    "monad.wireauth.state.responding_sessions";
pub const GAUGE_WIREAUTH_STATE_TRANSPORT_SESSIONS: &str = "monad.wireauth.state.transport_sessions";
pub const GAUGE_WIREAUTH_STATE_TOTAL_SESSIONS: &str = "monad.wireauth.state.total_sessions";
pub const GAUGE_WIREAUTH_STATE_ALLOCATED_INDICES: &str = "monad.wireauth.state.allocated_indices";
pub const GAUGE_WIREAUTH_STATE_SESSIONS_BY_PUBLIC_KEY: &str =
    "monad.wireauth.state.sessions_by_public_key";
pub const GAUGE_WIREAUTH_STATE_SESSIONS_BY_SOCKET: &str = "monad.wireauth.state.sessions_by_socket";
pub const GAUGE_WIREAUTH_STATE_SESSION_INDEX_ALLOCATED: &str =
    "monad.wireauth.state.session_index_allocated";
pub const GAUGE_WIREAUTH_STATE_SESSION_ESTABLISHED_INITIATOR: &str =
    "monad.wireauth.state.session_established_initiator";
pub const GAUGE_WIREAUTH_STATE_SESSION_ESTABLISHED_RESPONDER: &str =
    "monad.wireauth.state.session_established_responder";
pub const GAUGE_WIREAUTH_STATE_SESSION_TERMINATED: &str = "monad.wireauth.state.session_terminated";
pub const GAUGE_WIREAUTH_STATE_TIMERS_SIZE: &str = "monad.wireauth.state.timers_size";
pub const GAUGE_WIREAUTH_STATE_PACKET_QUEUE_SIZE: &str = "monad.wireauth.state.packet_queue_size";
pub const GAUGE_WIREAUTH_STATE_INITIATED_SESSION_BY_PEER_SIZE: &str =
    "monad.wireauth.state.initiated_session_by_peer_size";
pub const GAUGE_WIREAUTH_STATE_ACCEPTED_SESSIONS_BY_PEER_SIZE: &str =
    "monad.wireauth.state.accepted_sessions_by_peer_size";
pub const GAUGE_WIREAUTH_STATE_IP_SESSION_COUNTS_SIZE: &str =
    "monad.wireauth.state.ip_session_counts_size";
pub const GAUGE_WIREAUTH_FILTER_IP_REQUEST_HISTORY_SIZE: &str =
    "monad.wireauth.filter.ip_request_history_size";

pub const GAUGE_WIREAUTH_FILTER_PASS: &str = "monad.wireauth.filter.pass";
pub const GAUGE_WIREAUTH_FILTER_SEND_COOKIE: &str = "monad.wireauth.filter.send_cookie";
pub const GAUGE_WIREAUTH_FILTER_DROP: &str = "monad.wireauth.filter.drop";

pub const GAUGE_WIREAUTH_API_CONNECT: &str = "monad.wireauth.api.connect";
pub const GAUGE_WIREAUTH_API_DECRYPT: &str = "monad.wireauth.api.decrypt";
pub const GAUGE_WIREAUTH_API_ENCRYPT_BY_PUBLIC_KEY: &str =
    "monad.wireauth.api.encrypt_by_public_key";
pub const GAUGE_WIREAUTH_API_ENCRYPT_BY_SOCKET: &str = "monad.wireauth.api.encrypt_by_socket";
pub const GAUGE_WIREAUTH_API_DISCONNECT: &str = "monad.wireauth.api.disconnect";
pub const GAUGE_WIREAUTH_API_DISPATCH_CONTROL: &str = "monad.wireauth.api.dispatch_control";
pub const GAUGE_WIREAUTH_API_NEXT_PACKET: &str = "monad.wireauth.api.next_packet";
pub const GAUGE_WIREAUTH_API_TICK: &str = "monad.wireauth.api.tick";

pub const GAUGE_WIREAUTH_DISPATCH_HANDSHAKE_INIT: &str =
    "monad.wireauth.dispatch.handshake_initiation";
pub const GAUGE_WIREAUTH_DISPATCH_HANDSHAKE_RESPONSE: &str =
    "monad.wireauth.dispatch.handshake_response";
pub const GAUGE_WIREAUTH_DISPATCH_COOKIE_REPLY: &str = "monad.wireauth.dispatch.cookie_reply";
pub const GAUGE_WIREAUTH_DISPATCH_KEEPALIVE: &str = "monad.wireauth.dispatch.keepalive";

pub const GAUGE_WIREAUTH_ERROR_CONNECT: &str = "monad.wireauth.error.connect";
pub const GAUGE_WIREAUTH_ERROR_DECRYPT: &str = "monad.wireauth.error.decrypt";
pub const GAUGE_WIREAUTH_ERROR_DECRYPT_NONCE_OUTSIDE_WINDOW: &str =
    "monad.wireauth.error.decrypt.nonce_outside_window";
pub const GAUGE_WIREAUTH_ERROR_DECRYPT_NONCE_DUPLICATE: &str =
    "monad.wireauth.error.decrypt.nonce_duplicate";
pub const GAUGE_WIREAUTH_ERROR_DECRYPT_MAC: &str = "monad.wireauth.error.decrypt.mac";
pub const GAUGE_WIREAUTH_ERROR_ENCRYPT_BY_PUBLIC_KEY: &str =
    "monad.wireauth.error.encrypt_by_public_key";
pub const GAUGE_WIREAUTH_ERROR_ENCRYPT_BY_SOCKET: &str = "monad.wireauth.error.encrypt_by_socket";
pub const GAUGE_WIREAUTH_ERROR_DISPATCH_CONTROL: &str = "monad.wireauth.error.dispatch_control";

pub const GAUGE_WIREAUTH_ERROR_SESSION_EXHAUSTED: &str = "monad.wireauth.error.session_exhausted";
pub const GAUGE_WIREAUTH_ERROR_MAC1_VERIFICATION_FAILED: &str =
    "monad.wireauth.error.mac1_verification_failed";
pub const GAUGE_WIREAUTH_ERROR_TIMESTAMP_REPLAY: &str = "monad.wireauth.error.timestamp_replay";
pub const GAUGE_WIREAUTH_ERROR_SESSION_NOT_FOUND: &str = "monad.wireauth.error.session_not_found";
pub const GAUGE_WIREAUTH_ERROR_SESSION_INDEX_NOT_FOUND: &str =
    "monad.wireauth.error.session_index_not_found";
pub const GAUGE_WIREAUTH_ERROR_HANDSHAKE_INIT_VALIDATION: &str =
    "monad.wireauth.error.handshake_init_validation";
pub const GAUGE_WIREAUTH_ERROR_COOKIE_REPLY: &str = "monad.wireauth.error.cookie_reply";
pub const GAUGE_WIREAUTH_ERROR_HANDSHAKE_RESPONSE_VALIDATION: &str =
    "monad.wireauth.error.handshake_response_validation";

pub const GAUGE_WIREAUTH_ENQUEUED_HANDSHAKE_INIT: &str = "monad.wireauth.enqueued.handshake_init";
pub const GAUGE_WIREAUTH_ENQUEUED_HANDSHAKE_RESPONSE: &str =
    "monad.wireauth.enqueued.handshake_response";
pub const GAUGE_WIREAUTH_ENQUEUED_COOKIE_REPLY: &str = "monad.wireauth.enqueued.cookie_reply";
pub const GAUGE_WIREAUTH_ENQUEUED_KEEPALIVE: &str = "monad.wireauth.enqueued.keepalive";

pub const GAUGE_WIREAUTH_RATE_LIMIT_DROP: &str = "monad.wireauth.rate_limit.drop";

pub trait MetricNames {
    const STATE_INITIATING_SESSIONS: &'static str;
    const STATE_RESPONDING_SESSIONS: &'static str;
    const STATE_TRANSPORT_SESSIONS: &'static str;
    const STATE_TOTAL_SESSIONS: &'static str;
    const STATE_ALLOCATED_INDICES: &'static str;
    const STATE_SESSIONS_BY_PUBLIC_KEY: &'static str;
    const STATE_SESSIONS_BY_SOCKET: &'static str;
    const STATE_SESSION_INDEX_ALLOCATED: &'static str;
    const STATE_SESSION_ESTABLISHED_INITIATOR: &'static str;
    const STATE_SESSION_ESTABLISHED_RESPONDER: &'static str;
    const STATE_SESSION_TERMINATED: &'static str;
    const STATE_TIMERS_SIZE: &'static str;
    const STATE_PACKET_QUEUE_SIZE: &'static str;
    const STATE_INITIATED_SESSION_BY_PEER_SIZE: &'static str;
    const STATE_ACCEPTED_SESSIONS_BY_PEER_SIZE: &'static str;
    const STATE_IP_SESSION_COUNTS_SIZE: &'static str;

    const FILTER_PASS: &'static str;
    const FILTER_SEND_COOKIE: &'static str;
    const FILTER_DROP: &'static str;
    const FILTER_IP_REQUEST_HISTORY_SIZE: &'static str;

    const API_CONNECT: &'static str;
    const API_DECRYPT: &'static str;
    const API_ENCRYPT_BY_PUBLIC_KEY: &'static str;
    const API_ENCRYPT_BY_SOCKET: &'static str;
    const API_DISCONNECT: &'static str;
    const API_DISPATCH_CONTROL: &'static str;
    const API_NEXT_PACKET: &'static str;
    const API_TICK: &'static str;

    const DISPATCH_HANDSHAKE_INIT: &'static str;
    const DISPATCH_HANDSHAKE_RESPONSE: &'static str;
    const DISPATCH_COOKIE_REPLY: &'static str;
    const DISPATCH_KEEPALIVE: &'static str;

    const ERROR_CONNECT: &'static str;
    const ERROR_DECRYPT: &'static str;
    const ERROR_DECRYPT_NONCE_OUTSIDE_WINDOW: &'static str;
    const ERROR_DECRYPT_NONCE_DUPLICATE: &'static str;
    const ERROR_DECRYPT_MAC: &'static str;
    const ERROR_ENCRYPT_BY_PUBLIC_KEY: &'static str;
    const ERROR_ENCRYPT_BY_SOCKET: &'static str;
    const ERROR_DISPATCH_CONTROL: &'static str;

    const ERROR_SESSION_EXHAUSTED: &'static str;
    const ERROR_MAC1_VERIFICATION_FAILED: &'static str;
    const ERROR_TIMESTAMP_REPLAY: &'static str;
    const ERROR_SESSION_NOT_FOUND: &'static str;
    const ERROR_SESSION_INDEX_NOT_FOUND: &'static str;
    const ERROR_HANDSHAKE_INIT_VALIDATION: &'static str;
    const ERROR_COOKIE_REPLY: &'static str;
    const ERROR_HANDSHAKE_RESPONSE_VALIDATION: &'static str;

    const ENQUEUED_HANDSHAKE_INIT: &'static str;
    const ENQUEUED_HANDSHAKE_RESPONSE: &'static str;
    const ENQUEUED_COOKIE_REPLY: &'static str;
    const ENQUEUED_KEEPALIVE: &'static str;

    const RATE_LIMIT_DROP: &'static str;
}

#[macro_export]
macro_rules! impl_metric_names {
    ($type:ident, $transport:literal) => {
        impl $crate::metrics::MetricNames for $type {
            const STATE_INITIATING_SESSIONS: &'static str =
                concat!("monad.wireauth.", $transport, ".state.initiating_sessions");
            const STATE_RESPONDING_SESSIONS: &'static str =
                concat!("monad.wireauth.", $transport, ".state.responding_sessions");
            const STATE_TRANSPORT_SESSIONS: &'static str =
                concat!("monad.wireauth.", $transport, ".state.transport_sessions");
            const STATE_TOTAL_SESSIONS: &'static str =
                concat!("monad.wireauth.", $transport, ".state.total_sessions");
            const STATE_ALLOCATED_INDICES: &'static str =
                concat!("monad.wireauth.", $transport, ".state.allocated_indices");
            const STATE_SESSIONS_BY_PUBLIC_KEY: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".state.sessions_by_public_key"
            );
            const STATE_SESSIONS_BY_SOCKET: &'static str =
                concat!("monad.wireauth.", $transport, ".state.sessions_by_socket");
            const STATE_SESSION_INDEX_ALLOCATED: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".state.session_index_allocated"
            );
            const STATE_SESSION_ESTABLISHED_INITIATOR: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".state.session_established_initiator"
            );
            const STATE_SESSION_ESTABLISHED_RESPONDER: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".state.session_established_responder"
            );
            const STATE_SESSION_TERMINATED: &'static str =
                concat!("monad.wireauth.", $transport, ".state.session_terminated");
            const STATE_TIMERS_SIZE: &'static str =
                concat!("monad.wireauth.", $transport, ".state.timers_size");
            const STATE_PACKET_QUEUE_SIZE: &'static str =
                concat!("monad.wireauth.", $transport, ".state.packet_queue_size");
            const STATE_INITIATED_SESSION_BY_PEER_SIZE: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".state.initiated_session_by_peer_size"
            );
            const STATE_ACCEPTED_SESSIONS_BY_PEER_SIZE: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".state.accepted_sessions_by_peer_size"
            );
            const STATE_IP_SESSION_COUNTS_SIZE: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".state.ip_session_counts_size"
            );

            const FILTER_PASS: &'static str =
                concat!("monad.wireauth.", $transport, ".filter.pass");
            const FILTER_SEND_COOKIE: &'static str =
                concat!("monad.wireauth.", $transport, ".filter.send_cookie");
            const FILTER_DROP: &'static str =
                concat!("monad.wireauth.", $transport, ".filter.drop");
            const FILTER_IP_REQUEST_HISTORY_SIZE: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".filter.ip_request_history_size"
            );

            const API_CONNECT: &'static str =
                concat!("monad.wireauth.", $transport, ".api.connect");
            const API_DECRYPT: &'static str =
                concat!("monad.wireauth.", $transport, ".api.decrypt");
            const API_ENCRYPT_BY_PUBLIC_KEY: &'static str =
                concat!("monad.wireauth.", $transport, ".api.encrypt_by_public_key");
            const API_ENCRYPT_BY_SOCKET: &'static str =
                concat!("monad.wireauth.", $transport, ".api.encrypt_by_socket");
            const API_DISCONNECT: &'static str =
                concat!("monad.wireauth.", $transport, ".api.disconnect");
            const API_DISPATCH_CONTROL: &'static str =
                concat!("monad.wireauth.", $transport, ".api.dispatch_control");
            const API_NEXT_PACKET: &'static str =
                concat!("monad.wireauth.", $transport, ".api.next_packet");
            const API_TICK: &'static str = concat!("monad.wireauth.", $transport, ".api.tick");

            const DISPATCH_HANDSHAKE_INIT: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".dispatch.handshake_initiation"
            );
            const DISPATCH_HANDSHAKE_RESPONSE: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".dispatch.handshake_response"
            );
            const DISPATCH_COOKIE_REPLY: &'static str =
                concat!("monad.wireauth.", $transport, ".dispatch.cookie_reply");
            const DISPATCH_KEEPALIVE: &'static str =
                concat!("monad.wireauth.", $transport, ".dispatch.keepalive");

            const ERROR_CONNECT: &'static str =
                concat!("monad.wireauth.", $transport, ".error.connect");
            const ERROR_DECRYPT: &'static str =
                concat!("monad.wireauth.", $transport, ".error.decrypt");
            const ERROR_DECRYPT_NONCE_OUTSIDE_WINDOW: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".error.decrypt.nonce_outside_window"
            );
            const ERROR_DECRYPT_NONCE_DUPLICATE: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".error.decrypt.nonce_duplicate"
            );
            const ERROR_DECRYPT_MAC: &'static str =
                concat!("monad.wireauth.", $transport, ".error.decrypt.mac");
            const ERROR_ENCRYPT_BY_PUBLIC_KEY: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".error.encrypt_by_public_key"
            );
            const ERROR_ENCRYPT_BY_SOCKET: &'static str =
                concat!("monad.wireauth.", $transport, ".error.encrypt_by_socket");
            const ERROR_DISPATCH_CONTROL: &'static str =
                concat!("monad.wireauth.", $transport, ".error.dispatch_control");

            const ERROR_SESSION_EXHAUSTED: &'static str =
                concat!("monad.wireauth.", $transport, ".error.session_exhausted");
            const ERROR_MAC1_VERIFICATION_FAILED: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".error.mac1_verification_failed"
            );
            const ERROR_TIMESTAMP_REPLAY: &'static str =
                concat!("monad.wireauth.", $transport, ".error.timestamp_replay");
            const ERROR_SESSION_NOT_FOUND: &'static str =
                concat!("monad.wireauth.", $transport, ".error.session_not_found");
            const ERROR_SESSION_INDEX_NOT_FOUND: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".error.session_index_not_found"
            );
            const ERROR_HANDSHAKE_INIT_VALIDATION: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".error.handshake_init_validation"
            );
            const ERROR_COOKIE_REPLY: &'static str =
                concat!("monad.wireauth.", $transport, ".error.cookie_reply");
            const ERROR_HANDSHAKE_RESPONSE_VALIDATION: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".error.handshake_response_validation"
            );

            const ENQUEUED_HANDSHAKE_INIT: &'static str =
                concat!("monad.wireauth.", $transport, ".enqueued.handshake_init");
            const ENQUEUED_HANDSHAKE_RESPONSE: &'static str = concat!(
                "monad.wireauth.",
                $transport,
                ".enqueued.handshake_response"
            );
            const ENQUEUED_COOKIE_REPLY: &'static str =
                concat!("monad.wireauth.", $transport, ".enqueued.cookie_reply");
            const ENQUEUED_KEEPALIVE: &'static str =
                concat!("monad.wireauth.", $transport, ".enqueued.keepalive");

            const RATE_LIMIT_DROP: &'static str =
                concat!("monad.wireauth.", $transport, ".rate_limit.drop");
        }
    };
}

pub struct DefaultMetrics;

impl MetricNames for DefaultMetrics {
    const STATE_INITIATING_SESSIONS: &'static str = GAUGE_WIREAUTH_STATE_INITIATING_SESSIONS;
    const STATE_RESPONDING_SESSIONS: &'static str = GAUGE_WIREAUTH_STATE_RESPONDING_SESSIONS;
    const STATE_TRANSPORT_SESSIONS: &'static str = GAUGE_WIREAUTH_STATE_TRANSPORT_SESSIONS;
    const STATE_TOTAL_SESSIONS: &'static str = GAUGE_WIREAUTH_STATE_TOTAL_SESSIONS;
    const STATE_ALLOCATED_INDICES: &'static str = GAUGE_WIREAUTH_STATE_ALLOCATED_INDICES;
    const STATE_SESSIONS_BY_PUBLIC_KEY: &'static str = GAUGE_WIREAUTH_STATE_SESSIONS_BY_PUBLIC_KEY;
    const STATE_SESSIONS_BY_SOCKET: &'static str = GAUGE_WIREAUTH_STATE_SESSIONS_BY_SOCKET;
    const STATE_SESSION_INDEX_ALLOCATED: &'static str =
        GAUGE_WIREAUTH_STATE_SESSION_INDEX_ALLOCATED;
    const STATE_SESSION_ESTABLISHED_INITIATOR: &'static str =
        GAUGE_WIREAUTH_STATE_SESSION_ESTABLISHED_INITIATOR;
    const STATE_SESSION_ESTABLISHED_RESPONDER: &'static str =
        GAUGE_WIREAUTH_STATE_SESSION_ESTABLISHED_RESPONDER;
    const STATE_SESSION_TERMINATED: &'static str = GAUGE_WIREAUTH_STATE_SESSION_TERMINATED;
    const STATE_TIMERS_SIZE: &'static str = GAUGE_WIREAUTH_STATE_TIMERS_SIZE;
    const STATE_PACKET_QUEUE_SIZE: &'static str = GAUGE_WIREAUTH_STATE_PACKET_QUEUE_SIZE;
    const STATE_INITIATED_SESSION_BY_PEER_SIZE: &'static str =
        GAUGE_WIREAUTH_STATE_INITIATED_SESSION_BY_PEER_SIZE;
    const STATE_ACCEPTED_SESSIONS_BY_PEER_SIZE: &'static str =
        GAUGE_WIREAUTH_STATE_ACCEPTED_SESSIONS_BY_PEER_SIZE;
    const STATE_IP_SESSION_COUNTS_SIZE: &'static str = GAUGE_WIREAUTH_STATE_IP_SESSION_COUNTS_SIZE;

    const FILTER_PASS: &'static str = GAUGE_WIREAUTH_FILTER_PASS;
    const FILTER_SEND_COOKIE: &'static str = GAUGE_WIREAUTH_FILTER_SEND_COOKIE;
    const FILTER_DROP: &'static str = GAUGE_WIREAUTH_FILTER_DROP;
    const FILTER_IP_REQUEST_HISTORY_SIZE: &'static str =
        GAUGE_WIREAUTH_FILTER_IP_REQUEST_HISTORY_SIZE;

    const API_CONNECT: &'static str = GAUGE_WIREAUTH_API_CONNECT;
    const API_DECRYPT: &'static str = GAUGE_WIREAUTH_API_DECRYPT;
    const API_ENCRYPT_BY_PUBLIC_KEY: &'static str = GAUGE_WIREAUTH_API_ENCRYPT_BY_PUBLIC_KEY;
    const API_ENCRYPT_BY_SOCKET: &'static str = GAUGE_WIREAUTH_API_ENCRYPT_BY_SOCKET;
    const API_DISCONNECT: &'static str = GAUGE_WIREAUTH_API_DISCONNECT;
    const API_DISPATCH_CONTROL: &'static str = GAUGE_WIREAUTH_API_DISPATCH_CONTROL;
    const API_NEXT_PACKET: &'static str = GAUGE_WIREAUTH_API_NEXT_PACKET;
    const API_TICK: &'static str = GAUGE_WIREAUTH_API_TICK;

    const DISPATCH_HANDSHAKE_INIT: &'static str = GAUGE_WIREAUTH_DISPATCH_HANDSHAKE_INIT;
    const DISPATCH_HANDSHAKE_RESPONSE: &'static str = GAUGE_WIREAUTH_DISPATCH_HANDSHAKE_RESPONSE;
    const DISPATCH_COOKIE_REPLY: &'static str = GAUGE_WIREAUTH_DISPATCH_COOKIE_REPLY;
    const DISPATCH_KEEPALIVE: &'static str = GAUGE_WIREAUTH_DISPATCH_KEEPALIVE;

    const ERROR_CONNECT: &'static str = GAUGE_WIREAUTH_ERROR_CONNECT;
    const ERROR_DECRYPT: &'static str = GAUGE_WIREAUTH_ERROR_DECRYPT;
    const ERROR_DECRYPT_NONCE_OUTSIDE_WINDOW: &'static str =
        GAUGE_WIREAUTH_ERROR_DECRYPT_NONCE_OUTSIDE_WINDOW;
    const ERROR_DECRYPT_NONCE_DUPLICATE: &'static str =
        GAUGE_WIREAUTH_ERROR_DECRYPT_NONCE_DUPLICATE;
    const ERROR_DECRYPT_MAC: &'static str = GAUGE_WIREAUTH_ERROR_DECRYPT_MAC;
    const ERROR_ENCRYPT_BY_PUBLIC_KEY: &'static str = GAUGE_WIREAUTH_ERROR_ENCRYPT_BY_PUBLIC_KEY;
    const ERROR_ENCRYPT_BY_SOCKET: &'static str = GAUGE_WIREAUTH_ERROR_ENCRYPT_BY_SOCKET;
    const ERROR_DISPATCH_CONTROL: &'static str = GAUGE_WIREAUTH_ERROR_DISPATCH_CONTROL;

    const ERROR_SESSION_EXHAUSTED: &'static str = GAUGE_WIREAUTH_ERROR_SESSION_EXHAUSTED;
    const ERROR_MAC1_VERIFICATION_FAILED: &'static str =
        GAUGE_WIREAUTH_ERROR_MAC1_VERIFICATION_FAILED;
    const ERROR_TIMESTAMP_REPLAY: &'static str = GAUGE_WIREAUTH_ERROR_TIMESTAMP_REPLAY;
    const ERROR_SESSION_NOT_FOUND: &'static str = GAUGE_WIREAUTH_ERROR_SESSION_NOT_FOUND;
    const ERROR_SESSION_INDEX_NOT_FOUND: &'static str =
        GAUGE_WIREAUTH_ERROR_SESSION_INDEX_NOT_FOUND;
    const ERROR_HANDSHAKE_INIT_VALIDATION: &'static str =
        GAUGE_WIREAUTH_ERROR_HANDSHAKE_INIT_VALIDATION;
    const ERROR_COOKIE_REPLY: &'static str = GAUGE_WIREAUTH_ERROR_COOKIE_REPLY;
    const ERROR_HANDSHAKE_RESPONSE_VALIDATION: &'static str =
        GAUGE_WIREAUTH_ERROR_HANDSHAKE_RESPONSE_VALIDATION;

    const ENQUEUED_HANDSHAKE_INIT: &'static str = GAUGE_WIREAUTH_ENQUEUED_HANDSHAKE_INIT;
    const ENQUEUED_HANDSHAKE_RESPONSE: &'static str = GAUGE_WIREAUTH_ENQUEUED_HANDSHAKE_RESPONSE;
    const ENQUEUED_COOKIE_REPLY: &'static str = GAUGE_WIREAUTH_ENQUEUED_COOKIE_REPLY;
    const ENQUEUED_KEEPALIVE: &'static str = GAUGE_WIREAUTH_ENQUEUED_KEEPALIVE;

    const RATE_LIMIT_DROP: &'static str = GAUGE_WIREAUTH_RATE_LIMIT_DROP;
}
