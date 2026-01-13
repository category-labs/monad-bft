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

pub struct MetricNames {
    pub state_initiating_sessions: &'static str,
    pub state_responding_sessions: &'static str,
    pub state_transport_sessions: &'static str,
    pub state_total_sessions: &'static str,
    pub state_allocated_indices: &'static str,
    pub state_sessions_by_public_key: &'static str,
    pub state_sessions_by_socket: &'static str,
    pub state_session_index_allocated: &'static str,
    pub state_session_established_initiator: &'static str,
    pub state_session_established_responder: &'static str,
    pub state_session_terminated: &'static str,
    pub state_timers_size: &'static str,
    pub state_packet_queue_size: &'static str,
    pub state_initiated_session_by_peer_size: &'static str,
    pub state_accepted_sessions_by_peer_size: &'static str,
    pub state_ip_session_counts_size: &'static str,

    pub filter_pass: &'static str,
    pub filter_send_cookie: &'static str,
    pub filter_drop: &'static str,
    pub filter_ip_request_history_size: &'static str,

    pub api_connect: &'static str,
    pub api_decrypt: &'static str,
    pub api_encrypt_by_public_key: &'static str,
    pub api_encrypt_by_socket: &'static str,
    pub api_disconnect: &'static str,
    pub api_dispatch_control: &'static str,
    pub api_next_packet: &'static str,
    pub api_tick: &'static str,

    pub dispatch_handshake_init: &'static str,
    pub dispatch_handshake_response: &'static str,
    pub dispatch_cookie_reply: &'static str,
    pub dispatch_keepalive: &'static str,

    pub error_connect: &'static str,
    pub error_decrypt: &'static str,
    pub error_decrypt_nonce_outside_window: &'static str,
    pub error_decrypt_nonce_duplicate: &'static str,
    pub error_decrypt_mac: &'static str,
    pub error_encrypt_by_public_key: &'static str,
    pub error_encrypt_by_socket: &'static str,
    pub error_dispatch_control: &'static str,

    pub error_session_exhausted: &'static str,
    pub error_mac1_verification_failed: &'static str,
    pub error_timestamp_replay: &'static str,
    pub error_session_not_found: &'static str,
    pub error_session_index_not_found: &'static str,
    pub error_handshake_init_validation: &'static str,
    pub error_cookie_reply: &'static str,
    pub error_handshake_response_validation: &'static str,

    pub enqueued_handshake_init: &'static str,
    pub enqueued_handshake_response: &'static str,
    pub enqueued_cookie_reply: &'static str,
    pub enqueued_keepalive: &'static str,

    pub rate_limit_drop: &'static str,

    pub initiator_buffered_messages: &'static str,
    pub initiator_messages_sent_from_buffer: &'static str,
}

#[macro_export]
macro_rules! define_metric_names {
    ($name:ident, $transport:literal) => {
        pub static $name: $crate::MetricNames = $crate::MetricNames {
            state_initiating_sessions: concat!(
                "monad.wireauth.",
                $transport,
                ".state.initiating_sessions"
            ),
            state_responding_sessions: concat!(
                "monad.wireauth.",
                $transport,
                ".state.responding_sessions"
            ),
            state_transport_sessions: concat!(
                "monad.wireauth.",
                $transport,
                ".state.transport_sessions"
            ),
            state_total_sessions: concat!("monad.wireauth.", $transport, ".state.total_sessions"),
            state_allocated_indices: concat!(
                "monad.wireauth.",
                $transport,
                ".state.allocated_indices"
            ),
            state_sessions_by_public_key: concat!(
                "monad.wireauth.",
                $transport,
                ".state.sessions_by_public_key"
            ),
            state_sessions_by_socket: concat!(
                "monad.wireauth.",
                $transport,
                ".state.sessions_by_socket"
            ),
            state_session_index_allocated: concat!(
                "monad.wireauth.",
                $transport,
                ".state.session_index_allocated"
            ),
            state_session_established_initiator: concat!(
                "monad.wireauth.",
                $transport,
                ".state.session_established_initiator"
            ),
            state_session_established_responder: concat!(
                "monad.wireauth.",
                $transport,
                ".state.session_established_responder"
            ),
            state_session_terminated: concat!(
                "monad.wireauth.",
                $transport,
                ".state.session_terminated"
            ),
            state_timers_size: concat!("monad.wireauth.", $transport, ".state.timers_size"),
            state_packet_queue_size: concat!(
                "monad.wireauth.",
                $transport,
                ".state.packet_queue_size"
            ),
            state_initiated_session_by_peer_size: concat!(
                "monad.wireauth.",
                $transport,
                ".state.initiated_session_by_peer_size"
            ),
            state_accepted_sessions_by_peer_size: concat!(
                "monad.wireauth.",
                $transport,
                ".state.accepted_sessions_by_peer_size"
            ),
            state_ip_session_counts_size: concat!(
                "monad.wireauth.",
                $transport,
                ".state.ip_session_counts_size"
            ),

            filter_pass: concat!("monad.wireauth.", $transport, ".filter.pass"),
            filter_send_cookie: concat!("monad.wireauth.", $transport, ".filter.send_cookie"),
            filter_drop: concat!("monad.wireauth.", $transport, ".filter.drop"),
            filter_ip_request_history_size: concat!(
                "monad.wireauth.",
                $transport,
                ".filter.ip_request_history_size"
            ),

            api_connect: concat!("monad.wireauth.", $transport, ".api.connect"),
            api_decrypt: concat!("monad.wireauth.", $transport, ".api.decrypt"),
            api_encrypt_by_public_key: concat!(
                "monad.wireauth.",
                $transport,
                ".api.encrypt_by_public_key"
            ),
            api_encrypt_by_socket: concat!("monad.wireauth.", $transport, ".api.encrypt_by_socket"),
            api_disconnect: concat!("monad.wireauth.", $transport, ".api.disconnect"),
            api_dispatch_control: concat!("monad.wireauth.", $transport, ".api.dispatch_control"),
            api_next_packet: concat!("monad.wireauth.", $transport, ".api.next_packet"),
            api_tick: concat!("monad.wireauth.", $transport, ".api.tick"),

            dispatch_handshake_init: concat!(
                "monad.wireauth.",
                $transport,
                ".dispatch.handshake_initiation"
            ),
            dispatch_handshake_response: concat!(
                "monad.wireauth.",
                $transport,
                ".dispatch.handshake_response"
            ),
            dispatch_cookie_reply: concat!("monad.wireauth.", $transport, ".dispatch.cookie_reply"),
            dispatch_keepalive: concat!("monad.wireauth.", $transport, ".dispatch.keepalive"),

            error_connect: concat!("monad.wireauth.", $transport, ".error.connect"),
            error_decrypt: concat!("monad.wireauth.", $transport, ".error.decrypt"),
            error_decrypt_nonce_outside_window: concat!(
                "monad.wireauth.",
                $transport,
                ".error.decrypt.nonce_outside_window"
            ),
            error_decrypt_nonce_duplicate: concat!(
                "monad.wireauth.",
                $transport,
                ".error.decrypt.nonce_duplicate"
            ),
            error_decrypt_mac: concat!("monad.wireauth.", $transport, ".error.decrypt.mac"),
            error_encrypt_by_public_key: concat!(
                "monad.wireauth.",
                $transport,
                ".error.encrypt_by_public_key"
            ),
            error_encrypt_by_socket: concat!(
                "monad.wireauth.",
                $transport,
                ".error.encrypt_by_socket"
            ),
            error_dispatch_control: concat!(
                "monad.wireauth.",
                $transport,
                ".error.dispatch_control"
            ),

            error_session_exhausted: concat!(
                "monad.wireauth.",
                $transport,
                ".error.session_exhausted"
            ),
            error_mac1_verification_failed: concat!(
                "monad.wireauth.",
                $transport,
                ".error.mac1_verification_failed"
            ),
            error_timestamp_replay: concat!(
                "monad.wireauth.",
                $transport,
                ".error.timestamp_replay"
            ),
            error_session_not_found: concat!(
                "monad.wireauth.",
                $transport,
                ".error.session_not_found"
            ),
            error_session_index_not_found: concat!(
                "monad.wireauth.",
                $transport,
                ".error.session_index_not_found"
            ),
            error_handshake_init_validation: concat!(
                "monad.wireauth.",
                $transport,
                ".error.handshake_init_validation"
            ),
            error_cookie_reply: concat!("monad.wireauth.", $transport, ".error.cookie_reply"),
            error_handshake_response_validation: concat!(
                "monad.wireauth.",
                $transport,
                ".error.handshake_response_validation"
            ),

            enqueued_handshake_init: concat!(
                "monad.wireauth.",
                $transport,
                ".enqueued.handshake_init"
            ),
            enqueued_handshake_response: concat!(
                "monad.wireauth.",
                $transport,
                ".enqueued.handshake_response"
            ),
            enqueued_cookie_reply: concat!("monad.wireauth.", $transport, ".enqueued.cookie_reply"),
            enqueued_keepalive: concat!("monad.wireauth.", $transport, ".enqueued.keepalive"),

            rate_limit_drop: concat!("monad.wireauth.", $transport, ".rate_limit.drop"),

            initiator_buffered_messages: concat!(
                "monad.wireauth.",
                $transport,
                ".initiator.buffered_messages"
            ),
            initiator_messages_sent_from_buffer: concat!(
                "monad.wireauth.",
                $transport,
                ".initiator.messages_sent_from_buffer"
            ),
        };
    };
}

define_metric_names!(UDP_METRICS, "udp");
define_metric_names!(TCP_METRICS, "tcp");

pub static DEFAULT_METRICS: &MetricNames = &UDP_METRICS;
