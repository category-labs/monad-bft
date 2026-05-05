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

use monad_executor::{ExecutorMetrics, MetricDef};

pub struct MetricNames {
    pub state_initiating_sessions: &'static MetricDef,
    pub state_responding_sessions: &'static MetricDef,
    pub state_transport_sessions: &'static MetricDef,
    pub state_total_sessions: &'static MetricDef,
    pub state_allocated_indices: &'static MetricDef,
    pub state_sessions_by_public_key: &'static MetricDef,
    pub state_sessions_by_socket: &'static MetricDef,
    pub state_session_index_allocated: &'static MetricDef,
    pub state_session_established_initiator: &'static MetricDef,
    pub state_session_established_responder: &'static MetricDef,
    pub state_session_terminated: &'static MetricDef,
    pub state_timers_size: &'static MetricDef,
    pub state_packet_queue_size: &'static MetricDef,
    pub state_initiated_session_by_peer_size: &'static MetricDef,
    pub state_accepted_sessions_by_peer_size: &'static MetricDef,
    pub state_ip_session_counts_size: &'static MetricDef,

    pub filter_pass: &'static MetricDef,
    pub filter_send_cookie: &'static MetricDef,
    pub filter_drop: &'static MetricDef,
    pub filter_ip_request_history_size: &'static MetricDef,

    pub api_connect: &'static MetricDef,
    pub api_decrypt: &'static MetricDef,
    pub api_encrypt_by_public_key: &'static MetricDef,
    pub api_encrypt_by_socket: &'static MetricDef,
    pub api_disconnect: &'static MetricDef,
    pub api_dispatch_control: &'static MetricDef,
    pub api_next_packet: &'static MetricDef,
    pub api_tick: &'static MetricDef,

    pub dispatch_handshake_init: &'static MetricDef,
    pub dispatch_handshake_response: &'static MetricDef,
    pub dispatch_cookie_reply: &'static MetricDef,
    pub dispatch_keepalive: &'static MetricDef,

    pub error_connect: &'static MetricDef,
    pub error_decrypt: &'static MetricDef,
    pub error_decrypt_nonce_outside_window: &'static MetricDef,
    pub error_decrypt_nonce_duplicate: &'static MetricDef,
    pub error_decrypt_mac: &'static MetricDef,
    pub error_encrypt_by_public_key: &'static MetricDef,
    pub error_encrypt_by_socket: &'static MetricDef,
    pub error_dispatch_control: &'static MetricDef,

    pub error_session_exhausted: &'static MetricDef,
    pub error_mac1_verification_failed: &'static MetricDef,
    pub error_timestamp_replay: &'static MetricDef,
    pub error_session_not_found: &'static MetricDef,
    pub error_session_index_not_found: &'static MetricDef,
    pub error_handshake_init_validation: &'static MetricDef,
    pub error_cookie_reply: &'static MetricDef,
    pub error_handshake_response_validation: &'static MetricDef,

    pub enqueued_handshake_init: &'static MetricDef,
    pub enqueued_handshake_response: &'static MetricDef,
    pub enqueued_cookie_reply: &'static MetricDef,
    pub enqueued_keepalive: &'static MetricDef,

    pub rate_limit_drop: &'static MetricDef,
    pub rate_limit_connect: &'static MetricDef,

    pub initiator_buffered_messages: &'static MetricDef,
    pub initiator_messages_sent_from_buffer: &'static MetricDef,
}

impl MetricNames {
    fn all_metric_defs(&'static self) -> [&'static MetricDef; 56] {
        [
            self.state_initiating_sessions,
            self.state_responding_sessions,
            self.state_transport_sessions,
            self.state_total_sessions,
            self.state_allocated_indices,
            self.state_sessions_by_public_key,
            self.state_sessions_by_socket,
            self.state_session_index_allocated,
            self.state_session_established_initiator,
            self.state_session_established_responder,
            self.state_session_terminated,
            self.state_timers_size,
            self.state_packet_queue_size,
            self.state_initiated_session_by_peer_size,
            self.state_accepted_sessions_by_peer_size,
            self.state_ip_session_counts_size,
            self.filter_pass,
            self.filter_send_cookie,
            self.filter_drop,
            self.filter_ip_request_history_size,
            self.api_connect,
            self.api_decrypt,
            self.api_encrypt_by_public_key,
            self.api_encrypt_by_socket,
            self.api_disconnect,
            self.api_dispatch_control,
            self.api_next_packet,
            self.api_tick,
            self.dispatch_handshake_init,
            self.dispatch_handshake_response,
            self.dispatch_cookie_reply,
            self.dispatch_keepalive,
            self.error_connect,
            self.error_decrypt,
            self.error_decrypt_nonce_outside_window,
            self.error_decrypt_nonce_duplicate,
            self.error_decrypt_mac,
            self.error_encrypt_by_public_key,
            self.error_encrypt_by_socket,
            self.error_dispatch_control,
            self.error_session_exhausted,
            self.error_mac1_verification_failed,
            self.error_timestamp_replay,
            self.error_session_not_found,
            self.error_session_index_not_found,
            self.error_handshake_init_validation,
            self.error_cookie_reply,
            self.error_handshake_response_validation,
            self.enqueued_handshake_init,
            self.enqueued_handshake_response,
            self.enqueued_cookie_reply,
            self.enqueued_keepalive,
            self.rate_limit_drop,
            self.rate_limit_connect,
            self.initiator_buffered_messages,
            self.initiator_messages_sent_from_buffer,
        ]
    }

    pub(crate) fn state_metric_defs(&'static self) -> [&'static MetricDef; 14] {
        [
            self.state_initiating_sessions,
            self.state_responding_sessions,
            self.state_transport_sessions,
            self.state_total_sessions,
            self.state_allocated_indices,
            self.state_sessions_by_public_key,
            self.state_sessions_by_socket,
            self.state_session_index_allocated,
            self.state_session_established_initiator,
            self.state_session_established_responder,
            self.state_session_terminated,
            self.state_initiated_session_by_peer_size,
            self.state_accepted_sessions_by_peer_size,
            self.state_ip_session_counts_size,
        ]
    }

    pub(crate) fn filter_metric_defs(&'static self) -> [&'static MetricDef; 4] {
        [
            self.filter_pass,
            self.filter_send_cookie,
            self.filter_drop,
            self.filter_ip_request_history_size,
        ]
    }

    pub(crate) fn api_metric_defs(&'static self) -> [&'static MetricDef; 38] {
        [
            self.state_timers_size,
            self.state_packet_queue_size,
            self.api_connect,
            self.api_decrypt,
            self.api_encrypt_by_public_key,
            self.api_encrypt_by_socket,
            self.api_disconnect,
            self.api_dispatch_control,
            self.api_next_packet,
            self.api_tick,
            self.dispatch_handshake_init,
            self.dispatch_handshake_response,
            self.dispatch_cookie_reply,
            self.dispatch_keepalive,
            self.error_connect,
            self.error_decrypt,
            self.error_decrypt_nonce_outside_window,
            self.error_decrypt_nonce_duplicate,
            self.error_decrypt_mac,
            self.error_encrypt_by_public_key,
            self.error_encrypt_by_socket,
            self.error_dispatch_control,
            self.error_session_exhausted,
            self.error_mac1_verification_failed,
            self.error_timestamp_replay,
            self.error_session_not_found,
            self.error_session_index_not_found,
            self.error_handshake_init_validation,
            self.error_cookie_reply,
            self.error_handshake_response_validation,
            self.enqueued_handshake_init,
            self.enqueued_handshake_response,
            self.enqueued_cookie_reply,
            self.enqueued_keepalive,
            self.rate_limit_drop,
            self.rate_limit_connect,
            self.initiator_buffered_messages,
            self.initiator_messages_sent_from_buffer,
        ]
    }
}

pub fn init_executor_metrics(metric_names: &'static MetricNames) -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(metric_names.all_metric_defs())
}

pub(crate) fn init_state_executor_metrics(metric_names: &'static MetricNames) -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(metric_names.state_metric_defs())
}

pub(crate) fn init_filter_executor_metrics(metric_names: &'static MetricNames) -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(metric_names.filter_metric_defs())
}

pub(crate) fn init_api_executor_metrics(metric_names: &'static MetricNames) -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(metric_names.api_metric_defs())
}

#[macro_export]
macro_rules! define_metric_names {
    ($name:ident, $transport:literal) => {
        $crate::define_metric_names!(pub $name, $transport);
    };
    ($vis:vis $name:ident, $transport:literal) => {
        $vis static $name: $crate::MetricNames = $crate::MetricNames {
            state_initiating_sessions: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.initiating_sessions"),
                "sessions waiting for handshake response",
            ),
            state_responding_sessions: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.responding_sessions"),
                "sessions that received init and waiting for the first transport message",
            ),
            state_transport_sessions: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.transport_sessions"),
                "fully established sessions ready for data transmission",
            ),
            state_total_sessions: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.total_sessions"),
                "sum of all session states (initiating + responding + transport)",
            ),
            state_allocated_indices: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.allocated_indices"),
                "unique 32-bit identifiers assigned to active sessions",
            ),
            state_sessions_by_public_key: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.sessions_by_public_key"),
                "lookup table mapping peer public keys to sessions",
            ),
            state_sessions_by_socket: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.sessions_by_socket"),
                "lookup table mapping socket addresses to sessions",
            ),
            state_session_index_allocated: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.session_index_allocated"),
                "lifetime allocations of session indices",
            ),
            state_session_established_initiator: &monad_executor::MetricDef::new(
                concat!(
                    "monad.wireauth.",
                    $transport,
                    ".state.session_established_initiator"
                ),
                "successful handshakes where we initiated",
            ),
            state_session_established_responder: &monad_executor::MetricDef::new(
                concat!(
                    "monad.wireauth.",
                    $transport,
                    ".state.session_established_responder"
                ),
                "successful handshakes where we responded",
            ),
            state_session_terminated: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.session_terminated"),
                "sessions closed due to timeout or max duration",
            ),
            state_timers_size: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.timers_size"),
                "pending timer events (keepalive, rekey, timeout)",
            ),
            state_packet_queue_size: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.packet_queue_size"),
                "outbound packets waiting to be sent",
            ),
            state_initiated_session_by_peer_size: &monad_executor::MetricDef::new(
                concat!(
                    "monad.wireauth.",
                    $transport,
                    ".state.initiated_session_by_peer_size"
                ),
                "tracks one initiating session per peer to prevent duplicates",
            ),
            state_accepted_sessions_by_peer_size: &monad_executor::MetricDef::new(
                concat!(
                    "monad.wireauth.",
                    $transport,
                    ".state.accepted_sessions_by_peer_size"
                ),
                "tracks accepted sessions per peer for limiting",
            ),
            state_ip_session_counts_size: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".state.ip_session_counts_size"),
                "tracks session count per ip for rate limiting",
            ),

            filter_pass: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".filter.pass"),
                "handshake requests that passed all filters",
            ),
            filter_send_cookie: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".filter.send_cookie"),
                "cookie challenges sent (between low and high watermark)",
            ),
            filter_drop: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".filter.drop"),
                "handshake requests rejected due to rate limits",
            ),
            filter_ip_request_history_size: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".filter.ip_request_history_size"),
                "lru cache tracking recent handshake requests per ip",
            ),

            api_connect: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.connect"),
                "new outbound connection attempts",
            ),
            api_decrypt: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.decrypt"),
                "inbound data packets decrypted",
            ),
            api_encrypt_by_public_key: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.encrypt_by_public_key"),
                "outbound packets encrypted using peer public key lookup",
            ),
            api_encrypt_by_socket: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.encrypt_by_socket"),
                "outbound packets encrypted using socket address lookup",
            ),
            api_disconnect: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.disconnect"),
                "explicit session termination requests",
            ),
            api_dispatch_control: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.dispatch_control"),
                "control messages processed (handshake, cookie, keepalive)",
            ),
            api_next_packet: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.next_packet"),
                "outbound packet dequeues",
            ),
            api_tick: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".api.tick"),
                "timer processing cycles",
            ),

            dispatch_handshake_init: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".dispatch.handshake_initiation"),
                "first message of handshake sent/received",
            ),
            dispatch_handshake_response: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".dispatch.handshake_response"),
                "second message of handshake sent/received",
            ),
            dispatch_cookie_reply: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".dispatch.cookie_reply"),
                "cookie challenges sent in response to handshake init",
            ),
            dispatch_keepalive: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".dispatch.keepalive"),
                "empty data packets sent to maintain session liveness",
            ),

            error_connect: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.connect"),
                "failed connection attempts (no memory, duplicate, etc)",
            ),
            error_decrypt: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.decrypt"),
                "total decryption failures (includes all decrypt error subtypes)",
            ),
            error_decrypt_nonce_outside_window: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.decrypt.nonce_outside_window"),
                "packet counter outside replay window (too old)",
            ),
            error_decrypt_nonce_duplicate: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.decrypt.nonce_duplicate"),
                "duplicate packet counter detected (replay attack)",
            ),
            error_decrypt_mac: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.decrypt.mac"),
                "chacha20poly1305 mac authentication tag verification failed",
            ),
            error_encrypt_by_public_key: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.encrypt_by_public_key"),
                "encryption failures when looking up by public key",
            ),
            error_encrypt_by_socket: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.encrypt_by_socket"),
                "encryption failures when looking up by socket address",
            ),
            error_dispatch_control: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.dispatch_control"),
                "control message processing failures",
            ),
            error_session_exhausted: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.session_exhausted"),
                "rejected due to hitting max session limit",
            ),
            error_mac1_verification_failed: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.mac1_verification_failed"),
                "handshake mac1 authentication failed",
            ),
            error_timestamp_replay: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.timestamp_replay"),
                "handshake timestamp older than previous attempt",
            ),
            error_session_not_found: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.session_not_found"),
                "operation on non-existent session",
            ),
            error_session_index_not_found: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.session_index_not_found"),
                "data packet with unknown receiver index",
            ),
            error_handshake_init_validation: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.handshake_init_validation"),
                "malformed or invalid handshake initiation",
            ),
            error_cookie_reply: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".error.cookie_reply"),
                "cookie reply validation or generation failed",
            ),
            error_handshake_response_validation: &monad_executor::MetricDef::new(
                concat!(
                    "monad.wireauth.",
                    $transport,
                    ".error.handshake_response_validation"
                ),
                "malformed or invalid handshake response",
            ),

            enqueued_handshake_init: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".enqueued.handshake_init"),
                "handshake initiations added to outbound queue",
            ),
            enqueued_handshake_response: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".enqueued.handshake_response"),
                "handshake responses added to outbound queue",
            ),
            enqueued_cookie_reply: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".enqueued.cookie_reply"),
                "cookie challenges added to outbound queue",
            ),
            enqueued_keepalive: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".enqueued.keepalive"),
                "keepalive packets added to outbound queue",
            ),

            rate_limit_drop: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".rate_limit.drop"),
                "handshake requests dropped due to rate limiting",
            ),
            rate_limit_connect: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".rate_limit.connect"),
                "outbound connect attempts rejected due to rate limits",
            ),

            initiator_buffered_messages: &monad_executor::MetricDef::new(
                concat!("monad.wireauth.", $transport, ".initiator.buffered_messages"),
                "messages buffered while waiting for handshake to complete",
            ),
            initiator_messages_sent_from_buffer: &monad_executor::MetricDef::new(
                concat!(
                    "monad.wireauth.",
                    $transport,
                    ".initiator.messages_sent_from_buffer"
                ),
                "buffered messages sent after handshake completed",
            ),
        };
    };
}

// `MetricNames` instances are intended to be defined by integration crates (e.g. raptorcast),
// but keep a `DEFAULT_METRICS` for wireauth's own examples/tests.
define_metric_names!(pub(crate) DEFAULT_METRIC_NAMES, "udp");

pub static DEFAULT_METRICS: &MetricNames = &DEFAULT_METRIC_NAMES;
