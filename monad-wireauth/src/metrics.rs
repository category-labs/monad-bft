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

use monad_executor::ExecutorMetrics;

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
    pub rate_limit_connect: &'static str,
}

impl MetricNames {
    pub fn register_descriptions(&self, metrics: &mut ExecutorMetrics) {
        metrics.set_description(self.state_initiating_sessions, "Sessions in initiating state");
        metrics.set_description(self.state_responding_sessions, "Sessions in responding state");
        metrics.set_description(self.state_transport_sessions, "Sessions in transport state");
        metrics.set_description(self.state_total_sessions, "Current total wireauth sessions");
        metrics.set_description(self.state_allocated_indices, "Allocated session indices");
        metrics.set_description(self.state_sessions_by_public_key, "Sessions indexed by public key");
        metrics.set_description(self.state_sessions_by_socket, "Sessions indexed by socket");
        metrics.set_description(self.state_session_index_allocated, "Session indices allocated");
        metrics.set_description(
            self.state_session_established_initiator,
            "Sessions established as initiator",
        );
        metrics.set_description(
            self.state_session_established_responder,
            "Sessions established as responder",
        );
        metrics.set_description(self.state_session_terminated, "Sessions terminated");
        metrics.set_description(self.state_timers_size, "Active timers count");
        metrics.set_description(self.state_packet_queue_size, "Packet queue size");
        metrics.set_description(
            self.state_initiated_session_by_peer_size,
            "Initiated sessions by peer",
        );
        metrics.set_description(
            self.state_accepted_sessions_by_peer_size,
            "Accepted sessions by peer",
        );
        metrics.set_description(self.state_ip_session_counts_size, "IP session counts map size");

        metrics.set_description(self.filter_ip_request_history_size, "IP request history size");
        metrics.set_description(self.filter_pass, "Packets that passed the filter");
        metrics.set_description(self.filter_send_cookie, "Cookie replies sent by filter");
        metrics.set_description(self.filter_drop, "Packets dropped by filter");

        metrics.set_description(self.api_connect, "Connect API calls");
        metrics.set_description(self.api_decrypt, "Decrypt API calls");
        metrics.set_description(self.api_encrypt_by_public_key, "Encrypt by public key API calls");
        metrics.set_description(self.api_encrypt_by_socket, "Encrypt by socket API calls");
        metrics.set_description(self.api_disconnect, "Disconnect API calls");
        metrics.set_description(self.api_dispatch_control, "Dispatch control API calls");
        metrics.set_description(self.api_next_packet, "Next packet API calls");
        metrics.set_description(self.api_tick, "Tick API calls");

        metrics.set_description(
            self.dispatch_handshake_init,
            "Handshake initiation messages dispatched",
        );
        metrics.set_description(
            self.dispatch_handshake_response,
            "Handshake response messages dispatched",
        );
        metrics.set_description(self.dispatch_cookie_reply, "Cookie reply messages dispatched");
        metrics.set_description(self.dispatch_keepalive, "Keepalive messages dispatched");

        metrics.set_description(self.error_connect, "Connect errors");
        metrics.set_description(self.error_decrypt, "Decrypt errors");
        metrics.set_description(
            self.error_decrypt_nonce_outside_window,
            "Decrypt errors - nonce outside window",
        );
        metrics.set_description(
            self.error_decrypt_nonce_duplicate,
            "Decrypt errors - duplicate nonce",
        );
        metrics.set_description(self.error_decrypt_mac, "Decrypt errors - MAC verification failed");
        metrics.set_description(
            self.error_encrypt_by_public_key,
            "Encrypt by public key errors",
        );
        metrics.set_description(self.error_encrypt_by_socket, "Encrypt by socket errors");
        metrics.set_description(self.error_dispatch_control, "Dispatch control errors");
        metrics.set_description(self.error_session_exhausted, "Session exhausted errors");
        metrics.set_description(self.error_mac1_verification_failed, "MAC1 verification failures");
        metrics.set_description(self.error_timestamp_replay, "Timestamp replay errors");
        metrics.set_description(self.error_session_not_found, "Session not found errors");
        metrics.set_description(
            self.error_session_index_not_found,
            "Session index not found errors",
        );
        metrics.set_description(
            self.error_handshake_init_validation,
            "Handshake init validation errors",
        );
        metrics.set_description(self.error_cookie_reply, "Cookie reply errors");
        metrics.set_description(
            self.error_handshake_response_validation,
            "Handshake response validation errors",
        );

        metrics.set_description(self.enqueued_handshake_init, "Handshake init messages enqueued");
        metrics.set_description(
            self.enqueued_handshake_response,
            "Handshake response messages enqueued",
        );
        metrics.set_description(self.enqueued_cookie_reply, "Cookie reply messages enqueued");
        metrics.set_description(self.enqueued_keepalive, "Keepalive messages enqueued");

        metrics.set_description(self.rate_limit_drop, "Packets dropped due to rate limiting");
        metrics.set_description(
            self.rate_limit_connect,
            "Connect API calls dropped due to rate limiting",
        );
    }
}

#[macro_export]
macro_rules! define_metric_names {
    ($name:ident, $transport:literal) => {
        $crate::define_metric_names!(pub $name, $transport);
    };
    ($vis:vis $name:ident, $transport:literal) => {
        $vis static $name: $crate::MetricNames = $crate::MetricNames {
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
            rate_limit_connect: concat!("monad.wireauth.", $transport, ".rate_limit.connect"),
        };
    };
}

// `MetricNames` instances are intended to be defined by integration crates (e.g. raptorcast),
// but keep a `DEFAULT_METRICS` for wireauth's own examples/tests.
define_metric_names!(pub(crate) DEFAULT_METRIC_NAMES, "udp");

pub static DEFAULT_METRICS: &MetricNames = &DEFAULT_METRIC_NAMES;

#[cfg(test)]
mod tests {
    use monad_executor::ExecutorMetrics;

    use super::DEFAULT_METRICS;

    #[test]
    fn register_descriptions_populates_help_texts() {
        let mut metrics = ExecutorMetrics::default();
        DEFAULT_METRICS.register_descriptions(&mut metrics);

        assert_eq!(
            metrics.description(DEFAULT_METRICS.api_connect),
            "Connect API calls"
        );
        assert_eq!(
            metrics.description(DEFAULT_METRICS.rate_limit_connect),
            "Connect API calls dropped due to rate limiting"
        );
    }
}
