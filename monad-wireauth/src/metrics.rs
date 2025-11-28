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
