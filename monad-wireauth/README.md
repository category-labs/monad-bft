# monad-wireauth

authenticated udp protocol implementation with dos protection and session management.

## Components

### Protocol

core protocol implementation including:
- cryptographic primitives and key exchange
- message formats (handshake, data, cookie)
- handshake state machine

### Session

session management layer:
- initiator and responder state machines
- transport state with replay protection
- automatic session timeout and rekeying

### API

high-level api with dos protection:

the filter operates using two global handshake rate limits plus a hard session cap:

| condition | action |
|-----------|--------|
| cookie invalid and handshakes >= `handshake_cookie_unverified_rate_limit` | send cookie reply |
| cookie valid and handshakes >= `handshake_cookie_verified_rate_limit` | drop request |
| cookie valid and last verified request from the same ip is within `ip_rate_limit_window` | drop request |
| sessions >= `high_watermark_sessions` | drop request |
| otherwise | accept request |

defaults: `high_watermark_sessions`=40,000, `handshake_cookie_unverified_rate_limit`=500/sec, `handshake_cookie_verified_rate_limit`=1,000/sec, `ip_rate_limit_window`=10s, `ip_history_capacity`=1,000,000, `connect_rate_limit`=300/sec

these defaults are per instance. we expect to run more than one wireauth instance, so the per-instance handshake and session limits are intentionally lower and aggregate capacity should come from running multiple instances.

## Benchmarks

CPU: 12th Gen Intel(R) Core(TM) i9-12900KF

RUSTFLAGS: `-C target-cpu=haswell -C opt-level=3`

```
session_send_init       time:   [59.961 µs 60.097 µs 60.233 µs]
session_handle_init     time:   [112.87 µs 113.41 µs 114.09 µs]
session_handle_response time:   [51.680 µs 51.910 µs 52.178 µs]
session_encrypt         time:   [115.84 ns 116.07 ns 116.28 ns]
session_decrypt         time:   [166.11 ns 168.75 ns 171.20 ns]
```

## Metrics

### state gauges

| metric | description |
|--------|-------------|
| `monad.wireauth.state.initiating_sessions` | sessions waiting for handshake response |
| `monad.wireauth.state.responding_sessions` | sessions that received init and waiting for the first transport message |
| `monad.wireauth.state.transport_sessions` | fully established sessions ready for data transmission |
| `monad.wireauth.state.total_sessions` | sum of all session states (initiating + responding + transport) |
| `monad.wireauth.state.allocated_indices` | unique 32-bit identifiers assigned to active sessions |
| `monad.wireauth.state.sessions_by_public_key` | lookup table mapping peer public keys to sessions |
| `monad.wireauth.state.sessions_by_socket` | lookup table mapping socket addresses to sessions |
| `monad.wireauth.state.timers_size` | pending timer events (keepalive, rekey, timeout) |
| `monad.wireauth.state.packet_queue_size` | outbound packets waiting to be sent |
| `monad.wireauth.state.initiated_session_by_peer_size` | tracks one initiating session per peer to prevent duplicates |
| `monad.wireauth.state.accepted_sessions_by_peer_size` | tracks accepted sessions per peer for limiting |
| `monad.wireauth.state.ip_session_counts_size` | tracks session count per ip |

### state counters

| metric | description |
|--------|-------------|
| `monad.wireauth.state.session_index_allocated` | lifetime allocations of session indices |
| `monad.wireauth.state.session_established_initiator` | successful handshakes where we initiated |
| `monad.wireauth.state.session_established_responder` | successful handshakes where we responded |
| `monad.wireauth.state.session_terminated` | sessions closed due to timeout or max duration |

### dos filter

| metric | description |
|--------|-------------|
| `monad.wireauth.filter.ip_request_history_size` | lru cache tracking recent verified handshake requests per ip |
| `monad.wireauth.filter.pass` | handshake requests that passed all filters |
| `monad.wireauth.filter.send_cookie` | cookie challenges sent due to the unverified handshake rate limit |
| `monad.wireauth.filter.drop` | handshake requests rejected due to rate limits |
| `monad.wireauth.rate_limit.connect` | outbound connect attempts rejected due to rate limits |

### api operations

| metric | description |
|--------|-------------|
| `monad.wireauth.api.connect` | new outbound connection attempts |
| `monad.wireauth.api.decrypt` | inbound data packets decrypted |
| `monad.wireauth.api.encrypt_by_public_key` | outbound packets encrypted using peer public key lookup |
| `monad.wireauth.api.encrypt_by_socket` | outbound packets encrypted using socket address lookup |
| `monad.wireauth.api.disconnect` | explicit session termination requests |
| `monad.wireauth.api.dispatch_control` | control messages processed (handshake, cookie, keepalive) |
| `monad.wireauth.api.next_packet` | outbound packet dequeues |
| `monad.wireauth.api.tick` | timer processing cycles |

### message dispatch

| metric | description |
|--------|-------------|
| `monad.wireauth.dispatch.handshake_initiation` | first message of handshake sent/received |
| `monad.wireauth.dispatch.handshake_response` | second message of handshake sent/received |
| `monad.wireauth.dispatch.cookie_reply` | cookie challenges sent in response to handshake init |
| `monad.wireauth.dispatch.keepalive` | empty data packets sent to maintain session liveness |

### error counters

| metric | description |
|--------|-------------|
| `monad.wireauth.error.connect` | failed connection attempts (no memory, duplicate, etc) |
| `monad.wireauth.error.decrypt` | total decryption failures (includes all decrypt error subtypes) |
| `monad.wireauth.error.decrypt.nonce_outside_window` | packet counter outside replay window (too old) |
| `monad.wireauth.error.decrypt.nonce_duplicate` | duplicate packet counter detected (replay attack) |
| `monad.wireauth.error.decrypt.mac` | chacha20poly1305 mac authentication tag verification failed |
| `monad.wireauth.error.encrypt_by_public_key` | encryption failures when looking up by public key |
| `monad.wireauth.error.encrypt_by_socket` | encryption failures when looking up by socket address |
| `monad.wireauth.error.dispatch_control` | control message processing failures |
| `monad.wireauth.error.session_exhausted` | rejected due to hitting max session limit |
| `monad.wireauth.error.mac1_verification_failed` | handshake mac1 authentication failed |
| `monad.wireauth.error.timestamp_replay` | handshake timestamp older than previous attempt |
| `monad.wireauth.error.session_not_found` | operation on non-existent session |
| `monad.wireauth.error.session_index_not_found` | data packet with unknown receiver index |
| `monad.wireauth.error.handshake_init_validation` | malformed or invalid handshake initiation |
| `monad.wireauth.error.handshake_init_responder_new` | failed to create responder state machine |
| `monad.wireauth.error.cookie_reply` | cookie reply validation or generation failed |
| `monad.wireauth.error.handshake_response_validation` | malformed or invalid handshake response |

### enqueued messages

| metric | description |
|--------|-------------|
| `monad.wireauth.enqueued.handshake_init` | handshake initiations added to outbound queue |
| `monad.wireauth.enqueued.handshake_response` | handshake responses added to outbound queue |
| `monad.wireauth.enqueued.cookie_reply` | cookie challenges added to outbound queue |
| `monad.wireauth.enqueued.keepalive` | keepalive packets added to outbound queue |

### initiator buffering

| metric | description |
|--------|-------------|
| `monad.wireauth.initiator.buffered_messages` | messages buffered in initiator sessions during handshake |
| `monad.wireauth.initiator.messages_sent_from_buffer` | buffered messages sent after session established |

## Configuration

| parameter | type | default | description |
|-----------|------|---------|-------------|
| `session_timeout` | Duration | 10s | idle time before session expires (reset on any packet exchange) |
| `session_timeout_jitter` | Duration | 1s | randomization to prevent thundering herd on timeout |
| `keepalive_interval` | Duration | 3s | send empty packet after this idle time to maintain session |
| `keepalive_jitter` | Duration | 300ms | randomization to spread keepalive traffic |
| `rekey_interval` | Duration | 6h | time before initiating new handshake to rotate keys |
| `rekey_jitter` | Duration | 60s | randomization to avoid synchronized rekey storms |
| `max_session_duration` | Duration | 6h5m | absolute session lifetime regardless of activity (forces rekey) |
| `handshake_cookie_unverified_rate_limit` | u64 | 500 | max handshake initiations per second without a valid cookie |
| `handshake_cookie_verified_rate_limit` | u64 | 1000 | max handshake initiations per second with a valid cookie |
| `handshake_rate_reset_interval` | Duration | 1s | window for handshake rate limiting |
| `connect_rate_limit` | u64 | 300 | max outbound connect attempts per second (dos protection) |
| `connect_rate_reset_interval` | Duration | 1s | window for outbound connect rate limiting |
| `cookie_refresh_duration` | Duration | 120s | cookie validity period (responder rotates cookie key) |
| `ip_rate_limit_window` | Duration | 10s | time window for counting verified handshake requests per ip |
| `ip_history_capacity` | usize | 1000000 | lru cache size for tracking recent verified handshake requests per ip |
| `high_watermark_sessions` | usize | 40000 | at this threshold, drop all incoming handshake requests |
| `psk` | [u8; 32] | zeros | optional pre-shared key mixed into handshake for additional auth |
| `max_initiated_sessions` | usize | 1000 | max concurrent initiated sessions (handshakes in progress) |
| `max_buffered_bytes_per_session` | usize | 131072 | max bytes of buffered messages per initiated session (128KB) |
| `gc_idle_timeout` | Duration | 120s | idle time without useful data before session is garbage collected (keepalives don't reset) |
| `max_expired_timers_per_tick` | usize | 10000 | cap expired timers processed per `API::tick` |
