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

the filter operates in three modes based on load:

| condition | action |
|-----------|--------|
| sessions >= `high_watermark_sessions` or handshakes >= `handshake_rate_limit` | drop request |
| sessions >= `low_watermark_sessions` and cookie invalid | send cookie reply |
| sessions >= `low_watermark_sessions` and cookie valid | apply per-ip rate limiting via lru cache |
| sessions < `low_watermark_sessions` | no additional measures |

defaults: `high_watermark_sessions`=100,000, `handshake_rate_limit`=2000/sec, `low_watermark_sessions`=10,000, `ip_rate_limit_window`=10s, `max_sessions_per_ip`=10, `ip_history_capacity`=1,000,000

at 2000 handshakes/sec, approximately 400ms of cpu time per second is spent on handshake-related computation during such attack.

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
