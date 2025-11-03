# API

## DoS Protection

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
session_send_init       time:   [60.101 µs 60.224 µs 60.339 µs]
session_handle_init     time:   [112.86 µs 113.14 µs 113.41 µs]
session_handle_response time:   [51.354 µs 51.535 µs 51.756 µs]
session_encrypt         time:   [117.21 ns 117.46 ns 117.72 ns]
session_decrypt         time:   [175.48 ns 175.81 ns 176.16 ns]
```
