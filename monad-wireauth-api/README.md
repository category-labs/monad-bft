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
Timer precision: 26 ns
manager_bench                     fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ bench_session_decrypt          162.8 ns      │ 235.5 ns      │ 166.1 ns      │ 167.2 ns      │ 100     │ 1600
├─ bench_session_encrypt          133.5 ns      │ 136 ns        │ 134.6 ns      │ 134.6 ns      │ 100     │ 3200
├─ bench_session_handle_init      131.7 µs      │ 206.9 µs      │ 135.7 µs      │ 137.7 µs      │ 100     │ 100
├─ bench_session_handle_response  61.64 µs      │ 72.83 µs      │ 62.98 µs      │ 63.74 µs      │ 100     │ 100
╰─ bench_session_send_init        74.48 µs      │ 116.2 µs      │ 76.31 µs      │ 77.72 µs      │ 100     │ 100
```
