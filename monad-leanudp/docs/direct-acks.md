# Direct UDP selective acknowledgements

The v1 header remains six bytes: version (u8), message ID (u16 LE), fragment
sequence (u16 LE), flags (u8). Bit 0 marks the final data fragment; bit 1 marks
an ACK. ACKs have exactly 64 payload bytes, with bit `i % 8` in byte `i / 8`
indicating fragment `i`. ACK sequence is unused; ACK and END cannot be combined.
There are at most 512 fragments per message.

Message IDs use the encoder's global wrapping u16 counter. Outgoing state is
keyed by (peer, ID). A collision replaces old retry tracking without reserving
IDs or preventing a new initial send. Delayed ACKs across an ID wrap remain
ambiguous. Completed messages are not cached; duplicate delivery is allowed.

## Timings

The authenticated session records an initial RTT estimate from its handshake.
The initiator measures initiation to a validated response; the responder measures
its response to the first authenticated data or keepalive. The estimate includes
local queueing and peer processing. Starvation or denial of service can inflate
it, and it is not an absolute bound on future response times. Keepalives do not
refresh it; a new session gets a new estimate.

Direct UDP selects its interval internally as `min(50ms + 1.5 * session_rtt, 500ms)`.
The extra half RTT is a timing margin, not another required round trip. Unknown
RTT falls back to 500ms. Buffered messages adopt the measured interval when their
handshake completes. The upper bound limits the effect of inflated measurements
and allows the default three scheduled retries within the two-second lifetime.
An actual path RTT longer than the cap can cause premature retransmissions.

The caller selects a policy for each message, excluding the initial send:

- `RetryPolicy::NoRetry` sends the initial fragments once and retains no sender
  retry state or timer. Receivers still generate ACKs, which the sender ignores
  for this message. These sends do not increment the retry confirmation metric.
- `RetryPolicy::Retries(n)` permits 1 to 5 additional transmissions. Other
  counts are rejected before sending; use `NoRetry` for zero. The router uses
  `Retries(3)`, also the default policy.

Retries are separated by the selected interval within a fixed two-second
outgoing lifetime. With `Retries(3)` and a 500ms interval they occur at 500, 1000
and 1500ms, then the sender waits for ACKs until 2000ms. With `Retries(1)` and an
80ms interval, it retries at 80ms and then only waits for completion feedback.
With `Retries(5)` and an 80ms interval, retries occur at 80, 160, 240, 320 and
400ms. At a 500ms interval only three retries fit, even if five were requested:
expiry takes precedence at 2000ms.

Delayed polling sends at most one retry per message, then schedules the next
interval from that poll; it does not catch up with several retries back to back.
After the last permitted retry the payload remains tracked until completion,
expiry or pruning. A completion ACK cancels retries and releases the payload
immediately. Partial ACKs do not reset the retry timer or extend the lifetime.

For example, `write_buffered(&peer, payload, priority, RetryPolicy::Retries(1))`
allows one retry, and replacing the last argument with `RetryPolicy::NoRetry`
disables retransmissions. The policy only controls message-fragment retries;
WireAuth session establishment has its own behavior.

The retry clock starts when initial fragments are submitted to the authenticated
socket after a handshake. Before that, tracking has a bounded two-second
handshake wait. WireAuth's existing handshake buffer limits still apply.

The receiver retains each incomplete entry for two seconds from its first
fragment. Additional fragments and duplicates never extend that deadline.
A 512KiB message takes about 44–45ms to transmit at 100Mbps, including packet
overhead. The receiver generates a progress ACK 50ms after first receipt, and
rearms that timer on subsequent traffic after an ACK. It generates a completion ACK immediately,
with the bitmap captured before releasing reassembly state. Timers wake even
when no more network traffic arrives.

## State and retries

Each outgoing message retains one shared payload buffer. Missing fragments are
recreated with the same ID and sequence. Sender state defaults to 1024 messages
or 64MiB globally, and 100 messages or 32MiB per peer. Old tracking is pruned on
overflow. A payload too large for the tracking budget is still sent initially.
Existing receiver pool limits and eviction policies remain in effect.
`NoRetry` consumes none of this sender tracking budget. On an ID collision, even
a `NoRetry` send cancels old tracking for that peer and ID.

An ACK is a snapshot of currently retained receiver fragments. The sender
replaces its bitmap rather than ORing snapshots, allowing recovery when the
receiver prunes an incomplete message. A retry sends missing chunks from an ACK
received since the previous transmission. Without a fresh ACK it sends every
chunk. Identical duplicate fragments are accepted; conflicting payloads or END
markers discard the incomplete entry.

## Delivery rationale

For independent per-datagram loss probability `p`, `n` fragments and four
transmission opportunities, retaining fragments across attempts gives delivery
probability `(1 - p^4)^n`. Requiring a whole attempt to arrive gives
`1 - (1 - (1 - p)^n)^4`.
For a retry count `r`, replace the four opportunities with `r + 1`; the following
table uses the router's three-retry policy. Lifetime limits can reduce the actual
number of opportunities.

| Datagram loss | Five fragments, whole attempts | Five fragments, retained chunks |
| --- | --- | --- |
| 1% | 99.999423% | 99.999995% |
| 3% | 99.960176% | 99.999595% |
| 5% | 99.738111% | 99.996875% |

A 256KiB payload uses 183 fragments at the default 1434-byte chunk size.
Its retained-chunk delivery probabilities are about 99.999817%, 99.985178% and
99.885690% at 1%, 3% and 5% loss respectively. These figures assume all chunks
get four opportunities before expiry, no receiver eviction, and independent
loss. They are receiver delivery probabilities, not sender confirmation rates.
ACK loss can cause full retries and duplicate deliveries. Burst loss, RTT,
queueing and pool pressure must be measured separately.

## Metrics

- `monad.leanudp.decode.messages_completed`: receiver deliveries, including duplicates.
- `monad.leanudp.retry.acknowledged`: sender-confirmed messages.
- `monad.leanudp.retry.transmissions` and `.fragments`: retry traffic.
- `monad.leanudp.retry.expired` and `.pruned`: unconfirmed expiry and tracking eviction.
- `monad.leanudp.retry.messages` and `.bytes`: retained sender state.
- `monad.raptorcast.direct_udp.acks_generated` and `.acks_received`: feedback traffic.
- `monad.raptorcast.direct_udp.invalid_packets`: rejected authenticated packets.
- Existing `monad.leanudp.decode.evicted_*` and pool gauges: receiver pressure and expiry.
