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

monad_executor::metric_consts! {
    pub GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES {
        name: "monad.leanudp.pool.priority_messages",
        help: "Current number of in-flight priority pool messages",
    }
    pub GAUGE_LEANUDP_POOL_REGULAR_MESSAGES {
        name: "monad.leanudp.pool.regular_messages",
        help: "Current number of in-flight regular pool messages",
    }
    pub COUNTER_LEANUDP_DECODE_FRAGMENTS_RECEIVED {
        name: "monad.leanudp.decode.fragments_received",
        help: "Total LeanUDP fragments received",
    }
    pub COUNTER_LEANUDP_DECODE_BYTES_RECEIVED {
        name: "monad.leanudp.decode.bytes_received",
        help: "Total fragment payload bytes received (excluding LeanUDP header)",
    }
    pub COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY {
        name: "monad.leanudp.decode.fragments_priority",
        help: "Total fragments admitted to the priority pool",
    }
    pub COUNTER_LEANUDP_DECODE_BYTES_PRIORITY {
        name: "monad.leanudp.decode.bytes_priority",
        help: "Total payload bytes admitted to the priority pool",
    }
    pub COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR {
        name: "monad.leanudp.decode.fragments_regular",
        help: "Total fragments admitted to the regular pool",
    }
    pub COUNTER_LEANUDP_DECODE_BYTES_REGULAR {
        name: "monad.leanudp.decode.bytes_regular",
        help: "Total payload bytes admitted to the regular pool",
    }
    pub COUNTER_LEANUDP_DECODE_MESSAGES_COMPLETED {
        name: "monad.leanudp.decode.messages_completed",
        help: "Total reassembled LeanUDP messages completed",
    }
    pub COUNTER_LEANUDP_DECODE_BYTES_COMPLETED {
        name: "monad.leanudp.decode.bytes_completed",
        help: "Total payload bytes of completed reassembled messages",
    }
    pub COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT {
        name: "monad.leanudp.decode.evicted_timeout",
        help: "Total in-flight messages evicted due to timeout",
    }
    pub COUNTER_LEANUDP_DECODE_EVICTED_RANDOM {
        name: "monad.leanudp.decode.evicted_random",
        help: "Total in-flight messages evicted due to capacity pressure",
    }
    pub COUNTER_LEANUDP_ERROR_INVALID_HEADER {
        name: "monad.leanudp.error.invalid_header",
        help: "Total LeanUDP packets rejected due to invalid header",
    }
    pub COUNTER_LEANUDP_ERROR_UNSUPPORTED_VERSION {
        name: "monad.leanudp.error.unsupported_version",
        help: "Total LeanUDP packets rejected due to unsupported protocol version",
    }
    pub COUNTER_LEANUDP_ERROR_IDENTITY_LIMIT {
        name: "monad.leanudp.error.identity_limit",
        help: "Total fragments rejected due to per-identity in-flight limit",
    }
    pub COUNTER_LEANUDP_ERROR_DUPLICATE_FRAGMENT {
        name: "monad.leanudp.error.duplicate_fragment",
        help: "Total duplicate fragments rejected by decoder",
    }
    pub COUNTER_LEANUDP_ERROR_TOO_MANY_FRAGMENTS {
        name: "monad.leanudp.error.too_many_fragments",
        help: "Total fragments rejected because message exceeded fragment count limit",
    }
    pub COUNTER_LEANUDP_ERROR_CONFLICTING_END {
        name: "monad.leanudp.error.conflicting_end",
        help: "Total fragments rejected due to conflicting end marker",
    }
    pub COUNTER_LEANUDP_ERROR_MESSAGE_TOO_LARGE {
        name: "monad.leanudp.error.message_too_large",
        help: "Total messages rejected because reassembled size exceeded max_message_size",
    }
    pub COUNTER_LEANUDP_ERROR_POOL_FULL {
        name: "monad.leanudp.error.pool_full",
        help: "Total fragments rejected because the selected pool was full",
    }
    pub COUNTER_LEANUDP_ENCODE_MESSAGES {
        name: "monad.leanudp.encode.messages",
        help: "Total messages submitted to LeanUDP encoder",
    }
    pub COUNTER_LEANUDP_ENCODE_FRAGMENTS {
        name: "monad.leanudp.encode.fragments",
        help: "Total fragments emitted by LeanUDP encoder",
    }
    pub COUNTER_LEANUDP_ENCODE_BYTES {
        name: "monad.leanudp.encode.bytes",
        help: "Total message payload bytes submitted to LeanUDP encoder",
    }
    pub COUNTER_LEANUDP_ENCODE_ERROR_TOO_LARGE {
        name: "monad.leanudp.encode.error.too_large",
        help: "Total messages rejected by encoder because fragment count exceeded MAX_FRAGMENTS",
    }
}
