pub const GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES: &str = "monad.leanudp.pool.priority_messages";
pub const GAUGE_LEANUDP_POOL_REGULAR_MESSAGES: &str = "monad.leanudp.pool.regular_messages";

pub const COUNTER_LEANUDP_DECODE_FRAGMENTS_RECEIVED: &str =
    "monad.leanudp.decode.fragments_received";
/// Total fragment payload bytes received (does not include the LeanUDP header).
pub const COUNTER_LEANUDP_DECODE_BYTES_RECEIVED: &str = "monad.leanudp.decode.bytes_received";
pub const COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY: &str =
    "monad.leanudp.decode.fragments_priority";
/// Total fragment payload bytes attributed to the priority pool (does not include the LeanUDP header).
pub const COUNTER_LEANUDP_DECODE_BYTES_PRIORITY: &str = "monad.leanudp.decode.bytes_priority";
pub const COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR: &str = "monad.leanudp.decode.fragments_regular";
/// Total fragment payload bytes attributed to the regular pool (does not include the LeanUDP header).
pub const COUNTER_LEANUDP_DECODE_BYTES_REGULAR: &str = "monad.leanudp.decode.bytes_regular";
pub const COUNTER_LEANUDP_DECODE_MESSAGES_COMPLETED: &str =
    "monad.leanudp.decode.messages_completed";
/// Total message payload bytes completed (reassembled) by the decoder.
pub const COUNTER_LEANUDP_DECODE_BYTES_COMPLETED: &str = "monad.leanudp.decode.bytes_completed";
pub const COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT: &str = "monad.leanudp.decode.evicted_timeout";
pub const COUNTER_LEANUDP_DECODE_EVICTED_RANDOM: &str = "monad.leanudp.decode.evicted_random";

pub const COUNTER_LEANUDP_ERROR_INVALID_HEADER: &str = "monad.leanudp.error.invalid_header";
pub const COUNTER_LEANUDP_ERROR_UNSUPPORTED_VERSION: &str =
    "monad.leanudp.error.unsupported_version";
pub const COUNTER_LEANUDP_ERROR_IDENTITY_LIMIT: &str = "monad.leanudp.error.identity_limit";
pub const COUNTER_LEANUDP_ERROR_DUPLICATE_FRAGMENT: &str = "monad.leanudp.error.duplicate_fragment";
pub const COUNTER_LEANUDP_ERROR_TOO_MANY_FRAGMENTS: &str = "monad.leanudp.error.too_many_fragments";
pub const COUNTER_LEANUDP_ERROR_CONFLICTING_END: &str = "monad.leanudp.error.conflicting_end";
pub const COUNTER_LEANUDP_ERROR_MESSAGE_TOO_LARGE: &str = "monad.leanudp.error.message_too_large";
pub const COUNTER_LEANUDP_ERROR_POOL_FULL: &str = "monad.leanudp.error.pool_full";

pub const COUNTER_LEANUDP_ENCODE_MESSAGES: &str = "monad.leanudp.encode.messages";
pub const COUNTER_LEANUDP_ENCODE_FRAGMENTS: &str = "monad.leanudp.encode.fragments";
/// Total message payload bytes submitted to the encoder (does not include the LeanUDP header).
pub const COUNTER_LEANUDP_ENCODE_BYTES: &str = "monad.leanudp.encode.bytes";
pub const COUNTER_LEANUDP_ENCODE_ERROR_TOO_LARGE: &str = "monad.leanudp.encode.error.too_large";
