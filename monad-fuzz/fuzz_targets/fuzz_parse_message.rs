// Fuzz runner config:
//
// CORPUS_FILTER=*.parse_message.bin
// TIMEOUT_QUICK=5m
//
// Environments:
//
// AFL_HANG_TMOUT=100
// AFL_EXIT_ON_TIME=300000
// AFL_INPUT_LEN_MAX=1500

mod fuzz_deserialize;

use std::num::NonZero;

use lru::LruCache;
use bytes::Bytes;

use monad_raptorcast::udp::parse_message;
use monad_secp::SecpSignature;

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let mut sig_cache = LruCache::new(NonZero::new(1).unwrap());
        let payload = Bytes::copy_from_slice(data);
        let _ = parse_message::<SecpSignature>(&mut sig_cache, payload, u64::MAX);
    });
}
