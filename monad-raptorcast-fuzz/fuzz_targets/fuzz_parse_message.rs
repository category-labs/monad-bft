use std::num::NonZero;

use afl;
use lru::LruCache;
use bytes::Bytes;

use monad_raptorcast::udp::parse_message;
use monad_crypto::NopSignature;

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let mut sig_cache = LruCache::new(NonZero::new(1).unwrap());
        let payload = Bytes::copy_from_slice(data);
        let _ = parse_message::<NopSignature>(&mut sig_cache, payload, u64::MAX);
    });
}
